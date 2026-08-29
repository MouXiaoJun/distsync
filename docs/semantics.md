# distsync semantics

This document states precisely what distsync guarantees, what it does not,
and under which assumptions. It is the contract reviewers should hold the
library to.

## 1. Locking model

Every primitive is a **lease**: a Redis key (or sorted-set entry) that the
holder owns until it releases it or the lease expires.

```
Waiting → Held ──release──→ Released
              └──expiry/loss──→ Lost (guard.Context()/Lost() fire)
```

- Each acquisition mints a fresh random **owner token**; release and renew
  are compare-and-set on that token, so a stale holder can never unlock a
  newer owner.
- **Renewal** (default on) extends the TTL every `ttl/3` (±20% jitter). The
  TTL is a crash safety net, not a work budget: your critical section may
  take longer than the TTL *as long as renewal succeeds*.
- **Watchdog** (`NoAutoRenew` + `Watchdog`) detects expiry without
  renewing.

## 2. Fencing tokens

Every successful acquisition of an exclusive lease (Mutex, RWMutex writer,
Leader with `Fencing()`) increments a per-resource counter and returns its
new value.

**Guarantee.** Fencing tokens are *strictly increasing per resource* across
all holders, in every order they acquired.

**Usage.** Persist the token with the side effect and reject stale writes:

```sql
UPDATE orders SET status='paid', fencing_token=?
WHERE id=10001 AND fencing_token < ?;
```

**Boundary conditions.**

- Tokens are monotonic **per resource name**; different resources have
  independent counters.
- A token is minted only on a successful acquisition; failed attempts do
  not consume the counter.
- The counter key (`{name}:fencing`) is created on first acquisition and
  **never expires** (no TTL is set on it). If an operator deletes it, the
  sequence restarts — treat that like resetting a database sequence.
- Fencing does **not** prevent two holders from briefly coexisting. It
  makes the overlap *safe*: the older holder's writes are rejected. The
  window still exists (see §3); fencing is the only thing that closes it.

## 3. Lease expiry and the two-holder window

A lease holder can outlive its lease: a GC pause or a partition that
prevents renewal means the TTL expires server-side while the holder still
believes it owns the lock. Another process then acquires it. When the old
holder resumes, two holders coexist.

The library's response:

- **Renewal failure with definitive loss** (renewal says "not yours") fires
  `Guard.Lost()` and stops the heartbeat, so the old holder is told.
- **Transient Redis errors** during renewal are *retried*, not treated as
  loss: giving up on a network blip would drop the lease while the holder
  still believes it owns it — strictly worse. The cost is that a long
  partition lets the lease expire silently until Redis is reachable again.

**Clock-skew assumption.** TTL-based leases assume the server clock (which
enforces expiry) and client clocks (which stamp sorted-set scores for
permits/readers) are roughly synchronized. Moderate skew is tolerated by
the `ttl/3` renewal margin; keep skew well below `ttl/3`.

**Recommendation.** Use `ttl ≥ 10s`, and larger than your longest expected
GC pause or network stall. With fencing enabled, even a violated lease
cannot corrupt state.

## 4. Safe unlock

Unlock is a compare-and-delete on the owner token, so:

- a stale holder cannot release a newer owner's lease (tested);
- `Unlock`/`Release` are idempotent — the first call releases, later calls
  return nil;
- `Unlock` returns `ErrLost` when the lease was already gone before the
  call — i.e. your critical section may have overlapped with a newer
  holder. That is a signal to reconcile, not to panic.

## 5. Failure modes

| Failure | Behavior |
|---|---|
| Redis unreachable at acquire | Operation returns an error. An outage is **never** reported as `ErrNotAcquired` (busy) — callers can distinguish contention from failure. |
| Redis unreachable while held | Renewal keeps retrying; the lease expires server-side; when Redis returns, renewal reports loss and `Lost()` fires. A newer holder may already exist. |
| Process crash | The lease expires after `ttl`; permits and read locks are reclaimed atomically on the next acquire. |
| Queue ghost (RWMutex) | A canceled waiter dequeues itself; a crashed waiter is purged from the queue head after `2*ttl` of silence. |
| Clock skew | Degrades to the §3 assumption; fencing makes the residual risk safe. |

## 6. Guarantees

Guaranteed:

- Mutual exclusion while all holders' leases are valid.
- Fencing tokens strictly increasing per resource.
- Safe unlock (stale holders cannot release newer leases).
- No background goroutine leaks (renewal/watchdog loops stop synchronously
  on release).
- Redis Cluster safety: every key of a primitive shares one hash slot.
- Deterministic FIFO ordering for RWMutex contention.

Not guaranteed (and no Redis lock can):

- Mutual exclusion under arbitrary partition/GC (impossible without
  fencing-style coordination — that is exactly what fencing tokens are
  for).
- Real-time bounds on acquisition.
- Cross-resource atomicity.

## 7. Key layout (ops view)

All keys are derived from the hash-tagged resource name; `K = {name}`.

| Primitive | Keys |
|---|---|
| Mutex / Leader | `K`, `K:fencing` |
| RWMutex | `K:writer`, `K:readers`, `K:fencing`, `K:waiters`, `K:waiter-ts`, `K:seq` |
| Semaphore | `K` (sorted set of permits) |
| RateLimiter (bucket) | `K` (hash: tokens/ts) |
| RateLimiter (fixed window) | `K:<window-start>` (counter) |
| RateLimiter (sliding) | `K` (sorted set of request stamps) |

Primitives with no contention hold no live keys: mutex/leader keys vanish
on unlock, permit entries are reaped on the next acquire, rate-limiter keys
expire after they would drain.
