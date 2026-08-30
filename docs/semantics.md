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
- With both renewal and watchdog disabled, expiry is not reported proactively;
  a subsequent ownership operation can report it. Loss is terminal for a guard.
- Local validity is measured from before the successful request, with a 1%
  plus 1 ms margin. A reply arriving after that local deadline is not a new
  grant. Renewals must finish before the previously confirmed validity ends.
  Confirmed but late acquisitions attempt an owner-checked release with a
  separate 5-second context; cleanup errors accompany `ErrLost`. If cleanup
  cannot reach Redis, the remote grant remains until expiry/reclamation.

## 2. Fencing tokens

Every successful acquisition of an exclusive lease (Mutex, RWMutex writer,
Leader with `Fencing()`) increments a per-resource counter and returns its
new value.

**Conditional guarantee.** Tokens increase per resource only while its counter
is preserved without rollback and within the backend/script numeric range.

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
- No TTL does not protect a counter from eviction, restoring an old snapshot,
  or losing asynchronous replication writes during failover. `WAIT` does not
  turn Redis into a strongly consistent store. See the official
  [Redis lock constraints](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)
  and [WAIT limitations](https://redis.io/docs/latest/commands/wait/).
- Fencing does **not** prevent two holders from briefly coexisting. Every
  writer must participate, and the recipient must atomically check/store the
  token with the side effect. A recipient rejects older tokens only after it
  has recorded a newer one. Counter rollback breaks this ordering guarantee.
  Check affected rows in the SQL example: it illustrates one write per token,
  not a complete protocol for repeated writes within one lease. HTTP/file
  destinations that cannot enforce tokens do not automatically gain protection.
- The Go `uint64` return type is not a promise of exact full-range counters:
  Redis integers and Lua number conversion have narrower limits. Extremely
  large counters need separate validation; numeric-range changes are not part
  of this maintenance patch.

## 3. Lease expiry and the two-holder window

A lease holder can outlive its lease: a GC pause or a partition that
prevents renewal means the TTL expires server-side while the holder still
believes it owns the lock. Another process then acquires it. When the old
holder resumes, two holders coexist.

The library's response:

- **Renewal failure with definitive loss** (renewal says "not yours") fires
  `Guard.Lost()` and stops the heartbeat, so the old holder is told.
- **Transient Redis errors** are retried while local validity remains. An
  independent timer follows the last confirmed deadline when renewal/watchdog
  is enabled, revoking local use without waiting for another tick or reconnect.
  An explicit renewal also has an in-flight deadline alarm, so loss can be
  signaled even if the backend ignores cancellation.
  Loss may mean "no longer provably valid", not a confirmed new owner.
  Scheduling and GC pauses still prevent real-time notification guarantees.

**Clock-skew assumption.** TTL-based leases assume the server clock (which
enforces expiry) and client clocks (which stamp sorted-set scores for
permits/readers) are roughly synchronized. Moderate skew is tolerated by
the `ttl/3` renewal margin; keep skew well below `ttl/3`.

**Recommendation.** Use `ttl ≥ 10s`, and larger than your longest expected
GC pause or network stall. Fencing protects only the destinations and failure
models satisfying the conditions in §2; it is not a general corruption guarantee.

## 4. Safe unlock

Unlock is a compare-and-delete on the owner token, so:

- a stale holder cannot release a newer owner's lease (tested);
- `Unlock`/`Release` stop local use and renewal immediately. A confirmed release
  is idempotent; a transport/cancellation failure may be retried with the same
  original owner token, including through the Mutex/RWMutex convenience methods.
  A transport error is not evidence that the remote operation did not happen;
- `Unlock` returns `ErrLost` when the lease was already gone before the
  call — i.e. your critical section may have overlapped with a newer
  holder. That is a signal to reconcile, not to panic.

## 5. Failure modes

| Failure | Behavior |
|---|---|
| Redis unreachable at acquire | Operation returns an error. An outage is **never** reported as `ErrNotAcquired` (busy) — callers can distinguish contention from failure. |
| Redis unreachable while held | Retry within confirmed validity; renewal/watchdog revoke local use when validity cannot be established. A newer holder may already exist. |
| Process crash | The lease expires after `ttl`; permits and read locks are reclaimed atomically on the next acquire. |
| Queue ghost (RWMutex) | A canceled waiter dequeues itself; a crashed waiter is purged from the queue head after `2*ttl` of silence. |
| Clock skew | Degrades to the §3 assumption; fencing is conditional on §2, not a cure for arbitrary skew or counter rollback. |

## 6. Guarantees

Guaranteed:

- Mutual exclusion while all holders' leases are valid.
- Fencing token ordering under the counter-preservation assumptions in §2.
- Safe unlock (stale holders cannot release newer leases).
- Renewal/watchdog Stop cancels in-flight work and waits for it to return.
  Backends must honor context cancellation or have bounded I/O timeouts; a
  custom client that blocks forever cannot be forcibly terminated by the library.
- Redis Cluster safety: every key of a primitive shares one hash slot.
- Deterministic FIFO ordering for RWMutex contention.

Not guaranteed (and no Redis lock can):

- Mutual exclusion under arbitrary partition/GC (impossible without
  fencing-style coordination — that is exactly what fencing tokens are
  for).
- Real-time bounds on acquisition.
- Cross-resource atomicity.

Leader reuses the same lease lifecycle. On loss, its callback context is canceled;
`Run`/`TryRun` preserve both `ErrLeadershipLost` and a returned callback error.
Cleanup stops renewal before owner-checked release, and release errors are
returned instead of discarded. Its 5-second remote-release context starts only
after background work has stopped. Callbacks must cooperate with cancellation.

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
