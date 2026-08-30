# distsync

[![Go Reference](https://pkg.go.dev/badge/github.com/MouXiaoJun/distsync.svg)](https://pkg.go.dev/github.com/MouXiaoJun/distsync)
[![Go](https://img.shields.io/badge/Go-1.21+-blue)](https://go.dev/dl/)
[![CI](https://github.com/MouXiaoJun/distsync/actions/workflows/ci.yml/badge.svg)](https://github.com/MouXiaoJun/distsync/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Distributed synchronization primitives for Go, backed by Redis and Valkey.**

Not "another Redis client" — a `sync`-style toolkit. Where you would reach
for `sync.Mutex`, `sync.RWMutex`, or a counting channel inside one process,
distsync gives you the same shape across processes, with leases, fencing
tokens, and automatic renewal built in.

```go
client := distsync.New(rdb) // *redis.Client or *redis.ClusterClient

mu := client.Mutex("order:10001")
guard, err := mu.Lock(ctx)
if err != nil {
    return err
}
defer guard.Unlock(ctx)
```

## Table of contents

- [Install](#install)
- [Primitives](#primitives)
- [Quick start](#quick-start)
  - [Mutex — with fencing token](#mutex--with-fencing-token)
  - [RWMutex](#rwmutex)
  - [Semaphore](#semaphore)
  - [RateLimiter](#ratelimiter-four-algorithms)
  - [Leader election](#leader-election)
  - [Distributed single-flight](#distributed-single-flight)
- [Design](#design)
- [Semantics](#semantics)
- [Compatibility](#compatibility)
- [CI](#ci)
- [Roadmap](#roadmap-v06)

## Install

```sh
go get github.com/MouXiaoJun/distsync@latest
```

Requires Go 1.21+ and any go-redis v9 client.

## Primitives

| Primitive | Type | Use it for |
|---|---|---|
| `Mutex` | exclusive lock | serialize writes to one resource across all replicas |
| `RWMutex` | read-write lock | config updates, cache rebuilds, shared-resource modification |
| `Semaphore` | counting permit | "at most 20 AI calls", "at most 5 transcodes", "100 crawlers per tenant" |
| `RateLimiter` | 4 algorithms | aggregate cluster-wide rate limits (`PerSecond`, `PerMinute`) |
| `Leader` | leader election | cron, reconciliation, settlement, data sync — one replica only |

## Quick start

```go
import (
    "github.com/MouXiaoJun/distsync"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
client := distsync.New(rdb)
```

### Mutex — with fencing token

```go
mu := client.Mutex("payment:10001", distsync.Lease(10*time.Second))

guard, err := mu.Lock(ctx)
if err != nil {
    return err
}
defer guard.Unlock(ctx)

// The fencing token is strictly increasing per resource. Persist it with
// the side effect and reject stale writers:
//   UPDATE orders SET status='paid', fencing_token=? WHERE id=10001 AND fencing_token < ?
fmt.Println(guard.FencingToken())
```

This solves the classic lock-expiry race: A holds the lock → A pauses (GC /
network) → the lease expires → B acquires → A resumes → A and B both write.
With fencing tokens, A's write is rejected because its token is older.

Also available: `mu.TryLock(ctx)` (non-blocking, `ErrNotAcquired`),
`mu.Unlock(ctx)` (convenience), `guard.Renew(ctx)`, `guard.Lost()`.
A `NoAutoRenew` guard can still be told when its lease expires by adding
`distsync.Watchdog()` — a read-only check that fires `guard.Lost()` without
keeping the lease alive.

### RWMutex

```go
mu := client.RWMutex("config:tenant:1001")

rg, err := mu.RLock(ctx) // many readers may coexist
config := load()
rg.Unlock(ctx)

wg, err := mu.Lock(ctx) // exclusive writer, fencing token included
update()
wg.Unlock(ctx)
```

Contention is **strictly FIFO**: every contender joins a single
arrival queue, and grants happen in arrival order — a reader never jumps a
queued writer, and a writer never jumps anyone. A crashed contender is
purged from the queue automatically, and a canceled waiter leaves it
immediately, so the lock can never be blocked by a ghost.

### Semaphore

```go
sem := client.Semaphore("openai:gpt5", 20) // max 20 concurrent AI requests

permit, err := sem.Acquire(ctx, 1)
if err != nil {
    return err
}
defer permit.Release(ctx)
```

Permits expire and are reclaimed atomically, so a crashed holder never
leaks capacity forever. `sem.TryAcquire(ctx, n)`, `sem.Available(ctx)`.

### RateLimiter (four algorithms)

```go
limiter := client.RateLimiter("tenant:1001", distsync.PerSecond(100)) // token bucket (default)

if err := limiter.Acquire(ctx, 1); err != nil { // blocks until allowed
    return err
}
// non-blocking variant:
ok, retryAfter, err := limiter.Allow(ctx, 1)
```

Pick an algorithm per limiter — each is a single atomic Lua script:

| Algorithm | Option | Behavior |
|---|---|---|
| Token bucket | `distsync.TokenBucket()` (default) | budget refills at `Rate`; bursts up to `Capacity` |
| Fixed window | `distsync.FixedWindow()` | at most `Capacity` requests per window of `Capacity/Rate` |
| Sliding window | `distsync.SlidingWindow()` | exact rolling window (one entry per request — moderate rates) |
| Leaky bucket | `distsync.LeakyBucket()` | output smoothed at `Rate`; bursts absorbed, then throttled |

```go
strict := client.RateLimiter("api:public", distsync.PerSecond(100), distsync.SlidingWindow())
```

`PerMinute(n)` refills at `n/60` tokens per second with capacity `n` (one
minute of budget). This corrects the previous capacity of `n/60`; use
`PerMinute(n).WithBurst(n/60)` explicitly if that smaller burst is intended.
`WithBurst` also changes the window length for windowed algorithms.

`Allow` and `Acquire` reject negative, NaN, infinite, or over-capacity
requests before accessing Redis; `Acquire` does not wait for an impossible
request. Zero succeeds without consuming budget or accessing Redis, even
with an already-canceled context. Token and leaky buckets support fractional
tokens. Fixed and sliding windows round each positive request up to a whole
count, then check it against capacity (for example, `1.1` needs capacity `2`).

Construction panics for non-positive/non-finite rates or capacities, or a
refill period exceeding the `time.Duration` range at millisecond precision.
Windowed algorithms additionally require capacity in `[1, 2^53-1]` and a
window of at least 1ms; fractional milliseconds are truncated. Sliding windows
still store one entry per counted request, so use moderate capacities.

For fixed windows, Go computes `K:<window-index>` from the client's current
time and supplies that full key to Lua explicitly. `Reset` deletes only
the current window's counter; older counters expire normally. It does not
pause concurrent requests or reset neighboring windows.

### Leader election

```go
leader := client.Leader("scheduler")

if err := leader.Run(ctx, func(ctx context.Context) error {
    return scheduler.Start(ctx) // cron, reconciliation, settlement, sync
}); err != nil {
    return err
}
```

Only one replica runs the callback. If the leader dies or its lease
expires, another replica takes over; the callback's context is canceled on
loss so the old leader can shut down gracefully. `TryRun` gives a
non-blocking variant (`ErrNotAcquired` when someone else is leader).

To fence the leader's own writes (reconciliation, settlement), enable
fencing — tokens increase across every leadership change:

```go
leader := client.Leader("settlement", distsync.Fencing())
// inside the callback:
fmt.Println(leader.FencingToken()) // strictly increasing per leadership
```

### Distributed single-flight

```go
cfg, err := distsync.Once(ctx, mu, func(ctx context.Context) (Config, error) {
    return loadConfig(ctx) // serialized across the cluster
})
```

## Design

### One unified Lease

Every primitive is built on one `Lease` abstraction
(`internal/lease`), which handles ownership tokens, TTL, expiry, renewal,
Redis failures and context cancellation exactly once — not once per
primitive:

```
Mutex ──┐
RWMutex ┼──► Lease (SingleOwner / PermitSet) ──► Lua scripts ──► Redis / Valkey
Leader ─┘
Semaphore ──► PermitSet
```

- **Lease** — `ID()`, `Acquire(ctx)`, `Renew(ctx)`, `Release(ctx)`,
  `ExpiresAt()`.
- **Owner tokens** — every acquisition mints a fresh random token; release
  and renew are compare-and-set on the token, so a stale holder can never
  unlock a newer owner.
- **Automatic renewal** — one heartbeat goroutine per held lease (interval
  `ttl/3`), started on acquire, stopped synchronously on release. No
  goroutine leaks: `Release`/`Unlock`/`Run` block until the heartbeat has
  fully exited.
- **Safe on failure** — definitive ownership loss (renewal says "not
  yours") cancels the guard's `Context()`/`Lost()` and stops the heartbeat;
  transient Redis errors are retried rather than silently dropping the
  lease.

### Redis Cluster

Every key a primitive touches is derived from one hash-tagged name
(`"order:10001"` → `"{order:10001}"`, `"{order:10001}:fencing"`, ...), so
all multi-key Lua scripts stay on a single cluster slot and never hit
CROSSSLOT errors. Scripts run via EVALSHA with automatic EVAL fallback,
which go-redis handles per node.

### Observability

`dist.Metrics` is a small interface (`Acquire`, `Release`, `Renew`,
`RenewalStopped`); a Prometheus implementation ships in
`github.com/MouXiaoJun/distsync/metrics`:

```go
client := distsync.New(rdb, distsync.WithMetrics(metrics.New(nil)))
```

`dist.Tracer` accepts an OpenTelemetry adapter; a ready-made one ships in
`github.com/MouXiaoJun/distsync/telemetry`:

```go
client := distsync.New(rdb, distsync.WithTracer(telemetry.NewTracer(nil))) // nil = global OTel provider
```

Zero-cost no-op defaults — if you install neither, there is no overhead.

## Compatibility

- Redis >= 6.0 and Valkey (pure Lua, no Redis modules).
- Any go-redis v9 `Cmdable`: `*redis.Client`, `*redis.ClusterClient`, rings.
- Go >= 1.21.

## Semantics

Exactly what this library guarantees — and does not — is specified in
[docs/semantics.md](docs/semantics.md): fencing-token bounds, the
lease-expiry two-holder window, clock-skew assumptions, failure modes, the
guarantee table and the full key layout for ops.

## CI


GitHub Actions runs on every push and pull request:

- gofmt, `go vet`, `go test -race` on Go 1.21 and 1.25, plus golangci-lint;
- an integration job that runs the **entire** test suite against real
  **Redis 7** and real **Valkey 8** servers (the same suite normally runs on
  miniredis; set `DISTSYNC_TEST_REDIS_ADDR` to point it at any Redis-protocol
  server, e.g. `make test-redis`).

## Roadmap (v0.6+)

- Formal spec notes on fencing / lease-expiry semantics.

Deliberately out of scope: distributed map/queue/delayed
queue/topic/bloom filter/atomic counter/sets/lists/remote services. This
library stays a synchronization toolkit.

## License

MIT
