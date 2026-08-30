# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-08-30

### Changed

- Reject expired/partially expired renewal and late replies; notify loss at confirmed deadlines and cancel in-flight renewal on stop.
- Preserve owner tokens for release retries; clean up confirmed late acquisitions; retain Leader callback and cleanup errors.
- Compatibility: PerMinute(n) now has capacity n. Invalid/impossible requests return errors; invalid rate configurations panic early. Fixed-window Reset deletes the current counter.
- Document fencing assumptions and failure boundaries; complete the existing golang.org/x/sys v0.30.0 checksum.

- LICENSE now carries the actual copyright holder (`Copyright (c) 2026
  MouXiaoJun`) instead of a generic "contributors" line; MIT terms
  unchanged. CONTRIBUTING documents the license terms for contributions.

- Documentation polish: package comment now named `distsync` (was stale
  `dist`), README gained badges / table of contents / install section, and
  pkg.go.dev now renders type-level examples for Mutex, RWMutex,
  RateLimiter and Leader.


## [0.6.0] - 2026-08-29

### Added

- Formal semantics specification ([docs/semantics.md](docs/semantics.md)):
  fencing-token bounds, the lease-expiry two-holder window, clock-skew
  assumptions, failure-mode behavior, the guarantee table and the full key
  layout for operators.
- `examples/cluster`: run every primitive against a real Redis Cluster
  (start a 3-master cluster, then `go run ./examples/cluster`). Verified
  locally against a live 3-master cluster: all multi-key Lua scripts execute
  without CROSSSLOT errors.
- CI coverage gate: the `coverage` job fails below 75% statement coverage.


### Added

- Formal semantics specification ([docs/semantics.md](docs/semantics.md)):
  fencing-token bounds, the lease-expiry two-holder window, clock-skew
  assumptions, failure-mode behavior, the guarantee table and the full key
  layout.


## [0.5.0] - 2026-08-29

### Changed

- RWMutex is now strictly FIFO-fair. Every contender (reader or writer)
  joins a single arrival queue (`{name}:waiters`, scored by a monotonic
  sequence); grants happen in arrival order. A reader never jumps a queued
  writer, a writer never jumps anyone, and new arrivals behind a queued
  writer wait — so no writer can be starved by a reader stream. Queued
  readers may be granted together (they never conflict).
- A waiter that gives up (context canceled or failed Try* call) leaves the
  queue immediately; a crashed waiter is purged from the head after `2*ttl`
  of silence, so the queue can never be blocked by a ghost. The old
  best-effort "writer-waiting marker" is gone, replaced by the queue.

### Fixed

- `RWMutex` cancel-path cleanup used the canceled context to send its
  dequeue commands, so they never executed and left a ghost queue entry
  behind. Dequeue now runs with `context.WithoutCancel`.


### Added

- Real-server integration testing: the whole test suite runs against a real
  Redis or Valkey server by setting `DISTSYNC_TEST_REDIS_ADDR` (see
  `make test-redis`). CI now includes an integration job that runs every
  test against Redis 7 and Valkey 8 on each push. `make test`/`test-race`/
  `test-redis`/`lint`/`fmt` targets added.


## [0.4.0] - 2026-08-29

### Added

- Watchdog for non-renewing guards: `dist.Watchdog()` runs a lightweight
  background check (a plain read every ttl/3) that detects lease expiry
  WITHOUT renewing, and fires `Guard.Lost()`/`Context()` (and cancels a
  `Leader` callback, so a non-renewing leader still fails over promptly).
  Pair it with `NoAutoRenew`; with `AutoRenew` the heartbeat already
  detects loss. Applies to Mutex, RWMutex (readers and writers),
  Semaphore permits and Leader.
- The internal `Lease` interface gained `Held(ctx)`: a non-extending
  ownership check, implemented by all four lease shapes. For sorted-set
  leases (permits, readers) expiry is judged by score-vs-now, matching the
  acquire scripts.

## [0.3.0] - 2026-08-29

### Added

- Leader fencing: `dist.Fencing()` is now meaningful for `Leader`. Each
  leadership acquisition mints a strictly increasing fencing token,
  exposed as `Leader.FencingToken()` inside the callback — so a leader can
  fence its side effects (reconciliation, settlement writes) exactly like a
  mutex holder. Disabled by default; tokens increase across leadership
  changes and across replicas.
- Heartbeat jitter: automatic renewal runs at `ttl/3` with ±20% jitter, so
  a fleet of holders that acquired simultaneously no longer renews on
  aligned ticks.
- Redis-outage resilience tests: `Lock`/`TryLock`/`RLock`/`Acquire`/
  `Allow` all fail fast and honestly when the server is unreachable — and
  an outage is never misreported as `ErrNotAcquired` (busy).

## [0.2.0] - 2026-08-29

### Added

- RateLimiter now supports four algorithms, selectable per limiter:
  `dist.TokenBucket()` (default), `dist.FixedWindow()`, `dist.SlidingWindow()`
  and `dist.LeakyBucket()`. Each ships its own atomic Lua script; `Allow`,
  `Acquire` and `Wait` behave identically across algorithms.
- OpenTelemetry adapter: new `dist/telemetry` subpackage wires
  `go.opentelemetry.io/otel` into the `dist.Tracer` interface
  (`telemetry.NewTracer(provider)`).
- GitHub Actions CI: gofmt + `go vet` + `go test -race` on Go 1.21 and 1.25,
  plus golangci-lint (standard linter set).
- Benchmark suite (`Benchmark*`) covering the happy path of every primitive.
- Cluster-safety tests proving every derived key of a primitive shares one
  Redis Cluster hash slot (CRC16 over the hash tag).

### Changed

- `Client.RateLimiter` now takes `...RateLimiterOption` instead of the
  generic `...Option` (which it previously ignored). Existing call sites that
  passed no options are unaffected.

## [0.1.0] - 2026-08-29

### Added

- Five distributed synchronization primitives on Redis / Valkey:
  `Mutex` (fencing tokens, auto-renewal, safe compare-and-delete unlock),
  `RWMutex` (writer preference), `Semaphore` (counting permits with atomic
  expiry reclamation), `RateLimiter` (token bucket), `Leader` (election with
  automatic failover).
- One unified `Lease` layer (`internal/lease`) shared by every primitive:
  ownership tokens, TTL, background renewal, expiry, Redis failures and
  context cancellation handled exactly once.
- Redis Cluster safety by construction: every key is derived from one
  hash-tagged name, so all multi-key Lua scripts stay on a single slot.
- Optional Prometheus metrics (`dist/metrics`) and a `dist.Tracer` interface
  for distributed tracing.
- `distsync.Once[T]`: distributed single-flight helper.
- `examples/basic`: runnable demo of all primitives.
