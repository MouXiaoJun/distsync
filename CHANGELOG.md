# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
