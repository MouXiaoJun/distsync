# Contributing

Thanks for considering a contribution to distsync. This project aims to be a
small, correct, production-grade synchronization toolkit — not a kitchen sink.

## Scope

The library deliberately stays a synchronization toolkit. Distributed
data structures (map / queue / topic / bloom filter / counters / lists) are
out of scope. If your change adds one of those, it will be declined with
gratitude.

## Ground rules

- **One Lease layer.** Every primitive must be built on `internal/lease`.
  Do not hand-roll per-primitive Redis logic that duplicates ownership, TTL,
  renewal or release handling.
- **Redis Cluster safe by construction.** All keys of a primitive must be
  derived from one hash-tagged name (`internal/redis`). Multi-key Lua
  scripts must never cross slots.
- **No goroutine leaks.** Background renewal must stop synchronously on
  release. New background work needs an explicit, tested shutdown path.
- **context.Context first.** Every blocking operation accepts `context.Context`
  and honors cancellation.
- **Tests are mandatory.** New behavior needs tests. Integration tests run
  against miniredis (no real server required); run them with `-race`.

## Development

```sh
go test ./... -race -count=1                 # unit tests (miniredis-backed)
DISTSYNC_TEST_REDIS_ADDR=localhost:6379 go test ./... -race -count=1   # full suite against a real Redis/Valkey server
go vet ./...                   # static checks
gofmt -l .                     # formatting (must be empty)
golangci-lint run ./...        # lint (standard set)
```

CI runs the full suite against real Redis 7 and Valkey 8 on every push.

## Releasing

1. Update `CHANGELOG.md` (move `[Unreleased]` into a versioned section).
2. Tag and push:

```sh
git tag v0.2.0
git push origin main --tags
```

pkg.go.dev indexes new versions automatically; if it lags, request indexing
on the package page.
