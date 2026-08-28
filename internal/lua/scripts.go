// Package lua embeds every atomic operation as a Lua script.
//
// All scripts are pure Redis Lua and run identically on Redis >= 6.0 and
// Valkey. They are executed through go-redis's Script type, which performs
// EVALSHA with automatic EVAL fallback (per node, so Redis Cluster works).
//
// Every multi-key script is safe for Redis Cluster because all of its keys
// are derived from one hash-tagged name (see redisx.Key) and therefore live
// on the same slot.
package lua

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed single_acquire.lua
var singleAcquireSrc string

//go:embed single_renew.lua
var singleRenewSrc string

//go:embed single_release.lua
var singleReleaseSrc string

//go:embed rw_write_lock.lua
var rwWriteLockSrc string

//go:embed rw_read_lock.lua
var rwReadLockSrc string

//go:embed sem_acquire.lua
var semAcquireSrc string

//go:embed sem_renew.lua
var semRenewSrc string

//go:embed sem_release.lua
var semReleaseSrc string

//go:embed ratelimit.lua
var rateLimitSrc string

//go:embed ratelimit_fixed.lua
var rateLimitFixedSrc string

//go:embed ratelimit_sliding.lua
var rateLimitSlidingSrc string

//go:embed ratelimit_leaky.lua
var rateLimitLeakySrc string

var (
	// SingleAcquire takes an exclusive lease and optionally mints a
	// fencing token (mutex, RWMutex writer, leader election).
	SingleAcquire = redis.NewScript(singleAcquireSrc)

	// SingleRenew extends the TTL of an exclusively owned lease.
	SingleRenew = redis.NewScript(singleRenewSrc)

	// SingleRelease releases an exclusively owned lease (compare-and-delete).
	SingleRelease = redis.NewScript(singleReleaseSrc)

	// RWWriteLock acquires the writer role of a read-write lock.
	RWWriteLock = redis.NewScript(rwWriteLockSrc)

	// RWReadLock acquires the reader role of a read-write lock.
	RWReadLock = redis.NewScript(rwReadLockSrc)

	// SemAcquire takes one or more semaphore permits.
	SemAcquire = redis.NewScript(semAcquireSrc)

	// SemRenew refreshes owned permits / read locks.
	SemRenew = redis.NewScript(semRenewSrc)

	// SemRelease releases owned permits / read locks.
	SemRelease = redis.NewScript(semReleaseSrc)

	// RateLimit evaluates one token-bucket decision.
	RateLimit = redis.NewScript(rateLimitSrc)

	// RateLimitFixed evaluates one fixed-window decision.
	RateLimitFixed = redis.NewScript(rateLimitFixedSrc)

	// RateLimitSliding evaluates one sliding-window-log decision.
	RateLimitSliding = redis.NewScript(rateLimitSlidingSrc)

	// RateLimitLeaky evaluates one leaky-bucket decision.
	RateLimitLeaky = redis.NewScript(rateLimitLeakySrc)
)
