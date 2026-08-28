package distsync

import (
	"math"
	"math/rand"
	"time"
)

// config is the resolved per-primitive configuration.
type config struct {
	ttl       time.Duration
	autoRenew bool
	retry     retryPolicy
	fencing   bool
	watchdog  bool
}

// retryPolicy describes exponential backoff with jitter for acquisition.
type retryPolicy struct {
	base time.Duration
	max  time.Duration
}

func (r retryPolicy) delay(attempt int) time.Duration {
	if r.base <= 0 {
		r.base = 50 * time.Millisecond
	}
	if r.max <= 0 {
		r.max = 2 * time.Second
	}
	d := float64(r.base) * math.Pow(2, float64(attempt))
	if d > float64(r.max) {
		d = float64(r.max)
	}
	// 0.8x..1.2x jitter avoids thundering herds.
	jitter := 0.8 + 0.4*rand.Float64()
	return time.Duration(d * jitter)
}

// Option customizes a single primitive (Mutex, RWMutex, Semaphore, Leader).
type Option func(*config)

// Lease sets the lease TTL for this primitive. Pick a value comfortably
// larger than your critical section; with AutoRenew the lease is refreshed
// every ttl/3, so the TTL is a safety net for crashes, not a work budget.
func Lease(d time.Duration) Option {
	return func(c *config) { c.ttl = d }
}

// AutoRenew enables background heartbeat renewal (the default). Renewals
// run every ttl/3; if renewal fails because ownership was definitively
// lost, the guard reports it via its Context/Lost channel.
func AutoRenew() Option {
	return func(c *config) { c.autoRenew = true }
}

// NoAutoRenew disables background renewal. The lease then lives for at most
// ttl and expires on its own — use it for deliberately short critical
// sections where a missed renewal must yield ownership quickly. Pair with
// Watchdog if you still want to be told when the lease expires.
func NoAutoRenew() Option {
	return func(c *config) { c.autoRenew = false }
}

// Watchdog runs a lightweight background check that detects lease expiry
// WITHOUT renewing, and fires the guard's Lost()/Context() when ownership is
// gone. It only matters together with NoAutoRenew (with AutoRenew the
// heartbeat already detects loss). Checking is a plain read every ttl/3.
func Watchdog() Option {
	return func(c *config) { c.watchdog = true }
}

// Fencing enables fencing tokens. They are enabled by default for the
// exclusive locks (Mutex, RWMutex writers), so there this option is a
// no-op. For Leader it opts into fencing: each leadership acquisition then
// mints a strictly increasing token (see Leader.FencingToken) that the
// leader can persist with its side effects, exactly like a mutex.
func Fencing() Option {
	return func(c *config) { c.fencing = true }
}

// Retry sets the acquisition backoff bounds for this primitive (exponential
// with jitter, base..max).
func Retry(base, max time.Duration) Option {
	return func(c *config) { c.retry = retryPolicy{base: base, max: max} }
}
