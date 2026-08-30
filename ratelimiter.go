package distsync

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lease"
	"github.com/MouXiaoJun/distsync/internal/lua"
	"github.com/MouXiaoJun/distsync/internal/redis"
	"github.com/redis/go-redis/v9"
)

// RateLimiter is a distributed rate limiter shared across all processes, so
// the aggregate rate is enforced cluster-wide, not per node. Four algorithms
// are available (see TokenBucket, FixedWindow, SlidingWindow, LeakyBucket);
// token bucket is the default:
//
//	limiter := client.RateLimiter("tenant:1001", distsync.PerSecond(100))
//	if err := limiter.Acquire(ctx, 1); err != nil {
//	    return err
//	}
//
//	// or non-blocking:
//	ok, retryAfter, err := limiter.Allow(ctx, 1)
type RateLimiter struct {
	client    *Client
	name      string
	key       string
	capacity  float64
	rate      float64 // tokens per second
	algorithm Algorithm
}

// Algorithm selects the rate-limiter implementation for a RateLimiter.
type Algorithm int

// Rate limiter algorithms. The choice is a correctness/space tradeoff:
//
//   - TokenBucket: cheap, approximate (fine for most production loads).
//   - FixedWindow: cheapest, with window-boundary bursts.
//   - SlidingWindow: exact, but keeps one entry per request in a sorted
//     set — use for moderate rates.
//   - LeakyBucket: smooths output at the configured rate.
const (
	AlgorithmTokenBucket Algorithm = iota
	AlgorithmFixedWindow
	AlgorithmSlidingWindow
	AlgorithmLeakyBucket
)

// String implements fmt.Stringer.
func (a Algorithm) String() string {
	switch a {
	case AlgorithmFixedWindow:
		return "fixed-window"
	case AlgorithmSlidingWindow:
		return "sliding-window"
	case AlgorithmLeakyBucket:
		return "leaky-bucket"
	default:
		return "token-bucket"
	}
}

// RateLimiterOption customizes a RateLimiter (currently: algorithm choice).
type RateLimiterOption func(*rlConfig)

type rlConfig struct {
	algorithm Algorithm
}

// TokenBucket selects the token-bucket algorithm (the default).
func TokenBucket() RateLimiterOption {
	return func(c *rlConfig) { c.algorithm = AlgorithmTokenBucket }
}

// FixedWindow selects the fixed-window algorithm: at most Capacity requests
// per window of Capacity/Rate.
func FixedWindow() RateLimiterOption {
	return func(c *rlConfig) { c.algorithm = AlgorithmFixedWindow }
}

// SlidingWindow selects the sliding-window-log algorithm: exactly at most
// Capacity requests in any rolling window of Capacity/Rate.
func SlidingWindow() RateLimiterOption {
	return func(c *rlConfig) { c.algorithm = AlgorithmSlidingWindow }
}

// LeakyBucket selects the leaky-bucket algorithm: output is smoothed at
// Rate; bursts up to Capacity are absorbed, then throttled.
func LeakyBucket() RateLimiterOption {
	return func(c *rlConfig) { c.algorithm = AlgorithmLeakyBucket }
}

// Rate describes a limiter shape: how fast the budget refills and how much
// can be stored as a burst.
type Rate struct {
	// PerSecond is the refill rate in tokens per second (for windowed
	// algorithms it defines the window length: Capacity/PerSecond).
	PerSecond float64
	// Capacity is the burst size: the maximum number of tokens that can
	// accumulate (for windowed algorithms, the maximum requests per window).
	Capacity float64
}

// PerSecond builds a Rate that refills at n tokens/second with a burst of
// n (one second of budget).
func PerSecond(n float64) Rate {
	return Rate{PerSecond: n, Capacity: n}
}

// PerMinute builds a Rate that refills at n tokens/minute with a burst of
// n (one minute of budget).
func PerMinute(n float64) Rate {
	return Rate{PerSecond: n / 60, Capacity: n}
}

// WithBurst overrides the capacity (burst size) of a Rate.
func (r Rate) WithBurst(capacity float64) Rate {
	r.Capacity = capacity
	return r
}

// RateLimiter creates a rate limiter for the named resource. The default
// algorithm is token bucket; pass distsync.FixedWindow(), distsync.
// SlidingWindow() or distsync.LeakyBucket() to switch.
// It panics unless rate and capacity are positive and finite, and the
// refill period fits in time.Duration at millisecond precision. Windowed
// algorithms additionally require a window of at least 1ms and a capacity
// in [1, 2^53-1], so integer counts are exact in Redis Lua.
func (c *Client) RateLimiter(name string, rate Rate, opts ...RateLimiterOption) *RateLimiter {
	cfg := rlConfig{algorithm: AlgorithmTokenBucket}
	for _, o := range opts {
		o(&cfg)
	}
	rl := &RateLimiter{
		client:    c,
		name:      name,
		key:       redisx.Key(name),
		capacity:  rate.Capacity,
		rate:      rate.PerSecond,
		algorithm: cfg.algorithm,
	}
	if rl.rate <= 0 || rl.capacity <= 0 || math.IsNaN(rl.rate) || math.IsNaN(rl.capacity) || math.IsInf(rl.rate, 0) || math.IsInf(rl.capacity, 0) {
		panic("distsync: rate limiter requires positive finite rate and capacity")
	}
	windowMs := rl.capacity / rl.rate * 1000
	if windowMs > float64(math.MaxInt64/int64(time.Millisecond)) {
		panic("distsync: rate limiter refill period exceeds time.Duration range")
	}
	if rl.algorithm == AlgorithmFixedWindow || rl.algorithm == AlgorithmSlidingWindow {
		if windowMs < 1 || rl.capacity < 1 || rl.capacity > 1<<53-1 {
			panic("distsync: windowed rate limiter requires a window of at least 1ms and capacity in [1, 2^53-1]")
		}
	}
	return rl
}

// Name returns the resource name this limiter guards.
func (rl *RateLimiter) Name() string { return rl.name }

// Algorithm returns the algorithm this limiter uses.
func (rl *RateLimiter) Algorithm() Algorithm { return rl.algorithm }

// Limit reports the configured rate in tokens per second.
func (rl *RateLimiter) Limit() float64 { return rl.rate }

// window returns the window length used by the windowed algorithms
// (Capacity/Rate).
func (rl *RateLimiter) window() time.Duration {
	return time.Duration(rl.capacity / rl.rate * float64(time.Second))
}

func (rl *RateLimiter) fixedWindowKey(nowMs int64) string {
	return rl.key + ":" + strconv.FormatInt(nowMs/rl.window().Milliseconds(), 10)
}

// Allow checks whether n requests are admissible right now, without
// blocking. When not allowed it returns how long the caller should wait
// before retrying. n is a token count for token bucket / leaky bucket and a
// request count (rounded up to a whole request) for the windowed algorithms.
// Negative or non-finite n, or a count exceeding Capacity after rounding,
// returns an error without accessing Redis. Zero succeeds without consuming
// budget, including when ctx is canceled.
func (rl *RateLimiter) Allow(ctx context.Context, n float64) (allowed bool, retryAfter time.Duration, err error) {
	ctxNonNil(ctx)
	if n == 0 {
		return true, 0, nil
	}
	if n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return false, 0, fmt.Errorf("distsync: rate limiter %q: request count must be finite and non-negative", rl.name)
	}
	if rl.algorithm == AlgorithmFixedWindow || rl.algorithm == AlgorithmSlidingWindow {
		n = math.Ceil(n)
	}
	if n > rl.capacity {
		return false, 0, fmt.Errorf("distsync: rate limiter %q: request count %g exceeds capacity %g", rl.name, n, rl.capacity)
	}
	ctx, finish := rl.client.tracer.Start(ctx, "distsync.ratelimit.allow")
	defer func() { finish(err) }()

	switch rl.algorithm {
	case AlgorithmFixedWindow:
		now := time.Now().UnixMilli()
		return rl.allow(ctx, lua.RateLimitFixed, rl.fixedWindowKey(now),
			rl.capacity, rl.window().Milliseconds(), now, int64(n))
	case AlgorithmSlidingWindow:
		return rl.allow(ctx, lua.RateLimitSliding, rl.key,
			rl.capacity, rl.window().Milliseconds(), time.Now().UnixMilli(),
			int64(n), lease.Token())
	case AlgorithmLeakyBucket:
		return rl.allow(ctx, lua.RateLimitLeaky, rl.key,
			rl.capacity, rl.rate, time.Now().UnixMilli(), n)
	default:
		return rl.allow(ctx, lua.RateLimit, rl.key,
			rl.capacity, rl.rate, time.Now().UnixMilli(), n)
	}
}

func (rl *RateLimiter) allow(ctx context.Context, script *redis.Script, key string, args ...any) (bool, time.Duration, error) {
	res, err := script.Run(ctx, rl.client.rdb, []string{key}, args...).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) != 3 {
		return false, 0, fmt.Errorf("distsync: rate limiter %q: unexpected script result", rl.name)
	}
	allowed := res[0].(int64) == 1
	retryAfter := time.Duration(res[2].(int64)) * time.Millisecond
	return allowed, retryAfter, nil
}

// Acquire blocks until n requests are admissible or ctx is canceled. It
// polls the limiter, sleeping exactly the retry-after the algorithm reports,
// so contention does not hammer Redis. The same input rules as Allow apply;
// invalid or impossible requests return an error without waiting.
func (rl *RateLimiter) Acquire(ctx context.Context, n float64) error {
	ctxNonNil(ctx)
	for {
		allowed, retryAfter, err := rl.Allow(ctx, n)
		if err != nil {
			return fmt.Errorf("distsync: rate limiter %q: %w", rl.name, err)
		}
		if allowed {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("distsync: rate limiter %q: %w", rl.name, ctx.Err())
		}
		if retryAfter <= 0 {
			retryAfter = 10 * time.Millisecond // algorithm math edge: never busy-spin
		}
		timer := time.NewTimer(retryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("distsync: rate limiter %q: %w", rl.name, ctx.Err())
		case <-timer.C:
		}
	}
}

// Wait is an alias for Acquire that also reports how long the caller had to
// wait (useful for dashboards).
func (rl *RateLimiter) Wait(ctx context.Context, n float64) (waited time.Duration, err error) {
	start := time.Now()
	err = rl.Acquire(ctx, n)
	return time.Since(start), err
}

// Reset empties the limiter state (used by tests and admin tooling).
// For a fixed window it deletes only the current window's counter; older
// windows retain their expiry. Concurrent requests may consume budget again.
func (rl *RateLimiter) Reset(ctx context.Context) error {
	key := rl.key
	if rl.algorithm == AlgorithmFixedWindow {
		key = rl.fixedWindowKey(time.Now().UnixMilli())
	}
	return rl.client.rdb.Del(ctx, key).Err()
}
