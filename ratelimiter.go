package distsync

import (
	"context"
	"fmt"
	"time"

	"github.com/distsync/distsync/internal/lua"
	"github.com/distsync/distsync/internal/redis"
)

// RateLimiter is a distributed token-bucket rate limiter (v0.1 implements
// exactly one algorithm; sliding-window, fixed-window and leaky-bucket
// variants are planned for later versions). The bucket is shared across all
// processes, so the aggregate rate is enforced cluster-wide, not per node:
//
//	limiter := client.RateLimiter("tenant:1001", distsync.PerSecond(100))
//	if err := limiter.Acquire(ctx, 1); err != nil {
//	    return err
//	}
type RateLimiter struct {
	client   *Client
	name     string
	key      string
	capacity float64
	rate     float64 // tokens per second
}

// Rate describes a token-bucket shape. Capacity is the burst size; refill
// is how fast tokens accumulate.
type Rate struct {
	PerSecond float64
	Capacity  float64
}

// PerSecond builds a Rate that refills at n tokens/second with a burst of
// n (one second of budget).
func PerSecond(n float64) Rate {
	return Rate{PerSecond: n, Capacity: n}
}

// PerMinute builds a Rate that refills at n tokens/minute with a burst of
// n/60 (one minute of budget).
func PerMinute(n float64) Rate {
	return Rate{PerSecond: n / 60, Capacity: n / 60}
}

// WithBurst overrides the bucket capacity (burst size) of a Rate.
func (r Rate) WithBurst(capacity float64) Rate {
	r.Capacity = capacity
	return r
}

// RateLimiter creates a token-bucket limiter for the named resource.
func (c *Client) RateLimiter(name string, rate Rate, opts ...Option) *RateLimiter {
	// Options are accepted for forward compatibility (e.g. algorithm
	// selection in later versions).
	_ = c.resolved(opts...)
	rl := &RateLimiter{
		client:   c,
		name:     name,
		key:      redisx.Key(name),
		capacity: rate.Capacity,
		rate:     rate.PerSecond,
	}
	if rl.rate <= 0 || rl.capacity <= 0 {
		panic("distsync: rate limiter requires positive rate and capacity")
	}
	return rl
}

// Name returns the resource name this limiter guards.
func (rl *RateLimiter) Name() string { return rl.name }

// Allow checks whether n tokens are available right now, without blocking.
// It returns the number of milliseconds the caller should wait before
// retrying when not allowed.
func (rl *RateLimiter) Allow(ctx context.Context, n float64) (allowed bool, retryAfter time.Duration, err error) {
	ctxNonNil(ctx)
	ctx, finish := rl.client.tracer.Start(ctx, "distsync.ratelimit.allow")
	defer func() { finish(err) }()

	res, err := lua.RateLimit.Run(
		ctx, rl.client.rdb,
		[]string{rl.key},
		rl.capacity, rl.rate, time.Now().UnixMilli(), n,
	).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) != 3 {
		return false, 0, fmt.Errorf("distsync: rate limiter %q: unexpected script result", rl.name)
	}
	allowed = res[0].(int64) == 1
	retryAfter = time.Duration(res[2].(int64)) * time.Millisecond
	return allowed, retryAfter, nil
}

// Acquire blocks until n tokens are available or ctx is canceled. It polls
// the bucket, sleeping exactly the retry-after the bucket reports, so
// contention does not hammer Redis.
func (rl *RateLimiter) Acquire(ctx context.Context, n float64) error {
	ctxNonNil(ctx)
	if n <= 0 {
		return nil
	}
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
			retryAfter = 10 * time.Millisecond // bucket math edge: never busy-spin
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

// Reset empties the bucket (used by tests and admin tooling).
func (rl *RateLimiter) Reset(ctx context.Context) error {
	return rl.client.rdb.Del(ctx, rl.key).Err()
}

// Limit reports the configured refill rate in tokens per second.
func (rl *RateLimiter) Limit() float64 { return rl.rate }
