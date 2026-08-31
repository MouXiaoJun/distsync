package distsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
)

func TestRateLimiterBurstExhausted(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("tenant:1001", PerSecond(10)) // capacity 10

	for i := 0; i < 10; i++ {
		ok, _, err := rl.Allow(context.Background(), 1)
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("allow %d should pass (burst)", i)
		}
	}
	// Request the whole burst: refill during the loop (a few ms at 10/s)
	// can never reach 10 tokens.
	ok, retry, err := rl.Allow(context.Background(), 10)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if ok {
		t.Fatal("burst should be exhausted")
	}
	if retry <= 0 {
		t.Fatalf("retry-after should be positive, got %v", retry)
	}
}

func TestRateLimiterRefillsOverTime(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("tenant:2002", PerSecond(1000)) // 1000 tokens/s, burst 1000

	// Consume the whole burst in one atomic call (sequential Allow calls
	// would refill in between and make the count nondeterministic).
	ok, _, err := rl.Allow(context.Background(), 1000)
	if err != nil || !ok {
		t.Fatalf("burst consume: ok=%v err=%v", ok, err)
	}
	// Request the full burst again: even a slow test run refills only a few
	// tokens, far below the burst size.
	if ok, _, _ := rl.Allow(context.Background(), 1000); ok {
		t.Fatal("burst should be exhausted")
	}

	// Real time refills the bucket (~200 tokens at 1000/s).
	time.Sleep(200 * time.Millisecond)
	ok, _, err = rl.Allow(context.Background(), 1)
	if err != nil {
		t.Fatalf("allow after refill: %v", err)
	}
	if !ok {
		t.Fatal("should be allowed after refill")
	}
}

func TestRateLimiterAcquireBlocksUntilCtxDone(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("tenant:3003", PerSecond(1)) // capacity 1

	if ok, _, _ := rl.Allow(context.Background(), 1); !ok {
		t.Fatal("first token should pass")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := rl.Acquire(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire should time out, got %v", err)
	}
}

func TestRateLimiterRateBuilders(t *testing.T) {
	if r := PerMinute(60); r.PerSecond != 1 {
		t.Fatalf("PerMinute(60).PerSecond = %v, want 1", r.PerSecond)
	}
	r := PerSecond(5).WithBurst(50)
	if r.Capacity != 50 || r.PerSecond != 5 {
		t.Fatalf("WithBurst: got %+v", r)
	}
}

func TestRateLimiterReset(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("tenant:4004", PerSecond(1))

	if ok, _, _ := rl.Allow(context.Background(), 1); !ok {
		t.Fatal("first token should pass")
	}
	if err := rl.Reset(context.Background()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if ok, _, _ := rl.Allow(context.Background(), 1); !ok {
		t.Fatal("should be allowed after reset")
	}
}

func TestRateLimiterDefaultAlgorithmIsTokenBucket(t *testing.T) {
	c, _ := newTestClient(t)
	if got := c.RateLimiter("a", PerSecond(1)).Algorithm(); got != AlgorithmTokenBucket {
		t.Fatalf("default algorithm = %v, want token-bucket", got)
	}
	for alg, opt := range map[Algorithm]RateLimiterOption{
		AlgorithmFixedWindow:   FixedWindow(),
		AlgorithmSlidingWindow: SlidingWindow(),
		AlgorithmLeakyBucket:   LeakyBucket(),
		AlgorithmTokenBucket:   TokenBucket(),
	} {
		if got := c.RateLimiter("a", PerSecond(1), opt).Algorithm(); got != alg {
			t.Fatalf("algorithm = %v, want %v", got, alg)
		}
	}
}

// Fixed window: PerSecond(10) -> window of 1s, limit 10. The full burst
// passes, then a whole-burst request is rejected with a positive retry.
func TestRateLimiterFixedWindow(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("fixed:1", PerSecond(10), FixedWindow())

	for i := 0; i < 10; i++ {
		if ok, _, err := rl.Allow(context.Background(), 1); err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v", i, ok, err)
		}
	}
	ok, retry, err := rl.Allow(context.Background(), 10)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if ok {
		t.Fatal("fixed window should reject beyond its limit")
	}
	if retry <= 0 {
		t.Fatalf("retry-after should be positive, got %v", retry)
	}
}

// Sliding window with a tiny window (PerSecond(10).WithBurst(1) -> 100ms):
// one request in, the next is rejected, and after the entry ages out the
// request passes again — exactly the rolling-window behavior.
func TestRateLimiterSlidingWindow(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("sliding:1", PerSecond(10).WithBurst(1), SlidingWindow())

	if ok, _, err := rl.Allow(context.Background(), 1); err != nil || !ok {
		t.Fatalf("first request: ok=%v err=%v", ok, err)
	}
	if ok, _, _ := rl.Allow(context.Background(), 1); ok {
		t.Fatal("sliding window of 1 should reject the second request")
	}

	time.Sleep(200 * time.Millisecond) // > window (100ms)
	if ok, _, err := rl.Allow(context.Background(), 1); err != nil || !ok {
		t.Fatalf("request after window roll: ok=%v err=%v", ok, err)
	}
}

// Leaky bucket with burst 10: 10 requests fill the bucket, the 11th is
// rejected, and after draining it passes again.
func TestRateLimiterLeakyBucket(t *testing.T) {
	c, _ := newTestClient(t)
	rl := c.RateLimiter("leaky:1", PerSecond(10).WithBurst(10), LeakyBucket())
	// The algorithm accepts a client timestamp. Freeze it while filling: ten
	// real network round trips can exceed 100ms and legitimately drain a token.
	// Reuse the production script/response path without adding a library clock.
	if ok, _, err := rl.Allow(context.Background(), 1); err != nil || !ok {
		t.Fatalf("first public request: ok=%v err=%v", ok, err)
	}
	now, err := c.Redis().HGet(context.Background(), rl.key, "ts").Int64()
	if err != nil {
		t.Fatal(err)
	}
	allow := func(n float64) (bool, time.Duration, error) {
		return rl.allow(context.Background(), lua.RateLimitLeaky, rl.key,
			rl.capacity, rl.rate, now, n)
	}

	for i := 1; i < 10; i++ {
		if ok, _, err := allow(1); err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v", i, ok, err)
		}
	}
	ok, retry, err := allow(1)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if ok {
		t.Fatal("leaky bucket should reject once full")
	}
	if retry <= 0 {
		t.Fatalf("retry-after should be positive, got %v", retry)
	}

	now += 200 // drains exactly 2 tokens at 10/s
	if ok, _, err := allow(1); err != nil || !ok {
		t.Fatalf("request after drain: ok=%v err=%v", ok, err)
	}
}
