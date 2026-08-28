package distsync

import (
	"context"
	"errors"
	"testing"
	"time"
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
