package distsync

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newBenchClient wires a benchmark to an in-memory miniredis. Benchmarks
// measure the full path: Lua script (EVALSHA) + round trip, the same cost a
// real deployment pays per operation.
func newBenchClient(b *testing.B) *Client {
	b.Helper()
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	b.Cleanup(func() { _ = rdb.Close() })
	return New(rdb)
}

func BenchmarkMutexLockUnlock(b *testing.B) {
	c := newBenchClient(b)
	mu := c.Mutex("bench:mutex", NoAutoRenew())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := mu.Lock(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Unlock(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMutexTryLockUnlock(b *testing.B) {
	c := newBenchClient(b)
	mu := c.Mutex("bench:try", NoAutoRenew())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := mu.TryLock(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Unlock(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRWMutexLockUnlock(b *testing.B) {
	c := newBenchClient(b)
	mu := c.RWMutex("bench:rw", NoAutoRenew())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := mu.Lock(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Unlock(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRWMutexRLockRUnlock(b *testing.B) {
	c := newBenchClient(b)
	mu := c.RWMutex("bench:rlock", NoAutoRenew())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := mu.RLock(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Unlock(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSemaphoreAcquireRelease(b *testing.B) {
	c := newBenchClient(b)
	sem := c.Semaphore("bench:sem", 1<<20, NoAutoRenew())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := sem.Acquire(ctx, 1)
		if err != nil {
			b.Fatal(err)
		}
		if err := p.Release(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRateLimiterAllowTokenBucket(b *testing.B) {
	c := newBenchClient(b)
	rl := c.RateLimiter("bench:rl", PerSecond(1e9))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := rl.Allow(ctx, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRateLimiterAllowSlidingWindow(b *testing.B) {
	c := newBenchClient(b)
	rl := c.RateLimiter("bench:rlw", PerSecond(1e9).WithBurst(1e9), SlidingWindow())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := rl.Allow(ctx, 1); err != nil {
			b.Fatal(err)
		}
	}
}
