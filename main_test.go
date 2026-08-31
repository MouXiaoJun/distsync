package distsync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// testRedisAddr, when set, makes newTestClient talk to a REAL Redis/Valkey
// server instead of miniredis, so the whole suite doubles as an integration
// test. Used by CI (services) and by `make test-redis`.
const testRedisAddrEnv = "DISTSYNC_TEST_REDIS_ADDR"

// newTestClient spins up an in-memory miniredis (EVAL/EVALSHA with a real
// Lua interpreter) and returns a Client wired to it. When
// DISTSYNC_TEST_REDIS_ADDR is set it connects to that real server instead
// and returns a nil miniredis handle.
func newTestClient(t *testing.T, opts ...ClientOption) (*Client, *miniredis.Miniredis) {
	t.Helper()
	// Use the same snappy backoff on both backends. Contention tests still run
	// every acquisition/invariant, without spending minutes at the 2s ceiling.
	opts = append([]ClientOption{WithRetry(5*time.Millisecond, 50*time.Millisecond)}, opts...)
	if addr := os.Getenv(testRedisAddrEnv); addr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		if err := rdb.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("flush real redis: %v", err)
		}
		return New(rdb, opts...), nil
	}
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, opts...), s
}

// fastForward advances miniredis's internal clock. Against a real server
// (nil handle) it sleeps instead, so the same TTL-based tests pass against
// both backends. Note: key TTLs (mutex, writer lease) use the server clock,
// while sorted-set scores (semaphore permits, readers) are stamped from the
// client's real clock.
func fastForward(s *miniredis.Miniredis, d time.Duration) {
	if s == nil {
		// Real Redis: the TTL runs on the server's wall clock; wait it out
		// with a margin so the key is definitely expired.
		time.Sleep(d + 150*time.Millisecond)
		return
	}
	s.FastForward(d)
}

// expectBusy asserts err == ErrNotAcquired.
func expectBusy(t *testing.T, err error) {
	t.Helper()
	if err != ErrNotAcquired {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}
}
