package distsync

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestClient spins up an in-memory miniredis (which supports EVAL/EVALSHA
// with a real Lua interpreter) and returns a Client wired to it.
func newTestClient(t *testing.T, opts ...ClientOption) (*Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb, opts...), s
}

// fastForward advances miniredis's internal clock. Note: key TTLs (mutex,
// writer lease) use miniredis time, so they need FastForward; sorted-set
// scores (semaphore permits, readers) are stamped from the client's real
// clock, so those tests use real time instead.
func fastForward(s *miniredis.Miniredis, d time.Duration) {
	s.FastForward(d)
}

// expectBusy asserts err == ErrNotAcquired.
func expectBusy(t *testing.T, err error) {
	t.Helper()
	if err != ErrNotAcquired {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}
}
