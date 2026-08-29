package distsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// These tests simulate a Redis outage (server killed) and assert the
// library fails fast and honestly: operations return errors, they never
// hang, and an outage is never mistaken for "the resource is busy".
//
// They always use a dedicated miniredis — never DISTSYNC_TEST_REDIS_ADDR —
// because killing the shared real server would break every other test.

// newOutageClient builds an isolated miniredis-backed client whose server
// can be killed mid-test.
func newOutageClient(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb), s
}

func TestMutexLockFailsWhenRedisDown(t *testing.T) {
	c, s := newOutageClient(t)
	s.Close() // kill the server

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Mutex("outage:lock").Lock(ctx); err == nil {
		t.Fatal("Lock must fail when Redis is unreachable")
	}
}

func TestMutexTryLockDistinguishesOutageFromBusy(t *testing.T) {
	c, s := newOutageClient(t)
	s.Close()

	_, err := c.Mutex("outage:try").TryLock(context.Background())
	if err == nil {
		t.Fatal("TryLock must fail when Redis is unreachable")
	}
	if errors.Is(err, ErrNotAcquired) {
		t.Fatal("an outage must NOT be reported as ErrNotAcquired (busy)")
	}
}

func TestRWMutexFailsWhenRedisDown(t *testing.T) {
	c, s := newOutageClient(t)
	s.Close()

	if _, err := c.RWMutex("outage:rw").Lock(context.Background()); err == nil {
		t.Fatal("write lock must fail when Redis is unreachable")
	}
	if _, err := c.RWMutex("outage:rw").RLock(context.Background()); err == nil {
		t.Fatal("read lock must fail when Redis is unreachable")
	}
}

func TestSemaphoreFailsWhenRedisDown(t *testing.T) {
	c, s := newOutageClient(t)
	s.Close()

	if _, err := c.Semaphore("outage:sem", 5).Acquire(context.Background(), 1); err == nil {
		t.Fatal("semaphore acquire must fail when Redis is unreachable")
	}
}

func TestRateLimiterFailsWhenRedisDown(t *testing.T) {
	c, s := newOutageClient(t)
	s.Close()

	if _, _, err := c.RateLimiter("outage:rl", PerSecond(10)).Allow(context.Background(), 1); err == nil {
		t.Fatal("rate limiter Allow must fail when Redis is unreachable")
	}
}
