package distsync

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These tests simulate a Redis outage (server killed) and assert the
// library fails fast and honestly: operations return errors, they never
// hang, and an outage is never mistaken for "the resource is busy".

func TestMutexLockFailsWhenRedisDown(t *testing.T) {
	c, s := newTestClient(t)
	s.Close() // kill the server

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Mutex("outage:lock").Lock(ctx); err == nil {
		t.Fatal("Lock must fail when Redis is unreachable")
	}
}

func TestMutexTryLockDistinguishesOutageFromBusy(t *testing.T) {
	c, s := newTestClient(t)
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
	c, s := newTestClient(t)
	s.Close()

	if _, err := c.RWMutex("outage:rw").Lock(context.Background()); err == nil {
		t.Fatal("write lock must fail when Redis is unreachable")
	}
	if _, err := c.RWMutex("outage:rw").RLock(context.Background()); err == nil {
		t.Fatal("read lock must fail when Redis is unreachable")
	}
}

func TestSemaphoreFailsWhenRedisDown(t *testing.T) {
	c, s := newTestClient(t)
	s.Close()

	if _, err := c.Semaphore("outage:sem", 5).Acquire(context.Background(), 1); err == nil {
		t.Fatal("semaphore acquire must fail when Redis is unreachable")
	}
}

func TestRateLimiterFailsWhenRedisDown(t *testing.T) {
	c, s := newTestClient(t)
	s.Close()

	if _, _, err := c.RateLimiter("outage:rl", PerSecond(10)).Allow(context.Background(), 1); err == nil {
		t.Fatal("rate limiter Allow must fail when Redis is unreachable")
	}
}
