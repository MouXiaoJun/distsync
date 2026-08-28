package distsync

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSemaphoreCapacityEnforced(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("ai:gpt5", 2)

	p1, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("permit 1: %v", err)
	}
	defer func() { _ = p1.Release(context.Background()) }()

	p2, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("permit 2: %v", err)
	}
	defer func() { _ = p2.Release(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = sem.Acquire(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third acquire should time out, got %v", err)
	}
}

func TestSemaphoreReleaseFreesCapacity(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("jobs:transcode", 1)

	p, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := sem.Acquire(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("should be full, got %v", err)
	}

	if err := p.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}

	p2, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	_ = p2.Release(context.Background())
}

func TestSemaphoreMultiPermit(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("crawlers", 5)

	p, err := sem.Acquire(context.Background(), 3)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if p.Acquired() != 3 {
		t.Fatalf("Acquired() = %d, want 3", p.Acquired())
	}
	avail, err := sem.Available(context.Background())
	if err != nil {
		t.Fatalf("available: %v", err)
	}
	if avail != 2 {
		t.Fatalf("available = %d, want 2", avail)
	}
	_ = p.Release(context.Background())

	avail, err = sem.Available(context.Background())
	if err != nil {
		t.Fatalf("available: %v", err)
	}
	if avail != 5 {
		t.Fatalf("available after release = %d, want 5", avail)
	}
}

func TestSemaphoreExpiredPermitsReclaimed(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("leases", 2, Lease(150*time.Millisecond), NoAutoRenew())

	p, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = p // let it expire in real time (zset score expiry)

	time.Sleep(300 * time.Millisecond)

	// The expired permit must be reclaimed atomically: two permits now fit.
	p2, err := sem.Acquire(context.Background(), 2)
	if err != nil {
		t.Fatalf("acquire 2 after expiry: %v", err)
	}
	_ = p2.Release(context.Background())
}

func TestSemaphoreTryAcquire(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("try", 1)

	p, err := sem.TryAcquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("try acquire: %v", err)
	}
	defer func() { _ = p.Release(context.Background()) }()

	_, err = sem.TryAcquire(context.Background(), 1)
	expectBusy(t, err)
}

func TestSemaphoreRenewKeepsPermitAlive(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("renewed", 1, Lease(200*time.Millisecond)) // autoRenew on

	p, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer func() { _ = p.Release(context.Background()) }()

	// Hold well past the TTL; the heartbeat should keep the permit.
	time.Sleep(600 * time.Millisecond)
	_, err = sem.TryAcquire(context.Background(), 1)
	expectBusy(t, err)
}
