package distsync

import (
	"context"
	"testing"
	"time"

	"github.com/MouXiaoJun/distsync/internal/redis"
	"github.com/redis/go-redis/v9"
)

// TestRWMutexFIFOGrantOrder is the heart of the fair lock: three contenders
// queue in order W1, R2, W3 behind a holder, and after release they are
// granted in exactly that order — a reader may not jump a queued writer and
// no one may jump an earlier arrival.
func TestRWMutexFIFOGrantOrder(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("fifo:order")

	holder, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("holder: %v", err)
	}

	order := make(chan string, 3)
	enqueue := func(name string, acquire func() (*Guard, error)) {
		go func() {
			g, err := acquire()
			if err != nil {
				return // missing entry -> the test times out below
			}
			order <- name
			time.Sleep(50 * time.Millisecond) // hold briefly so grants serialize
			_ = g.Unlock(context.Background())
		}()
	}
	enqueue("W1", func() (*Guard, error) { return mu.Lock(context.Background()) })
	time.Sleep(100 * time.Millisecond) // W1 joins the queue
	enqueue("R2", func() (*Guard, error) { return mu.RLock(context.Background()) })
	time.Sleep(100 * time.Millisecond) // R2 joins behind W1
	enqueue("W3", func() (*Guard, error) { return mu.Lock(context.Background()) })
	time.Sleep(100 * time.Millisecond) // W3 joins behind R2

	if err := holder.Unlock(context.Background()); err != nil {
		t.Fatalf("holder unlock: %v", err)
	}

	got := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case name := <-order:
			got = append(got, name)
		case <-time.After(5 * time.Second):
			t.Fatalf("grant order incomplete after timeout: got %v", got)
		}
	}
	if got[0] != "W1" || got[1] != "R2" || got[2] != "W3" {
		t.Fatalf("grant order = %v, want [W1 R2 W3]", got)
	}
}

// TestRWMutexReaderWaitsForQueuedWriter: a reader arriving after a queued
// writer must not be granted while that writer still waits.
func TestRWMutexReaderWaitsForQueuedWriter(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("fifo:nojump")

	holder, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("holder: %v", err)
	}

	wAcquired := make(chan struct{})
	go func() {
		g, err := mu.Lock(context.Background())
		if err != nil {
			return
		}
		close(wAcquired)
		time.Sleep(50 * time.Millisecond)
		_ = g.Unlock(context.Background())
	}()
	time.Sleep(150 * time.Millisecond) // writer is queued

	// A new reader must be refused: the writer is ahead in the queue.
	if _, err := mu.TryRLock(context.Background()); err != ErrNotAcquired {
		t.Fatalf("TryRLock should be refused while a writer is queued, got %v", err)
	}

	if err := holder.Unlock(context.Background()); err != nil {
		t.Fatalf("holder unlock: %v", err)
	}
	select {
	case <-wAcquired:
	case <-time.After(3 * time.Second):
		t.Fatal("queued writer never acquired after the holder released")
	}
}

// TestRWMutexStaleWaiterPurged: a crashed waiter (entry in the queue with a
// long-stale timestamp) is purged from the head, so it can never block the
// queue. We inject the stale entry directly, as a crash would leave it.
func TestRWMutexStaleWaiterPurged(t *testing.T) {
	c, _ := newTestClient(t)
	const name = "fifo:stale"
	mu := c.RWMutex(name, Lease(300*time.Millisecond))

	key := redisx.Key(name)
	waiters := redisx.Derived(key, "waiters")
	waiterTS := redisx.Derived(key, "waiter-ts")
	rdb := c.Redis()
	ctx := context.Background()

	// A crashed writer queued at the head (score 0 < any fresh INCR) whose
	// last attempt is long past the 2*ttl purge threshold.
	if err := rdb.ZAdd(ctx, waiters, redis.Z{Score: 0, Member: "W:crashed"}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}
	if err := rdb.HSet(ctx, waiterTS, "W:crashed", time.Now().Add(-3*300*time.Millisecond).UnixMilli()).Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}

	// A real writer must purge the crashed head and acquire promptly — if
	// the purge were broken, it would be stuck behind the stale entry.
	lockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	g, err := mu.Lock(lockCtx)
	if err != nil {
		t.Fatalf("lock blocked by stale waiter (purge broken?): %v", err)
	}
	_ = g.Unlock(ctx)

	// And the crashed entry is actually gone.
	n, err := rdb.ZCard(ctx, waiters).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if n != 0 {
		t.Fatalf("waiters queue has %d leftover entries, want 0", n)
	}
}

// TestRWMutexCanceledWaiterLeavesQueue: a waiter whose context is canceled
// dequeues itself, so later arrivals are not blocked behind it.
func TestRWMutexCanceledWaiterLeavesQueue(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("fifo:cancel")

	holder, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("holder: %v", err)
	}

	// A writer that gives up after 100ms of waiting.
	wctx, wcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer wcancel()
	if _, err := mu.Lock(wctx); err == nil {
		t.Fatal("writer should have timed out while the holder holds")
	}
	// (Lock's cancel path dequeues automatically.)

	// Release the holder: the next writer must acquire immediately, not be
	// stuck behind the canceled waiter's queue entry.
	if err := holder.Unlock(context.Background()); err != nil {
		t.Fatalf("holder unlock: %v", err)
	}
	actx, acancel := context.WithTimeout(context.Background(), time.Second)
	defer acancel()
	g, err := mu.Lock(actx)
	if err != nil {
		t.Fatalf("second writer blocked by canceled waiter's queue entry: %v", err)
	}
	_ = g.Unlock(context.Background())
}
