package distsync

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The watchdog gives a NoAutoRenew guard the ability to notice lease expiry
// without keeping the lease alive: Lost()/Context() fire once the key (or
// permit score) expires server-side.

func TestMutexWatchdogFiresLostOnExpiry(t *testing.T) {
	c, s := newTestClient(t)
	mu := c.Mutex("wd:mutex", Lease(150*time.Millisecond), NoAutoRenew(), Watchdog())

	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}

	fastForward(s, 200*time.Millisecond) // lease expires in miniredis time
	select {
	case <-g.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog must fire Lost() after lease expiry")
	}

	if err := g.Renew(context.Background()); !errors.Is(err, ErrLost) {
		t.Fatalf("renew after expiry should report ErrLost, got %v", err)
	}
}

// TestMutexWithoutWatchdogStaysSilent is the negative control: without the
// watchdog, a NoAutoRenew guard never notices expiry on its own.
func TestMutexWithoutWatchdogStaysSilent(t *testing.T) {
	c, s := newTestClient(t)
	mu := c.Mutex("wd:silent", Lease(150*time.Millisecond), NoAutoRenew())

	g, err := mu.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	fastForward(s, 200*time.Millisecond)
	select {
	case <-g.Lost():
		t.Fatal("Lost() must not fire without a watchdog")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestSemaphoreWatchdogFiresLostOnExpiry(t *testing.T) {
	c, _ := newTestClient(t)
	sem := c.Semaphore("wd:sem", 5, Lease(150*time.Millisecond), NoAutoRenew(), Watchdog())

	p, err := sem.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Permit expiry is real-time (zset scores are client-stamped), so wait.
	time.Sleep(300 * time.Millisecond)
	select {
	case <-p.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog must fire Lost() after permit expiry")
	}
}

func TestRWMutexReaderWatchdogFiresLostOnExpiry(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.RWMutex("wd:reader", Lease(150*time.Millisecond), NoAutoRenew(), Watchdog())

	g, err := mu.RLock(context.Background())
	if err != nil {
		t.Fatalf("rlock: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // reader expiry is real-time
	select {
	case <-g.Lost():
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog must fire Lost() after reader lease expiry")
	}
}

// TestLeaderWatchdogFailover: a non-renewing leader with the watchdog still
// fails over: its callback is canceled when the lease expires, Run reports
// ErrLeadershipLost, and a queued replica takes over.
func TestLeaderWatchdogFailover(t *testing.T) {
	c, s := newTestClient(t)
	leader1 := c.Leader("wd:leader", Lease(150*time.Millisecond), NoAutoRenew(), Watchdog())

	callbackCanceled := make(chan struct{})
	runErr := make(chan error, 1)
	go func() {
		runErr <- leader1.Run(context.Background(), func(lctx context.Context) error {
			<-lctx.Done()
			close(callbackCanceled)
			return nil
		})
	}()

	// Let it become leader.
	deadline := time.Now().Add(3 * time.Second)
	for !leader1.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !leader1.IsLeader() {
		t.Fatal("leader1 never became leader")
	}

	fastForward(s, 200*time.Millisecond) // lease expires

	select {
	case <-callbackCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog must cancel the leader callback after lease expiry")
	}
	select {
	case err := <-runErr:
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("Run should return ErrLeadershipLost, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned")
	}

	// A queued replica can now take over.
	leader2 := c.Leader("wd:leader", NoAutoRenew())
	secondRan := make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- leader2.Run(ctx, func(lctx context.Context) error {
			close(secondRan)
			<-lctx.Done()
			return nil
		})
	}()
	select {
	case <-secondRan:
	case <-time.After(3 * time.Second):
		t.Fatal("leader2 must take over after leader1's lease expired")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("leader2 Run never returned")
	}
}
