package distsync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaderRunExecutesCallback(t *testing.T) {
	c, _ := newTestClient(t)
	leader := c.Leader("scheduler")

	ran := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- leader.Run(ctx, func(lctx context.Context) error {
			close(ran)
			<-lctx.Done()
			return nil
		})
	}()

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("callback never ran")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned after cancel")
	}
}

// TestLeaderFailover is the "service-2 dies, service-4 becomes leader" flow:
// the second handle must not run its callback until the first releases.
func TestLeaderFailover(t *testing.T) {
	c, _ := newTestClient(t)
	leader1 := c.Leader("scheduler")
	leader2 := c.Leader("scheduler")

	var firstRan, secondRan atomic.Bool

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	acquired1 := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- leader1.Run(ctx1, func(lctx context.Context) error {
			firstRan.Store(true)
			close(acquired1)
			<-lctx.Done()
			return nil
		})
	}()
	<-acquired1 // leader1 provably holds the lease

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- leader2.Run(ctx2, func(lctx context.Context) error {
			secondRan.Store(true)
			<-lctx.Done()
			return nil
		})
	}()

	time.Sleep(300 * time.Millisecond)
	if secondRan.Load() {
		t.Fatal("leader2 must not run while leader1 holds")
	}

	cancel1() // leader1 dies
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("leader1 Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("leader1 Run never returned")
	}

	// lease expires -> leader2 takes over
	select {
	case <-waitForTrue(&secondRan):
	case <-time.After(5 * time.Second):
		t.Fatal("leader2 never became leader after failover")
	}

	cancel2()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("leader2 Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("leader2 Run never returned")
	}
}

func waitForTrue(v *atomic.Bool) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for !v.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		close(ch)
	}()
	return ch
}

func TestLeaderTryRun(t *testing.T) {
	c, _ := newTestClient(t)
	leader := c.Leader("single-flight")

	stop := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- leader.TryRun(context.Background(), func(lctx context.Context) error {
			<-stop
			return nil
		})
	}()

	time.Sleep(200 * time.Millisecond) // let it acquire

	err := leader.TryRun(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("second TryRun should report ErrNotAcquired, got %v", err)
	}

	close(stop)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("first TryRun returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first TryRun never returned")
	}

	if err := leader.TryRun(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("TryRun after release: %v", err)
	}
}

// TestLeaderLossCancelsCallback: if the lease is lost while running (e.g.
// the key is deleted), the callback context must be canceled and Run must
// report ErrLeadershipLost.
func TestLeaderLossCancelsCallback(t *testing.T) {
	c, _ := newTestClient(t)
	leader := c.Leader("watchdog", Lease(200*time.Millisecond)) // autoRenew on

	callbackCanceled := make(chan struct{})
	runErr := make(chan error, 1)
	go func() {
		runErr <- leader.Run(context.Background(), func(lctx context.Context) error {
			<-lctx.Done()
			close(callbackCanceled)
			return nil
		})
	}()

	time.Sleep(200 * time.Millisecond) // leader is up

	// Simulate the lease being stolen/lost: delete the key. The next renew
	// fails with ErrLost and cancels the callback.
	if err := c.Redis().Del(context.Background(), "{watchdog}").Err(); err != nil {
		t.Fatalf("del: %v", err)
	}

	select {
	case <-callbackCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("callback context was never canceled after lease loss")
	}

	select {
	case err := <-runErr:
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("Run should return ErrLeadershipLost, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned after lease loss")
	}
}

func TestOnceSerializesCallers(t *testing.T) {
	c, _ := newTestClient(t)
	mu := c.Mutex("once:init")

	var concurrent, maxConcurrent atomic.Int64
	const callers = 5
	done := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			_, err := Once(context.Background(), mu, func(ctx context.Context) (int, error) {
				n := concurrent.Add(1)
				for {
					m := maxConcurrent.Load()
					if n <= m || maxConcurrent.CompareAndSwap(m, n) {
						break
					}
				}
				time.Sleep(30 * time.Millisecond)
				concurrent.Add(-1)
				return 42, nil
			})
			if err != nil {
				t.Errorf("Once: %v", err)
			}
		}()
	}
	for i := 0; i < callers; i++ {
		<-done
	}
	if maxConcurrent.Load() != 1 {
		t.Fatalf("max concurrent fn executions = %d, want 1 (serialized)", maxConcurrent.Load())
	}
}

// TestLeaderFencingDisabledByDefault: without the Fencing option the leader
// mints no fencing token.
func TestLeaderFencingDisabledByDefault(t *testing.T) {
	c, _ := newTestClient(t)
	leader := c.Leader("fence:off")

	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- leader.Run(ctx, func(lctx context.Context) error {
			<-lctx.Done()
			return nil
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for !leader.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !leader.IsLeader() {
		t.Fatal("never became leader")
	}
	if token := leader.FencingToken(); token != 0 {
		t.Fatalf("FencingToken = %d, want 0 (fencing disabled)", token)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned")
	}
}

// TestLeaderFencingTokensIncreaseAcrossLeaderships: with Fencing() enabled,
// each leadership mints a strictly increasing token, so a stale leader can
// never clobber a newer leader's writes (the same guarantee mutexes give).
func TestLeaderFencingTokensIncreaseAcrossLeaderships(t *testing.T) {
	c, _ := newTestClient(t)
	leader1 := c.Leader("fence:on", Fencing())
	leader2 := c.Leader("fence:on", Fencing())

	var firstToken, secondToken atomic.Uint64
	ctx1, cancel1 := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- leader1.Run(ctx1, func(lctx context.Context) error {
			// Wait until the token is visible, then capture it.
			for leader1.FencingToken() == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			firstToken.Store(leader1.FencingToken())
			<-lctx.Done()
			return nil
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for firstToken.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if firstToken.Load() == 0 {
		t.Fatal("leader1 never minted a fencing token")
	}
	cancel1()
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("leader1 Run never returned")
	}

	// leader2 takes over and must mint a strictly larger token.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- leader2.Run(ctx2, func(lctx context.Context) error {
			for leader2.FencingToken() == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			secondToken.Store(leader2.FencingToken())
			<-lctx.Done()
			return nil
		})
	}()

	deadline = time.Now().Add(5 * time.Second)
	for secondToken.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if secondToken.Load() == 0 {
		t.Fatal("leader2 never minted a fencing token")
	}
	if secondToken.Load() <= firstToken.Load() {
		t.Fatalf("fencing tokens must increase across leaderships: %d then %d", firstToken.Load(), secondToken.Load())
	}
	cancel2()
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("leader2 Run never returned")
	}
}
