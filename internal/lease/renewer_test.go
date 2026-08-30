package lease

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestRenewerStopsOnLost: a definitive ownership loss (ErrLost) ends the
// loop, fires onLost exactly once, and closes Done.
func TestRenewerStopsOnLost(t *testing.T) {
	var calls atomic.Int64
	var lostCalls atomic.Int64
	r := NewRenewer(10*time.Millisecond, func(ctx context.Context) error {
		if calls.Add(1) >= 2 {
			return ErrLost
		}
		return nil
	}, func() { lostCalls.Add(1) })

	r.Start()
	select {
	case <-r.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("renewer loop did not exit after ErrLost")
	}
	// Give the onLost callback a beat to run if it were going to.
	time.Sleep(20 * time.Millisecond)
	if lostCalls.Load() != 1 {
		t.Fatalf("onLost called %d times, want 1", lostCalls.Load())
	}
}

// TestRenewerTransientErrorsRetried: a non-ErrLost error (network blip)
// must NOT stop the loop nor fire onLost; Stop() still terminates it.
func TestRenewerTransientErrorsRetried(t *testing.T) {
	var calls atomic.Int64
	var lostCalls atomic.Int64
	r := NewRenewer(5*time.Millisecond, func(ctx context.Context) error {
		calls.Add(1)
		return errors.New("network blip")
	}, func() { lostCalls.Add(1) })

	r.Start()
	time.Sleep(40 * time.Millisecond) // several failed ticks
	if calls.Load() == 0 {
		t.Fatal("renew should have been attempted")
	}
	if lostCalls.Load() != 0 {
		t.Fatal("onLost must not fire for transient errors")
	}

	r.Stop() // must return: the loop is alive and stoppable
	select {
	case <-r.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("renewer loop did not exit after Stop")
	}
}

// TestRenewerStopIdempotent: repeated Stop calls neither panic nor hang, and
// Stop before Start is safe too.
func TestRenewerStopIdempotent(t *testing.T) {
	r := NewRenewer(5*time.Millisecond, func(ctx context.Context) error { return nil }, nil)
	r.Start()
	time.Sleep(15 * time.Millisecond)
	r.Stop()
	r.Stop() // second stop

	r2 := NewRenewer(5*time.Millisecond, func(ctx context.Context) error { return nil }, nil)
	r2.Stop()  // stop without start
	r2.Start() // a stopped renewer must not restart or close Done twice
	r2.Stop()
	select {
	case <-r2.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("loop never started but Done must still close")
	}
}

// TestRenewerNoGoroutineLeak: after Stop, the renewal goroutine is gone —
// a hard assertion for the library's "no background goroutine leaks" claim.
func TestRenewerNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		r := NewRenewer(time.Millisecond, func(ctx context.Context) error { return nil }, nil)
		r.Start()
		time.Sleep(3 * time.Millisecond)
		r.Stop()
	}
	// Give stragglers a moment; then the count must be back to baseline.
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Fatalf("goroutines after 20 renewers: %d > baseline %d (leak?)", got, before)
	}
}

func TestRenewerStopCancelsInFlightCall(t *testing.T) {
	entered := make(chan struct{})
	fallback := make(chan struct{})
	r := NewRenewer(time.Millisecond, func(ctx context.Context) error {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fallback:
			return ErrLost
		}
	}, nil)
	r.Start()
	<-entered
	stopped := make(chan struct{})
	go func() { r.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		close(fallback)
		<-stopped
		t.Fatal("Stop did not cancel the in-flight renewal")
	}
}
