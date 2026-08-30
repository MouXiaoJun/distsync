package lease

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Renewer drives periodic Renew calls in a single background goroutine. It
// is the one place automatic renewal lives, so every primitive shares the
// same heartbeat behavior — and the same no-goroutine-leak guarantee:
// Stop() is idempotent, safe to call in any state (even before Start), and
// blocks until the loop has fully exited. Closing the loop's own stop
// channel can never deadlock with the loop itself.
type Renewer struct {
	interval time.Duration
	renew    func(ctx context.Context) error
	onLost   func()

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	startOnce sync.Once
}

// NewRenewer builds a renewer. onLost (may be nil) is invoked exactly when
// renewal fails because ownership was definitively lost (ErrLost); it must
// not block on the renewer itself.
func NewRenewer(interval time.Duration, renew func(context.Context) error, onLost func()) *Renewer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Renewer{
		interval: interval,
		renew:    renew,
		onLost:   onLost,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

// Start launches the renewal loop. Call it at most once; later calls are
// no-ops.
func (r *Renewer) Start() {
	r.startOnce.Do(func() {
		go r.loop()
	})
}

// Done is closed once the loop has fully exited (or was never started).
// Useful for tests that assert on goroutine cleanup.
func (r *Renewer) Done() <-chan struct{} { return r.done }

// Stop terminates the loop and blocks until it has exited. Idempotent and
// safe to call before Start (the loop simply never runs).
func (r *Renewer) Stop() {
	r.cancel()
	// Exactly one of Start or Stop owns closing done. Starting after Stop
	// becomes a no-op, including when those calls race.
	r.startOnce.Do(func() { close(r.done) })
	<-r.done
}

func (r *Renewer) loop() {
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.ctx.Err() != nil {
				return
			}
			err := r.renew(r.ctx)
			if r.ctx.Err() != nil {
				return
			}
			if err == nil {
				continue
			}
			// A definitive ownership loss stops the loop and notifies the
			// holder so it can cancel its critical section.
			if errors.Is(err, ErrLost) {
				if r.onLost != nil {
					r.onLost()
				}
				return
			}
			// Transient Redis errors are retried on the next tick. Giving
			// up on a network blip would drop the lease while the holder
			// still believes it owns it, which is strictly worse.
		}
	}
}
