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
// Stop() is idempotent, blocks until the loop has fully exited, and closing
// the loop's own stop channel can never deadlock with the loop itself.
type Renewer struct {
	interval time.Duration
	renew    func(ctx context.Context) error
	onLost   func()

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewRenewer builds a renewer. onLost (may be nil) is invoked exactly when
// renewal fails because ownership was definitively lost (ErrLost); it must
// not block on the renewer itself.
func NewRenewer(interval time.Duration, renew func(context.Context) error, onLost func()) *Renewer {
	return &Renewer{
		interval: interval,
		renew:    renew,
		onLost:   onLost,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the renewal loop. Call it at most once.
func (r *Renewer) Start() {
	go r.loop()
}

// Done is closed once the loop has fully exited. Useful for tests that
// assert on goroutine cleanup.
func (r *Renewer) Done() <-chan struct{} { return r.done }

// Stop terminates the loop and blocks until it has exited. Idempotent.
func (r *Renewer) Stop() {
	r.once.Do(func() {
		close(r.stop)
		<-r.done
	})
}

func (r *Renewer) loop() {
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			err := r.renew(context.Background())
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
