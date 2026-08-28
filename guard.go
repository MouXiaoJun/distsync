package distsync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distsync/distsync/internal/lease"
)

// handle is the shared lifecycle of any acquired lease: ownership token,
// background renewal, idempotent release and a context that is canceled
// when ownership is lost. Guard (exclusive locks) and Permit (semaphore)
// both embed one.
type handle struct {
	resource  string
	primitive string
	metrics   Metrics
	tracer    Tracer
	leas      lease.Lease
	renewer   *lease.Renewer

	released atomic.Bool

	lostCtx    context.Context
	lostCancel context.CancelFunc
	stopOnce   sync.Once
}

func newHandle(resource, primitive string, m Metrics, t Tracer, l lease.Lease) *handle {
	ctx, cancel := context.WithCancel(context.Background())
	return &handle{
		resource:   resource,
		primitive:  primitive,
		metrics:    m,
		tracer:     t,
		leas:       l,
		lostCtx:    ctx,
		lostCancel: cancel,
	}
}

// startRenewal launches the heartbeat goroutine (interval = ttl/3 in
// practice). The goroutine is guaranteed to exit on release or on
// definitive ownership loss. Heartbeats share the exact code path of manual
// renewals (h.renew), so metrics and tracing stay consistent.
func (h *handle) startRenewal(interval time.Duration) {
	h.renewer = lease.NewRenewer(interval, h.renew, func() {
		h.metrics.RenewalStopped(h.primitive, h.resource, "lost")
		h.markLost()
	})
	h.renewer.Start()
}

// release performs the idempotent release: at most one caller actually
// releases; later calls return nil.
func (h *handle) release(ctx context.Context) (err error) {
	if !h.released.CompareAndSwap(false, true) {
		return nil
	}
	ctx, finish := h.tracer.Start(ctx, "distsync.release")
	defer func() { finish(err) }()

	h.stopOnce.Do(func() {
		if h.renewer != nil {
			h.metrics.RenewalStopped(h.primitive, h.resource, "released")
			h.renewer.Stop()
		}
	})
	err = h.leas.Release(ctx)
	h.metrics.Release(h.primitive, h.resource)
	h.markLost()
	return err
}

// renew refreshes the lease, either manually or from the heartbeat.
func (h *handle) renew(ctx context.Context) (err error) {
	if h.released.Load() {
		return ErrLost
	}
	ctx, finish := h.tracer.Start(ctx, "distsync.renew")
	defer func() { finish(err) }()

	err = h.leas.Renew(ctx)
	h.metrics.Renew(h.primitive, h.resource, err == nil)
	if err != nil && errors.Is(err, lease.ErrLost) {
		h.markLost()
	}
	return err
}

func (h *handle) markLost() {
	h.lostCancel()
}

// Guard is the handle returned by Lock/TryLock (and RLock/TryRLock). It
// proves ownership: it carries the owner token and, for exclusive locks,
// the fencing token.
type Guard struct {
	*handle
	fencing uint64
}

// FencingToken returns the strictly increasing fencing token minted for
// this acquisition. Use it to make side effects safe against a stale
// holder:
//
//	UPDATE orders SET status='paid', fencing_token=? WHERE id=? AND fencing_token < ?
//
// It is 0 for read locks, which never write.
func (g *Guard) FencingToken() uint64 { return g.fencing }

// Token returns the random owner token that proves this guard owns the
// lease. Mainly useful for debugging and auditing.
func (g *Guard) Token() string { return g.leas.ID() }

// ExpiresAt returns the current lease expiry (zero when not held).
func (g *Guard) ExpiresAt() time.Time { return g.leas.ExpiresAt() }

// Context is canceled when this guard loses ownership (lease lost or
// released), letting a critical section observe that it is no longer safe
// to continue.
func (g *Guard) Context() context.Context { return g.lostCtx }

// Lost is closed when the guard loses ownership.
func (g *Guard) Lost() <-chan struct{} { return g.lostCtx.Done() }

// Renew manually extends the lease (TTL resets from now). Returns ErrLost
// when ownership is gone.
func (g *Guard) Renew(ctx context.Context) error {
	return g.renew(ctx)
}

// Unlock releases the lease and stops background renewal. It is idempotent:
// the first call releases, subsequent calls return nil. It returns ErrLost
// when the lease had already expired before the unlock.
func (g *Guard) Unlock(ctx context.Context) error {
	return g.release(ctx)
}

func ctxNonNil(ctx context.Context) {
	if ctx == nil {
		panic("distsync: nil context")
	}
}
