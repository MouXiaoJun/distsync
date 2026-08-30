package distsync

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lease"
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

	released    atomic.Bool
	releaseMu   sync.Mutex
	releaseDone bool

	lostCtx       context.Context
	lostCancel    context.CancelFunc
	stopOnce      sync.Once
	timerMu       sync.Mutex
	deadlineTimer *time.Timer
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
	h.checkDeadline()
	h.renewer.Start()
}

// startWatchdog launches a non-extending ownership check (for NoAutoRenew
// guards): it fires Lost()/Context() when the lease expires server-side,
// without keeping it alive. Transient Redis errors are retried, not fatal.
func (h *handle) startWatchdog(interval time.Duration) {
	h.renewer = lease.NewRenewer(interval, func(ctx context.Context) error {
		return h.withLeaseDeadline(ctx, func(ctx context.Context) error {
			held, err := h.leas.Held(ctx)
			if err != nil {
				return err
			}
			if !held {
				return lease.ErrLost
			}
			return nil
		})
	}, func() {
		h.metrics.RenewalStopped(h.primitive, h.resource, "lost")
		h.markLost()
	})
	h.checkDeadline()
	h.renewer.Start()
}

// checkDeadline runs independently of Redis calls and heartbeat intervals.
// A successful renewal moves ExpiresAt; the old timer then follows that deadline.
func (h *handle) checkDeadline() {
	h.timerMu.Lock()
	defer h.timerMu.Unlock()
	if h.lostCtx.Err() != nil {
		return
	}
	remaining := time.Until(h.leas.ExpiresAt())
	if remaining <= 0 {
		h.lostCancel()
		h.deadlineTimer = nil
		return
	}
	h.deadlineTimer = time.AfterFunc(remaining, h.checkDeadline)
}

// release stops local use immediately. An uncertain remote release may be
// retried with the same owner token; completed releases remain idempotent.
func (h *handle) release(ctx context.Context) (err error) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	if h.releaseDone {
		return nil
	}
	h.stop()
	ctx, finish := h.tracer.Start(ctx, "distsync.release")
	defer func() { finish(err) }()

	err = h.leas.Release(ctx)
	h.metrics.Release(h.primitive, h.resource)
	h.releaseDone = err == nil || errors.Is(err, lease.ErrLost)
	return err
}

// stop ends local ownership and joins background work before remote cleanup.
func (h *handle) stop() {
	h.released.Store(true)
	h.markLost()
	h.stopOnce.Do(func() {
		if h.renewer != nil {
			h.metrics.RenewalStopped(h.primitive, h.resource, "released")
			h.renewer.Stop()
		}
	})
}

// renew refreshes the lease, either manually or from the heartbeat.
func (h *handle) renew(ctx context.Context) (err error) {
	if h.released.Load() || h.lostCtx.Err() != nil {
		return ErrLost
	}
	ctx, finish := h.tracer.Start(ctx, "distsync.renew")
	defer func() { finish(err) }()

	err = h.withLeaseDeadline(ctx, h.leas.Renew)
	h.metrics.Renew(h.primitive, h.resource, err == nil)
	if err != nil && errors.Is(err, lease.ErrLost) {
		h.markLost()
	}
	return err
}

// withLeaseDeadline bounds an ownership check by the last confirmed local
// deadline. The timer notifies the holder even if Redis ignores cancellation;
// Stop still waits for that client's I/O to return.
func (h *handle) withLeaseDeadline(ctx context.Context, call func(context.Context) error) error {
	if h.released.Load() || h.lostCtx.Err() != nil {
		return ErrLost
	}
	deadline := h.leas.ExpiresAt()
	if !deadline.After(time.Now()) {
		h.markLost()
		return ErrLost
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	stop := context.AfterFunc(callCtx, func() {
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) && !h.leas.ExpiresAt().After(time.Now()) {
			h.markLost()
		}
	})
	defer stop()
	err := call(callCtx)
	if h.lostCtx.Err() != nil || errors.Is(err, ErrLost) || !h.leas.ExpiresAt().After(time.Now()) {
		h.markLost()
		return errors.Join(ErrLost, err)
	}
	return err
}

func (h *handle) markLost() {
	h.lostCancel()
	h.timerMu.Lock()
	defer h.timerMu.Unlock()
	if h.deadlineTimer != nil {
		h.deadlineTimer.Stop()
		h.deadlineTimer = nil
	}
}

// Guard is the handle returned by Lock/TryLock (and RLock/TryRLock). It
// proves ownership: it carries the owner token and, for exclusive locks,
// the fencing token.
type Guard struct {
	*handle
	fencing uint64
}

// FencingToken returns the token minted for this acquisition. Ordering requires
// that its Redis counter never rolls back; the destination must atomically
// enforce it with each side effect (see docs/semantics.md):
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
// completed releases return nil on subsequent calls. Transport failures may
// be retried. It returns ErrLost when the lease was already gone.
func (g *Guard) Unlock(ctx context.Context) error {
	return g.release(ctx)
}

func ctxNonNil(ctx context.Context) {
	if ctx == nil {
		panic("distsync: nil context")
	}
}

// renewalInterval returns ttl/3 with ±20% jitter. A fleet of holders that
// acquired at the same moment would otherwise renew on aligned ticks and
// thundering-herd Redis; the jitter spreads the heartbeats.
func renewalInterval(ttl time.Duration) time.Duration {
	base := ttl / 3
	if base <= 0 {
		base = time.Second
	}
	jitter := 0.8 + 0.4*rand.Float64()
	return time.Duration(float64(base) * jitter)
}
