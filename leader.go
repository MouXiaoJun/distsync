package distsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lease"
	"github.com/MouXiaoJun/distsync/internal/redis"
)

// Leader elects a single leader among N replicas for a named role, using an
// exclusive lease with automatic renewal. When the leader dies or loses its
// lease, another replica takes over:
//
//	leader := client.Leader("scheduler")
//	if err := leader.Run(ctx, func(ctx context.Context) error {
//	    return scheduler.Start(ctx) // cron, reconciliation, data sync, ...
//	}); err != nil {
//	    return err
//	}
//
// The callback receives a context that is canceled when leadership is lost
// (failover) or the parent context is canceled, so the leader can shut its
// work down gracefully.
type Leader struct {
	client  *Client
	name    string
	cfg     config
	key     string
	fkey    string
	fencing bool

	stateMu sync.Mutex
	active  *handle
	fence   uint64
}

// Leader creates a leader-election handle for the named role.
func (c *Client) Leader(name string, opts ...Option) *Leader {
	cfg := c.resolved(opts...)
	key := redisx.Key(name)
	return &Leader{
		client:  c,
		name:    name,
		cfg:     cfg,
		key:     key,
		fkey:    redisx.Derived(key, "fencing"),
		fencing: cfg.fencing,
	}
}

// Name returns the role name this leader guards.
func (l *Leader) Name() string { return l.name }

// Run acquires leadership (retrying until ctx is canceled), then executes
// fn while leader. Run returns fn's error; if the lease was lost before fn
// completed, its error also matches ErrLeadershipLost. Cleanup errors are
// preserved as well; callbacks must observe cancellation.
func (l *Leader) Run(ctx context.Context, fn func(context.Context) error) (err error) {
	ctxNonNil(ctx)
	ctx, finish := l.client.tracer.Start(ctx, "distsync.leader.run")
	defer func() { finish(err) }()

	if fn == nil {
		return fmt.Errorf("distsync: leader %q: nil callback", l.name)
	}
	le := lease.NewSingleOwner(l.client.rdb, l.key, l.fkey, l.cfg.ttl, l.fencing)

	start := time.Now()
	var fence uint64
	for attempt := 0; ; attempt++ {
		f, err := le.TryAcquire(ctx)
		if err == nil {
			fence = f
			break
		}
		if !errors.Is(err, lease.ErrBusy) {
			return fmt.Errorf("distsync: leader %q: %w", l.name, err)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("distsync: leader %q: %w", l.name, ctx.Err())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("distsync: leader %q: %w", l.name, ctx.Err())
		case <-time.After(l.cfg.retry.delay(attempt)):
		}
	}
	l.client.metrics.Acquire("leader", l.name, true, time.Since(start))
	return l.runHeld(ctx, le, fence, fn)
}

// TryRun attempts to become leader without blocking. It returns
// ErrNotAcquired when another replica is currently the leader.
func (l *Leader) TryRun(ctx context.Context, fn func(context.Context) error) (err error) {
	ctxNonNil(ctx)
	ctx, finish := l.client.tracer.Start(ctx, "distsync.leader.tryrun")
	defer func() { finish(err) }()

	if fn == nil {
		return fmt.Errorf("distsync: leader %q: nil callback", l.name)
	}
	le := lease.NewSingleOwner(l.client.rdb, l.key, l.fkey, l.cfg.ttl, l.fencing)
	fence, err := le.TryAcquire(ctx)
	if err != nil {
		if errors.Is(err, lease.ErrBusy) {
			return ErrNotAcquired
		}
		return err
	}
	l.client.metrics.Acquire("leader", l.name, true, 0)
	return l.runHeld(ctx, le, fence, fn)
}

// IsLeader reports whether this handle currently holds the leader lease
// (only meaningful from within Run/TryRun).
func (l *Leader) IsLeader() bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	return l.active != nil && l.active.lostCtx.Err() == nil
}

// FencingToken returns the fencing token minted for the current leadership
// (0 when fencing is disabled, or when this handle is not the leader).
// Tokens increase per role while its Redis counter is preserved without
// rollback. The destination must enforce them atomically (see docs/semantics.md):
//
//	UPDATE settlements SET ledger_id=?, fencing_token=? WHERE id=? AND fencing_token < ?
func (l *Leader) FencingToken() uint64 {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.active == nil || l.active.lostCtx.Err() != nil {
		return 0
	}
	return l.fence
}

// runHeld executes fn while this handle provably holds the leader lease.
func (l *Leader) runHeld(ctx context.Context, le *lease.SingleOwner, fence uint64, fn func(context.Context) error) (err error) {
	h := newHandle(l.name, "leader", l.client.metrics, l.client.tracer, le)
	l.stateMu.Lock()
	l.active = h
	l.fence = fence
	l.stateMu.Unlock()
	defer func() {
		l.stateMu.Lock()
		if l.active == h {
			l.active = nil
			l.fence = 0
		}
		l.stateMu.Unlock()
	}()

	leadCtx, leadCancel := context.WithCancelCause(ctx)
	stopLost := context.AfterFunc(h.lostCtx, func() { leadCancel(ErrLeadershipLost) })
	if l.cfg.autoRenew {
		h.startRenewal(renewalInterval(l.cfg.ttl))
	} else if l.cfg.watchdog {
		h.startWatchdog(renewalInterval(l.cfg.ttl))
	}
	defer func() {
		stopLost()
		leadCancel(nil)
		h.stop()
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if releaseErr := h.release(rctx); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}()

	err = fn(leadCtx)
	if h.lostCtx.Err() != nil {
		return errors.Join(err, ErrLeadershipLost)
	}
	return err
}
