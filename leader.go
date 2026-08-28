package distsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	active  bool
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
// completed, it returns ErrLeadershipLost.
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
	return l.active
}

// FencingToken returns the fencing token minted for the current leadership
// (0 when fencing is disabled, or when this handle is not the leader).
// Tokens are strictly increasing per role across all replicas and across
// leadership changes, so the leader can fence its side effects exactly like
// a mutex holder:
//
//	UPDATE settlements SET ledger_id=?, fencing_token=? WHERE id=? AND fencing_token < ?
func (l *Leader) FencingToken() uint64 {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	return l.fence
}

// runHeld executes fn while this handle provably holds the leader lease.
func (l *Leader) runHeld(ctx context.Context, le *lease.SingleOwner, fence uint64, fn func(context.Context) error) error {
	l.stateMu.Lock()
	l.active = true
	l.fence = fence
	l.stateMu.Unlock()
	defer func() {
		l.stateMu.Lock()
		l.active = false
		l.fence = 0
		l.stateMu.Unlock()
	}()

	// LIFO order on return: cancel callback ctx first, stop renewal, then
	// release the lease (so a queued replica can take over promptly).
	leadCtx, leadCancel := context.WithCancel(ctx)
	defer leadCancel()

	var lost atomic.Bool
	if l.cfg.autoRenew {
		r := lease.NewRenewer(renewalInterval(l.cfg.ttl), func(rctx context.Context) error {
			return le.Renew(rctx)
		}, func() {
			lost.Store(true)
			leadCancel()
		})
		r.Start()
		defer r.Stop()
	} else if l.cfg.watchdog {
		// NoAutoRenew + Watchdog: detect lease expiry without extending it,
		// so a non-renewing leader still fails over promptly.
		r := lease.NewRenewer(renewalInterval(l.cfg.ttl), func(rctx context.Context) error {
			held, err := le.Held(rctx)
			if err != nil {
				return err
			}
			if !held {
				return lease.ErrLost
			}
			return nil
		}, func() {
			lost.Store(true)
			leadCancel()
		})
		r.Start()
		defer r.Stop()
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = le.Release(rctx)
	}()

	err := fn(leadCtx)
	if err == nil && lost.Load() {
		return ErrLeadershipLost
	}
	return err
}
