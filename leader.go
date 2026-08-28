package distsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distsync/distsync/internal/lease"
	"github.com/distsync/distsync/internal/redis"
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
	client *Client
	name   string
	cfg    config
	key    string
	fkey   string

	stateMu sync.Mutex
	active  bool
}

// Leader creates a leader-election handle for the named role.
func (c *Client) Leader(name string, opts ...Option) *Leader {
	cfg := c.resolved(opts...)
	key := redisx.Key(name)
	return &Leader{
		client: c,
		name:   name,
		cfg:    cfg,
		key:    key,
		fkey:   redisx.Derived(key, "fencing"),
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
	le := lease.NewSingleOwner(l.client.rdb, l.key, l.fkey, l.cfg.ttl, false)

	start := time.Now()
	for attempt := 0; ; attempt++ {
		err := le.Acquire(ctx)
		if err == nil {
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
	return l.runHeld(ctx, le, fn)
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
	le := lease.NewSingleOwner(l.client.rdb, l.key, l.fkey, l.cfg.ttl, false)
	if err := le.Acquire(ctx); err != nil {
		if errors.Is(err, lease.ErrBusy) {
			return ErrNotAcquired
		}
		return err
	}
	l.client.metrics.Acquire("leader", l.name, true, 0)
	return l.runHeld(ctx, le, fn)
}

// IsLeader reports whether this handle currently holds the leader lease
// (only meaningful from within Run/TryRun).
func (l *Leader) IsLeader() bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	return l.active
}

// runHeld executes fn while this handle provably holds the leader lease.
func (l *Leader) runHeld(ctx context.Context, le *lease.SingleOwner, fn func(context.Context) error) error {
	l.stateMu.Lock()
	l.active = true
	l.stateMu.Unlock()
	defer func() {
		l.stateMu.Lock()
		l.active = false
		l.stateMu.Unlock()
	}()

	// LIFO order on return: cancel callback ctx first, stop renewal, then
	// release the lease (so a queued replica can take over promptly).
	leadCtx, leadCancel := context.WithCancel(ctx)
	defer leadCancel()

	var lost atomic.Bool
	if l.cfg.autoRenew {
		r := lease.NewRenewer(l.cfg.ttl/3, func(rctx context.Context) error {
			return le.Renew(rctx)
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
