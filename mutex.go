package distsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/distsync/distsync/internal/lease"
	"github.com/distsync/distsync/internal/redis"
)

// Mutex is a distributed mutual-exclusion lock with lease-based ownership,
// fencing tokens and (optionally) automatic renewal. Use it anywhere you
// would use sync.Mutex, but across processes:
//
//	mu := client.Mutex("order:10001")
//	guard, err := mu.Lock(ctx)
//	if err != nil {
//	    return err
//	}
//	defer guard.Unlock(ctx)
//
// The name is normalized into a Redis Cluster-safe hash tag automatically,
// so every derived key of this mutex lives on the same slot.
type Mutex struct {
	client *Client
	name   string
	cfg    config
	key    string
	fkey   string

	holderMu sync.Mutex
	holder   *Guard
}

// Mutex creates a Mutex for the named resource.
func (c *Client) Mutex(name string, opts ...Option) *Mutex {
	cfg := c.resolved(opts...)
	key := redisx.Key(name)
	return &Mutex{
		client: c,
		name:   name,
		cfg:    cfg,
		key:    key,
		fkey:   redisx.Derived(key, "fencing"),
	}
}

// Name returns the resource name this mutex guards.
func (m *Mutex) Name() string { return m.name }

// newLease returns a fresh lease instance. A fresh owner token per Lock
// call is what makes unlock ownership-safe: a stale holder can never
// release a newer acquisition.
func (m *Mutex) newLease() *lease.SingleOwner {
	return lease.NewSingleOwner(m.client.rdb, m.key, m.fkey, m.cfg.ttl, true)
}

// Lock acquires the lock, retrying with exponential backoff until it
// succeeds or ctx is canceled. The returned Guard must be released with
// guard.Unlock(ctx); the context passed to Lock only governs acquisition —
// canceling it after Lock returns does not release the lock.
func (m *Mutex) Lock(ctx context.Context) (g *Guard, err error) {
	ctxNonNil(ctx)
	ctx, finish := m.client.tracer.Start(ctx, "distsync.mutex.lock")
	defer func() { finish(err) }()

	l := m.newLease()
	start := time.Now()
	for attempt := 0; ; attempt++ {
		fence, err := l.TryAcquire(ctx)
		if err == nil {
			g = m.guard(l, fence)
			m.setHolder(g)
			m.client.metrics.Acquire("mutex", m.name, true, time.Since(start))
			return g, nil
		}
		if !errors.Is(err, lease.ErrBusy) {
			m.client.metrics.Acquire("mutex", m.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: lock %q: %w", m.name, err)
		}
		if ctx.Err() != nil {
			m.client.metrics.Acquire("mutex", m.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: lock %q: %w", m.name, ctx.Err())
		}
		select {
		case <-ctx.Done():
			m.client.metrics.Acquire("mutex", m.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: lock %q: %w", m.name, ctx.Err())
		case <-time.After(m.cfg.retry.delay(attempt)):
		}
	}
}

// TryLock attempts a single acquisition without blocking. It returns
// ErrNotAcquired when the lock is held by someone else.
func (m *Mutex) TryLock(ctx context.Context) (*Guard, error) {
	ctxNonNil(ctx)
	l := m.newLease()
	fence, err := l.TryAcquire(ctx)
	if err != nil {
		if errors.Is(err, lease.ErrBusy) {
			m.client.metrics.Acquire("mutex", m.name, false, 0)
			return nil, ErrNotAcquired
		}
		return nil, err
	}
	g := m.guard(l, fence)
	m.setHolder(g)
	m.client.metrics.Acquire("mutex", m.name, true, 0)
	return g, nil
}

// Unlock releases the most recently acquired lock on this Mutex instance —
// a convenience mirroring `defer mu.Unlock(ctx)`. The guard-based API
// (guard.Unlock) is preferred and safe under concurrency; this one is
// intended for the common one-lock-per-instance pattern.
func (m *Mutex) Unlock(ctx context.Context) error {
	m.holderMu.Lock()
	g := m.holder
	m.holder = nil
	m.holderMu.Unlock()
	if g == nil {
		return nil
	}
	return g.Unlock(ctx)
}

func (m *Mutex) guard(l *lease.SingleOwner, fence uint64) *Guard {
	g := &Guard{handle: newHandle(m.name, "mutex", m.client.metrics, m.client.tracer, l), fencing: fence}
	if m.cfg.autoRenew {
		g.startRenewal(m.cfg.ttl / 3)
	}
	return g
}

func (m *Mutex) setHolder(g *Guard) {
	m.holderMu.Lock()
	m.holder = g
	m.holderMu.Unlock()
}
