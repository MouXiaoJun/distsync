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

// RWMutex is a distributed read-write lock. Any number of readers may hold
// the lock concurrently, but never together with a writer, and a queued
// writer takes priority over new readers (writer preference):
//
//	mu := client.RWMutex("config:tenant:1001")
//
//	// readers
//	rguard, err := mu.RLock(ctx)
//	config := load()
//	rguard.Unlock(ctx)
//
//	// writer
//	wguard, err := mu.Lock(ctx)
//	update()
//	wguard.Unlock(ctx)
//
// Write guards carry a fencing token; read guards do not (they never write).
type RWMutex struct {
	client *Client
	name   string
	cfg    config

	writerKey  string
	readersKey string
	fencingKey string
	waitingKey string

	holderMu    sync.Mutex
	writeHolder *Guard
	readHolder  *Guard
}

// RWMutex creates a read-write lock for the named resource.
func (c *Client) RWMutex(name string, opts ...Option) *RWMutex {
	cfg := c.resolved(opts...)
	key := redisx.Key(name)
	return &RWMutex{
		client:     c,
		name:       name,
		cfg:        cfg,
		writerKey:  redisx.Derived(key, "writer"),
		readersKey: redisx.Derived(key, "readers"),
		fencingKey: redisx.Derived(key, "fencing"),
		waitingKey: redisx.Derived(key, "writer-waiting"),
	}
}

// Name returns the resource name this lock guards.
func (rw *RWMutex) Name() string { return rw.name }

// Lock acquires the write side, retrying until it succeeds or ctx is
// canceled.
func (rw *RWMutex) Lock(ctx context.Context) (g *Guard, err error) {
	ctxNonNil(ctx)
	ctx, finish := rw.client.tracer.Start(ctx, "distsync.rwmutex.lock")
	defer func() { finish(err) }()

	l := lease.NewRWWriter(rw.client.rdb, rw.writerKey, rw.readersKey, rw.fencingKey, rw.waitingKey, rw.cfg.ttl)
	start := time.Now()
	for attempt := 0; ; attempt++ {
		fence, err := l.TryAcquire(ctx)
		if err == nil {
			g = rw.guard(l, fence)
			rw.setWriteHolder(g)
			rw.client.metrics.Acquire("rwmutex", rw.name, true, time.Since(start))
			return g, nil
		}
		if !errors.Is(err, lease.ErrBusy) {
			rw.client.metrics.Acquire("rwmutex", rw.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: write lock %q: %w", rw.name, err)
		}
		if ctx.Err() != nil {
			rw.client.metrics.Acquire("rwmutex", rw.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: write lock %q: %w", rw.name, ctx.Err())
		}
		select {
		case <-ctx.Done():
			rw.client.metrics.Acquire("rwmutex", rw.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: write lock %q: %w", rw.name, ctx.Err())
		case <-time.After(rw.cfg.retry.delay(attempt)):
		}
	}
}

// TryLock attempts a single write acquisition; ErrNotAcquired when a writer
// or any reader holds the lock.
func (rw *RWMutex) TryLock(ctx context.Context) (*Guard, error) {
	ctxNonNil(ctx)
	l := lease.NewRWWriter(rw.client.rdb, rw.writerKey, rw.readersKey, rw.fencingKey, rw.waitingKey, rw.cfg.ttl)
	fence, err := l.TryAcquire(ctx)
	if err != nil {
		if errors.Is(err, lease.ErrBusy) {
			rw.client.metrics.Acquire("rwmutex", rw.name, false, 0)
			return nil, ErrNotAcquired
		}
		return nil, err
	}
	g := rw.guard(l, fence)
	rw.setWriteHolder(g)
	rw.client.metrics.Acquire("rwmutex", rw.name, true, 0)
	return g, nil
}

// RLock acquires the read side, retrying until it succeeds or ctx is
// canceled. Read guards carry fencing token 0.
func (rw *RWMutex) RLock(ctx context.Context) (g *Guard, err error) {
	ctxNonNil(ctx)
	ctx, finish := rw.client.tracer.Start(ctx, "distsync.rwmutex.rlock")
	defer func() { finish(err) }()

	l := lease.NewRWReader(rw.client.rdb, rw.writerKey, rw.readersKey, rw.waitingKey, rw.cfg.ttl)
	start := time.Now()
	for attempt := 0; ; attempt++ {
		err := l.Acquire(ctx)
		if err == nil {
			g = rw.guard(l, 0)
			rw.setReadHolder(g)
			rw.client.metrics.Acquire("rwmutex-read", rw.name, true, time.Since(start))
			return g, nil
		}
		if !errors.Is(err, lease.ErrBusy) {
			rw.client.metrics.Acquire("rwmutex-read", rw.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: read lock %q: %w", rw.name, err)
		}
		if ctx.Err() != nil {
			rw.client.metrics.Acquire("rwmutex-read", rw.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: read lock %q: %w", rw.name, ctx.Err())
		}
		select {
		case <-ctx.Done():
			rw.client.metrics.Acquire("rwmutex-read", rw.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: read lock %q: %w", rw.name, ctx.Err())
		case <-time.After(rw.cfg.retry.delay(attempt)):
		}
	}
}

// TryRLock attempts a single read acquisition; ErrNotAcquired when a writer
// holds or is queued for the lock.
func (rw *RWMutex) TryRLock(ctx context.Context) (*Guard, error) {
	ctxNonNil(ctx)
	l := lease.NewRWReader(rw.client.rdb, rw.writerKey, rw.readersKey, rw.waitingKey, rw.cfg.ttl)
	err := l.Acquire(ctx)
	if err != nil {
		if errors.Is(err, lease.ErrBusy) {
			rw.client.metrics.Acquire("rwmutex-read", rw.name, false, 0)
			return nil, ErrNotAcquired
		}
		return nil, err
	}
	g := rw.guard(l, 0)
	rw.setReadHolder(g)
	rw.client.metrics.Acquire("rwmutex-read", rw.name, true, 0)
	return g, nil
}

// Unlock releases the most recently acquired write lock (convenience; the
// guard-based API is preferred).
func (rw *RWMutex) Unlock(ctx context.Context) error {
	rw.holderMu.Lock()
	g := rw.writeHolder
	rw.writeHolder = nil
	rw.holderMu.Unlock()
	if g == nil {
		return nil
	}
	return g.Unlock(ctx)
}

// RUnlock releases the most recently acquired read lock (convenience; the
// guard-based API is preferred). With concurrent readers, keep the guard
// returned by RLock and call guard.Unlock instead.
func (rw *RWMutex) RUnlock(ctx context.Context) error {
	rw.holderMu.Lock()
	g := rw.readHolder
	rw.readHolder = nil
	rw.holderMu.Unlock()
	if g == nil {
		return nil
	}
	return g.Unlock(ctx)
}

func (rw *RWMutex) guard(l lease.Lease, fence uint64) *Guard {
	g := &Guard{handle: newHandle(rw.name, "rwmutex", rw.client.metrics, rw.client.tracer, l), fencing: fence}
	if rw.cfg.autoRenew {
		g.startRenewal(renewalInterval(rw.cfg.ttl))
	} else if rw.cfg.watchdog {
		g.startWatchdog(renewalInterval(rw.cfg.ttl))
	}
	return g
}

func (rw *RWMutex) setWriteHolder(g *Guard) {
	rw.holderMu.Lock()
	rw.writeHolder = g
	rw.holderMu.Unlock()
}

func (rw *RWMutex) setReadHolder(g *Guard) {
	rw.holderMu.Lock()
	rw.readHolder = g
	rw.holderMu.Unlock()
}
