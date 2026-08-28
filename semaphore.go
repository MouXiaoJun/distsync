package distsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lease"
	"github.com/MouXiaoJun/distsync/internal/redis"
)

// Semaphore is a distributed counting semaphore. At most capacity permits
// can be held at once, across all processes:
//
//	sem := client.Semaphore("openai:gpt5", 20) // max 20 concurrent AI calls
//	permit, err := sem.Acquire(ctx, 1)
//	if err != nil {
//	    return err
//	}
//	defer permit.Release(ctx)
//
// Permits expire (and are reclaimed atomically on the next acquire), so a
// crashed holder can never leak permits forever. Write your own Lua? No —
// that is this library's job.
type Semaphore struct {
	client   *Client
	name     string
	cfg      config
	key      string
	capacity int
}

// Semaphore creates a counting semaphore with the given capacity.
func (c *Client) Semaphore(name string, capacity int, opts ...Option) *Semaphore {
	if capacity < 1 {
		panic("distsync: semaphore capacity must be >= 1")
	}
	cfg := c.resolved(opts...)
	return &Semaphore{
		client:   c,
		name:     name,
		cfg:      cfg,
		key:      redisx.Key(name),
		capacity: capacity,
	}
}

// Name returns the resource name this semaphore guards.
func (s *Semaphore) Name() string { return s.name }

// Capacity returns the maximum number of concurrent permits.
func (s *Semaphore) Capacity() int { return s.capacity }

// Acquire takes n permits, blocking until enough capacity is available or
// ctx is canceled. The returned Permit must be released with permit.Release.
func (s *Semaphore) Acquire(ctx context.Context, n int) (p *Permit, err error) {
	ctxNonNil(ctx)
	if n < 1 {
		return nil, fmt.Errorf("distsync: semaphore %q: permits must be >= 1", s.name)
	}
	ctx, finish := s.client.tracer.Start(ctx, "distsync.semaphore.acquire")
	defer func() { finish(err) }()

	l := lease.NewPermitSet(s.client.rdb, s.key, s.cfg.ttl, s.capacity)
	start := time.Now()
	for attempt := 0; ; attempt++ {
		err := l.TryAcquire(ctx, n)
		if err == nil {
			p = s.permit(l, n)
			s.client.metrics.Acquire("semaphore", s.name, true, time.Since(start))
			return p, nil
		}
		if !errors.Is(err, lease.ErrBusy) {
			s.client.metrics.Acquire("semaphore", s.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: semaphore %q: %w", s.name, err)
		}
		if ctx.Err() != nil {
			s.client.metrics.Acquire("semaphore", s.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: semaphore %q: %w", s.name, ctx.Err())
		}
		select {
		case <-ctx.Done():
			s.client.metrics.Acquire("semaphore", s.name, false, time.Since(start))
			return nil, fmt.Errorf("distsync: semaphore %q: %w", s.name, ctx.Err())
		case <-time.After(s.cfg.retry.delay(attempt)):
		}
	}
}

// TryAcquire attempts to take n permits without blocking; ErrNotAcquired
// when the remaining capacity is insufficient.
func (s *Semaphore) TryAcquire(ctx context.Context, n int) (*Permit, error) {
	ctxNonNil(ctx)
	if n < 1 {
		return nil, fmt.Errorf("distsync: semaphore %q: permits must be >= 1", s.name)
	}
	l := lease.NewPermitSet(s.client.rdb, s.key, s.cfg.ttl, s.capacity)
	if err := l.TryAcquire(ctx, n); err != nil {
		if errors.Is(err, lease.ErrBusy) {
			s.client.metrics.Acquire("semaphore", s.name, false, 0)
			return nil, ErrNotAcquired
		}
		return nil, err
	}
	p := s.permit(l, n)
	s.client.metrics.Acquire("semaphore", s.name, true, 0)
	return p, nil
}

// Available returns the number of free permits right now. It is a
// best-effort stat: expired permits are purged atomically on the next
// Acquire, not on every read.
func (s *Semaphore) Available(ctx context.Context) (int, error) {
	used, err := s.client.rdb.ZCard(ctx, s.key).Result()
	if err != nil {
		return 0, err
	}
	free := s.capacity - int(used)
	if free < 0 {
		free = 0
	}
	return free, nil
}

func (s *Semaphore) permit(l *lease.PermitSet, n int) *Permit {
	p := &Permit{handle: newHandle(s.name, "semaphore", s.client.metrics, s.client.tracer, l), n: n}
	if s.cfg.autoRenew {
		p.startRenewal(s.cfg.ttl / 3)
	}
	return p
}

// Permit is the handle returned by Acquire/TryAcquire. It proves ownership
// of n permits.
type Permit struct {
	*handle
	n int
}

// Acquired returns how many permits this Permit holds.
func (p *Permit) Acquired() int { return p.n }

// ExpiresAt returns the current permit expiry (zero when not held).
func (p *Permit) ExpiresAt() time.Time { return p.leas.ExpiresAt() }

// Renew manually extends the permit TTL.
func (p *Permit) Renew(ctx context.Context) error {
	return p.renew(ctx)
}

// Release returns the permits and stops background renewal. Idempotent.
func (p *Permit) Release(ctx context.Context) error {
	return p.release(ctx)
}

// Context is canceled when the permit grant is lost (expired/stolen).
func (p *Permit) Context() context.Context { return p.lostCtx }

// Lost is closed when the permit grant is lost.
func (p *Permit) Lost() <-chan struct{} { return p.lostCtx.Done() }
