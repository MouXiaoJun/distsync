package lease

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
	"github.com/redis/go-redis/v9"
)

// PermitSet is a lease that allows up to capacity concurrent holders, each
// holding one or more permits. It backs the Semaphore primitive and (with a
// different acquire script) RWMutex readers.
//
// Permits live in a Redis sorted set: member = random permit token,
// score = expiry timestamp. Expired permits are reclaimed atomically on
// every acquisition, so a crashed holder can never leak permits forever.
type PermitSet struct {
	id       string
	key      string
	rdb      redis.Cmdable
	ttl      time.Duration
	capacity int

	mu        sync.Mutex
	tokens    []string
	acquired  bool
	expiresAt time.Time
}

// NewPermitSet builds a multi-permit lease on the sorted set key.
func NewPermitSet(rdb redis.Cmdable, key string, ttl time.Duration, capacity int) *PermitSet {
	return &PermitSet{
		id:       Token(),
		key:      key,
		rdb:      rdb,
		ttl:      ttl,
		capacity: capacity,
	}
}

// ID implements Lease.
func (p *PermitSet) ID() string { return p.id }

// Acquire implements Lease: a single-permit acquisition.
func (p *PermitSet) Acquire(ctx context.Context) error {
	return p.TryAcquire(ctx, 1)
}

// TryAcquire attempts to take n permits in one atomic step. It never blocks
// and returns ErrBusy when the remaining capacity is insufficient.
func (p *PermitSet) TryAcquire(ctx context.Context, n int) error {
	if n < 1 {
		return nil
	}
	tokens := make([]string, n)
	for i := 0; i < n; i++ {
		tokens[i] = p.id + "#" + strconv.Itoa(i)
	}
	args := make([]any, 0, 3+len(tokens))
	args = append(args, time.Now().UnixMilli(), p.ttl.Milliseconds(), p.capacity)
	for _, t := range tokens {
		args = append(args, t)
	}
	if _, err := lua.SemAcquire.Run(ctx, p.rdb, []string{p.key}, args...).Int64(); err != nil {
		if err == redis.Nil {
			return ErrBusy
		}
		return err
	}

	p.mu.Lock()
	p.tokens = tokens
	p.acquired = true
	p.expiresAt = time.Now().Add(p.ttl)
	p.mu.Unlock()
	return nil
}

// Renew implements Lease. Ownership of every permit is checked first, so a
// partially-expired grant is treated as fully lost.
func (p *PermitSet) Renew(ctx context.Context) error {
	p.mu.Lock()
	tokens := append([]string(nil), p.tokens...)
	p.mu.Unlock()
	if len(tokens) == 0 {
		return ErrLost
	}

	args := make([]any, 0, 2+len(tokens))
	args = append(args, time.Now().UnixMilli(), p.ttl.Milliseconds())
	for _, t := range tokens {
		args = append(args, t)
	}
	ok, err := lua.SemRenew.Run(ctx, p.rdb, []string{p.key}, args...).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	p.mu.Lock()
	p.expiresAt = time.Now().Add(p.ttl)
	p.mu.Unlock()
	return nil
}

// Release implements Lease. A holder can only ever remove its own random
// permit tokens.
func (p *PermitSet) Release(ctx context.Context) error {
	p.mu.Lock()
	tokens := p.tokens
	p.tokens = nil
	p.acquired = false
	p.expiresAt = time.Time{}
	p.mu.Unlock()
	if len(tokens) == 0 {
		return ErrLost
	}

	args := make([]any, 0, len(tokens))
	for _, t := range tokens {
		args = append(args, t)
	}
	removed, err := lua.SemRelease.Run(ctx, p.rdb, []string{p.key}, args...).Int64()
	if err != nil {
		return err
	}
	if removed != int64(len(tokens)) {
		return ErrLost
	}
	return nil
}

// ExpiresAt implements Lease.
func (p *PermitSet) ExpiresAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expiresAt
}

// Acquired reports whether this grant is currently marked as acquired
// locally (it may still have expired server-side).
func (p *PermitSet) Acquired() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquired
}
