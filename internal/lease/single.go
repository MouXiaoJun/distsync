package lease

import (
	"context"
	"sync"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
	"github.com/redis/go-redis/v9"
)

// SingleOwner is a lease held by at most one owner, protected by a random
// owner token and a compare-and-set release. It backs Mutex, RWMutex
// writers and Leader election.
type SingleOwner struct {
	id      string
	key     string
	fkey    string // fencing counter key, same hash slot
	rdb     redis.Cmdable
	ttl     time.Duration
	fencing bool

	mu        sync.Mutex
	expiresAt time.Time
}

// NewSingleOwner builds an exclusive lease. key and fencingKey must share a
// Redis Cluster hash slot (redisx.Key / redisx.Derived guarantee this).
func NewSingleOwner(rdb redis.Cmdable, key, fencingKey string, ttl time.Duration, fencing bool) *SingleOwner {
	return &SingleOwner{
		id:      Token(),
		key:     key,
		fkey:    fencingKey,
		rdb:     rdb,
		ttl:     ttl,
		fencing: fencing,
	}
}

// ID implements Lease.
func (l *SingleOwner) ID() string { return l.id }

// Acquire implements Lease: a single, non-blocking attempt.
func (l *SingleOwner) Acquire(ctx context.Context) error {
	_, err := l.TryAcquire(ctx)
	return err
}

// TryAcquire attempts a single acquisition and returns the fencing token
// on success (0 when fencing is disabled).
func (l *SingleOwner) TryAcquire(ctx context.Context) (uint64, error) {
	fence := "0"
	if l.fencing {
		fence = "1"
	}
	res, err := lua.SingleAcquire.Run(
		ctx, l.rdb,
		[]string{l.key, l.fkey},
		l.id, l.ttl.Milliseconds(), fence,
	).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrBusy
		}
		return 0, err
	}

	l.mu.Lock()
	l.expiresAt = time.Now().Add(l.ttl)
	l.mu.Unlock()
	return uint64(res), nil
}

// Renew implements Lease.
func (l *SingleOwner) Renew(ctx context.Context) error {
	ok, err := lua.SingleRenew.Run(
		ctx, l.rdb,
		[]string{l.key},
		l.id, l.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	l.mu.Lock()
	l.expiresAt = time.Now().Add(l.ttl)
	l.mu.Unlock()
	return nil
}

// Release implements Lease. It is a compare-and-delete: a stale holder can
// never release a newer owner's lease.
func (l *SingleOwner) Release(ctx context.Context) error {
	ok, err := lua.SingleRelease.Run(ctx, l.rdb, []string{l.key}, l.id).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	l.mu.Lock()
	l.expiresAt = time.Time{}
	l.mu.Unlock()
	return nil
}

// Held implements Lease: our owner token is still stored under the key and
// the key has not expired. Read-only — never extends the lease.
func (l *SingleOwner) Held(ctx context.Context) (bool, error) {
	val, err := l.rdb.Get(ctx, l.key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val == l.id, nil
}

// ExpiresAt implements Lease.
func (l *SingleOwner) ExpiresAt() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expiresAt
}
