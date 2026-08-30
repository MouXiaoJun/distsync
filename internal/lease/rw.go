package lease

import (
	"context"
	"sync"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
	"github.com/redis/go-redis/v9"
)

// RWKeys are the Redis keys of one read-write lock. All of them are derived
// from one hash-tagged name, so every multi-key script stays on a single
// Redis Cluster slot.
type RWKeys struct {
	Writer   string // exclusive writer key
	Readers  string // sorted set of active readers (score = expiry ms)
	Fencing  string // fencing counter for writers
	Waiters  string // FIFO arrival queue: sorted set, score = arrival seq
	WaiterTS string // queue-member -> last attempt ms (crash detection)
	Seq      string // monotonic arrival-sequence counter
}

// waiterTimeout returns how long a queue entry may go silent before being
// declared crashed and purged. Must comfortably exceed the acquisition
// backoff (default max 2s); 2x the lease TTL is a safe bound.
func waiterTimeout(ttl time.Duration) int64 {
	return (ttl * 2).Milliseconds()
}

// RWWriter is the writer role of a distributed read-write lock with strict
// FIFO fairness: it joins the arrival queue and is only granted when it
// reaches the head, no other writer holds, and no reader is active. Renew
// and Release reuse the single-owner scripts on the writer key.
type RWWriter struct {
	id   string
	keys RWKeys
	rdb  redis.Cmdable
	ttl  time.Duration

	mu        sync.Mutex
	expiresAt time.Time
}

// NewRWWriter builds a writer lease. All keys must share a hash slot.
func NewRWWriter(rdb redis.Cmdable, keys RWKeys, ttl time.Duration) *RWWriter {
	return &RWWriter{id: Token(), keys: keys, rdb: rdb, ttl: ttl}
}

// ID implements Lease.
func (w *RWWriter) ID() string { return w.id }

// member is this writer's entry name in the arrival queue.
func (w *RWWriter) member() string { return "W:" + w.id }

// Dequeue removes a still-waiting writer from the arrival queue. Best-effort
// cleanup for callers that give up (context canceled, failed TryLock) so the
// queue keeps draining.
func (w *RWWriter) Dequeue(ctx context.Context) {
	_ = w.rdb.ZRem(ctx, w.keys.Waiters, w.member()).Err()
	_ = w.rdb.HDel(ctx, w.keys.WaiterTS, w.member()).Err()
}

// Acquire implements Lease: a single, non-blocking attempt.
func (w *RWWriter) Acquire(ctx context.Context) error {
	_, err := w.TryAcquire(ctx)
	return err
}

// TryAcquire attempts a single acquisition and returns the fencing token on
// success. The caller joins the FIFO queue on the first attempt; repeated
// attempts keep the position.
func (w *RWWriter) TryAcquire(ctx context.Context) (uint64, error) {
	started := time.Now()
	deadline := requestExpiry(started, w.ttl)
	res, err := lua.RWWriteLock.Run(
		ctx, w.rdb,
		[]string{w.keys.Writer, w.keys.Readers, w.keys.Fencing,
			w.keys.Waiters, w.keys.WaiterTS, w.keys.Seq},
		w.id, w.ttl.Milliseconds(), started.UnixMilli(), waiterTimeout(w.ttl),
	).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrBusy
		}
		return 0, err
	}

	w.mu.Lock()
	if !deadline.After(time.Now()) {
		w.mu.Unlock()
		return 0, discardLateGrant(w)
	}
	w.expiresAt = deadline
	w.mu.Unlock()
	return uint64(res), nil
}

// Renew implements Lease.
func (w *RWWriter) Renew(ctx context.Context) error {
	deadline := requestExpiry(time.Now(), w.ttl)
	ok, err := lua.SingleRenew.Run(ctx, w.rdb, []string{w.keys.Writer}, w.id, w.ttl.Milliseconds()).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.expiresAt.After(time.Now()) || !deadline.After(time.Now()) {
		return ErrLost
	}
	if deadline.After(w.expiresAt) {
		w.expiresAt = deadline
	}
	return nil
}

// Release implements Lease (compare-and-delete on the writer key).
func (w *RWWriter) Release(ctx context.Context) error {
	ok, err := lua.SingleRelease.Run(ctx, w.rdb, []string{w.keys.Writer}, w.id).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	w.mu.Lock()
	w.expiresAt = time.Time{}
	w.mu.Unlock()
	return nil
}

// Held implements Lease for the writer role: the writer key still stores
// our token. Read-only — never extends.
func (w *RWWriter) Held(ctx context.Context) (bool, error) {
	val, err := w.rdb.Get(ctx, w.keys.Writer).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val == w.id, nil
}

// ExpiresAt implements Lease.
func (w *RWWriter) ExpiresAt() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.expiresAt
}

// RWReader is the reader role of a distributed read-write lock with strict
// FIFO fairness: one reader token in the shared readers set. Acquisition is
// refused while a writer holds or while a writer is queued ahead. Renew and
// Release reuse the sorted-set scripts shared with semaphore permits.
type RWReader struct {
	id   string
	keys RWKeys
	rdb  redis.Cmdable
	ttl  time.Duration

	mu        sync.Mutex
	expiresAt time.Time
}

// NewRWReader builds a reader lease.
func NewRWReader(rdb redis.Cmdable, keys RWKeys, ttl time.Duration) *RWReader {
	return &RWReader{id: Token(), keys: keys, rdb: rdb, ttl: ttl}
}

// ID implements Lease.
func (r *RWReader) ID() string { return r.id }

// member is this reader's entry name in the arrival queue.
func (r *RWReader) member() string { return "R:" + r.id }

// Dequeue removes a still-waiting reader from the arrival queue (best-effort).
func (r *RWReader) Dequeue(ctx context.Context) {
	_ = r.rdb.ZRem(ctx, r.keys.Waiters, r.member()).Err()
	_ = r.rdb.HDel(ctx, r.keys.WaiterTS, r.member()).Err()
}

// Acquire implements Lease: a single, non-blocking attempt.
func (r *RWReader) Acquire(ctx context.Context) error {
	started := time.Now()
	deadline := requestExpiry(started, r.ttl)
	res, err := lua.RWReadLock.Run(
		ctx, r.rdb,
		[]string{r.keys.Writer, r.keys.Readers, r.keys.Waiters, r.keys.WaiterTS, r.keys.Seq},
		r.id, r.ttl.Milliseconds(), started.UnixMilli(), waiterTimeout(r.ttl),
	).Int64()
	if err != nil {
		if err == redis.Nil {
			return ErrBusy
		}
		return err
	}
	_ = res

	r.mu.Lock()
	if !deadline.After(time.Now()) {
		r.mu.Unlock()
		return discardLateGrant(r)
	}
	r.expiresAt = deadline
	r.mu.Unlock()
	return nil
}

// Renew implements Lease.
func (r *RWReader) Renew(ctx context.Context) error {
	started := time.Now()
	deadline := requestExpiry(started, r.ttl)
	ok, err := lua.SemRenew.Run(
		ctx, r.rdb,
		[]string{r.keys.Readers},
		started.UnixMilli(), r.ttl.Milliseconds(), r.id,
	).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.expiresAt.After(time.Now()) || !deadline.After(time.Now()) {
		return ErrLost
	}
	if deadline.After(r.expiresAt) {
		r.expiresAt = deadline
	}
	return nil
}

// Release implements Lease.
func (r *RWReader) Release(ctx context.Context) error {
	removed, err := lua.SemRelease.Run(ctx, r.rdb, []string{r.keys.Readers}, r.id).Int64()
	if err != nil {
		return err
	}
	if removed != 1 {
		return ErrLost
	}

	r.mu.Lock()
	r.expiresAt = time.Time{}
	r.mu.Unlock()
	return nil
}

// Held implements Lease for the reader role: our token is still in the
// readers set with a non-expired score. Read-only — never extends.
func (r *RWReader) Held(ctx context.Context) (bool, error) {
	score, err := r.rdb.ZScore(ctx, r.keys.Readers, r.id).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return score > float64(time.Now().UnixMilli()), nil
}

// ExpiresAt implements Lease.
func (r *RWReader) ExpiresAt() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.expiresAt
}
