package lease

import (
	"context"
	"sync"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
	"github.com/redis/go-redis/v9"
)

// RWWriter is the writer role of a distributed read-write lock. It is an
// exclusive lease whose acquisition also checks that no readers hold the
// lock and announces writer intent (writer preference). Renew and Release
// reuse the single-owner scripts on the writer key.
type RWWriter struct {
	id      string
	writer  string // writer key
	readers string // readers sorted set
	fencing string // fencing counter key
	waiting string // writer-waiting marker
	rdb     redis.Cmdable
	ttl     time.Duration

	mu        sync.Mutex
	expiresAt time.Time
}

// NewRWWriter builds a writer lease. All keys must share a hash slot.
func NewRWWriter(rdb redis.Cmdable, writer, readers, fencing, waiting string, ttl time.Duration) *RWWriter {
	return &RWWriter{
		id:      Token(),
		writer:  writer,
		readers: readers,
		fencing: fencing,
		waiting: waiting,
		rdb:     rdb,
		ttl:     ttl,
	}
}

// ID implements Lease.
func (w *RWWriter) ID() string { return w.id }

// Acquire implements Lease: a single, non-blocking attempt.
func (w *RWWriter) Acquire(ctx context.Context) error {
	_, err := w.TryAcquire(ctx)
	return err
}

// TryAcquire attempts a single acquisition and returns the fencing token on
// success.
func (w *RWWriter) TryAcquire(ctx context.Context) (uint64, error) {
	res, err := lua.RWWriteLock.Run(
		ctx, w.rdb,
		[]string{w.writer, w.readers, w.fencing, w.waiting},
		w.id, w.ttl.Milliseconds(), time.Now().UnixMilli(),
	).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrBusy
		}
		return 0, err
	}

	w.mu.Lock()
	w.expiresAt = time.Now().Add(w.ttl)
	w.mu.Unlock()
	return uint64(res), nil
}

// Renew implements Lease.
func (w *RWWriter) Renew(ctx context.Context) error {
	ok, err := lua.SingleRenew.Run(ctx, w.rdb, []string{w.writer}, w.id, w.ttl.Milliseconds()).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	w.mu.Lock()
	w.expiresAt = time.Now().Add(w.ttl)
	w.mu.Unlock()
	return nil
}

// Release implements Lease (compare-and-delete on the writer key).
func (w *RWWriter) Release(ctx context.Context) error {
	ok, err := lua.SingleRelease.Run(ctx, w.rdb, []string{w.writer}, w.id).Int64()
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
	val, err := w.rdb.Get(ctx, w.writer).Result()
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

// RWReader is the reader role of a distributed read-write lock: one reader
// token in the shared readers sorted set. Acquisition is refused while a
// writer holds the lock or is queued (writer preference). Renew and Release
// reuse the sorted-set scripts shared with semaphore permits.
type RWReader struct {
	id      string
	writer  string
	readers string
	waiting string
	rdb     redis.Cmdable
	ttl     time.Duration

	mu        sync.Mutex
	expiresAt time.Time
}

// NewRWReader builds a reader lease.
func NewRWReader(rdb redis.Cmdable, writer, readers, waiting string, ttl time.Duration) *RWReader {
	return &RWReader{
		id:      Token(),
		writer:  writer,
		readers: readers,
		waiting: waiting,
		rdb:     rdb,
		ttl:     ttl,
	}
}

// ID implements Lease.
func (r *RWReader) ID() string { return r.id }

// Acquire implements Lease: a single, non-blocking attempt.
func (r *RWReader) Acquire(ctx context.Context) error {
	res, err := lua.RWReadLock.Run(
		ctx, r.rdb,
		[]string{r.writer, r.readers, r.waiting},
		r.id, time.Now().UnixMilli(), r.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		if err == redis.Nil {
			return ErrBusy
		}
		return err
	}
	_ = res

	r.mu.Lock()
	r.expiresAt = time.Now().Add(r.ttl)
	r.mu.Unlock()
	return nil
}

// Renew implements Lease.
func (r *RWReader) Renew(ctx context.Context) error {
	ok, err := lua.SemRenew.Run(
		ctx, r.rdb,
		[]string{r.readers},
		time.Now().UnixMilli(), r.ttl.Milliseconds(), r.id,
	).Int64()
	if err != nil {
		return err
	}
	if ok != 1 {
		return ErrLost
	}

	r.mu.Lock()
	r.expiresAt = time.Now().Add(r.ttl)
	r.mu.Unlock()
	return nil
}

// Release implements Lease.
func (r *RWReader) Release(ctx context.Context) error {
	removed, err := lua.SemRelease.Run(ctx, r.rdb, []string{r.readers}, r.id).Int64()
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
	score, err := r.rdb.ZScore(ctx, r.readers, r.id).Result()
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
