// Package lease is the unified abstraction that every dist primitive is
// built on. Mutex, RWMutex, Semaphore, RateLimiter and Leader all reduce to
// one of two lease shapes backed by a single Redis key family:
//
//   - SingleOwner: at most one holder (mutex, RWMutex writer, leader).
//   - PermitSet:   N concurrent holders (semaphore, RWMutex readers).
//
// Everything else — ownership tokens, TTL, expiry, renewal, Redis failures,
// context cancellation — is handled here once, instead of being re-implemented
// per primitive.
package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// Sentinel errors returned by leases. The dist package re-exports the ones
// its public API needs (distsync.ErrLost, ...), so callers never import this
// package.
var (
	// ErrBusy is returned when the lease is currently held by someone else.
	ErrBusy = errors.New("distsync: lease is busy")

	// ErrLost is returned when the caller no longer owns the lease: it
	// expired, was stolen, or was already released.
	ErrLost = errors.New("distsync: lease lost")
)

// Lease is the unified distributed lease. Every primitive owns one (or
// more) of these and relies on them for ownership, TTL, renewal and safe
// release.
type Lease interface {
	// ID returns the owner token of this lease instance. It is unique per
	// lease object, so a fresh lease object per acquisition is what makes
	// release/renew ownership-safe.
	ID() string

	// Acquire attempts a single acquisition and never blocks. It returns
	// ErrBusy when someone else currently holds the lease.
	Acquire(ctx context.Context) error

	// Renew extends the lease TTL, but only if the caller still owns it.
	// Returns ErrLost when ownership is gone.
	Renew(ctx context.Context) error

	// Release hands the lease back, but only if the caller still owns it.
	// Returns ErrLost when ownership is gone.
	Release(ctx context.Context) error

	// ExpiresAt returns the current expiry time of the lease (zero when
	// not held).
	ExpiresAt() time.Time
}

// Token returns a fresh random owner token. Each acquisition must use a
// fresh lease object (and therefore a fresh token): reusing a token across
// sequential acquisitions would let a stale holder unlock a newer one.
func Token() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("distsync: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
