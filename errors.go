package distsync

import (
	"errors"

	"github.com/distsync/distsync/internal/lease"
)

// Sentinel errors returned by the public API. Use errors.Is to match them.
var (
	// ErrNotAcquired is returned by the non-blocking Try* variants when the
	// resource is currently held by someone else.
	ErrNotAcquired = errors.New("distsync: not acquired")

	// ErrLost is returned when a lease the caller believed it owned is no
	// longer theirs: it expired, was stolen, or was already released.
	ErrLost = lease.ErrLost

	// ErrLeadershipLost is returned by Leader.Run when the leader lease was
	// lost while the callback was still running (failover happened).
	ErrLeadershipLost = errors.New("distsync: leadership lost")
)
