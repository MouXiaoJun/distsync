// Package distsync provides distributed synchronization primitives for Go,
// backed by Redis and Valkey.
//
// It is not "another Redis client": it is a sync-style toolkit. Where you
// would reach for sync.Mutex, sync.RWMutex, or a counting channel inside
// one process, distsync gives you the same shape across processes:
//
//	client := distsync.New(rdb) // *redis.Client or *redis.ClusterClient
//
//	mu := client.Mutex("order:10001")
//	guard, err := mu.Lock(ctx)
//	if err != nil {
//	    return err
//	}
//	defer guard.Unlock(ctx)
//
// Five primitives ship in v0.x: Mutex, RWMutex, Semaphore, RateLimiter and
// Leader (see each type's documentation for usage). Every primitive is
// built on one unified Lease layer (internal/lease), which handles
// ownership tokens, TTL, background renewal, expiry, Redis failures and
// context cancellation exactly once. Nothing here is a pile of hand-rolled
// SET NX scripts.
//
// # Safety properties
//
//   - Fencing tokens (Mutex, RWMutex writers, Leader): each acquisition
//     mints a strictly increasing token for the resource. Persist it with
//     the side effect and reject writes whose stored token is not older:
//
//     UPDATE orders SET status='paid', fencing_token=? WHERE id=? AND fencing_token < ?
//
//   - Safe unlock: release is a compare-and-delete on a random owner token,
//     so a stale holder can never release a newer owner's lease.
//
//   - Redis Cluster: every key a primitive touches is derived from one
//     hash-tagged name, so all multi-key Lua scripts stay on a single slot
//     and never hit CROSSSLOT errors. Verified against a live cluster (see
//     examples/cluster) and in CI against Redis 7 and Valkey 8.
//
//   - No goroutine leaks: each guard/permit/leadership owns at most one
//     renewal or watchdog goroutine, stopped synchronously on release.
//
// The precise guarantees — fencing bounds, the lease-expiry two-holder
// window, clock-skew assumptions, failure modes — are specified in
// docs/semantics.md.
package distsync

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Client is the entry point for every primitive. It wraps a go-redis
// Cmdable, so it works with *redis.Client, *redis.ClusterClient, rings and
// anything else implementing the interface — on Redis or Valkey.
type Client struct {
	rdb     redis.Cmdable
	metrics Metrics
	tracer  Tracer

	defaults config
}

// New creates a Client from any go-redis-compatible client. Options
// configure cluster-wide defaults; individual primitives can still be
// customized per call.
func New(rdb redis.Cmdable, opts ...ClientOption) *Client {
	c := &Client{
		rdb:     rdb,
		metrics: noopMetrics{},
		tracer:  noopTracer{},
		defaults: config{
			ttl:       10 * time.Second,
			autoRenew: true,
			retry:     retryPolicy{base: 50 * time.Millisecond, max: 2 * time.Second},
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// resolved merges per-primitive options over the client defaults.
func (c *Client) resolved(opts ...Option) config {
	cfg := c.defaults
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.ttl <= 0 {
		cfg.ttl = c.defaults.ttl
	}
	return cfg
}

// Redis exposes the underlying client, e.g. for the fencing-token SQL
// pattern or operational queries.
func (c *Client) Redis() redis.Cmdable { return c.rdb }

// ClientOption configures the Client (defaults for all primitives).
type ClientOption func(*Client)

// WithMetrics installs a Metrics sink (Prometheus, OTel, ...).
func WithMetrics(m Metrics) ClientOption {
	return func(c *Client) {
		if m != nil {
			c.metrics = m
		}
	}
}

// WithTracer installs a Tracer for distributed-trace propagation.
func WithTracer(t Tracer) ClientOption {
	return func(c *Client) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithDefaultLease sets the default lease TTL for every primitive.
func WithDefaultLease(d time.Duration) ClientOption {
	return func(c *Client) { c.defaults.ttl = d }
}

// WithAutoRenew toggles the default background renewal for every primitive.
func WithAutoRenew(enabled bool) ClientOption {
	return func(c *Client) { c.defaults.autoRenew = enabled }
}

// WithRetry sets the default acquisition backoff bounds (exponential with
// jitter) for every primitive.
func WithRetry(base, max time.Duration) ClientOption {
	return func(c *Client) { c.defaults.retry = retryPolicy{base: base, max: max} }
}
