package distsync

import (
	"context"
	"time"
)

// Metrics receives lifecycle events from every primitive. Implementations
// must be safe for concurrent use. Install one with New(..., WithMetrics(m));
// the metrics subpackage ships a Prometheus implementation.
type Metrics interface {
	// Acquire reports one acquisition attempt and how long it waited.
	Acquire(primitive, resource string, ok bool, wait time.Duration)
	// Release reports one release.
	Release(primitive, resource string)
	// Renew reports one heartbeat renewal.
	Renew(primitive, resource string, ok bool)
	// RenewalStopped reports that background renewal ended, with a reason
	// ("released", "lost", "error").
	RenewalStopped(primitive, resource, reason string)
}

type noopMetrics struct{}

func (noopMetrics) Acquire(string, string, bool, time.Duration) {}
func (noopMetrics) Release(string, string)                      {}
func (noopMetrics) Renew(string, string, bool)                  {}
func (noopMetrics) RenewalStopped(string, string, string)       {}

// Tracer creates spans around primitive operations (Lock, Renew, Release,
// Acquire, Leader.Run). The Start implementation returns the derived
// context and a function that finishes the span, reporting its error.
// Wire it to OpenTelemetry with a small adapter.
type Tracer interface {
	Start(ctx context.Context, name string) (context.Context, func(error))
}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string) (context.Context, func(error)) {
	return ctx, func(error) {}
}
