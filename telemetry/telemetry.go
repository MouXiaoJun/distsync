// Package telemetry adapts OpenTelemetry to distsync's Tracer interface:
//
//	client := distsync.New(rdb, distsync.WithTracer(telemetry.NewTracer(nil)))
//
// Pass nil to use the process-wide OTel tracer provider (configured with
// otel.SetTracerProvider). Spans are named after the operation
// (distsync.mutex.lock, distsync.renew, distsync.release, ...) and are
// marked error when the operation fails.
package telemetry

import (
	"context"

	"github.com/MouXiaoJun/distsync"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracer implements distsync.Tracer with OpenTelemetry spans.
type Tracer struct {
	provider trace.TracerProvider
}

// NewTracer builds an OTel-backed distsync.Tracer. A nil provider uses the
// global OTel tracer provider.
func NewTracer(provider trace.TracerProvider) *Tracer {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &Tracer{provider: provider}
}

// Start implements distsync.Tracer. The returned finish function records the
// operation error (if any) on the span and ends it.
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, func(error)) {
	ctx, span := t.provider.Tracer("distsync").Start(ctx, name)
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// compile-time check that Tracer implements distsync.Tracer.
var _ distsync.Tracer = (*Tracer)(nil)
