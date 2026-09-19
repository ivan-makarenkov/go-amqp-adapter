// Package otel provides ready-made PublishHeadersBuilder and ConsumeHeadersExtractor
// helpers for correlation ID and OpenTelemetry trace context propagation via AMQP headers.
package otel

import (
	"context"

	"github.com/ivan-makarenkov/go-amqp-adapter"
	"go.opentelemetry.io/otel/propagation"
)

// PropagationConfig configures header keys for correlation and trace context.
type PropagationConfig struct {
	CorrelationIDKey string
	TraceKeys        []string
	GetCorrelationID func(ctx context.Context) string
	SetCorrelationID func(ctx context.Context, value string) context.Context
	Propagator       propagation.TextMapPropagator
}

// NewPublishHeadersBuilder creates a builder that writes correlation and trace headers on publish.
func NewPublishHeadersBuilder(cfg PropagationConfig) amqpadapter.PublishHeadersBuilder {
	propagator := cfg.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator()
	}

	return func(ctx context.Context) map[string]any {
		headers := make(map[string]any)

		if cfg.GetCorrelationID != nil && cfg.CorrelationIDKey != "" {
			if value := cfg.GetCorrelationID(ctx); value != "" {
				headers[cfg.CorrelationIDKey] = value
			}
		}

		if len(cfg.TraceKeys) > 0 {
			carrier := propagation.MapCarrier{}
			propagator.Inject(ctx, carrier)
			for _, key := range cfg.TraceKeys {
				if value := carrier.Get(key); value != "" {
					headers[key] = value
				}
			}
		}

		return headers
	}
}

// NewConsumeHeadersExtractor creates an extractor that restores context from AMQP headers.
func NewConsumeHeadersExtractor(cfg PropagationConfig) amqpadapter.ConsumeHeadersExtractor {
	propagator := cfg.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator()
	}

	return func(parent context.Context, headers map[string]any) context.Context {
		ctx := parent

		if cfg.SetCorrelationID != nil && cfg.CorrelationIDKey != "" {
			if value, ok := headers[cfg.CorrelationIDKey].(string); ok && value != "" {
				ctx = cfg.SetCorrelationID(ctx, value)
			}
		}

		if len(cfg.TraceKeys) > 0 {
			carrier := propagation.MapCarrier{}
			for _, key := range cfg.TraceKeys {
				if value, ok := headers[key].(string); ok && value != "" {
					carrier.Set(key, value)
				}
			}
			if len(carrier) > 0 {
				ctx = propagator.Extract(ctx, carrier)
			}
		}

		return ctx
	}
}
