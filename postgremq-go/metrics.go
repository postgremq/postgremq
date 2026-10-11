package postgremq

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// WithMeterProvider enables client metrics. The application owns the provider,
// readers, exporters and shutdown. Without this option metrics are disabled,
// even if a global provider is configured. Pass otel.GetMeterProvider() explicitly
// to use the global provider.
func WithMeterProvider(provider metric.MeterProvider) ConnectionOption {
	return func(c *Connection) { c.meterProvider = provider }
}

type clientMetrics struct {
	operation   metric.Float64Histogram
	process     metric.Float64Histogram
	consumed    metric.Int64Counter
	sent        metric.Int64Counter
	active      metric.Int64UpDownCounter
	renewalLost metric.Int64Counter
}

func newClientMetrics(provider metric.MeterProvider) (*clientMetrics, error) {
	if provider == nil {
		return nil, nil
	}
	meter := provider.Meter("postgremq", metric.WithInstrumentationVersion("1"))
	m := &clientMetrics{}
	var err error
	boundaries := metric.WithExplicitBucketBoundaries(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)
	m.operation, err = meter.Float64Histogram("messaging.client.operation.duration", metric.WithUnit("s"), boundaries)
	if err != nil {
		return nil, err
	}
	m.process, err = meter.Float64Histogram("messaging.process.duration", metric.WithUnit("s"), boundaries)
	if err != nil {
		return nil, err
	}
	m.sent, err = meter.Int64Counter("messaging.client.sent.messages", metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	m.consumed, err = meter.Int64Counter("messaging.client.consumed.messages", metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	m.active, err = meter.Int64UpDownCounter("postgremq.client.handlers.active", metric.WithUnit("{handler}"))
	if err != nil {
		return nil, err
	}
	m.renewalLost, err = meter.Int64Counter("postgremq.client.renewal.lost", metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	return m, nil
}

func metricAttributes(operation, destination string) []attribute.KeyValue {
	kind := operation
	switch operation {
	case "publish":
		kind = "send"
	case "consume":
		kind = "receive"
	case "ack", "nack", "release":
		kind = "settle"
	}
	attrs := []attribute.KeyValue{attribute.String("messaging.system", "postgremq"), attribute.String("messaging.operation.name", operation), attribute.String("messaging.operation.type", kind)}
	if destination != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", destination))
	}
	return attrs
}

func metricError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, ErrQueueNotFound), errors.Is(err, ErrQueueGone):
		return "queue_not_found"
	case errors.Is(err, ErrValidation):
		return "validation"
	case errors.Is(err, ErrConnectionClosed):
		return "connection_closed"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "other"
	}
}

// Observe logical operations once, including pool wait and internal retries.
// The transaction attribute describes caller ownership, never commit outcome.
func (m *clientMetrics) startOperation(ctx context.Context, operation, destination string, transaction bool) func(error) {
	if m == nil {
		return func(error) {}
	}
	start := time.Now()
	attrs := append(metricAttributes(operation, destination), attribute.Bool("postgremq.transaction", transaction))
	return func(err error) {
		if code := metricError(err); code != "" {
			attrs = append(attrs, attribute.String("error.type", code))
		}
		m.operation.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
	}
}

func (m *clientMetrics) recordConsumed(ctx context.Context, queue string, messages []*Message, err error) {
	if m == nil {
		return
	}
	var redelivered int64
	for _, msg := range messages {
		if msg.DeliveryAttempt > 1 {
			redelivered++
		}
	}
	attrs := metricAttributes("consume", queue)
	if code := metricError(err); code != "" {
		attrs = append(attrs, attribute.String("error.type", code))
	}
	for _, entry := range []struct {
		count       int64
		redelivered bool
	}{{int64(len(messages)) - redelivered, false}, {redelivered, true}} {
		if entry.count > 0 {
			m.consumed.Add(ctx, entry.count, metric.WithAttributes(append(attrs, attribute.Bool("postgremq.redelivered", entry.redelivered))...))
		}
	}
}

func (m *clientMetrics) startHandler(ctx context.Context, queue string) func(string) {
	if m == nil {
		return func(string) {}
	}
	attrs := metricAttributes("process", queue)
	start := time.Now()
	m.active.Add(ctx, 1, metric.WithAttributes(attrs...))
	return func(code string) {
		m.active.Add(ctx, -1, metric.WithAttributes(attrs...))
		if code != "" {
			attrs = append(attrs, attribute.String("error.type", code))
		}
		m.process.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
	}
}

// Recorded only when the renewal actor retires a still-tracked delivery.
func (m *clientMetrics) recordRenewalLost(queue string) {
	if m == nil {
		return
	}
	m.renewalLost.Add(context.Background(), 1, metric.WithAttributes(attribute.String("messaging.system", "postgremq"), attribute.String("messaging.destination.name", queue)))
}

// Count a message when a publish SQL attempt finishes, including failed attempts.
// Retries are separate attempts; transaction success does not imply commit.
func (m *clientMetrics) recordSent(ctx context.Context, topic string, transaction bool, err error) {
	if m == nil {
		return
	}
	attrs := append(metricAttributes("publish", topic), attribute.Bool("postgremq.transaction", transaction))
	if code := metricError(mapPgError(err)); code != "" {
		attrs = append(attrs, attribute.String("error.type", code))
	}
	m.sent.Add(ctx, 1, metric.WithAttributes(attrs...))
}
