package postgremq_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"postgremq.dev/postgremq-go"
)

func TestMetricsContractWithTransactionsAndRedelivery(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(ctx)) }()
	conn, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithMeterProvider(provider), postgremq.WithoutRetries())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	require.NoError(t, conn.CreateTopic(ctx, "metrics"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "metrics", false))
	_, err = conn.Publish(ctx, "metrics", []byte(`{}`))
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = conn.PublishWithTx(ctx, tx, "metrics", []byte(`{}`))
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))
	_, err = conn.Publish(ctx, "missing", []byte(`{}`))
	require.ErrorIs(t, err, postgremq.ErrQueueNotFound)
	take := func() *postgremq.Message {
		msgs, e := conn.ConsumeMessages(ctx, "q", 1, 30)
		require.NoError(t, e)
		require.Len(t, msgs, 1)
		return msgs[0]
	}
	msg := take()
	_, err = msg.SetVT(ctx, 30)
	require.NoError(t, err)
	tx, err = pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, msg.AckWithTx(ctx, tx))
	require.NoError(t, tx.Rollback(ctx))
	// Local duplicate settlement does not execute an operation or emit one.
	require.ErrorIs(t, msg.Ack(ctx), postgremq.ErrLeaseLost)
	_, err = pool.Exec(ctx, "UPDATE postgremq.queue_messages SET vt=clock_timestamp()-interval '1 second'")
	require.NoError(t, err)
	msg = take()
	require.NoError(t, msg.Nack(ctx))
	msg = take()
	require.NoError(t, msg.Release(ctx))
	// No live messages left, but an empty receive is still a measured operation.
	_, err = pool.Exec(ctx, "DELETE FROM postgremq.queue_messages")
	require.NoError(t, err)
	msgs, err := conn.ConsumeMessages(ctx, "q", 1, 30)
	require.NoError(t, err)
	require.Empty(t, msgs)
	// A delivery whose server lease disappears produces a settlement error.
	_, err = pool.Exec(ctx, "SELECT postgremq.publish_message('metrics','{}')")
	require.NoError(t, err)
	lost := take()
	_, err = pool.Exec(ctx, "UPDATE postgremq.queue_messages SET consumer_token=gen_random_uuid()::text")
	require.NoError(t, err)
	require.ErrorIs(t, lost.Ack(ctx), postgremq.ErrLeaseLost)

	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &data))
	raw, err := os.ReadFile("../observability/client-contract.json")
	require.NoError(t, err)
	var contract struct {
		Scope               string
		Version             string
		HistogramBoundaries []float64 `json:"histogram_boundaries"`
		Metrics             map[string]string
		Operations          []struct {
			Name, Destination, Error, Type string
			Transaction                    bool
			Count                          uint64
		} `json:"scenario_operations"`
		Received struct{ First, Redelivered int64 }
		Sent     []struct {
			Destination, Error string
			Transaction        bool
			Count              int64
		}
	}
	require.NoError(t, json.Unmarshal(raw, &contract))
	require.Len(t, data.ScopeMetrics, 1)
	scope := data.ScopeMetrics[0]
	require.Equal(t, contract.Scope, scope.Scope.Name)
	require.Equal(t, contract.Version, scope.Scope.Version)
	var operations []metricdata.HistogramDataPoint[float64]
	var sent []metricdata.DataPoint[int64]
	totals := map[bool]int64{}
	for _, m := range scope.Metrics {
		require.Equal(t, contract.Metrics[m.Name], m.Unit)
		switch m.Name {
		case "messaging.client.operation.duration":
			operations = m.Data.(metricdata.Histogram[float64]).DataPoints
		case "messaging.client.sent.messages":
			sent = m.Data.(metricdata.Sum[int64]).DataPoints
		case "messaging.client.consumed.messages":
			for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
				v, _ := p.Attributes.Value(attribute.Key("postgremq.redelivered"))
				totals[v.AsBool()] += p.Value
			}
		}
	}
	require.Len(t, sent, len(contract.Sent))
	for _, expected := range contract.Sent {
		found := false
		for _, point := range sent {
			attrs := []attribute.KeyValue{
				attribute.String("messaging.system", "postgremq"), attribute.String("messaging.operation.name", "publish"),
				attribute.String("messaging.operation.type", "send"), attribute.String("messaging.destination.name", expected.Destination),
				attribute.Bool("postgremq.transaction", expected.Transaction),
			}
			if expected.Error != "" {
				attrs = append(attrs, attribute.String("error.type", expected.Error))
			}
			set := attribute.NewSet(attrs...)
			if point.Attributes.Equals(&set) {
				found = true
				require.Equal(t, expected.Count, point.Value)
			}
		}
		require.True(t, found, "missing sent counter: %+v", expected)
	}
	require.Len(t, operations, len(contract.Operations))
	for _, expected := range contract.Operations {
		found := false
		for _, p := range operations {
			get := func(k string) attribute.Value { v, _ := p.Attributes.Value(attribute.Key(k)); return v }
			if get("messaging.operation.name").AsString() != expected.Name || get("messaging.destination.name").AsString() != expected.Destination || get("postgremq.transaction").AsBool() != expected.Transaction || get("error.type").AsString() != expected.Error {
				continue
			}
			found = true
			require.Equal(t, expected.Count, p.Count)
			require.GreaterOrEqual(t, p.Sum, 0.0)
			require.Equal(t, contract.HistogramBoundaries, p.Bounds)
			require.Equal(t, "postgremq", get("messaging.system").AsString())
			require.Equal(t, expected.Type, get("messaging.operation.type").AsString())
			require.Equal(t, 5+boolInt(expected.Error != ""), p.Attributes.Len(), "bounded attributes only")
		}
		require.True(t, found, "missing metric: %+v", expected)
	}
	require.Equal(t, contract.Received.First, totals[false])
	require.Equal(t, contract.Received.Redelivered, totals[true])
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestMetricsCountLogicalOperationAcrossRetry(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(ctx)) }()
	conn, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithMeterProvider(provider))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	require.NoError(t, conn.CreateTopic(ctx, "metrics"))
	sql, err := os.ReadFile("../observability/retry-once.sql")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(sql))
	require.NoError(t, err)
	_, err = conn.Publish(ctx, "metrics", []byte(`{}`))
	require.NoError(t, err)
	var attempts int
	require.NoError(t, pool.QueryRow(ctx, "SELECT last_value FROM public.metrics_retry_probe").Scan(&attempts))
	require.Equal(t, 2, attempts)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &data))
	var points []metricdata.HistogramDataPoint[float64]
	attemptsByError := map[string]int64{}
	for _, m := range data.ScopeMetrics[0].Metrics {
		switch m.Name {
		case "messaging.client.operation.duration":
			points = m.Data.(metricdata.Histogram[float64]).DataPoints
		case "messaging.client.sent.messages":
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				code, _ := point.Attributes.Value(attribute.Key("error.type"))
				attemptsByError[code.AsString()] += point.Value
			}
		}
	}
	require.Equal(t, map[string]int64{"": 1, "other": 1}, attemptsByError)

	require.Len(t, points, 1)
	require.EqualValues(t, 1, points[0].Count)
	_, hasError := points[0].Attributes.Value(attribute.Key("error.type"))
	require.False(t, hasError)
	require.GreaterOrEqual(t, points[0].Sum, 0.1, "duration includes retry backoff")
}

// A global SDK does not implicitly enable telemetry; connection shutdown never
// owns the application provider, whether explicitly supplied or omitted.
func TestMetricsProviderIsOptionalAndApplicationOwned(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "omitted", true: "provided"}[enabled], func(t *testing.T) {
			pool, ctx := setupTestConnection(t)
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { require.NoError(t, provider.Shutdown(ctx)) }()
			previous := otel.GetMeterProvider()
			otel.SetMeterProvider(provider)
			defer otel.SetMeterProvider(previous)
			var opts []postgremq.ConnectionOption
			if enabled {
				opts = append(opts, postgremq.WithMeterProvider(provider))
			}
			conn, err := postgremq.DialFromPool(context.Background(), pool, opts...)
			require.NoError(t, err)
			defer func() { require.NoError(t, conn.Close()) }()
			require.NoError(t, conn.CreateTopic(ctx, "optional"))
			require.NoError(t, conn.CreateQueue(ctx, "q", "optional", false))
			_, err = conn.Publish(ctx, "optional", []byte(`{}`))
			require.NoError(t, err)
			messages, err := conn.ConsumeMessages(ctx, "q", 1, 30)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.NoError(t, messages[0].Ack(ctx))
			require.NoError(t, conn.Close())
			// This API rejection must not increment the attempted-send counter.
			_, err = conn.Publish(ctx, "optional", []byte(`{}`))
			require.ErrorIs(t, err, postgremq.ErrConnectionClosed)
			probe, err := provider.Meter("application").Int64Counter("application.probe")
			require.NoError(t, err)
			probe.Add(ctx, 1)
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(ctx, &data))
			foundClient := false
			foundProbe := false
			for _, scope := range data.ScopeMetrics {
				if scope.Scope.Name == "application" {
					foundProbe = true
					require.EqualValues(t, 1, scope.Metrics[0].Data.(metricdata.Sum[int64]).DataPoints[0].Value)
				}
				if scope.Scope.Name == "postgremq" {
					foundClient = true
					for _, m := range scope.Metrics {
						if m.Name == "messaging.client.sent.messages" {
							points := m.Data.(metricdata.Sum[int64]).DataPoints
							require.Len(t, points, 1)
							require.EqualValues(t, 1, points[0].Value)
						}
					}
				}
			}
			require.True(t, foundProbe)
			require.Equal(t, enabled, foundClient)
		})
	}
}
