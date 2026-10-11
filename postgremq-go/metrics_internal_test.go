package postgremq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestHandlerMetricsMeasureCallbackAndBalanceActive(t *testing.T) {
	for _, outcome := range []string{"", "handler_error", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
			metrics, err := newClientMetrics(provider)
			require.NoError(t, err)
			started, release := make(chan struct{}), make(chan struct{})
			hc := &HandlerConsumer{conn: &Connection{metrics: metrics}, logger: NoopLogger{}}
			hc.handler = func(context.Context, *Message) {
				close(started)
				<-release
				if outcome == "handler_error" {
					panic("private detail")
				}
			}
			msg := &Message{queue: "q", StoppedCtx: ctx}
			// Explicitly settled delivery avoids automatic database settlement, while
			// the callback still has work left. Processing must finish at callback return.
			msg.settlementStarted.Store(true)
			hc.handlerWg.Add(1)
			go hc.runHandler(msg)
			<-started
			collect := func() map[string]metricdata.Metrics {
				var data metricdata.ResourceMetrics
				require.NoError(t, reader.Collect(context.Background(), &data))
				out := map[string]metricdata.Metrics{}
				for _, s := range data.ScopeMetrics {
					for _, m := range s.Metrics {
						out[m.Name] = m
					}
				}
				return out
			}
			during := collect()
			require.EqualValues(t, 1, during["postgremq.client.handlers.active"].Data.(metricdata.Sum[int64]).DataPoints[0].Value)
			_, exists := during["messaging.process.duration"]
			require.False(t, exists)
			if outcome == "cancelled" {
				cancel()
			}
			close(release)
			hc.handlerWg.Wait()
			after := collect()
			active := after["postgremq.client.handlers.active"]
			require.Equal(t, "{handler}", active.Unit)
			require.Zero(t, active.Data.(metricdata.Sum[int64]).DataPoints[0].Value)
			process := after["messaging.process.duration"]
			require.Equal(t, "s", process.Unit)
			points := process.Data.(metricdata.Histogram[float64]).DataPoints
			require.Len(t, points, 1)
			require.EqualValues(t, 1, points[0].Count)
			code, _ := points[0].Attributes.Value(attribute.Key("error.type"))
			require.Equal(t, outcome, code.AsString())
		})
	}
}

func TestRenewalLossMetricUsesLiveOwnership(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	metrics, err := newClientMetrics(provider)
	require.NoError(t, err)
	s := newExtScheduler(&Connection{metrics: metrics}, 10)
	for i, outcome := range []string{"missing", "expired", "deregistered", "busy"} {
		e := &extEntry{queue: "q", id: int64(i), token: outcome, expiresAt: time.Now().Add(time.Minute)}
		s.add(e)
		res := extResult{popped: []*extEntry{e}}
		switch outcome {
		case "expired":
			e.expiresAt = time.Now().Add(-time.Second)
			res.err = errors.New("network")
		case "deregistered":
			s.remove(e.key())
		case "busy":
			res.locks = []MultiLock{{Queue: e.queue, ID: e.id, Token: e.token, Busy: true}}
		}
		s.apply(res)
		s.apply(res) // retired entries cannot emit a second loss
	}
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	require.Len(t, data.ScopeMetrics, 1)
	metricsData := data.ScopeMetrics[0].Metrics
	require.Len(t, metricsData, 1)
	require.Equal(t, "postgremq.client.renewal.lost", metricsData[0].Name)
	require.EqualValues(t, 2, metricsData[0].Data.(metricdata.Sum[int64]).DataPoints[0].Value)
}

func TestConsumedMetricsPreservePartialReceiveError(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	metrics, err := newClientMetrics(provider)
	require.NoError(t, err)
	metrics.recordConsumed(context.Background(), "q", []*Message{{DeliveryAttempt: 1}, {DeliveryAttempt: 2}}, errors.New("truncated response with private detail"))
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)
	m := data.ScopeMetrics[0].Metrics[0]
	require.Equal(t, "messaging.client.consumed.messages", m.Name)
	points := m.Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, points, 2)
	for _, point := range points {
		require.EqualValues(t, 1, point.Value)
		code, ok := point.Attributes.Value(attribute.Key("error.type"))
		require.True(t, ok)
		require.Equal(t, "other", code.AsString())
	}
}
