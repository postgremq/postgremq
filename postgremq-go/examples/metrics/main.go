// Run against observability/compose.yaml; see docs/observability.md.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"postgremq.dev/postgremq-go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// OTEL_EXPORTER_OTLP_ENDPOINT selects the Collector; default is localhost:4318.
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return err
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(5*time.Second))),
		sdkmetric.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "postgremq-go-example"))),
	)
	defer func() {
		shutdownCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = provider.Shutdown(shutdownCtx)
	}()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgremq@localhost:55432/postgremq?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	conn, err := postgremq.Dial(ctx, cfg, postgremq.WithMeterProvider(provider), postgremq.WithShutdownTimeout(5*time.Second))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err = conn.CreateTopic(ctx, "metrics_go"); err != nil {
		return err
	}
	if err = conn.CreateQueue(ctx, "metrics_go", "metrics_go", false); err != nil {
		return err
	}
	done := make(chan error, 1)
	consumer, err := conn.ConsumeHandler("metrics_go", func(handlerCtx context.Context, msg *postgremq.Message) {
		// Business work goes here. Explicit Ack's latency is measured separately.
		done <- msg.Ack(handlerCtx)
	}, postgremq.WithMaxInFlight(1))
	if err != nil {
		return err
	}
	if _, err = conn.Publish(ctx, "metrics_go", []byte(`{"example":"metrics"}`)); err != nil {
		return err
	}
	select {
	case err = <-done:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	consumer.Stop()
	if err = conn.Close(); err != nil {
		return err
	}
	// Flush after the queue connection drains, so final settlements are included.
	if err = provider.ForceFlush(ctx); err != nil {
		return err
	}
	fmt.Println("Go client metrics exported")
	return nil
}
