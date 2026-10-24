// Continuous demo workload for the Grafana dashboards: run it against
// observability/compose.yaml; see docs/observability.md. Not for production.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"postgremq.dev/postgremq-go"
)

// payload marks the messages a handler fails on purpose.
type payload struct {
	N      int  `json:"n"`
	Poison bool `json:"poison,omitempty"` // every attempt panics: dead-lettered after the last attempt
	Lost   bool `json:"lost,omitempty"`   // removed while in flight: the automatic renewal loses its lease
	Slow   bool `json:"slow,omitempty"`   // outlives its lease: lease lost, then exhausted
}

func main() {
	rate := flag.Float64("rate", 20, "mean orders published per second (events: a quarter of that)")
	duration := flag.Duration("duration", 0, "stop after this long (0: until interrupted)")
	flag.Parse()
	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "-rate must be positive")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	if err := run(ctx, *rate); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rate float64) error {
	setupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// OTEL_EXPORTER_OTLP_ENDPOINT selects the Collector; default is localhost:4318.
	exporter, err := otlpmetrichttp.New(setupCtx)
	if err != nil {
		return err
	}
	host, _ := os.Hostname()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(5*time.Second))),
		sdkmetric.WithResource(resource.NewWithAttributes("",
			attribute.String("service.name", "postgremq-loadgen"),
			attribute.String("service.instance.id", host+"-"+strconv.Itoa(os.Getpid())))),
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
	// The application owns this pool: it also runs the transactional publishes.
	pool, err := pgxpool.New(setupCtx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	conn, err := postgremq.DialFromPool(setupCtx, pool, postgremq.WithMeterProvider(provider),
		postgremq.WithShutdownTimeout(5*time.Second))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	for _, topic := range []string{"orders", "events"} {
		if err = conn.CreateTopic(setupCtx, topic); err != nil {
			return err
		}
	}
	queues := []struct {
		name, topic string
		exclusive   bool
		opts        []postgremq.QueueOption
	}{
		{"orders.billing", "orders", false, []postgremq.QueueOption{postgremq.WithMaxDeliveryAttempts(5)}},
		{"orders.shipping", "orders", false, []postgremq.QueueOption{postgremq.WithMaxDeliveryAttempts(3)}},
		{"events.audit", "events", false, []postgremq.QueueOption{postgremq.WithMaxDeliveryAttempts(2)}},
		{"events.live", "events", true, []postgremq.QueueOption{postgremq.WithKeepAliveInterval(30 * time.Second)}},
	}
	// Reaps events.live if it expired since the last run: an expired exclusive
	// queue cannot be declared again.
	if _, err = conn.MaintenanceFast(setupCtx); err != nil {
		return err
	}
	for _, q := range queues {
		if err = conn.CreateQueue(setupCtx, q.name, q.topic, q.exclusive, q.opts...); err != nil {
			return err
		}
	}

	// billing: fast, the occasional nack is redelivered after a second.
	if _, err = conn.ConsumeHandler("orders.billing", func(ctx context.Context, msg *postgremq.Message) {
		sleep(ctx, 5, 50)
		if rand.Float64() < 0.03 {
			_ = msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(time.Second)))
			return
		}
		_ = msg.Ack(ctx)
	}, postgremq.WithMaxInFlight(8)); err != nil {
		return err
	}
	// shipping: slower than the publish peaks, so a backlog and ready age
	// build up and drain; slow calls outlive half the 2 s lease and are
	// auto-extended; poison messages panic until the last attempt
	// dead-letters them; a "lost" delivery is removed out of band, so its
	// next renewal finds it gone and cancels the handler.
	if _, err = conn.ConsumeHandler("orders.shipping", func(ctx context.Context, msg *postgremq.Message) {
		var p payload
		_ = json.Unmarshal(msg.Payload, &p)
		switch {
		case p.Poison:
			panic("poison message")
		case p.Lost:
			_ = conn.DeleteQueueMessage(ctx, "orders.shipping", msg.ID)
			sleep(ctx, 3000, 3000)
		case rand.Float64() < 0.05:
			sleep(ctx, 1200, 1800)
		default:
			sleep(ctx, 50, 400)
		}
	}, postgremq.WithVT(2), postgremq.WithMaxInFlight(8)); err != nil {
		return err
	}
	// audit: no auto-extension, so a slow call outlives its 2 s lease. The
	// first attempt is claimed again meanwhile (its ack fails with
	// lease_lost); the last one stays exhausted until maintenance
	// dead-letters it, before its own ack fails.
	if _, err = conn.ConsumeHandler("events.audit", func(ctx context.Context, msg *postgremq.Message) {
		var p payload
		_ = json.Unmarshal(msg.Payload, &p)
		if p.Slow {
			sleep(ctx, 20000, 20000)
		} else {
			sleep(ctx, 5, 30)
		}
		_ = msg.Ack(ctx)
	}, postgremq.WithVT(2), postgremq.WithNoAutoExtension(), postgremq.WithMaxInFlight(8)); err != nil {
		return err
	}
	// live: an exclusive queue, kept alive by the connection.
	if _, err = conn.ConsumeHandler("events.live", func(ctx context.Context, msg *postgremq.Message) {
		_ = msg.Ack(ctx)
	}); err != nil {
		return err
	}

	fmt.Printf("publishing %.1f orders/s on average\n", rate)
	start := time.Now()
	next := time.NewTimer(0)
	defer next.Stop()
	maintenance := time.NewTicker(15 * time.Second)
	defer maintenance.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return shutdown(conn, provider)
		case <-maintenance.C:
			// Moves exhausted deliveries to the DLQ; one caller per database is enough.
			_, _ = conn.MaintenanceFast(ctx)
			n--
			continue
		case <-next.C:
		}
		// The rate swings between 20% and 180% of the mean every 5 minutes.
		phase := 2 * math.Pi * time.Since(start).Seconds() / 300
		next.Reset(time.Duration(float64(time.Second) / (rate * (1 + 0.8*math.Sin(phase)))))
		if err = publish(ctx, conn, pool, n); err != nil && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "publish:", err)
		}
	}
}

func publish(ctx context.Context, conn *postgremq.Connection, pool *pgxpool.Pool, n int) error {
	order, _ := json.Marshal(payload{N: n, Poison: n%97 == 0, Lost: n%151 == 0})
	switch {
	case n%10 == 0:
		// A transactional publish, committed with the application's own writes.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err = conn.PublishWithTx(ctx, tx, "orders", order); err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	case n%7 == 0:
		if _, err := conn.Publish(ctx, "orders", order, postgremq.WithDeliverAfter(time.Now().Add(20*time.Second))); err != nil {
			return err
		}
	default:
		if _, err := conn.Publish(ctx, "orders", order); err != nil {
			return err
		}
	}
	if n%4 == 0 {
		event, _ := json.Marshal(payload{N: n, Slow: n%240 == 0})
		if _, err := conn.Publish(ctx, "events", event); err != nil {
			return err
		}
	}
	switch n % 100 {
	case 0:
		// A topic that does not exist: a failed send (error.type queue_not_found).
		if _, err := conn.Publish(ctx, "missing", order); !errors.Is(err, postgremq.ErrQueueNotFound) {
			return fmt.Errorf("publish to a missing topic: %v", err)
		}
	case 50:
		// A caller deadline that has already passed: a failed send to a real
		// topic (error.type deadline_exceeded).
		expired, cancel := context.WithDeadline(ctx, time.Now())
		defer cancel()
		if _, err := conn.Publish(expired, "orders", order); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("publish past its deadline: %v", err)
		}
	}
	return nil
}

// shutdown drains the queue connection, then flushes its final metrics.
func shutdown(conn *postgremq.Connection, provider *sdkmetric.MeterProvider) error {
	if err := conn.Close(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return provider.ForceFlush(ctx)
}

// sleep waits a random time in [minMs, maxMs] milliseconds, or until ctx ends.
func sleep(ctx context.Context, minMs, maxMs int) {
	d := time.Duration(minMs+rand.IntN(maxMs-minMs+1)) * time.Millisecond
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
