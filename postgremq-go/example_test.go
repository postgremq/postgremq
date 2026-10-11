package postgremq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExampleConnection_Publish demonstrates publishing a message to a topic.
//
// This example shows the API usage but doesn't execute in tests.
func ExampleConnection_Publish() {
	ctx := context.Background()
	cfg, _ := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/postgres")
	conn, _ := Dial(ctx, cfg)
	defer conn.Close()

	_ = conn.CreateTopic(ctx, "orders")

	payload := json.RawMessage(`{"order_id":12345,"status":"new"}`)
	_, _ = conn.Publish(ctx, "orders", payload)
}

// ExampleConnection_Consume demonstrates creating a consumer and iterating
// through messages.
//
// This example shows the API usage but doesn't execute in tests.
func ExampleConnection_Consume() {
	ctx := context.Background()
	cfg, _ := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/postgres")
	conn, _ := Dial(ctx, cfg)
	defer conn.Close()

	_ = conn.CreateTopic(ctx, "jobs")
	_ = conn.CreateQueue(ctx, "worker-queue", "jobs", false)

	consumer, _ := conn.Consume("worker-queue", WithBatchSize(10), WithVT(30))
	defer consumer.Stop()

	for msg := range consumer.Messages() {
		// process ...
		_ = msg.Ack(ctx)
		// break for example brevity
		break
	}
}

// ExampleMessage_Ack shows acknowledging, negatively acknowledging with a
// delay, and releasing a message.
//
// This example shows the API usage but doesn't execute in tests.
func ExampleMessage_Ack() {
	ctx := context.Background()
	cfg, _ := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/postgres")
	conn, _ := Dial(ctx, cfg)
	defer conn.Close()

	_ = conn.CreateTopic(ctx, "events")
	_ = conn.CreateQueue(ctx, "events-q", "events", false)

	consumer, _ := conn.Consume("events-q", WithVT(60))
	defer consumer.Stop()

	msg := <-consumer.Messages()
	// Acknowledge
	_ = msg.Ack(ctx)

	// Or Nack for retry after 5 seconds
	// _ = msg.Nack(ctx, WithDelayUntil(time.Now().Add(5*time.Second)))

	// Or Release without incrementing attempts
	// _ = msg.Release(ctx)
	_ = time.Second // silence unused import in example
}
