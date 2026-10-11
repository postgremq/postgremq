// Smoke test of the published Go client, built from outside the repository
// (scripts/release/smoke-go.sh): migrate a fresh database, connect (protocol
// check), then publish, consume and ack one message.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"postgremq.dev/postgremq-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("SMOKE_DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := postgremq.Migrate(pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	status, err := postgremq.GetMigrationStatus(pool)
	if err != nil || status.NeedsMigration || status.Dirty {
		log.Fatalf("status %+v: %v", status, err)
	}
	conn, err := postgremq.DialFromPool(ctx, pool)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopic(ctx, "smoke"); err != nil {
		log.Fatal(err)
	}
	if err := conn.CreateQueue(ctx, "smoke-q", "smoke", false); err != nil {
		log.Fatal(err)
	}
	id, err := conn.Publish(ctx, "smoke", []byte(`{"ok":true}`))
	if err != nil {
		log.Fatal(err)
	}
	consumer, err := conn.Consume("smoke-q", postgremq.WithBatchSize(1))
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Stop()
	select {
	case msg := <-consumer.Messages():
		if msg.ID != id {
			log.Fatalf("consumed %d, published %d", msg.ID, id)
		}
		if err := msg.Ack(ctx); err != nil {
			log.Fatal(err)
		}
	case <-ctx.Done():
		log.Fatal("no message")
	}
	fmt.Printf("go smoke ok: protocol majors %v, schema version %d\n",
		postgremq.SupportedProtocolMajors(), status.CurrentVersion)
}
