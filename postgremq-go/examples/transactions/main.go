package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"postgremq.dev/postgremq-go"
)

func main() {
	// Get database URL from environment or use default
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	ctx := context.Background()

	// Create PostgreSQL pool directly
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Connect PostgreMQ using existing pool
	conn, err := postgremq.DialFromPool(ctx, pool)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("Connected to PostgreMQ")

	// Setup
	topicName := "orders"
	queueName := "order-processor"

	// CreateTopic and CreateQueue are idempotent on identical parameters.
	if err := conn.CreateTopic(ctx, topicName); err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}
	if err := conn.CreateQueue(ctx, queueName, topicName, false); err != nil {
		log.Fatalf("CreateQueue failed: %v", err)
	}

	// Example 1: Publish within transaction
	log.Println("\n=== Publishing within transaction ===")
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// You could perform other database operations here
	// _, err = tx.Exec(ctx, "INSERT INTO orders ...")

	// Publish message within transaction
	payload := json.RawMessage(`{"order_id": 100, "status": "new"}`)
	messageID, err := conn.PublishWithTx(ctx, tx, topicName, payload)
	if err != nil {
		tx.Rollback(ctx)
		log.Fatalf("Failed to publish: %v", err)
	}

	log.Printf("Published message ID %d within transaction", messageID)

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}

	log.Println("Transaction committed successfully")

	// Example 2: Consume and acknowledge within transaction
	log.Println("\n=== Consuming and acknowledging within transaction ===")

	consumer, err := conn.Consume(queueName,
		postgremq.WithBatchSize(1),
		postgremq.WithVT(30))
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Stop()

	// Get one message
	msg := <-consumer.Messages()
	log.Printf("Received message ID %d: %s", msg.ID, string(msg.Payload))

	// Start transaction for acknowledgment
	tx, err = pool.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// You could perform database operations based on the message
	// _, err = tx.Exec(ctx, "UPDATE orders SET status='processed' WHERE id=$1", orderID)

	// Acknowledge within transaction
	if err := msg.AckWithTx(ctx, tx); err != nil {
		tx.Rollback(ctx)
		log.Fatalf("Failed to ack: %v", err)
	}

	log.Println("Acknowledged within transaction")

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}

	log.Println("Transaction committed - message processing complete")

	// Example 3: Transaction rollback
	log.Println("\n=== Transaction rollback example ===")

	tx, err = pool.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	payload = json.RawMessage(`{"order_id": 200, "status": "pending"}`)
	messageID, err = conn.PublishWithTx(ctx, tx, topicName, payload)
	if err != nil {
		tx.Rollback(ctx)
		log.Fatalf("Failed to publish: %v", err)
	}

	log.Printf("Published message ID %d within transaction", messageID)

	// Simulate error - rollback transaction
	log.Println("Simulating error - rolling back transaction")
	if err := tx.Rollback(ctx); err != nil {
		log.Printf("Rollback error: %v", err)
	}

	log.Println("Transaction rolled back - message was not published")

	fmt.Println("\nAll transaction examples completed successfully")
}
