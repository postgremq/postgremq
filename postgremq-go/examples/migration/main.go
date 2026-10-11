// Example: Programmatic Database Migration
//
// This example demonstrates how to apply PostgreMQ database migrations
// programmatically from your Go application. This is useful for:
//
//   - Ensuring the database schema is up-to-date before starting your application
//   - Running migrations as part of your deployment process
//   - Integrating schema management into your application lifecycle
//
// For CLI-based migrations, see the postgremq CLI in postgremq-go/cmd/postgremq/.
package main

import (
	"context"
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

	// Create a connection pool
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	log.Println("Connected to database")

	// Check current migration status
	status, err := postgremq.GetMigrationStatus(pool)
	if err != nil {
		log.Fatalf("Failed to get migration status: %v", err)
	}

	fmt.Printf("Current schema version: %s\n", postgremq.FormatSchemaVersion(status.CurrentVersion))
	fmt.Printf("Latest available version: %s\n", postgremq.FormatSchemaVersion(status.LatestVersion))
	fmt.Printf("Schema is dirty: %v\n", status.Dirty)

	// Check if migration is needed
	if !status.NeedsMigration {
		log.Println("Database schema is up to date. No migration needed.")
		return
	}

	// Handle dirty state (requires manual intervention)
	if status.Dirty {
		log.Fatal("Database is in a dirty state. Manual intervention required. " +
			"This usually means a previous migration failed partway through.")
	}

	fmt.Printf("\nMigration needed: %s -> %s\n",
		postgremq.FormatSchemaVersion(status.CurrentVersion), postgremq.FormatSchemaVersion(status.LatestVersion))
	fmt.Println("Applying migrations...")

	// Apply migrations
	if err := postgremq.Migrate(pool); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	// Verify the migration succeeded
	newStatus, err := postgremq.GetMigrationStatus(pool)
	if err != nil {
		log.Fatalf("Failed to verify migration: %v", err)
	}

	fmt.Printf("\nMigration completed successfully!\n")
	fmt.Printf("New schema version: %s\n", postgremq.FormatSchemaVersion(newStatus.CurrentVersion))

	// Now you can use PostgreMQ normally
	log.Println("\nPostgreMQ is ready to use.")

	// Example: Create a connection and verify everything works
	conn, err := postgremq.DialFromPool(ctx, pool)
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	if err := conn.CreateTopic(ctx, "test-topic"); err != nil {
		log.Printf("Note: Could not create test topic (may already exist): %v", err)
	} else {
		log.Println("Successfully created test topic")
		// Clean up
		_ = conn.DeleteTopic(ctx, "test-topic")
	}
}
