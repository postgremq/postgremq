package main

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"postgremq.dev/postgremq-go"
)

func runMigrate(_ context.Context, pool *pgxpool.Pool, out io.Writer) error {
	status, err := postgremq.GetMigrationStatus(pool)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	fmt.Fprintf(out, "Current version: %d\n", status.CurrentVersion)
	fmt.Fprintf(out, "Latest version:  %d\n", status.LatestVersion)

	if status.Dirty {
		return fmt.Errorf("database is in dirty state - manual intervention required")
	}

	if !status.NeedsMigration {
		fmt.Fprintln(out, "✓ Database is up to date")
		return nil
	}

	fmt.Fprintln(out, "Running migrations...")
	if err := postgremq.Migrate(pool); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	fmt.Fprintln(out, "✓ Migration completed successfully")
	return nil
}
