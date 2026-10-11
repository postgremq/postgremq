package main

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"postgremq.dev/postgremq-go"
)

func runStatus(_ context.Context, pool *pgxpool.Pool, out io.Writer) error {
	status, err := postgremq.GetMigrationStatus(pool)
	if err != nil {
		return fmt.Errorf("failed to get migration status: %w", err)
	}

	fmt.Fprintf(out, "Current version: %d\n", status.CurrentVersion)
	fmt.Fprintf(out, "Latest version:  %d\n", status.LatestVersion)
	fmt.Fprintf(out, "Dirty:           %v\n", status.Dirty)

	if status.NeedsMigration {
		pending := status.LatestVersion - status.CurrentVersion
		fmt.Fprintf(out, "\n⚠ Migration needed: %d pending migration(s)\n", pending)
	} else {
		fmt.Fprintln(out, "\n✓ Database is up to date")
	}

	return nil
}
