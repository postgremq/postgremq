package main

import (
	"context"
	"errors"
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

	fmt.Fprintf(out, "Current version: %s\n", describeVersion(status.CurrentVersion))
	fmt.Fprintf(out, "Latest version:  %s\n", describeVersion(status.LatestVersion))

	// The status is read without the migration lock, so a dirty flag may be
	// a migration still in progress: Migrate waits for it and decides.
	if !status.NeedsMigration && !status.Dirty {
		fmt.Fprintln(out, "✓ Database is up to date")
		return nil
	}

	fmt.Fprintln(out, "Running migrations...")
	if err := postgremq.Migrate(pool); err != nil {
		switch {
		case errors.Is(err, postgremq.ErrDirtySchema):
			return fmt.Errorf("database is in dirty state - manual intervention required")
		case errors.Is(err, postgremq.ErrMigrationFailed), errors.Is(err, postgremq.ErrUnknownSchemaVersion):
			return err
		}
		return fmt.Errorf("migration failed: %w", err)
	}

	fmt.Fprintln(out, "✓ Migration completed successfully")
	return nil
}
