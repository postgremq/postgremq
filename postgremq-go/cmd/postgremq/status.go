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

	fmt.Fprintf(out, "Current version: %s\n", describeVersion(status.CurrentVersion))
	fmt.Fprintf(out, "Latest version:  %s\n", describeVersion(status.LatestVersion))
	fmt.Fprintf(out, "Dirty:           %v\n", status.Dirty)

	if status.NeedsMigration {
		fmt.Fprintf(out, "\n⚠ Migration needed: %s -> %s\n",
			describeVersion(status.CurrentVersion), postgremq.FormatSchemaVersion(status.LatestVersion))
	} else {
		fmt.Fprintln(out, "\n✓ Database is up to date")
	}

	return nil
}

// describeVersion shows a schema version as its mq version and the number
// the database stores; 0 is an empty database.
func describeVersion(version uint) string {
	if version == 0 {
		return "none (schema version 0)"
	}
	return fmt.Sprintf("%s (schema version %d)", postgremq.FormatSchemaVersion(version), version)
}
