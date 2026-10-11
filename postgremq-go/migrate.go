package postgremq

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

const MigrationsTable = "postgremq_migrations"

// migrationsFS holds the schema migrations: a copy of mq/migrations under
// numbered names (<schema version>_<mq version>.up.sql) that `make sync` keeps
// in step with it (a module cannot embed files outside its directory). Do not
// edit them here.
//
//go:embed migrations/*.up.sql
var migrationsFS embed.FS

// MigrationStatus represents the current state of migrations. Versions are
// schema versions: an mq version stored as one number, major*10^8 +
// minor*10^4 + patch; FormatSchemaVersion gives the mq version.
type MigrationStatus struct {
	CurrentVersion uint
	Dirty          bool
	LatestVersion  uint
	NeedsMigration bool
}

// FormatSchemaVersion returns the mq version (major.minor.patch) of a schema
// version, the number the version table stores: 10000 is "0.1.0".
func FormatSchemaVersion(version uint) string { return formatSchemaVersion(uint64(version)) }

func formatSchemaVersion(v uint64) string {
	return fmt.Sprintf("%d.%d.%d", v/100_000_000, v/10_000%10_000, v%10_000)
}

// Migrate applies the embedded migrations the database has not applied yet,
// up to the latest embedded version. It only migrates up: a database already
// at a newer version (migrated by a newer client) is left unchanged and Migrate
// returns nil. A dirty database (a migration failed partway) is an error.
// This is a standalone function for schema management, separate from
// the Connection type which is used for message queue operations.
//
// No ctx parameter: golang-migrate's API doesn't accept one, so a ctx
// would be ignored anyway.
func Migrate(pool *pgxpool.Pool) error {
	source, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create a sql.DB connection using pgx stdlib driver
	db := openMigrationDB(pool)
	defer db.Close()

	// The driver creates its version table before executing migration SQL.
	if err := ensureSchema(db); err != nil {
		return fmt.Errorf("failed to create queue schema: %w", err)
	}

	// Create the database driver with our custom config
	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{
		MigrationsTable: MigrationsTable,
		SchemaName:      "postgremq",
	})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	migErr := m.Up()
	if migErr == nil || errors.Is(migErr, migrate.ErrNoChange) {
		return nil
	}
	// golang-migrate fails when the recorded version is not one of the
	// embedded migrations. A clean version above ours means a newer client
	// already migrated the database: nothing to do.
	if errors.Is(migErr, os.ErrNotExist) {
		if version, dirty, err := m.Version(); err == nil && !dirty && version > getLatestMigrationVersion() {
			return nil
		}
	}
	return fmt.Errorf("migration failed: %w", migErr)
}

// ensureSchema creates the postgremq schema if it is missing, under the
// migration lock (the advisory lock golang-migrate takes for the version
// table, shared with the other clients). CREATE SCHEMA IF NOT EXISTS is not
// safe against a concurrent creator, and it needs the CREATE privilege on the
// database even when the schema exists, so it only runs when the schema is
// missing.
func ensureSchema(db *sql.DB) (err error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	// db is private to Migrate and closed when it returns, which ends the
	// session (and the lock) even if the unlock below fails.
	defer conn.Close()

	var dbName string
	if err := conn.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		return err
	}
	lockID, err := database.GenerateAdvisoryLockId(dbName, "postgremq", MigrationsTable)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return err
	}
	defer func() {
		if _, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	var missing bool
	if err := conn.QueryRowContext(ctx, "SELECT to_regnamespace('postgremq') IS NULL").Scan(&missing); err != nil {
		return err
	}
	if missing {
		_, err = conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS postgremq")
	}
	return err
}

// GetMigrationStatus returns current migration status using the provided pool.
// This is a standalone function for schema management, separate from
// the Connection type which is used for message queue operations.
func GetMigrationStatus(pool *pgxpool.Pool) (*MigrationStatus, error) {
	db := openMigrationDB(pool)
	defer db.Close()

	status := &MigrationStatus{LatestVersion: getLatestMigrationVersion()}
	var exists bool
	if err := db.QueryRow("SELECT to_regclass('postgremq.postgremq_migrations') IS NOT NULL").Scan(&exists); err != nil {
		return nil, err
	}
	if exists {
		err := db.QueryRow("SELECT version, dirty FROM postgremq.postgremq_migrations LIMIT 1").Scan(&status.CurrentVersion, &status.Dirty)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}
	status.NeedsMigration = status.CurrentVersion < status.LatestVersion
	return status, nil
}

// openMigrationDB preserves the pool's connection parameters without changing
// its search_path or registering a global connection configuration.
func openMigrationDB(pool *pgxpool.Pool) *sql.DB {
	cfg := pool.Config().ConnConfig.Copy()
	return stdlib.OpenDB(*cfg)
}

// getLatestMigrationVersion returns the highest migration version from embedded files
func getLatestMigrationVersion() uint {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return 0
	}

	var latestVersion uint
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		// Parse version from filename like "000010000_0.1.0.up.sql"
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 1 {
			continue
		}

		ver, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			continue
		}

		if uint(ver) > latestVersion {
			latestVersion = uint(ver)
		}
	}

	return latestVersion
}
