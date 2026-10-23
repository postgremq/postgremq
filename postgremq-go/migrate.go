package postgremq

import (
	"cmp"
	"context"
	"embed"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
	// NeedsMigration is CurrentVersion < LatestVersion. A dirty database may
	// report false; Migrate refuses it either way. It is also true for a
	// version below the latest that none of the migrations knows, which
	// Migrate refuses.
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
// returns nil. Its errors are typed (see ErrMigrationFailed, ErrDirtySchema
// and ErrUnknownSchemaVersion):
//
//   - a migration that fails is a *MigrationError, with its version and,
//     when PostgreSQL reports a position it can map, the line and column in
//     its file; its version is left dirty;
//   - a dirty database (a migration failed partway) is a *DirtySchemaError;
//   - a recorded version below the latest that is none of the migrations is
//     an *UnknownSchemaVersionError; nothing is applied.
//
// This is a standalone function for schema management, separate from
// the Connection type which is used for message queue operations.
//
// Concurrent callers, in any client and in releases that applied migrations
// with golang-migrate, are serialized by the advisory lock golang-migrate
// takes for the version table; waiting for it has no time limit.
func Migrate(pool *pgxpool.Pool) error {
	migrations, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("failed to read the embedded migrations: %w", err)
	}
	return runMigrations(context.Background(), pool, migrations)
}

// migration is an embedded up migration.
type migration struct {
	version uint64
	sql     string
}

// loadMigrations returns the embedded migrations in version order.
var loadMigrations = sync.OnceValues(func() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		// <schema version>_<mq version>.up.sql
		prefix, _, _ := strings.Cut(entry.Name(), "_")
		version, err := strconv.ParseUint(prefix, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migration %s: %w", entry.Name(), err)
		}
		sql, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, migration{version: version, sql: string(sql)})
	}
	slices.SortFunc(migrations, func(a, b migration) int { return cmp.Compare(a.version, b.version) })
	return migrations, nil
})

// runMigrations applies the migrations above the recorded version, under the
// migration lock and golang-migrate's dirty-flag protocol (the other clients
// and the CLI use the same).
func runMigrations(ctx context.Context, pool *pgxpool.Pool, migrations []migration) error {
	// A session of its own, not one from the pool: it holds a session-level
	// advisory lock, which closing it releases.
	conn, err := connectMigrationSession(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer closeMigrationSession(conn)

	var database string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return fmt.Errorf("failed to read the database name: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID(database)); err != nil {
		return fmt.Errorf("failed to take the migration lock: %w", err)
	}
	if err := ensureVersionTable(ctx, conn); err != nil {
		return fmt.Errorf("failed to create queue schema: %w", err)
	}
	current, dirty, err := readVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to read the schema version: %w", err)
	}
	if dirty {
		return &DirtySchemaError{Version: uint(current)}
	}
	// A version below the latest that is none of the migrations is not a
	// state they can upgrade from. A version above it was recorded by a
	// newer client: nothing to do.
	if current != 0 && len(migrations) > 0 && current < migrations[len(migrations)-1].version &&
		!slices.ContainsFunc(migrations, func(m migration) bool { return m.version == current }) {
		return &UnknownSchemaVersionError{Version: uint(current)}
	}
	positions := positionUnitOf(
		conn.PgConn().ParameterStatus("server_encoding"), conn.PgConn().ParameterStatus("client_encoding"))
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if err := setVersion(ctx, conn, m.version, true); err != nil {
			return fmt.Errorf("failed to record schema version %d: %w", m.version, err)
		}
		// No arguments: the simple query protocol runs the whole file in one
		// implicit transaction.
		if _, err := conn.Exec(ctx, m.sql); err != nil {
			return migrationError(m, err, positions)
		}
		if err := setVersion(ctx, conn, m.version, false); err != nil {
			return fmt.Errorf("failed to record schema version %d: %w", m.version, err)
		}
	}
	return nil
}

// migrationError is a *MigrationError for m's err, located in m's file when
// the server reports a position.
func migrationError(m migration, err error, positions positionUnit) error {
	failed := &MigrationError{Version: uint(m.version), Err: err}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if line, column, ok := sqlPosition(m.sql, int(pgErr.Position), positions); ok {
			failed.Line, failed.Column = line, column
		}
	}
	return failed
}

// positionUnit is what a PostgreSQL error position counts in the UTF-8
// migration text.
type positionUnit int

const (
	// positionsUnmapped: characters of another multibyte encoding, which
	// sqlPosition does not decode.
	positionsUnmapped positionUnit = iota
	positionsInCharacters
	positionsInBytes
)

// positionUnitOf returns what the server counts an error position in. It
// counts characters of the text in the database's encoding, converted from
// client_encoding unless that is the database's or SQL_ASCII (taken as it
// is); conversion keeps the characters, except into EUC_JIS_2004, which
// combines some pairs into one. A SQL_ASCII database counts bytes.
func positionUnitOf(serverEncoding, clientEncoding string) positionUnit {
	switch serverEncoding {
	case "SQL_ASCII":
		return positionsInBytes
	case "EUC_JIS_2004":
		return positionsUnmapped
	}
	if clientEncoding == "UNICODE" { // UTF8's alias, reported as it was set
		clientEncoding = "UTF8"
	}
	encoding := clientEncoding
	if clientEncoding == serverEncoding || clientEncoding == "SQL_ASCII" {
		encoding = serverEncoding
	}
	switch encoding {
	case "UTF8":
		return positionsInCharacters
	case "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5", "LATIN6", "LATIN7", "LATIN8", "LATIN9", "LATIN10",
		"ISO_8859_5", "ISO_8859_6", "ISO_8859_7", "ISO_8859_8", "KOI8R", "KOI8U", "WIN866", "WIN874",
		"WIN1250", "WIN1251", "WIN1252", "WIN1253", "WIN1254", "WIN1255", "WIN1256", "WIN1257", "WIN1258":
		return positionsInBytes // each byte is one character
	}
	return positionsUnmapped
}

// sqlPosition returns the line and column, from 1 and the column in
// characters, of a PostgreSQL error position in sql. The position counts from
// 1 in positions' unit; one past the end is the end of the input. 0 is no
// position.
func sqlPosition(sql string, position int, positions positionUnit) (line, column int, ok bool) {
	if position <= 0 || positions == positionsUnmapped {
		return 0, 0, false
	}
	offset := position - 1 // in bytes
	if positions == positionsInCharacters {
		offset = len(sql)
		for characters, i := 0, 0; i < len(sql); characters++ {
			if characters == position-1 {
				offset = i
				break
			}
			_, size := utf8.DecodeRuneInString(sql[i:])
			i += size
		}
		if offset == len(sql) && utf8.RuneCountInString(sql) != position-1 {
			return 0, 0, false
		}
	}
	if offset > len(sql) {
		return 0, 0, false
	}
	before := sql[:offset]
	line = strings.Count(before, "\n") + 1
	column = utf8.RuneCountInString(before[strings.LastIndexByte(before, '\n')+1:]) + 1
	return line, column, true
}

// connectMigrationSession opens a connection with the pool's connection
// parameters, outside the pool.
func connectMigrationSession(ctx context.Context, pool *pgxpool.Pool) (*pgx.Conn, error) {
	return pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
}

// closeMigrationSession closes a connection from connectMigrationSession,
// waiting at most 5 seconds for the server.
func closeMigrationSession(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}

// migrationLockSalt is golang-migrate's salt for advisory lock keys.
const migrationLockSalt = 1_486_364_155

// migrationLockID is golang-migrate's advisory lock key for the version table,
// database.GenerateAdvisoryLockId(database, "postgremq", MigrationsTable): the
// CRC-32 (IEEE) of "postgremq\x00postgremq_migrations\x00<database>", times its
// salt.
func migrationLockID(database string) int64 {
	return int64(crc32.ChecksumIEEE([]byte("postgremq\x00"+MigrationsTable+"\x00"+database)) * migrationLockSalt)
}

// ensureVersionTable creates the postgremq schema and the version table if
// they are missing. It runs under the migration lock, since CREATE ... IF NOT
// EXISTS is not safe against a concurrent creator, and checks first, since
// CREATE SCHEMA needs the CREATE privilege on the database even when the
// schema exists.
func ensureVersionTable(ctx context.Context, conn *pgx.Conn) error {
	var schemaExists, tableExists bool
	if err := conn.QueryRow(ctx, `SELECT to_regnamespace('postgremq') IS NOT NULL,
		to_regclass('postgremq.postgremq_migrations') IS NOT NULL`).Scan(&schemaExists, &tableExists); err != nil {
		return err
	}
	if !schemaExists {
		if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS postgremq"); err != nil {
			return err
		}
	}
	if !tableExists {
		if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS postgremq.postgremq_migrations (version bigint not null primary key, dirty boolean not null)"); err != nil {
			return err
		}
	}
	return nil
}

// readVersion returns the recorded version and dirty flag; 0 when there is
// none. The version table must exist.
func readVersion(ctx context.Context, conn *pgx.Conn) (uint64, bool, error) {
	var version int64
	var dirty bool
	err := conn.QueryRow(ctx, "SELECT version, dirty FROM postgremq.postgremq_migrations LIMIT 1").Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	// golang-migrate records -1 (no version) after a failed first down
	// migration.
	return uint64(max(version, 0)), dirty, nil
}

// setVersion replaces the version row in one transaction.
func setVersion(ctx context.Context, conn *pgx.Conn, version uint64, dirty bool) error {
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "TRUNCATE postgremq.postgremq_migrations"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO postgremq.postgremq_migrations (version, dirty) VALUES ($1, $2)", int64(version), dirty)
		return err
	})
}

// GetMigrationStatus returns current migration status using the provided pool.
// This is a standalone function for schema management, separate from
// the Connection type which is used for message queue operations.
func GetMigrationStatus(pool *pgxpool.Pool) (*MigrationStatus, error) {
	ctx := context.Background()
	// Not from the pool, so the status can be read while the application
	// holds all of its connections.
	conn, err := connectMigrationSession(ctx, pool)
	if err != nil {
		return nil, err
	}
	defer closeMigrationSession(conn)

	status := &MigrationStatus{LatestVersion: getLatestMigrationVersion()}
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('postgremq.postgremq_migrations') IS NOT NULL").Scan(&exists); err != nil {
		return nil, err
	}
	if exists {
		version, dirty, err := readVersion(ctx, conn)
		if err != nil {
			return nil, err
		}
		status.CurrentVersion, status.Dirty = uint(version), dirty
	}
	status.NeedsMigration = status.CurrentVersion < status.LatestVersion
	return status, nil
}

// getLatestMigrationVersion returns the highest embedded migration version.
func getLatestMigrationVersion() uint {
	migrations, err := loadMigrations()
	if err != nil || len(migrations) == 0 {
		return 0
	}
	return uint(migrations[len(migrations)-1].version)
}
