package postgremq_test

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// setupEmptyTestDatabase creates a database WITHOUT the PostgreMQ schema
// This is different from setupTestConnection which pre-initializes the schema
func setupEmptyTestDatabase(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()

	// Create a unique database for this test
	dbName := fmt.Sprintf("pgmq_migrate_test_%s_%d", sanitizeTestName(t.Name()), time.Now().UnixNano())

	// Connect to the server's admin database
	adminConn, err := pgxpool.NewWithConfig(ctx, sharedServer.config)
	require.NoError(t, err, "Failed to connect to postgres")
	defer adminConn.Close()

	// Create the test database
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	require.NoError(t, err, "Failed to create test database")

	// Update the connection config to point to the new database
	testConfig := sharedServer.config.Copy()
	testConfig.ConnConfig.Database = dbName
	testConfig.MaxConns = 10
	testConfig.MinConns = 2

	// Connect to the new test database (WITHOUT initializing schema)
	testPool, err := pgxpool.NewWithConfig(ctx, testConfig)
	require.NoError(t, err, "Failed to connect to test database")

	t.Logf("Created empty test database: %s", dbName)

	// Register cleanup to drop the database when the test is done
	t.Cleanup(func() {
		testPool.Close()
		time.Sleep(100 * time.Millisecond)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = sharedServer.adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			t.Logf("Warning: Failed to drop test database %s: %v", dbName, err)
		}
	})

	return testPool, ctx
}

func TestFormatSchemaVersion(t *testing.T) {
	for version, want := range map[uint]string{
		0:             "0.0.0",
		1:             "0.0.1",
		10_000:        "0.1.0",
		100_020_003:   "1.2.3",
		1_299_999_999: "12.9999.9999",
	} {
		assert.Equal(t, want, postgremq.FormatSchemaVersion(version), "schema version %d", version)
	}
}

func TestMigration_StatusOnEmptyDatabase(t *testing.T) {
	pool, _ := setupEmptyTestDatabase(t)

	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)

	assert.Equal(t, uint(0), status.CurrentVersion, "Empty database should have version 0")
	assert.False(t, status.Dirty, "Empty database should not be dirty")
	assert.True(t, status.NeedsMigration, "Empty database should need migration")
	assert.Greater(t, status.LatestVersion, uint(0), "Latest version should be > 0")
}

func TestMigration_MigrateEmptyDatabase(t *testing.T) {
	pool, _ := setupEmptyTestDatabase(t)

	// Run migration
	err := postgremq.Migrate(pool)
	require.NoError(t, err)

	// Verify migration succeeded
	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)

	assert.Equal(t, status.LatestVersion, status.CurrentVersion, "Should be at the latest version after migration")
	assert.False(t, status.Dirty, "Should not be dirty after successful migration")
	assert.False(t, status.NeedsMigration, "Should not need migration after migrating")
}

func TestMigration_Idempotency(t *testing.T) {
	pool, _ := setupEmptyTestDatabase(t)

	// Run migration first time
	err := postgremq.Migrate(pool)
	require.NoError(t, err)

	// Run migration second time - should be idempotent
	err = postgremq.Migrate(pool)
	require.NoError(t, err, "Running migrate twice should not fail")

	// Verify still at correct version
	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)

	assert.Equal(t, status.LatestVersion, status.CurrentVersion)
	assert.False(t, status.NeedsMigration)
}

func TestMigration_SchemaIsUsable(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Run migration first
	err := postgremq.Migrate(pool)
	require.NoError(t, err)

	// Now create a connection to test message queue operations
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	// Verify the schema is actually usable by performing typical operations
	topicName := "test-topic"
	queueName := "test-queue"

	// Create topic
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Should be able to create topic after migration")

	// Create queue
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Should be able to create queue after migration")

	// Publish message
	msgID, err := conn.Publish(ctx, topicName, []byte(`{"test": "data"}`))
	require.NoError(t, err, "Should be able to publish message after migration")
	assert.Greater(t, msgID, int64(0), "Message ID should be positive")

	// Consume message
	consumer, err := conn.Consume(queueName, postgremq.WithBatchSize(1), postgremq.WithVT(30))
	require.NoError(t, err, "Should be able to create consumer after migration")
	defer consumer.Stop()

	// Wait for message
	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, msgID, msg.ID)
		err = msg.Ack(ctx)
		require.NoError(t, err, "Should be able to ack message after migration")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestMigration_MigrationsTableName(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	// Run migration
	err := postgremq.Migrate(pool)
	require.NoError(t, err)

	// Verify the migrations table uses our custom name
	var tableName string
	err = pool.QueryRow(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'postgremq' AND table_name = 'postgremq_migrations'
	`).Scan(&tableName)
	require.NoError(t, err, "Should find postgremq_migrations table")
	assert.Equal(t, "postgremq_migrations", tableName)

	// Verify the default golang-migrate table was NOT created
	var count int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_name = 'schema_migrations'
	`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Default schema_migrations table should not exist")
}

func TestMigration_NewerDatabaseIsLeftUnchanged(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	require.NoError(t, postgremq.Migrate(pool))

	// A newer client migrated this database past the embedded migrations.
	_, err := pool.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET version = 900000000")
	require.NoError(t, err)

	require.NoError(t, postgremq.Migrate(pool), "a newer database is not an error")

	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	assert.Equal(t, uint(900_000_000), status.CurrentVersion, "the version must not be lowered")
	assert.False(t, status.Dirty)
	assert.False(t, status.NeedsMigration)
}

func TestMigration_DirtyDatabaseFails(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	require.NoError(t, postgremq.Migrate(pool))

	_, err := pool.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET dirty = true")
	require.NoError(t, err)

	err = postgremq.Migrate(pool)
	require.ErrorIs(t, err, postgremq.ErrDirtySchema)
	status, statusErr := postgremq.GetMigrationStatus(pool)
	require.NoError(t, statusErr)
	var dirty *postgremq.DirtySchemaError
	require.ErrorAs(t, err, &dirty)
	assert.Equal(t, status.LatestVersion, dirty.Version)
	assert.Contains(t, err.Error(), fmt.Sprintf("dirty at version %d", status.LatestVersion))
}

func TestMigration_NegativeVersionReadsAsNoVersion(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	require.NoError(t, postgremq.Migrate(pool))

	// golang-migrate records -1 (no version) after a failed first down migration.
	_, err := pool.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET version = -1, dirty = true")
	require.NoError(t, err)

	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	assert.Equal(t, uint(0), status.CurrentVersion)
	assert.True(t, status.Dirty)

	err = postgremq.Migrate(pool)
	var dirty *postgremq.DirtySchemaError
	require.ErrorAs(t, err, &dirty)
	assert.Equal(t, uint(0), dirty.Version)
}

// migrationLockID is golang-migrate's advisory lock key for the version table,
// database.GenerateAdvisoryLockId(db, "postgremq", "postgremq_migrations"),
// which every client and older releases take.
func migrationLockID(database string) int64 {
	return int64(crc32.ChecksumIEEE([]byte("postgremq\x00postgremq_migrations\x00"+database)) * 1_486_364_155)
}

func TestMigration_WaitsForTheLockOtherClientsTake(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	holder, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		_, _ = holder.Exec(ctx, "SELECT pg_advisory_unlock_all()")
		holder.Release()
	}()
	var database string
	require.NoError(t, holder.QueryRow(ctx, "SELECT current_database()").Scan(&database))
	_, err = holder.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID(database))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- postgremq.Migrate(pool) }()
	// Migrate's session waits for that lock.
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted AND classid = 0 AND objid = $1
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))`,
			migrationLockID(database)).Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Migrate did not wait for the lock: %v", err)
	default:
	}
	var installed bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regnamespace('postgremq') IS NOT NULL").Scan(&installed))
	assert.False(t, installed, "nothing is created before the lock is taken")

	_, err = holder.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockID(database))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Migrate did not finish after the lock was released")
	}
	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	assert.Equal(t, status.LatestVersion, status.CurrentVersion)
}

func versionRow(t *testing.T, pool *pgxpool.Pool) (int64, bool) {
	t.Helper()
	var version int64
	var dirty bool
	require.NoError(t, pool.QueryRow(context.Background(),
		"SELECT version, dirty FROM postgremq.postgremq_migrations").Scan(&version, &dirty))
	return version, dirty
}

func tableExists(t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists))
	return exists
}

func TestMigration_UpgradesByThePendingMigrationsOnly(t *testing.T) {
	pool, _ := setupEmptyTestDatabase(t)
	embedded, err := postgremq.EmbeddedMigrations()
	require.NoError(t, err)
	upgrade := []postgremq.TestMigration{
		embedded[0],
		{Version: 2, SQL: "CREATE TABLE postgremq.upgraded (id int);"},
	}

	require.NoError(t, postgremq.MigrateWith(pool, upgrade[:1]))
	version, dirty := versionRow(t, pool)
	assert.Equal(t, int64(1), version)
	assert.False(t, dirty)

	require.NoError(t, postgremq.MigrateWith(pool, upgrade))
	version, dirty = versionRow(t, pool)
	assert.Equal(t, int64(2), version)
	assert.False(t, dirty)
	assert.True(t, tableExists(t, pool, "postgremq.upgraded"))
}

func TestMigration_UpgradesALatestSQLInstall(t *testing.T) {
	pool, _ := setupTestConnection(t) // installs latest.sql
	installed, dirty := versionRow(t, pool)
	require.False(t, dirty)
	embedded, err := postgremq.EmbeddedMigrations()
	require.NoError(t, err)
	// The embedded migrations plus one more, as a later release adds.
	upgrade := append(embedded, postgremq.TestMigration{
		Version: uint64(installed) + 1,
		SQL:     "CREATE TABLE postgremq.upgraded (id int);",
	})

	require.NoError(t, postgremq.MigrateWith(pool, upgrade))

	version, dirty := versionRow(t, pool)
	assert.Equal(t, installed+1, version)
	assert.False(t, dirty)
	assert.True(t, tableExists(t, pool, "postgremq.upgraded"))
}

func TestMigration_RefusesAnUnknownVersion(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	embedded, err := postgremq.EmbeddedMigrations()
	require.NoError(t, err)
	unknown := embedded[1].Version - 1 // between two released migrations
	require.NoError(t, postgremq.MigrateWith(pool, embedded[:1]))
	_, err = pool.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET version = $1", int64(unknown))
	require.NoError(t, err)

	err = postgremq.Migrate(pool)
	require.ErrorIs(t, err, postgremq.ErrUnknownSchemaVersion)
	var unknownErr *postgremq.UnknownSchemaVersionError
	require.ErrorAs(t, err, &unknownErr)
	assert.Equal(t, uint(unknown), unknownErr.Version)
	assert.Contains(t, err.Error(), fmt.Sprintf("database schema version %d (mq %s) is not one of this client's migrations",
		unknown, postgremq.FormatSchemaVersion(uint(unknown))))

	version, dirty := versionRow(t, pool)
	assert.Equal(t, int64(unknown), version, "nothing is applied")
	assert.False(t, dirty)
}

func TestMigration_StatusDoesNotNeedAPoolConnection(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	require.NoError(t, postgremq.Migrate(pool))
	var held []*pgxpool.Conn
	for range pool.Config().MaxConns {
		conn, err := pool.Acquire(ctx)
		require.NoError(t, err)
		held = append(held, conn)
	}
	defer func() {
		for _, conn := range held {
			conn.Release()
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := postgremq.GetMigrationStatus(pool)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("GetMigrationStatus waited for a pool connection")
	}
}

func TestMigration_AFailingMigrationLeavesItsVersionDirtyAndStops(t *testing.T) {
	pool, _ := setupEmptyTestDatabase(t)
	embedded, err := postgremq.EmbeddedMigrations()
	require.NoError(t, err)
	withFailure := []postgremq.TestMigration{
		embedded[0],
		{Version: 2, SQL: "CREATE TABLE postgremq.half_done (id int); SELECT * FROM no_such_table;"},
		{Version: 3, SQL: "CREATE TABLE postgremq.after_broken (id int);"},
	}

	err = postgremq.MigrateWith(pool, withFailure)
	require.ErrorIs(t, err, postgremq.ErrMigrationFailed)
	var migrationErr *postgremq.MigrationError
	require.ErrorAs(t, err, &migrationErr)
	assert.Equal(t, uint(2), migrationErr.Version)
	assert.Equal(t, [2]int{1, 58}, [2]int{migrationErr.Line, migrationErr.Column})
	assert.Contains(t, err.Error(), "no_such_table")

	version, dirty := versionRow(t, pool)
	assert.Equal(t, int64(2), version)
	assert.True(t, dirty)
	assert.False(t, tableExists(t, pool, "postgremq.half_done"), "the failed migration is rolled back")
	assert.False(t, tableExists(t, pool, "postgremq.after_broken"), "later migrations are not run")

	err = postgremq.MigrateWith(pool, withFailure)
	var dirtyErr *postgremq.DirtySchemaError
	require.ErrorAs(t, err, &dirtyErr)
	assert.Equal(t, uint(2), dirtyErr.Version)
	assert.False(t, tableExists(t, pool, "postgremq.after_broken"))
}

// A failed migration's error says where in the file it failed, as
// golang-migrate's did, and keeps the database error.
func TestMigration_AFailingMigrationSaysWhereItFailed(t *testing.T) {
	embedded, err := postgremq.EmbeddedMigrations()
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		sql          string
		code         string
		line, column int
		want         []string
	}{
		"syntax error": {
			// Positions count characters, not bytes.
			sql:  "-- café ☕\nCREATE TABLE postgremq.t (id int);\n  SELEC 1;\n",
			code: "42601", line: 3, column: 3,
			want: []string{"postgremq: migration 9.0.2 (schema version 900000002) failed at line 3, column 3: ", `syntax error at or near "SELEC"`},
		},
		"syntax error at the end of the file": {
			sql:  "CREATE TABLE postgremq.t (id int);\nSELECT (\n",
			code: "42601", line: 3, column: 1,
			want: []string{"failed at line 3, column 1: ", "syntax error at end of input"},
		},
		"error in a function's definition": {
			sql:  "CREATE FUNCTION postgremq.f() RETURNS int LANGUAGE sql AS $$\n  SELECT 1;\n  SELECT x FROM no_such_table;\n$$;\n",
			code: "42P01", line: 3, column: 17,
			want: []string{"failed at line 3, column 17: ", `relation "no_such_table" does not exist`},
		},
		"error while a function runs": {
			sql: "CREATE FUNCTION postgremq.h() RETURNS bigint LANGUAGE plpgsql AS $$\nBEGIN\n" +
				"  RETURN (SELECT count(*) FROM no_such_table);\nEND $$;\nSELECT postgremq.h();\n",
			code: "42P01",
			want: []string{"query: (SELECT count(*) FROM no_such_table)", "context: PL/pgSQL function postgremq.h() line 3 at RETURN"},
		},
		"error with a detail": {
			sql: "CREATE TABLE postgremq.t (id int PRIMARY KEY);\n" +
				"INSERT INTO postgremq.t VALUES (1), (1);\n",
			code: "23505",
			want: []string{"duplicate key value", "detail: Key (id)=(1) already exists."},
		},
		"error in a function body": {
			sql:  "DO $$\nBEGIN\n  RAISE EXCEPTION 'boom' USING HINT = 'look here';\nEND $$;\n",
			code: "P0001",
			want: []string{"boom", "hint: look here", "context: PL/pgSQL function inline_code_block line 3 at RAISE"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			pool, _ := setupEmptyTestDatabase(t)
			err := postgremq.MigrateWith(pool, []postgremq.TestMigration{
				embedded[0],
				{Version: 900_000_002, SQL: tc.sql},
			})
			var migrationErr *postgremq.MigrationError
			require.ErrorAs(t, err, &migrationErr)
			assert.Equal(t, uint(900_000_002), migrationErr.Version)
			assert.Equal(t, [2]int{tc.line, tc.column}, [2]int{migrationErr.Line, migrationErr.Column})
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			assert.Equal(t, tc.code, pgErr.Code)
			for _, want := range tc.want {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

func TestMigration_Concurrent(t *testing.T) {
	// Several instances starting together against an empty database.
	for round := 0; round < 3; round++ {
		pool, _ := setupEmptyTestDatabase(t)
		const migrators = 6
		errs := make([]error, migrators)
		var wg sync.WaitGroup
		for i := range migrators {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = postgremq.Migrate(pool)
			}()
		}
		wg.Wait()
		for i, err := range errs {
			require.NoError(t, err, "round %d, migrator %d", round, i)
		}
		status, err := postgremq.GetMigrationStatus(pool)
		require.NoError(t, err)
		assert.Equal(t, status.LatestVersion, status.CurrentVersion)
		assert.False(t, status.Dirty)
	}
}

func TestMigration_LatestSQLInstallIsCurrent(t *testing.T) {
	pool, _ := setupTestConnection(t) // installs latest.sql

	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	assert.Equal(t, status.LatestVersion, status.CurrentVersion, "latest.sql records its version")
	assert.False(t, status.Dirty)
	assert.False(t, status.NeedsMigration)

	require.NoError(t, postgremq.Migrate(pool), "nothing to apply")
	status, err = postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	assert.Equal(t, status.LatestVersion, status.CurrentVersion)
	assert.False(t, status.Dirty)
}

// PostgreSQL counts an error position in characters of the text converted to
// the database's encoding; the error is located in the file when that is UTF8
// or a single-byte encoding.
func TestMigration_AFailingMigrationIsLocatedInEveryEncoding(t *testing.T) {
	for _, tc := range []struct{ database, client string }{
		{"UTF8", ""},
		{"UTF8", "SQL_ASCII"}, // characters: the UTF8 database takes the bytes as they are
		{"UTF8", "LATIN1"},    // bytes: each one is converted to a character
		{"SQL_ASCII", ""},
		{"SQL_ASCII", "UTF8"}, // bytes: a SQL_ASCII database does not convert
		{"LATIN1", ""},
		{"EUC_JP", "UTF8"}, // characters: converted
	} {
		t.Run(tc.database+"/"+tc.client, func(t *testing.T) {
			pool := encodingTestPool(t, tc.database, tc.client)

			// é is two bytes in UTF-8 and one character in every encoding here.
			err := postgremq.MigrateWith(pool, []postgremq.TestMigration{
				{Version: 900_000_002, SQL: "-- é é\n  SELEC 1;\n"},
			})

			var migrationErr *postgremq.MigrationError
			require.ErrorAs(t, err, &migrationErr)
			assert.Equal(t, [2]int{2, 3}, [2]int{migrationErr.Line, migrationErr.Column})
		})
	}
}

// The server's position counts neither the file's characters (9 here) nor
// its bytes (13 with 㡡㡡) when it is in another multibyte encoding: with
// EUC_JP on both ends it counts the UTF-8 bytes as EUC_JP characters, and
// converting UTF8 into EUC_JIS_2004 combines か and ゚ into one. The error is
// reported without a location rather than a wrong one.
func TestMigration_AFailingMigrationInAnotherMultibyteEncodingIsNotLocated(t *testing.T) {
	for _, tc := range []struct {
		database, client, sql string
		position              int32
	}{
		{"EUC_JP", "EUC_JP", "-- 㡡㡡\n  SELEC 1;\n", 10},
		{"EUC_JIS_2004", "UTF8", "-- \u304b\u309a\n  SELEC 1;\n", 8},
	} {
		t.Run(tc.database+"/"+tc.client, func(t *testing.T) {
			pool := encodingTestPool(t, tc.database, tc.client)

			err := postgremq.MigrateWith(pool, []postgremq.TestMigration{{Version: 900_000_002, SQL: tc.sql}})

			var migrationErr *postgremq.MigrationError
			require.ErrorAs(t, err, &migrationErr)
			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			assert.Equal(t, tc.position, pgErr.Position, "the server reports a position")
			assert.Equal(t, [2]int{0, 0}, [2]int{migrationErr.Line, migrationErr.Column})
			assert.NotContains(t, err.Error(), "at line")
		})
	}
}

// encodingTestPool is a pool on a new empty database in encoding, with
// client_encoding set, or the database's when it is empty.
func encodingTestPool(t *testing.T, encoding, clientEncoding string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("pgmq_encoding_%s_%d", strings.ToLower(encoding), time.Now().UnixNano())
	_, err := sharedServer.adminPool.Exec(ctx, fmt.Sprintf(
		"CREATE DATABASE %s ENCODING '%s' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0", name, encoding))
	require.NoError(t, err)
	cfg := sharedServer.config.Copy()
	cfg.ConnConfig.Database = name
	delete(cfg.ConnConfig.RuntimeParams, "client_encoding") // POSTGREMQ_TEST_DATABASE_URL may set one
	if clientEncoding != "" {
		cfg.ConnConfig.RuntimeParams["client_encoding"] = clientEncoding
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Close()
		_, _ = sharedServer.adminPool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	var got string
	require.NoError(t, pool.QueryRow(ctx, "SHOW client_encoding").Scan(&got))
	if clientEncoding == "" {
		clientEncoding = encoding
	}
	require.Equal(t, clientEncoding, got, "client_encoding")
	return pool
}
