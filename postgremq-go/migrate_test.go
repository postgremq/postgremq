package postgremq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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

	// Connect to the container's default postgres database
	adminConn, err := pgxpool.NewWithConfig(ctx, sharedContainer.config)
	require.NoError(t, err, "Failed to connect to postgres")
	defer adminConn.Close()

	// Create the test database
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	require.NoError(t, err, "Failed to create test database")

	// Update the connection config to point to the new database
	testConfig := sharedContainer.config.Copy()
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

		_, err = sharedContainer.adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			t.Logf("Warning: Failed to drop test database %s: %v", dbName, err)
		}
	})

	return testPool, ctx
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
	_, err := pool.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET version = 999")
	require.NoError(t, err)

	require.NoError(t, postgremq.Migrate(pool), "a newer database is not an error")

	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	assert.Equal(t, uint(999), status.CurrentVersion, "the version must not be lowered")
	assert.False(t, status.Dirty)
	assert.False(t, status.NeedsMigration)
}

func TestMigration_DirtyDatabaseFails(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)
	require.NoError(t, postgremq.Migrate(pool))

	_, err := pool.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET dirty = true")
	require.NoError(t, err)

	err = postgremq.Migrate(pool)
	require.Error(t, err)
	status, statusErr := postgremq.GetMigrationStatus(pool)
	require.NoError(t, statusErr)
	assert.Contains(t, err.Error(), fmt.Sprintf("Dirty database version %d", status.LatestVersion))
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
