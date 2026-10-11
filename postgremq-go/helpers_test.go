package postgremq_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type postgresContainer struct {
	container testcontainers.Container
	config    *pgxpool.Config
	adminPool *pgxpool.Pool
}

// Global variables
var (
	sharedContainer *postgresContainer
)

// TestMain handles setup and teardown for all tests in this package
func TestMain(m *testing.M) {
	// Setup
	ctx := context.Background()
	var err error

	// Set up the container before any tests run
	sharedContainer, err = newPostgresContainer(ctx)
	if err != nil {
		fmt.Printf("Failed to start postgres container: %v\n", err)
		os.Exit(1)
	}

	// Run the tests
	exitCode := m.Run()

	// Teardown - clean up the container after all tests finish
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sharedContainer.cleanup(cleanupCtx); err != nil {
		fmt.Printf("Warning: Failed to cleanup postgres container: %v\n", err)
	}

	os.Exit(exitCode)
}

// skipIfShort skips a test if we're running in short mode
func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}
}

func newPostgresContainer(ctx context.Context) (*postgresContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		Cmd: []string{
			"postgres",
			"-c", "max_connections=200", // Increase max connections for parallel tests
		},
		WaitingFor: wait.ForAll(
			// First, we wait for the container to log readiness twice.
			// This is because it will restart itself after the first startup.
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			// Then, we wait for docker to actually serve the port on localhost.
			// For non-linux OSes like Mac and Windows, Docker or Rancher Desktop will have to start a separate proxy.
			// Without this, the tests will be flaky on those OSes!
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get host: %w", err)
	}

	connString := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres", host, mappedPort.Port())
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}
	// Increase pool size for concurrent test scenarios
	// Admin pool needs many connections since all tests try to create databases in parallel
	config.MaxConns = 100
	config.MinConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create a pool: %w", err)
	}

	return &postgresContainer{
		container: container,
		config:    config,
		adminPool: pool,
	}, nil
}

func (pc *postgresContainer) cleanup(ctx context.Context) error {
	if pc.adminPool != nil {
		pc.adminPool.Close()
	}
	return pc.container.Terminate(ctx)
}

// createTestDatabase creates a unique database for the test
func createTestDatabase(t *testing.T, ctx context.Context) (*pgxpool.Pool, string, error) {
	// Create a unique database name for this test
	dbName := fmt.Sprintf("pgmq_test_%s_%d", sanitizeTestName(t.Name()), time.Now().UnixNano())

	// Connect to the container's default postgres database
	adminConn, err := pgxpool.NewWithConfig(ctx, sharedContainer.config)
	if err != nil {
		return nil, "", fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer adminConn.Close()

	// Create the test database
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		return nil, "", fmt.Errorf("failed to create test database: %w", err)
	}

	// Update the connection config to point to the new database
	testConfig := sharedContainer.config.Copy()
	testConfig.ConnConfig.Database = dbName
	// Increase pool size for concurrent test scenarios
	testConfig.MaxConns = 50
	testConfig.MinConns = 5

	// Connect to the new test database
	testPool, err := pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		// Try to clean up the database if we failed to connect
		_, _ = adminConn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s", dbName))
		return nil, "", fmt.Errorf("failed to connect to test database: %w", err)
	}

	return testPool, dbName, nil
}

// sanitizeTestName converts test name to a valid PostgreSQL identifier
func sanitizeTestName(name string) string {
	// Replace invalid characters with underscores
	replacer := strings.NewReplacer(
		"/", "_",
		" ", "_",
		"(", "",
		")", "",
		"#", "",
		".", "_",
	)
	sanitized := replacer.Replace(name)

	// Ensure the name is not too long for PostgreSQL
	if len(sanitized) > 50 {
		sanitized = sanitized[:50]
	}

	return strings.ToLower(sanitized)
}

// setupTestConnection returns a unique database connection for each test
func setupTestConnection(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()

	// Create a unique database for this test
	testPool, dbName, err := createTestDatabase(t, ctx)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	t.Logf("Created test database: %s", dbName)
	// Now initialize the database schema with more robust error handling
	_, err = testPool.Exec(ctx, latestSQL(t))
	if err != nil {
		testPool.Close()
		t.Fatalf("failed to initialize schema: %v", err)
	}
	// Register cleanup to drop the database when the test is done
	t.Cleanup(func() {

		// Close the test pool first to release all connections
		testPool.Close()

		// Allow a small delay to ensure connections are fully closed
		time.Sleep(100 * time.Millisecond)

		// Use a separate context for cleanup to avoid canceled contexts
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// drop the database
		_, err = sharedContainer.adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))

		if err != nil {
			t.Logf("Warning: Failed to drop test database %s: %v", dbName, err)
		}
	})

	return testPool, ctx
}

// Since we use a separate database for each test, this function is primarily needed
// for subtests (t.Run) that share the same database connection
func cleanTestData(t *testing.T, pool *pgxpool.Pool, ctx context.Context) {
	// Important: We need to clean data for subtests (t.Run) that share the same database
	t.Helper()

	_, err := pool.Exec(ctx, `
		DELETE FROM postgremq.dead_letter_queue;
		DELETE FROM postgremq.queue_messages;
		DELETE FROM postgremq.messages;
		DELETE FROM postgremq.queues;
		DELETE FROM postgremq.topics;
	`)
	if err != nil {
		t.Logf("Warning: Failed to clean test data: %v", err)
	}
}

type TestLogger struct {
	t *testing.T
}

func (t TestLogger) Printf(format string, v ...interface{}) {
	t.t.Logf(format, v...)
}

// latestSQL is the fresh-install schema, read from the repository (tests run
// in it; the module does not ship latest.sql).
func latestSQL(t *testing.T) string {
	t.Helper()
	sql, err := os.ReadFile("../mq/sql/latest.sql")
	if err != nil {
		t.Fatalf("read latest.sql: %v", err)
	}
	return string(sql)
}
