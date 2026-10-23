package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"postgremq.dev/postgremq-go/internal/testserver"
)

type testServer struct {
	server *testserver.Server
	pool   *pgxpool.Pool
}

var (
	sharedServer *testServer
	cliBinary    string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Build the CLI binary
	tmpBinary, err := buildCLI()
	if err != nil {
		fmt.Printf("Failed to build CLI: %v\n", err)
		os.Exit(1)
	}
	cliBinary = tmpBinary

	// Connect to POSTGREMQ_TEST_DATABASE_URL, or start a PostgreSQL container
	sharedServer, err = newTestServer(ctx)
	if err != nil {
		fmt.Printf("Failed to start the test server: %v\n", err)
		os.RemoveAll(cliBinary)
		os.Exit(1)
	}

	// Run tests
	exitCode := m.Run()

	// Cleanup
	sharedServer.cleanup()
	os.RemoveAll(cliBinary)

	os.Exit(exitCode)
}

func buildCLI() (string, error) {
	// Create temp dir for the binary
	tmpDir, err := os.MkdirTemp("", "postgremq-cli-test")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	binaryPath := tmpDir + "/postgremq"

	// Build the CLI
	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("failed to build CLI: %w\nOutput: %s", err, output)
	}

	return binaryPath, nil
}

func newTestServer(ctx context.Context) (*testServer, error) {
	server, err := testserver.Start(ctx)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(ctx, server.URL)
	if err != nil {
		_ = server.Close()
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	return &testServer{
		server: server,
		pool:   pool,
	}, nil
}

func (ts *testServer) cleanup() error {
	if ts.pool != nil {
		ts.pool.Close()
	}
	return ts.server.Close()
}

func createTestDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	dbName := fmt.Sprintf("cli_test_%d", time.Now().UnixNano())

	_, err := sharedServer.pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = sharedServer.pool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	})

	// Build DSN for the test database
	dsn, err := testserver.DatabaseURL(sharedServer.server.URL, dbName)
	require.NoError(t, err)
	return dsn
}

// exitCode is the CLI's exit status in a runCLI error.
func exitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func runCLI(args ...string) (string, string, error) {
	cmd := exec.Command(cliBinary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// versions reads the "Current version" and "Latest version" lines; the
// latest is the newest embedded migration, which grows with every mq release.
func versions(t *testing.T, stdout string) (current, latest int) {
	t.Helper()
	c := regexp.MustCompile(`Current version: .*\(schema version (\d+)\)`).FindStringSubmatch(stdout)
	l := regexp.MustCompile(`Latest version:  .*\(schema version (\d+)\)`).FindStringSubmatch(stdout)
	require.NotNil(t, c, stdout)
	require.NotNil(t, l, stdout)
	current, _ = strconv.Atoi(c[1])
	latest, _ = strconv.Atoi(l[1])
	return current, latest
}

func TestCLI_StatusEmptyDatabase(t *testing.T) {
	dsn := createTestDatabase(t)

	stdout, stderr, err := runCLI("status", "--dsn", dsn)
	require.NoError(t, err, "stderr: %s", stderr)

	current, latest := versions(t, stdout)
	assert.Equal(t, 0, current)
	assert.GreaterOrEqual(t, latest, 1)
	assert.Contains(t, stdout, "Migration needed")
}

func TestCLI_MigrateEmptyDatabase(t *testing.T) {
	dsn := createTestDatabase(t)

	stdout, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "stderr: %s", stderr)

	current, latest := versions(t, stdout)
	assert.Equal(t, 0, current)
	assert.GreaterOrEqual(t, latest, 1)
	assert.Contains(t, stdout, "Running migrations...")
	assert.Contains(t, stdout, "Migration completed successfully")
}

func TestCLI_StatusAfterMigration(t *testing.T) {
	dsn := createTestDatabase(t)

	// First migrate
	_, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "migrate stderr: %s", stderr)

	// Then check status
	stdout, stderr, err := runCLI("status", "--dsn", dsn)
	require.NoError(t, err, "status stderr: %s", stderr)

	current, latest := versions(t, stdout)
	assert.Equal(t, latest, current)
	assert.Contains(t, stdout, "Database is up to date")
}

func TestCLI_MigrateIdempotent(t *testing.T) {
	dsn := createTestDatabase(t)

	// First migration
	_, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "first migrate stderr: %s", stderr)

	// Second migration should be idempotent
	stdout, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "second migrate stderr: %s", stderr)

	assert.Contains(t, stdout, "Database is up to date")
}

func TestCLI_MigrateRefusesADirtyDatabase(t *testing.T) {
	dsn := createTestDatabase(t)
	_, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "stderr: %s", stderr)
	conn, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), "UPDATE postgremq.postgremq_migrations SET dirty = true")
	require.NoError(t, err)

	_, stderr, err = runCLI("migrate", "--dsn", dsn)
	require.Error(t, err)
	assert.Contains(t, stderr, "database is in dirty state - manual intervention required")
}

// A migration in progress records its version as dirty while it runs, under
// the migration lock. migrate must wait for it, not call the database dirty.
func TestCLI_MigrateWaitsForAMigrationInProgress(t *testing.T) {
	ctx := context.Background()
	dsn := createTestDatabase(t)
	_, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "stderr: %s", stderr)

	// Another migrator, partway through its migration.
	migrator, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer migrator.Close(ctx)
	var database string
	require.NoError(t, migrator.QueryRow(ctx, "SELECT current_database()").Scan(&database))
	// golang-migrate's lock key for the version table, which every client takes.
	key := int64(crc32.ChecksumIEEE([]byte("postgremq\x00postgremq_migrations\x00"+database)) * 1_486_364_155)
	_, err = migrator.Exec(ctx, "SELECT pg_advisory_lock($1)", key)
	require.NoError(t, err)
	_, err = migrator.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET dirty = true")
	require.NoError(t, err)

	type result struct {
		stdout, stderr string
		err            error
	}
	done := make(chan result, 1)
	go func() {
		stdout, stderr, err := runCLI("migrate", "--dsn", dsn)
		done <- result{stdout, stderr, err}
	}()
	require.Eventually(t, func() bool {
		select {
		case r := <-done:
			done <- r
			return true
		default:
		}
		var waiting bool
		err := migrator.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted AND objid = $1::bigint % 4294967296)`, key).Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("migrate did not wait for the migration in progress: %v\nstdout: %s\nstderr: %s", r.err, r.stdout, r.stderr)
	default:
	}

	// The other migrator finishes.
	_, err = migrator.Exec(ctx, "UPDATE postgremq.postgremq_migrations SET dirty = false")
	require.NoError(t, err)
	_, err = migrator.Exec(ctx, "SELECT pg_advisory_unlock($1)", key)
	require.NoError(t, err)
	select {
	case r := <-done:
		require.NoError(t, r.err, "stdout: %s\nstderr: %s", r.stdout, r.stderr)
	case <-time.After(10 * time.Second):
		t.Fatal("migrate did not finish after the migration in progress did")
	}
}

func TestCLI_MigratePrintsAFailedMigration(t *testing.T) {
	dsn := createTestDatabase(t)
	conn, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	// The first migration creates this table.
	_, err = conn.Exec(context.Background(), "CREATE SCHEMA postgremq; CREATE TABLE postgremq.topics (id int)")
	require.NoError(t, err)

	_, stderr, err := runCLI("migrate", "--dsn", dsn)

	require.Error(t, err)
	assert.Contains(t, stderr, `Error: postgremq: migration 0.0.1 (schema version 1) failed`)
	assert.Contains(t, stderr, `relation "topics" already exists (SQLSTATE 42P07)`)
}

func TestCLI_MigratePrintsAnUnknownVersion(t *testing.T) {
	dsn := createTestDatabase(t)
	_, stderr, err := runCLI("migrate", "--dsn", dsn)
	require.NoError(t, err, "stderr: %s", stderr)
	conn, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), "UPDATE postgremq.postgremq_migrations SET version = 2")
	require.NoError(t, err)

	_, stderr, err = runCLI("migrate", "--dsn", dsn)

	require.Error(t, err)
	assert.Contains(t, stderr, "Error: postgremq: database schema version 2 (mq 0.0.2) is not one of this client's migrations")
}

func TestCLI_MissingDSN(t *testing.T) {
	_, stderr, err := runCLI("migrate")
	require.Error(t, err)
	assert.Equal(t, 2, exitCode(err), "usage error")
	assert.Contains(t, stderr, "--dsn is required")
}

func TestCLI_UnknownCommand(t *testing.T) {
	_, stderr, err := runCLI("upgrade", "--dsn", "postgres://localhost/x")
	require.Error(t, err)
	assert.Equal(t, 2, exitCode(err), "usage error")
	assert.Contains(t, stderr, `unknown command "upgrade"`)
}

func TestCLI_NoCommand(t *testing.T) {
	_, stderr, err := runCLI()
	require.Error(t, err)
	assert.Equal(t, 2, exitCode(err), "usage error")
	assert.Contains(t, stderr, "Commands:")
}

func TestCLI_InvalidDSN(t *testing.T) {
	_, stderr, err := runCLI("migrate", "--dsn", "invalid://dsn")
	require.Error(t, err)
	assert.Equal(t, 1, exitCode(err))
	assert.Contains(t, stderr, "failed to connect")
}

func TestCLI_ConnectionFailed(t *testing.T) {
	_, stderr, err := runCLI("status", "--dsn", "postgres://postgres:postgres@localhost:65535/nonexistent?sslmode=disable")
	require.Error(t, err)
	assert.Contains(t, stderr, "failed to connect")
}

func TestCLI_Help(t *testing.T) {
	stdout, _, err := runCLI("--help")
	require.NoError(t, err)

	assert.Contains(t, stdout, "PostgreMQ")
	assert.Contains(t, stdout, "migrate")
	assert.Contains(t, stdout, "status")
}

func TestCLI_MigrateHelp(t *testing.T) {
	stdout, _, err := runCLI("migrate", "--help")
	require.NoError(t, err)

	assert.Contains(t, stdout, "Usage: postgremq migrate")
	assert.Contains(t, stdout, "--dsn")
	assert.NotContains(t, stdout, "--target")
}

func TestCLI_StatusHelp(t *testing.T) {
	stdout, _, err := runCLI("status", "--help")
	require.NoError(t, err)

	assert.Contains(t, stdout, "--dsn")
}
