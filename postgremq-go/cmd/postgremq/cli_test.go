package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type postgresContainer struct {
	container testcontainers.Container
	dsn       string
	pool      *pgxpool.Pool
}

var (
	sharedContainer *postgresContainer
	cliBinary       string
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

	// Start PostgreSQL container
	sharedContainer, err = newPostgresContainer(ctx)
	if err != nil {
		fmt.Printf("Failed to start postgres container: %v\n", err)
		os.RemoveAll(cliBinary)
		os.Exit(1)
	}

	// Run tests
	exitCode := m.Run()

	// Cleanup
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sharedContainer.cleanup(cleanupCtx)
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

func newPostgresContainer(ctx context.Context) (*postgresContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
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

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", host, mappedPort.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	return &postgresContainer{
		container: container,
		dsn:       dsn,
		pool:      pool,
	}, nil
}

func (pc *postgresContainer) cleanup(ctx context.Context) error {
	if pc.pool != nil {
		pc.pool.Close()
	}
	return pc.container.Terminate(ctx)
}

func createTestDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	dbName := fmt.Sprintf("cli_test_%d", time.Now().UnixNano())

	_, err := sharedContainer.pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = sharedContainer.pool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	})

	// Build DSN for the test database
	return strings.Replace(sharedContainer.dsn, "/postgres?", "/"+dbName+"?", 1)
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
