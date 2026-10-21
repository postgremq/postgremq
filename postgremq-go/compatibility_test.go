package postgremq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

func TestCompatibility_SupportedVersions(t *testing.T) {
	versions := postgremq.SupportedMQVersions()
	assert.Equal(t, []string{"0.2"}, versions)
	versions[0] = "99"
	assert.Equal(t, []string{"0.2"}, postgremq.SupportedMQVersions(), "callers get a copy")
}

func TestCompatibility_InstalledSchemaIsSupported(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	var version int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT (postgremq.info()->>'schema_version')::bigint").Scan(&version))
	line := fmt.Sprint(version / 100_000_000)
	if version < 100_000_000 {
		line = fmt.Sprintf("0.%d", version/10_000)
	}
	assert.Contains(t, postgremq.SupportedMQVersions(), line)

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	conn.Close()
}

// fakeInfo replaces postgremq.info() with one reporting the given JSON.
func fakeInfo(t *testing.T, ctx context.Context, pool *pgxpool.Pool, info string) {
	t.Helper()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
		LANGUAGE sql STABLE AS $$ SELECT '%s'::jsonb $$`, info))
	require.NoError(t, err)
}

func TestCompatibility_OtherVersionsOfTheSameLineAreAccepted(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	// protocol_major is only for older clients: it is ignored.
	for _, info := range []string{
		`{"schema_version": 20000, "protocol_major": 99}`,
		`{"schema_version": 29999}`,
		`{"schema_version": 20001}`,
	} {
		fakeInfo(t, ctx, pool, info)
		conn, err := postgremq.DialFromPool(ctx, pool)
		require.NoError(t, err, info)
		conn.Close()
	}
}

func TestCompatibility_UnsupportedMajorIsRejected(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	fakeInfo(t, ctx, pool, `{"schema_version": 100020003, "protocol_major": 1}`)

	_, err := postgremq.DialFromPool(ctx, pool)

	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var compat *postgremq.CompatibilityError
	require.ErrorAs(t, err, &compat)
	assert.Equal(t, int64(100_020_003), compat.SchemaVersion)
	assert.Equal(t, []string{"0.2"}, compat.SupportedVersions)
	assert.Contains(t, err.Error(), "mq 1.2.3 (schema version 100020003)")
	assert.Contains(t, err.Error(), "supports mq [0.2]")
}

// Before 1.0 a breaking schema change bumps the minor, so another 0.y is
// another contract.
func TestCompatibility_OtherMinorBefore1IsRejected(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	for _, version := range []int64{10_002, 30_000, 1, 99_999_999} {
		fakeInfo(t, ctx, pool, fmt.Sprintf(`{"schema_version": %d, "protocol_major": 1}`, version))

		_, err := postgremq.DialFromPool(ctx, pool)

		require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema, version)
		var compat *postgremq.CompatibilityError
		require.ErrorAs(t, err, &compat)
		assert.Equal(t, version, compat.SchemaVersion)
	}
}

func TestCompatibility_NoSchemaVersionNeedsUpgrade(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	for _, info := range []string{
		`{"schema_version": null, "protocol_major": 1}`,
		`{}`,
		`{"protocol_major": 1}`,
		`{"schema_version": "one"}`,
		`{"schema_version": "10001"}`,
		`{"schema_version": -1}`,
		`{"schema_version": 0}`,
		`{"schema_version": 1.5}`,
		`{"schema_version": 1e30}`,
		`{"schema_version": 9223372036854775808}`,
		`"text"`,
		`[1]`,
	} {
		fakeInfo(t, ctx, pool, info)

		_, err := postgremq.DialFromPool(ctx, pool)

		require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema, info)
		var compat *postgremq.CompatibilityError
		require.ErrorAs(t, err, &compat)
		assert.Equal(t, int64(0), compat.SchemaVersion)
		assert.Contains(t, err.Error(), "no schema version")
	}
}

func TestCompatibility_DialRejectsAndClosesItsPool(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	fakeInfo(t, ctx, pool, `{"schema_version": 100000000}`)

	conn, err := postgremq.Dial(ctx, pool.Config())

	assert.Nil(t, conn)
	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
}

func TestCompatibility_MissingDiscoveryNeedsUpgrade(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	_, err := pool.Exec(ctx, "DROP FUNCTION postgremq.info()")
	require.NoError(t, err)

	_, err = postgremq.DialFromPool(ctx, pool)

	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var compat *postgremq.CompatibilityError
	require.ErrorAs(t, err, &compat)
	assert.Equal(t, int64(0), compat.SchemaVersion)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "the database error is kept")
	assert.Equal(t, "42883", pgErr.Code)
	assert.Contains(t, err.Error(), "upgrade")
}

func TestCompatibility_NotInstalledNeedsInstallation(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	_, err := postgremq.DialFromPool(ctx, pool)

	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "3F000", pgErr.Code)
}

func TestCompatibility_PermissionErrorKeepsItsCause(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	role := fmt.Sprintf("pmq_noaccess_%d", time.Now().UnixNano())
	_, err := pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'x'", role))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = sharedContainer.adminPool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	cfg := pool.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, "x"
	restricted, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer restricted.Close()

	_, err = postgremq.DialFromPool(ctx, restricted)

	require.Error(t, err)
	assert.NotErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "42501", pgErr.Code)
}

func TestCompatibility_ConnectionErrorKeepsItsCause(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Port = 1
	cfg.ConnConfig.Fallbacks = nil // they repeat the reachable host and port
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second

	conn, err := postgremq.Dial(ctx, cfg)
	if conn != nil {
		conn.Close()
	}

	require.Error(t, err)
	assert.NotErrorIs(t, err, postgremq.ErrIncompatibleSchema)
}

func TestCompatibility_MissingFunctionErrorPropagates(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()
	_, err = pool.Exec(ctx, "DROP FUNCTION postgremq.list_topics()")
	require.NoError(t, err)

	_, err = conn.ListTopics(ctx)

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "the database error comes through normal error handling")
	assert.Equal(t, "42883", pgErr.Code)
	assert.NotErrorIs(t, err, postgremq.ErrIncompatibleSchema)
}
