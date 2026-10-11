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

func TestProtocol_SupportedMajors(t *testing.T) {
	majors := postgremq.SupportedProtocolMajors()
	assert.Equal(t, []int{1}, majors)
	majors[0] = 99
	assert.Equal(t, []int{1}, postgremq.SupportedProtocolMajors(), "callers get a copy")
}

func TestProtocol_InstalledSchemaIsSupported(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	var major int
	require.NoError(t, pool.QueryRow(ctx, "SELECT (postgremq.info()->>'protocol_major')::int").Scan(&major))
	assert.Contains(t, postgremq.SupportedProtocolMajors(), major)

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	conn.Close()
}

func TestProtocol_UnsupportedMajorIsRejected(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	_, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
		LANGUAGE sql STABLE AS $$ SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 99) $$`)
	require.NoError(t, err)

	_, err = postgremq.DialFromPool(ctx, pool)

	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var compat *postgremq.CompatibilityError
	require.ErrorAs(t, err, &compat)
	assert.Equal(t, int64(42), compat.SchemaVersion)
	assert.Equal(t, 99, compat.ProtocolMajor)
	assert.Equal(t, []int{1}, compat.SupportedMajors)
	assert.Contains(t, err.Error(), "version 42")
	assert.Contains(t, err.Error(), "99")
}

func TestProtocol_DialRejectsAndClosesItsPool(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	_, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
		LANGUAGE sql STABLE AS $$ SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 99) $$`)
	require.NoError(t, err)

	conn, err := postgremq.Dial(ctx, pool.Config())

	assert.Nil(t, conn)
	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
}

func TestProtocol_MissingDiscoveryNeedsUpgrade(t *testing.T) {
	pool, ctx := setupTestConnection(t)
	_, err := pool.Exec(ctx, "DROP FUNCTION postgremq.info()")
	require.NoError(t, err)

	_, err = postgremq.DialFromPool(ctx, pool)

	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var compat *postgremq.CompatibilityError
	require.ErrorAs(t, err, &compat)
	assert.Equal(t, 0, compat.ProtocolMajor)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "the database error is kept")
	assert.Equal(t, "42883", pgErr.Code)
	assert.Contains(t, err.Error(), "upgrade")
}

func TestProtocol_NotInstalledNeedsInstallation(t *testing.T) {
	pool, ctx := setupEmptyTestDatabase(t)

	_, err := postgremq.DialFromPool(ctx, pool)

	require.ErrorIs(t, err, postgremq.ErrIncompatibleSchema)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "3F000", pgErr.Code)
}

func TestProtocol_PermissionErrorKeepsItsCause(t *testing.T) {
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

func TestProtocol_ConnectionErrorKeepsItsCause(t *testing.T) {
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

func TestProtocol_MissingFunctionErrorPropagates(t *testing.T) {
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
