package postgremq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

func TestSchemaIsolationWithApplicationTransactions(t *testing.T) {
	base, ctx := setupEmptyTestDatabase(t)
	_, err := base.Exec(ctx, "CREATE SCHEMA app")
	require.NoError(t, err)
	for _, name := range []string{"topics", "queues", "messages", "queue_messages", "dead_letter_queue"} {
		_, err = base.Exec(ctx, fmt.Sprintf("CREATE TABLE app.%s (value text)", name))
		require.NoError(t, err)
	}
	_, err = base.Exec(ctx, `
		CREATE TABLE app.postgremq_migrations (version bigint, dirty boolean);
		INSERT INTO app.postgremq_migrations VALUES (99, true);
		CREATE FUNCTION app.publish_message(varchar, jsonb) RETURNS bigint
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'application publish called'; END $$;
		CREATE FUNCTION app.ack_message(varchar, bigint, varchar) RETURNS void
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'application ack called'; END $$;
	`)
	require.NoError(t, err)
	cfg := base.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = "app, pg_temp"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()

	status, err := postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	require.Zero(t, status.CurrentVersion)
	var exists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='postgremq')").Scan(&exists))
	require.False(t, exists, "status must not create the queue schema")
	require.NoError(t, postgremq.Migrate(pool))
	status, err = postgremq.GetMigrationStatus(pool)
	require.NoError(t, err)
	require.Equal(t, status.LatestVersion, status.CurrentVersion)
	require.False(t, status.Dirty)
	var version int
	require.NoError(t, pool.QueryRow(ctx, "SELECT version FROM app.postgremq_migrations").Scan(&version))
	require.Equal(t, 99, version)
	var publicObjects int
	require.NoError(t, pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace) +
		(SELECT count(*) FROM pg_proc WHERE pronamespace='public'::regnamespace)`).Scan(&publicObjects))
	require.Zero(t, publicObjects)

	c, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithShutdownTimeout(time.Second))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.CreateTopic(ctx, "in"))
	require.NoError(t, c.CreateTopic(ctx, "out"))
	require.NoError(t, c.CreateQueue(ctx, "q", "in", false))
	incoming, err := c.Publish(ctx, "in", []byte(`{}`))
	require.NoError(t, err)
	consumer, err := c.Consume("q", postgremq.WithBatchSize(1), postgremq.WithVT(30), postgremq.WithNoAutoExtension(), postgremq.WithCheckTimeout(20*time.Millisecond))
	require.NoError(t, err)

	for _, commit := range []bool{false, true} {
		var msg *postgremq.Message
		select {
		case msg = <-consumer.Messages():
		case <-time.After(3 * time.Second):
			t.Fatal("message not delivered")
		}
		require.Equal(t, incoming, msg.ID)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, "INSERT INTO messages(value) VALUES ('application data')")
		require.NoError(t, err)
		outgoing, err := c.PublishWithTx(ctx, tx, "out", []byte(`{}`))
		require.NoError(t, err)
		require.NoError(t, msg.AckWithTx(ctx, tx))
		var path string
		require.NoError(t, tx.QueryRow(ctx, "SHOW search_path").Scan(&path))
		require.Equal(t, "app, pg_temp", path)
		var appRows, published int
		var state string
		check := func(wantRows int, wantState string) {
			t.Helper()
			require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM app.messages").Scan(&appRows))
			require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM postgremq.messages WHERE id=$1", outgoing).Scan(&published))
			require.NoError(t, pool.QueryRow(ctx, "SELECT status FROM postgremq.queue_messages WHERE message_id=$1", incoming).Scan(&state))
			require.Equal(t, wantRows, appRows)
			require.Equal(t, wantRows, published)
			require.Equal(t, wantState, state)
		}
		check(0, "processing")
		if commit {
			require.NoError(t, tx.Commit(ctx))
			check(1, "completed")
		} else {
			require.NoError(t, tx.Rollback(ctx))
			check(0, "processing")
			_, err = pool.Exec(ctx, "UPDATE postgremq.queue_messages SET vt=clock_timestamp()-interval '1 second' WHERE message_id=$1", incoming)
			require.NoError(t, err)
		}
	}
}
