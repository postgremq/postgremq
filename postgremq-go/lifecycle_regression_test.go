package postgremq_test

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
	"testing"
	"time"
)

func TestCloseAllowsDeliverySettlement(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	c, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithShutdownTimeout(time.Second))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.CreateTopic(ctx, "t"))
	require.NoError(t, c.CreateQueue(ctx, "q", "t", false))
	_, err = c.Publish(ctx, "t", []byte(`{}`))
	require.NoError(t, err)
	cons, err := c.Consume("q", postgremq.WithBatchSize(1))
	require.NoError(t, err)
	var msg *postgremq.Message
	select {
	case msg = <-cons.Messages():
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery")
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	<-msg.StoppedCtx.Done()
	require.NoError(t, msg.Ack(ctx))
	<-closed
	var status string
	require.NoError(t, pool.QueryRow(ctx, "SELECT status FROM postgremq.queue_messages WHERE message_id=$1", msg.ID).Scan(&status))
	require.Equal(t, "completed", status)
}
func TestCancelledHandlerReturnDoesNotAcknowledge(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	c, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.CreateTopic(ctx, "t"))
	require.NoError(t, c.CreateQueue(ctx, "q", "t", false))
	started := make(chan struct{})
	hc, err := c.ConsumeHandler("q", func(ctx context.Context, msg *postgremq.Message) { close(started); <-ctx.Done() }, postgremq.WithBatchSize(1), postgremq.WithMaxInFlight(1))
	require.NoError(t, err)
	id, err := c.Publish(ctx, "t", []byte(`{}`))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not started")
	}
	hc.Stop()
	var status string
	var attempts int
	require.NoError(t, pool.QueryRow(ctx, "SELECT status,delivery_attempts FROM postgremq.queue_messages WHERE message_id=$1", id).Scan(&status, &attempts))
	require.Equal(t, "pending", status)
	require.Equal(t, 1, attempts)
	require.Equal(t, 0, c.EventListener().SubscriberCount("pmq:t:t"))
}
func TestCloseCancelsBlockedFetch(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	c, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithShutdownTimeout(50*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.CreateTopic(ctx, "t"))
	require.NoError(t, c.CreateQueue(ctx, "q", "t", false))
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "LOCK TABLE postgremq.queue_messages IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)
	_, err = c.Consume("q")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var waiting bool
		_ = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FROM postgremq.consume_message(%')").Scan(&waiting)
		return waiting
	}, time.Second, 5*time.Millisecond)
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("shutdown stuck in fetch")
	}
}

func TestFailedQueueDeletionKeepsItsLeaseAlive(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	c, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.CreateTopic(ctx, "t"))
	require.NoError(t, c.CreateQueue(ctx, "q", "t", true, postgremq.WithKeepAliveInterval(400*time.Millisecond), postgremq.WithMaxDeliveryAttempts(1)))
	_, err = c.Publish(ctx, "t", []byte(`{}`))
	require.NoError(t, err)
	raw, err := c.ConsumeMessages(ctx, "q", 1, 30)
	require.NoError(t, err)
	require.Len(t, raw, 1)
	require.NoError(t, raw[0].Nack(ctx))
	require.Error(t, c.DeleteQueue(ctx, "q")) // DLQ reference prevents deletion.
	time.Sleep(600 * time.Millisecond)
	var live bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT keep_alive_until > clock_timestamp() FROM postgremq.queues WHERE name='q'").Scan(&live))
	require.True(t, live, "failed deletion must not stop keepalive")
}

func TestAmbiguousPublishIsNotRetried(t *testing.T) {
	calls := 0
	pool := &MockPool{QueryRowFunc: func(context.Context, string, ...interface{}) pgx.Row {
		calls++
		return &mockErrRow{err: &pgconn.PgError{Code: "08006", Message: "response lost"}}
	}}
	c, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Publish(context.Background(), "t", []byte(`{}`))
	require.Error(t, err)
	require.Equal(t, 1, calls)
}
