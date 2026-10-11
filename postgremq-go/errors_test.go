package postgremq_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// TestErrLeaseLost_AckTwice verifies that a second ack on the same message
// surfaces as ErrLeaseLost (PMQ01) — the lease was consumed by the first ack.
func TestErrLeaseLost_AckTwice(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "ELLTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "ELLQueue", "ELLTopic", false))
	_, err = conn.Publish(ctx, "ELLTopic", json.RawMessage(`{}`))
	require.NoError(t, err)

	consumer, err := conn.Consume("ELLQueue", postgremq.WithVT(30))
	require.NoError(t, err)
	defer consumer.Stop()

	msg := <-consumer.Messages()
	require.NoError(t, msg.Ack(ctx))

	// Second ack on the same message: lease has been consumed.
	err = msg.Ack(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrLeaseLost),
		"second ack should surface ErrLeaseLost; got %v", err)
}

// TestErrLeaseLost_SetVTAfterExpiry verifies that SetVT after the vt has
// lapsed surfaces as ErrLeaseLost (PMQ01). set_vt is the operation that
// explicitly requires `vt > NOW()`; ack/nack/release rely on
// status='processing' AND the original token, which a stolen-by-another-
// consumer scenario detects.
func TestErrLeaseLost_SetVTAfterExpiry(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "VTLossTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "VTLossQueue", "VTLossTopic", false))
	_, err = conn.Publish(ctx, "VTLossTopic", json.RawMessage(`{}`))
	require.NoError(t, err)

	// Consume with a tiny vt and disable auto-extension so the lease lapses.
	consumer, err := conn.Consume("VTLossQueue",
		postgremq.WithVT(1),
		postgremq.WithNoAutoExtension(),
	)
	require.NoError(t, err)
	defer consumer.Stop()

	msg := <-consumer.Messages()

	// Wait for the lease to expire.
	time.Sleep(1500 * time.Millisecond)

	_, err = msg.SetVT(ctx, 60)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrLeaseLost),
		"SetVT after vt expired should be ErrLeaseLost; got %v", err)

	// Release the message so consumer.Stop can drain the in-flight tracker.
	// Status is still 'processing' and the token still matches; only set_vt
	// (which checks vt > NOW()) actually fails on lease expiry.
	require.NoError(t, msg.Release(ctx))
}

// TestErrLeaseLost_NackAfterStolen verifies that a nack from the original
// consumer after another consumer has re-consumed the message surfaces as
// ErrLeaseLost (token mismatch).
func TestErrLeaseLost_NackAfterStolen(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "StolenTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "StolenQueue", "StolenTopic", false))
	_, err = conn.Publish(ctx, "StolenTopic", json.RawMessage(`{}`))
	require.NoError(t, err)

	consumer, err := conn.Consume("StolenQueue",
		postgremq.WithVT(1),
		postgremq.WithNoAutoExtension(),
	)
	require.NoError(t, err)
	defer consumer.Stop()

	msg := <-consumer.Messages()

	// Wait for the lease to expire so the message becomes consumable again.
	time.Sleep(1500 * time.Millisecond)

	// "Steal" via raw SQL: re-consume the message with a fresh token. This
	// changes consumer_token in the row.
	_, err = pool.Exec(ctx, "SELECT * FROM postgremq.consume_message($1, 30, 1)", "StolenQueue")
	require.NoError(t, err)

	// Original consumer's token no longer matches.
	err = msg.Nack(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrLeaseLost),
		"nack with stale token should be ErrLeaseLost; got %v", err)
}

// TestErrLeaseLost_ReleaseAfterAck verifies release on an already-acked
// message surfaces as ErrLeaseLost.
func TestErrLeaseLost_ReleaseAfterAck(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "RelLLTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "RelLLQueue", "RelLLTopic", false))
	_, err = conn.Publish(ctx, "RelLLTopic", json.RawMessage(`{}`))
	require.NoError(t, err)

	consumer, err := conn.Consume("RelLLQueue", postgremq.WithVT(30))
	require.NoError(t, err)
	defer consumer.Stop()

	msg := <-consumer.Messages()
	require.NoError(t, msg.Ack(ctx))

	err = msg.Release(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrLeaseLost),
		"release after ack should surface ErrLeaseLost; got %v", err)
}

// TestErrQueueNotFound_PublishUnknownTopic verifies publish to a topic that
// doesn't exist surfaces as ErrQueueNotFound (PMQ02).
func TestErrQueueNotFound_PublishUnknownTopic(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Publish(ctx, "GhostTopic", json.RawMessage(`{}`))
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrQueueNotFound),
		"publish to unknown topic should be ErrQueueNotFound; got %v", err)
}

// TestErrQueueNotFound_CreateQueueUnknownTopic verifies that creating a
// queue against a missing topic surfaces as ErrQueueNotFound (PMQ02) —
// parity with publish_message rather than a raw FK violation.
func TestErrQueueNotFound_CreateQueueUnknownTopic(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateQueue(ctx, "OrphanQueue", "GhostTopic", false)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrQueueNotFound),
		"create_queue against missing topic should be ErrQueueNotFound; got %v", err)
}

// TestErrValidation_SetVTNegative verifies SetVT(-1) surfaces as
// ErrValidation (PMQ03).
func TestErrValidation_SetVTNegative(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "ValTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "ValQueue", "ValTopic", false))
	_, err = conn.Publish(ctx, "ValTopic", json.RawMessage(`{}`))
	require.NoError(t, err)

	consumer, err := conn.Consume("ValQueue", postgremq.WithVT(30))
	require.NoError(t, err)
	defer consumer.Stop()

	msg := <-consumer.Messages()

	_, err = msg.SetVT(ctx, -1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrValidation),
		"SetVT(-1) should be ErrValidation; got %v", err)

	require.NoError(t, msg.Ack(ctx))
}

// TestErrValidation_CreateTopicInvalidName verifies that an invalid topic
// name surfaces as ErrValidation (PMQ03).
func TestErrValidation_CreateTopicInvalidName(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateTopic(ctx, "bad name with spaces")
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrValidation),
		"create_topic with invalid name should be ErrValidation; got %v", err)
}

// TestErrValidation_CreateQueueInvalidName verifies invalid queue name
// surfaces as ErrValidation.
func TestErrValidation_CreateQueueInvalidName(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "VTopic"))
	err = conn.CreateQueue(ctx, "bad queue", "VTopic", false)
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrValidation),
		"create_queue with invalid name should be ErrValidation; got %v", err)
}

// TestErrValidation_QueueParamMismatch verifies that re-creating a queue
// with parameters that differ from the existing row surfaces as
// ErrValidation. (Re-creating with identical parameters is idempotent
// success — covered by TestCreateQueue_Idempotent.)
func TestErrValidation_QueueParamMismatch(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "DupTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "DupExcl", "DupTopic", true,
		postgremq.WithMaxDeliveryAttempts(2)))

	// Same name, different max_delivery_attempts → PMQ03 / ErrValidation.
	err = conn.CreateQueue(ctx, "DupExcl", "DupTopic", true,
		postgremq.WithMaxDeliveryAttempts(5))
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrValidation),
		"queue param mismatch should be ErrValidation; got %v", err)
	assert.Contains(t, err.Error(), "already exists with different parameters")
}

// TestCreateQueue_Idempotent verifies that re-creating a queue with the
// exact same parameters is a no-op success — RabbitMQ-style idempotent
// queue.declare.
func TestCreateQueue_Idempotent(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "IdemTopic"))
	// Two identical exclusive creates → both succeed.
	require.NoError(t, conn.CreateQueue(ctx, "IdemExcl", "IdemTopic", true,
		postgremq.WithKeepAliveInterval(60*time.Second)))
	require.NoError(t, conn.CreateQueue(ctx, "IdemExcl", "IdemTopic", true,
		postgremq.WithKeepAliveInterval(60*time.Second)))
	// Two identical non-exclusive creates → both succeed.
	require.NoError(t, conn.CreateQueue(ctx, "IdemNx", "IdemTopic", false))
	require.NoError(t, conn.CreateQueue(ctx, "IdemNx", "IdemTopic", false))
}

// TestErrValidation_DeleteTopicWithMessages verifies that deleting a topic
// that still has messages surfaces as ErrValidation (state precondition).
func TestErrValidation_DeleteTopicWithMessages(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "DTTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "DTQueue", "DTTopic", false))
	_, err = conn.Publish(ctx, "DTTopic", json.RawMessage(`{}`))
	require.NoError(t, err)

	err = conn.DeleteTopic(ctx, "DTTopic")
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrValidation),
		"delete_topic on non-empty topic should be ErrValidation; got %v", err)
}

// TestErrValidation_SetVTBatchMismatchedArrays verifies that mismatched
// batch arrays surface as ErrValidation (PMQ03).
func TestErrValidation_SetVTBatchMismatchedArrays(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "BatchValTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "BatchValQueue", "BatchValTopic", false))

	// set_vt_batch_multi rejects mismatched parallel arrays with PMQ03. We craft
	// the mismatch by issuing the SQL directly through the pool to bypass the
	// client-side normalization that would always pass equal-length arrays.
	_, err = pool.Exec(ctx,
		`SELECT * FROM postgremq.set_vt_batch_multi(ARRAY[$1]::varchar[], ARRAY[1,2]::bigint[], ARRAY['a']::varchar[], ARRAY[60]::int[])`,
		"BatchValQueue")
	require.Error(t, err)
	// The raw-SQL path doesn't go through mapPgError. We verify the SQLSTATE
	// directly via pgconn.PgError: errors.Is on the wrapped sentinel works
	// only on paths that go through the client's wrappers.
	require.Contains(t, err.Error(), "SQLSTATE PMQ03")
}
