// Message tests cover ack/nack/release semantics, delayed publish and delayed
// redelivery, manual visibility extension (SetVT), state transitions, message
// payload handling, and transactional ack behavior (commit/rollback).
package postgremq_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"postgremq.dev/postgremq-go"
)

// -----------------------------------------------------------------------
// Message Tests
// -----------------------------------------------------------------------

// TestMessageAcknowledgment tests successful message acknowledgment
func TestMessageAcknowledgment(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_ack_topic"
	queueName := "test_ack_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	messageID, err := conn.Publish(ctx, topicName, []byte(`{"test":"ack"}`))
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		require.Equal(t, messageID, msg.ID, "Message ID mismatch")
		err := msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")

		// Verify message is completed
		messages, err := conn.ListMessages(ctx, queueName)
		require.NoError(t, err, "Failed to list messages")
		require.Len(t, messages, 1, "Expected one message")
		require.Equal(t, messageID, messages[0].MessageID, "Message ID mismatch")
		require.Equal(t, postgremq.MessageStatusCompleted, messages[0].Status, "Expected message status to be completed")
	case <-time.After(1 * time.Second):
		require.Fail(t, "Timeout waiting for message")
	}
}

// TestMessageNegativeAcknowledgment tests negative acknowledgment behavior
func TestMessageNegativeAcknowledgment(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_nack_topic"
	queueName := "test_nack_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	messageID, err := conn.Publish(ctx, topicName, []byte(`{"test":"nack"}`))
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, messageID, msg.ID, "Message ID mismatch")
		err := msg.Nack(ctx)
		require.NoError(t, err, "Failed to nack message")
		consumer.Stop()

		// Verify message is available again
		messages, err := conn.ListMessages(ctx, queueName)
		require.NoError(t, err, "Failed to list messages")
		assert.Len(t, messages, 1, "Expected message to be available")
		assert.Equal(t, messageID, messages[0].MessageID, "Message ID mismatch")
		assert.Equal(t, 1, messages[0].DeliveryAttempts, "Wrong delivery attempts count")
		assert.Equal(t, postgremq.MessageStatusPending, messages[0].Status, "Expected message status to be pending")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message")
	}
}

// TestMessageVisibilityTimeout tests setting visibility timeout manually
func TestMessageVisibilityTimeout(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_vt_topic"
	queueName := "test_vt_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	messageID, err := conn.Publish(ctx, topicName, []byte(`{"test":"vt"}`))
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName, postgremq.WithVT(5))
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	<-time.After(50 * time.Millisecond)
	select {
	case msg := <-consumer.Messages():
		defer func() { _ = msg.Ack(ctx) }()
		initialVT := msg.GetVT()
		require.Equal(t, messageID, msg.ID, "Message ID mismatch")
		newVT, err := msg.SetVT(ctx, 30)
		require.NoError(t, err, "Failed to set visibility timeout")
		assert.Greater(t, newVT.UnixMilli(), initialVT.UnixMilli(), "Expected visibility timeout to be extended")

		// Verify visibility timeout extension
		messages, err := conn.ListMessages(ctx, queueName)
		require.NoError(t, err, "Failed to list messages")
		assert.Len(t, messages, 1, "Expected one message")
		assert.Greater(t, messages[0].VT.UnixMilli(), initialVT.UnixMilli(), "Expected visibility timeout to be extended")
	case <-time.After(1 * time.Second):
		require.Fail(t, "Timeout waiting for message")
	}
}

// TestMessageDelayedDelivery tests delayed message delivery
func TestMessageDelayedDelivery(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	logger := MockLogger{}
	conn, err := postgremq.DialFromPool(context.Background(), pool, postgremq.WithLogger(&logger))
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_delayed_pub_topic"
	queueName := "test_delayed_pub_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	deliverAfter := time.Now().Add(2 * time.Second)
	messageID, err := conn.Publish(ctx, topicName, []byte(`{"test":"delayed"}`), postgremq.WithDeliverAfter(deliverAfter.UTC()))
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	// Message shouldn't be available immediately
	select {
	case msg := <-consumer.Messages():
		require.Fail(t, "Message should not be available yet", "Got message ID: %d", msg.ID)
	case <-time.After(1 * time.Second):
		// Expected timeout
	}

	msgs, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages")
	require.Len(t, msgs, 1, "Expected one message in the queue")

	// Message should be available after delay
	// Use 2 seconds timeout to account for race detector overhead
	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, messageID, msg.ID, "Message ID mismatch")
		err := msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")
	case <-time.After(2 * time.Second):
		require.Failf(t, "Timeout waiting for delayed message.", " Logs: \n\t%s", strings.Join(logger.Messages, "\n\t"))
	}
}

// TestMessageDelayedRedelivery tests Nack with delay until parameter
func TestMessageDelayedRedelivery(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_delayed_nack_topic"
	queueName := "test_delayed_nack_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	messageID, err := conn.Publish(ctx, topicName, []byte(`{"test":"delayed-nack"}`))
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, messageID, msg.ID, "Message ID mismatch")
		delayUntil := time.Now().Add(2 * time.Second)
		err := msg.Nack(ctx, postgremq.WithDelayUntil(delayUntil))
		require.NoError(t, err, "Failed to nack message")

		// Message shouldn't be available immediately
		select {
		case msg := <-consumer.Messages():
			assert.Fail(t, "Message should not be available yet", "Got message ID: %d", msg.ID)
		case <-time.After(1 * time.Second):
			// Expected timeout
		}

		// Message should be available after delay
		select {
		case msg := <-consumer.Messages():
			assert.Equal(t, messageID, msg.ID, "Message ID mismatch")
			assert.Equal(t, 2, msg.DeliveryAttempt, "Expected delivery attempt to be 2")
			err := msg.Ack(ctx)
			require.NoError(t, err, "Failed to ack message")
		case <-time.After(2 * time.Second):
			assert.Fail(t, "Timeout waiting for delayed message")
		}
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for initial message")
	}
}

// TestMessageStatusTransitions tests message state transitions
func TestMessageStatusTransitions(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_status_topic"
	queueName := "test_status_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Publish a message with properly formatted JSON payload.
	messageID, err := conn.Publish(ctx, topicName, []byte(`{"test":"status"}`))
	require.NoError(t, err, "Failed to publish message")

	// Immediately after publishing, the message should be pending.
	messages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages")
	require.Len(t, messages, 1, "Expected one message")
	assert.Equal(t, messageID, messages[0].MessageID, "Message ID mismatch")
	assert.Equal(t, postgremq.MessageStatusPending, messages[0].Status, "Message should be pending on publication")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		// While processing, the status should become "processing".
		messages, err = conn.ListMessages(ctx, queueName)
		require.NoError(t, err, "Failed to list messages")
		assert.Equal(t, postgremq.MessageStatusProcessing, messages[0].Status, "Message should be processing while handled")

		err = msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")

		// Stop the consumer to prevent it from automatically consuming messages
		// which could lead to race conditions during verification
		consumer.Stop()

		// After ack, the message should be completed
		messages, err = conn.ListMessages(ctx, queueName)
		require.NoError(t, err, "Failed to list messages")
		assert.Equal(t, postgremq.MessageStatusCompleted, messages[0].Status, "Message should be completed after ack")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message")
	}
}

// TestMessageProperties tests message properties and payload handling
func TestMessageProperties(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_properties_topic"
	queueName := "test_properties_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Create message with various payload types
	type TestPayload struct {
		Int    int     `json:"int"`
		Float  float64 `json:"float"`
		String string  `json:"string"`
		Bool   bool    `json:"bool"`
		Array  []int   `json:"array"`
	}

	payload := []byte(`{
		"int": 123,
		"float": 45.67,
		"string": "test message",
		"bool": true,
		"array": [1, 2, 3]
	}`)

	messageID, err := conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		// Verify message properties
		assert.Equal(t, messageID, msg.ID, "Message ID mismatch")
		assert.Equal(t, 1, msg.DeliveryAttempt, "Expected delivery attempt to be 1")
		assert.False(t, msg.GetVT().IsZero(), "Expected valid visibility timeout")

		// Verify we can unmarshal and access properties
		var data TestPayload
		err = json.Unmarshal(msg.Payload, &data)
		require.NoError(t, err, "Failed to unmarshal JSON payload")

		assert.Equal(t, 123, data.Int, "Integer property mismatch")
		assert.Equal(t, 45.67, data.Float, "Float property mismatch")
		assert.Equal(t, "test message", data.String, "String property mismatch")
		assert.Equal(t, true, data.Bool, "Boolean property mismatch")
		assert.Equal(t, []int{1, 2, 3}, data.Array, "Array property mismatch")

		err = msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Timeout waiting for message")
	}
}

// TestAckWithTx verifies that messages are acknowledged within a transaction
func TestAckWithTx(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Test Case 1: Successful commit of acknowledge
	t.Run("CommitTransaction", func(t *testing.T) {

		conn, err := postgremq.DialFromPool(context.Background(), pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		// Create topic
		topicName := "test_ack_tx_topic_commit"
		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "Failed to create topic")

		// Create queue
		queueName := "test_ack_tx_queue_commit"
		err = conn.CreateQueue(ctx, queueName, topicName, false)
		require.NoError(t, err, "Failed to create queue")

		// Publish a message
		payload := []byte(`{"key":"ack_commit_test","number":789}`)
		msgID, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Failed to publish message")
		require.Greater(t, msgID, int64(0), "Message ID should be positive")

		// Consume the message
		consumer, err := conn.Consume(queueName)
		require.NoError(t, err, "Failed to create consumer")
		defer consumer.Stop()

		var msg *postgremq.Message
		select {
		case msg = <-consumer.Messages():
			require.Equal(t, msgID, msg.ID, "Message ID mismatch")
		case <-time.After(3 * time.Second):
			require.Fail(t, "Timeout waiting for message")
		}

		// Start a transaction
		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "Failed to begin transaction")

		// Acknowledge the message within the transaction
		err = msg.AckWithTx(ctx, tx)
		require.NoError(t, err, "Failed to acknowledge message within transaction")

		// Commit the transaction
		err = tx.Commit(ctx)
		require.NoError(t, err, "Failed to commit transaction")

		messages, err := conn.ListMessages(ctx, queueName)
		require.NoError(t, err, "Failed to list messages")
		require.Equal(t, 1, len(messages), "Expected one message")
		require.Equal(t, postgremq.MessageStatusCompleted, messages[0].Status, "Message should be completed")
	})

	// Test Case 2: Rollback of acknowledge (message should remain available)
	t.Run("RollbackTransaction", func(t *testing.T) {

		cleanTestData(t, pool, ctx)
		conn, err := postgremq.DialFromPool(context.Background(), pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		// Create topic with a unique name
		topicName := "test_ack_tx_topic_rollback"
		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "Failed to create topic")

		// Create queue with a unique name
		queueName := "test_ack_tx_queue_rollback"
		err = conn.CreateQueue(ctx, queueName, topicName, false)
		require.NoError(t, err, "Failed to create queue")

		// Publish a message
		payload := []byte(`{"key":"ack_rollback_test","number":999}`)
		msgID, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Failed to publish message")
		require.Greater(t, msgID, int64(0), "Message ID should be positive")

		// Consume the message with a very short visibility timeout (1 second)
		consumer, err := conn.Consume(queueName, postgremq.WithVT(1), postgremq.WithCheckTimeout(1*time.Second))
		require.NoError(t, err, "Failed to create consumer")
		defer consumer.Stop()

		var msg *postgremq.Message
		select {
		case msg = <-consumer.Messages():
			require.Equal(t, msgID, msg.ID, "Message ID mismatch")
		case <-time.After(3 * time.Second):
			require.Fail(t, "Timeout waiting for message")
		}

		// Start a transaction
		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "Failed to begin transaction")

		// Acknowledge the message within the transaction
		err = msg.AckWithTx(ctx, tx)
		require.NoError(t, err, "Failed to acknowledge message within transaction")

		// Rollback the transaction
		err = tx.Rollback(ctx)
		require.NoError(t, err, "Failed to rollback transaction")

		// Wait for visibility timeout to expire and message to be redelivered
		// With 1 second visibility timeout, we should only need to wait a bit longer
		time.Sleep(2 * time.Second)

		// The message should be redelivered since the ack was rolled back
		select {
		case redeliveredMsg := <-consumer.Messages():
			// Verify it's the same message
			require.Equal(t, msgID, redeliveredMsg.ID, "Redelivered message ID mismatch")
			require.Equal(t, 2, redeliveredMsg.DeliveryAttempt, "Expected delivery attempt to be 2")
			// Finally ack the message so it doesn't keep getting redelivered
			err = redeliveredMsg.Ack(ctx)
			require.NoError(t, err, "Failed to ack redelivered message")
		case <-time.After(3 * time.Second):
			require.Fail(t, "Timeout waiting for redelivered message after rollback")
		}
	})
}

// TestMessageID_BeyondInt32 verifies that publish/consume correctly round-trips
// a message id above 2^31. messages.id is BIGSERIAL; without the schema/Go-side
// widening to int64, ids past int32 max would either error or silently
// truncate.
func TestMessageID_BeyondInt32(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const seedTo int64 = 2_200_000_000 // > math.MaxInt32 (2_147_483_647)
	_, err := pool.Exec(ctx, "SELECT setval('postgremq.messages_id_seq', $1)", seedTo)
	require.NoError(t, err, "setval messages_id_seq")

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topicName = "test_int64_topic"
	const queueName = "test_int64_queue"
	require.NoError(t, conn.CreateTopic(ctx, topicName))
	require.NoError(t, conn.CreateQueue(ctx, queueName, topicName, false))

	publishedID, err := conn.Publish(ctx, topicName, []byte(`{"big": true}`))
	require.NoError(t, err)
	require.Greater(t, publishedID, int64(seedTo), "publish_message must hand back the post-setval id")

	consumer, err := conn.Consume(queueName)
	require.NoError(t, err)
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		require.Equal(t, publishedID, msg.ID, "consumed id must equal published id")
		require.NoError(t, msg.Ack(ctx))
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for big-id message")
	}
}
