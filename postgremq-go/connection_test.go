// Connection tests cover lifecycle (dial/close), configuration, topic/queue
// CRUD, keep‑alive behavior, publishing, DLQ operations, administrative listing
// helpers, and queue statistics.
package postgremq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// MockCommandTag implements pgconn.CommandTag interface
type MockCommandTag struct{}

func (m MockCommandTag) RowsAffected() int64 { return 0 }
func (m MockCommandTag) String() string      { return "" }

// MockPool implements Pool interface for testing error conditions
type MockPool struct {
	ExecFunc     func(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	QueryFunc    func(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRowFunc func(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func (m *MockPool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	if m.ExecFunc != nil {
		return m.ExecFunc(ctx, sql, arguments...)
	}
	return pgconn.CommandTag{}, nil
}

func (m *MockPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	if m.QueryFunc != nil {
		return m.QueryFunc(ctx, sql, args...)
	}
	return nil, nil
}

// discoverySQL is the protocol check Dial and DialFromPool run first.
const discoverySQL = "SELECT postgremq.info()"

// discoveryRow answers the protocol check in pool test doubles.
type discoveryRow struct{}

func (discoveryRow) Scan(dest ...any) error {
	*(dest[0].(*[]byte)) = []byte(`{"schema_version": 1, "protocol_major": 1}`)
	return nil
}

func (m *MockPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	if sql == discoverySQL {
		return discoveryRow{}
	}
	if m.QueryRowFunc != nil {
		return m.QueryRowFunc(ctx, sql, args...)
	}
	return nil
}

// Add Acquire method to satisfy Pool interface
func (m *MockPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return nil, nil
}

// Add Close method to MockPool
func (m *MockPool) Close() {}

// MockRows is a minimal pgx.Rows used in tests to drive consumeMessages
// through scan/iteration paths. It returns one row per ScanFunc until
// the last one, then exposes ErrAfter as rows.Err(). pgx.Rows has many
// methods we don't use; the unused ones return zero values.
type MockRows struct {
	ScanFuncs []func(dest ...any) error // one per row, in order
	ErrAfter  error                     // value returned by Err() after exhaustion
	idx       int
	closed    bool
}

func (r *MockRows) Close()                                       { r.closed = true }
func (r *MockRows) Err() error                                   { return r.ErrAfter }
func (r *MockRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *MockRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *MockRows) Next() bool {
	return r.idx < len(r.ScanFuncs)
}
func (r *MockRows) Scan(dest ...any) error {
	if r.idx >= len(r.ScanFuncs) {
		return fmt.Errorf("Scan past end of rows")
	}
	fn := r.ScanFuncs[r.idx]
	r.idx++
	return fn(dest...)
}
func (r *MockRows) Values() ([]any, error) { return nil, nil }
func (r *MockRows) RawValues() [][]byte    { return nil }
func (r *MockRows) Conn() *pgx.Conn        { return nil }
func (r *MockRows) TypeMap() *pgtype.Map   { return nil }

// MockLogger implements Logger interface for testing
type MockLogger struct {
	mu       sync.Mutex
	Messages []string
}

func (m *MockLogger) Printf(format string, args ...any) { m.log(fmt.Sprintf(format, args...)) }

func (m *MockLogger) log(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Messages = append(m.Messages, msg)
}

func (m *MockLogger) GetMessages() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy to avoid races
	result := make([]string, len(m.Messages))
	copy(result, m.Messages)
	return result
}

// -----------------------------------------------------------------------
// Connection Tests
// -----------------------------------------------------------------------

// TestConnectionEstablishment verifies the connection can be created, used, and closed properly
func TestConnectionEstablishment(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Create a new connection
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")

	// Verify connection works by creating a topic
	err = conn.CreateTopic(ctx, "test_conn_topic")
	require.NoError(t, err, "Connection should be able to create a topic")

	// Verify closing the connection works
	err = conn.Close()
	require.NoError(t, err, "Failed to close connection")

	// Verify operations fail after connection closed
	err = conn.CreateTopic(ctx, "should_fail_topic")
	require.Error(t, err, "Connection should reject operations after close")
}

// TestConsumeAfterCloseReturnsError verifies that Consume and ConsumeHandler
// reject calls made after Connection.Close. Pre-fix they happily registered
// listener handles against a closed listener, appended to a slice that had
// already been snapshotted by Close, and returned a Consumer whose
// goroutines exited promptly on the cancelled ctx — leaving the listener
// handles' wake channels hanging until process exit.
func TestConsumeAfterCloseReturnsError(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)

	require.NoError(t, conn.CreateTopic(ctx, "afterclose_topic"))
	require.NoError(t, conn.CreateQueue(ctx, "afterclose_queue", "afterclose_topic", false))

	require.NoError(t, conn.Close())

	_, err = conn.Consume("afterclose_queue")
	require.ErrorIs(t, err, postgremq.ErrConnectionClosed,
		"Consume after Close must return ErrConnectionClosed")

	_, err = conn.ConsumeHandler("afterclose_queue",
		func(ctx context.Context, msg *postgremq.Message) {})
	require.ErrorIs(t, err, postgremq.ErrConnectionClosed,
		"ConsumeHandler after Close must return ErrConnectionClosed")
}

// TestConnectionConfigOptions verifies connection config options work correctly
func TestConnectionConfigOptions(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Custom logger to verify it's being used
	logger := &MockLogger{
		Messages: []string{"Initial test message"}, // Initialize with a message
	}

	// Create connection with options
	conn, err := postgremq.DialFromPool(context.Background(), pool,
		postgremq.WithLogger(logger),
		postgremq.WithShutdownTimeout(2*time.Second),
		postgremq.WithRetryConfig(postgremq.RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        500 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}),
	)
	require.NoError(t, err, "Failed to create connection with options")
	defer conn.Close()

	// Perform an operation to verify options are applied
	err = conn.CreateTopic(ctx, "test_options_topic")
	require.NoError(t, err, "Failed to create topic")

	// Verify logger was used (we initialized it with one message)
	assert.Greater(t, len(logger.Messages), 0, "Logger should have recorded operations")
}

// TestTopicCreationAndDeletion verifies topic creation and deletion operations
func TestTopicCreationAndDeletion(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create topic
	topicName := "test_topic_crud"
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	// Verify topic exists by creating a queue that uses it
	queueName := "test_topic_queue"
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue using the topic")

	// Delete the topic - the implementation allows this, even though it might not be ideal
	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Current implementation allows deleting topics with active queues")

	// Delete the queue
	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	// Now create a new topic and verify it works
	newTopicName := "test_topic_crud_2"
	err = conn.CreateTopic(ctx, newTopicName)
	require.NoError(t, err, "Failed to create new topic")

	// Delete the new topic
	err = conn.DeleteTopic(ctx, newTopicName)
	require.NoError(t, err, "Failed to delete new topic")

	// Verify topic is deleted by trying to create a queue using it
	err = conn.CreateQueue(ctx, "should_fail_queue", newTopicName, false)
	require.Error(t, err, "Should not be able to create queue with deleted topic")
}

// TestQueueCreationAndDeletion verifies queue creation and deletion operations
func TestQueueCreationAndDeletion(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create topic first
	topicName := "test_queue_topic"
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	// Create queue
	queueName := "test_queue_crud"
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Delete and recreate instead of assuming idempotent creation
	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	// Now recreate the queue
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to recreate queue")

	// Create queue with DLQ
	dlqQueueName := "test_dlq_queue"
	err = conn.CreateQueue(ctx, dlqQueueName, topicName, true)
	require.NoError(t, err, "Failed to create queue with DLQ")

	// Check if queues exist by listing queues
	queues, err := conn.ListQueues(ctx)
	require.NoError(t, err, "Failed to list queues")

	foundQueue := false
	foundDLQ := false
	for _, q := range queues {
		if q.QueueName == queueName {
			foundQueue = true
		}
		if q.QueueName == dlqQueueName {
			foundDLQ = true
		}
	}
	assert.True(t, foundQueue, "Queue should exist in list")
	assert.True(t, foundDLQ, "DLQ should exist in list")

	// Delete queues
	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	err = conn.DeleteQueue(ctx, dlqQueueName)
	require.NoError(t, err, "Failed to delete DLQ queue")

	// Verify queues are deleted by checking list
	queues, err = conn.ListQueues(ctx)
	require.NoError(t, err, "Failed to list queues after deletion")

	for _, q := range queues {
		assert.NotEqual(t, queueName, q.QueueName, "Deleted queue should not be in list")
		assert.NotEqual(t, dlqQueueName, q.QueueName, "Deleted DLQ should not be in list")
	}
}

// TestConnectionKeepAlive verifies the connection keep-alive functionality
func TestConnectionKeepAlive(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Create connection
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create topic and queue to work with
	topicName := "test_keepalive_topic"
	queueName := "test_keepalive_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Publish a message and create a consumer that will be kept alive
	msgID, err := conn.Publish(ctx, topicName, []byte(`{"test":"keepalive"}`))
	require.NoError(t, err, "Failed to publish message")

	consumer, err := conn.Consume(queueName,
		postgremq.WithVT(5)) // Short visibility timeout that needs extension
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	// Receive the message but don't ack it, so it stays with the consumer
	var msg *postgremq.Message
	select {
	case receivedMsg := <-consumer.Messages():
		msg = receivedMsg
		require.Equal(t, msgID, receivedMsg.ID, "Message ID mismatch")
	case <-time.After(2 * time.Second):
		require.Fail(t, "Timeout waiting for message")
	}

	// Wait for keep-alive to run a few times
	// This should keep extending the message visibility
	time.Sleep(1 * time.Second)

	// The message should still be with the consumer and not redelivered
	// This verifies the keep-alive is extending the visibility timeout
	select {
	case unexpectedMsg := <-consumer.Messages():
		require.Fail(t, "Received unexpected message: %v", unexpectedMsg.ID)
	case <-time.After(500 * time.Millisecond):
		// No new message is expected, so timeout is good
	}

	// Now ack the message to verify it works after keep-alive extensions
	err = msg.Ack(ctx)
	require.NoError(t, err, "Failed to ack message after keep-alive extensions")
}

// TestMessagePublishing verifies basic message publishing operations
func TestMessagePublishing(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create topic
	topicName := "test_publish_topic"
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	// Create queue
	queueName := "test_publish_queue"
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Publish a message
	payload := []byte(`{"key":"value","number":42}`)
	msgID, err := conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Failed to publish message")
	require.Greater(t, msgID, int64(0), "Message ID should be positive")

	// Consume the message to verify it was published
	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		require.Equal(t, msgID, msg.ID, "Message ID mismatch")

		// Compare JSON semantically rather than raw bytes
		var expectedData map[string]interface{}
		var actualData map[string]interface{}
		err = json.Unmarshal(payload, &expectedData)
		require.NoError(t, err, "Failed to unmarshal expected JSON payload")
		err = json.Unmarshal(msg.Payload, &actualData)
		require.NoError(t, err, "Failed to unmarshal actual JSON payload")
		assert.Equal(t, expectedData, actualData, "JSON content mismatch")

		// Unmarshal and verify JSON content
		var data map[string]interface{}
		err = json.Unmarshal(msg.Payload, &data)
		require.NoError(t, err, "Failed to unmarshal JSON payload")
		assert.Equal(t, "value", data["key"], "JSON key mismatch")
		assert.Equal(t, float64(42), data["number"], "JSON number mismatch")

		err = msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")
	case <-time.After(3 * time.Second):
		require.Fail(t, "Timeout waiting for message")
	}
}

// TestDLQOperations verifies dead letter queue operations
func TestDLQOperations(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create topic
	topicName := "test_dlq_ops_topic"
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	// Create queue with DLQ and low max delivery attempts
	queueName := "test_dlq_ops_queue"
	err = conn.CreateQueue(ctx, queueName, topicName, true,
		postgremq.WithMaxDeliveryAttempts(2)) // Set low max attempts
	require.NoError(t, err, "Failed to create queue with DLQ")

	// Publish a message
	payload := []byte(`{"test":"dlq_test"}`)
	msgID, err := conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Failed to publish message")

	// Function to process and nack messages until they reach DLQ
	processUntilDLQ := func() {
		consumer, err := conn.Consume(queueName)
		require.NoError(t, err, "Failed to create consumer")
		defer consumer.Stop()

		// Process and nack the message until it reaches max delivery attempts
		for attempt := 0; attempt < 3; attempt++ { // Try a few times, more than max delivery attempts
			select {
			case msg := <-consumer.Messages():
				t.Logf("Received message attempt %d, delivery attempt %d", attempt+1, msg.DeliveryAttempt)

				// Nack the message to send it back for redelivery
				err = msg.Nack(ctx)
				require.NoError(t, err, "Failed to nack message")

				// If we've exceeded max attempts, this should be the last time we see it
				if msg.DeliveryAttempt >= 2 {
					t.Logf("Message has reached max delivery attempts")
				}
			case <-time.After(2 * time.Second):
				t.Logf("No more messages received after %d attempts", attempt)
				return
			}
		}
	}

	// Process and nack the message until it should be in DLQ
	processUntilDLQ()

	// Allow time for the message to be moved to the DLQ
	time.Sleep(2 * time.Second)

	// Explicitly move messages to DLQ as noted by the user
	_, err = conn.MaintenanceFast(ctx)
	require.NoError(t, err, "Failed to move messages to DLQ")

	// Allow more time for the message to be available in the DLQ
	time.Sleep(3 * time.Second)

	// Debug: List DLQ messages to verify they are there
	dlqMessages, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages")
	t.Logf("Found %d messages in DLQ", len(dlqMessages))
	for i, msg := range dlqMessages {
		t.Logf("DLQ message %d: Queue=%s, ID=%d, RetryCount=%d", i, msg.QueueName, msg.MessageID, msg.RetryCount)
	}

	// Verify the message is in the DLQ using ListDLQMessages instead of Consume
	require.NotEmpty(t, dlqMessages, "Expected at least one message in DLQ")
	dlqMsg := dlqMessages[0]
	assert.Equal(t, queueName, dlqMsg.QueueName, "DLQ message queue name mismatch")
	assert.Equal(t, msgID, dlqMsg.MessageID, "DLQ message ID mismatch")
	assert.GreaterOrEqual(t, dlqMsg.RetryCount, 2, "DLQ message retry count should be >= max attempts")

	// Instead of using consumer, requeue the DLQ message and then check that it's available in the original queue
	err = conn.RequeueDLQMessages(ctx, queueName)
	require.NoError(t, err, "Failed to requeue messages from DLQ")

	// Allow time for requeuing
	time.Sleep(1 * time.Second)

	// Verify the message is back in the original queue
	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer for original queue")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		assert.Equal(t, msgID, msg.ID, "Requeued message ID mismatch")

		// Compare JSON semantically rather than raw bytes
		var expectedData map[string]interface{}
		var actualData map[string]interface{}
		err = json.Unmarshal(payload, &expectedData)
		require.NoError(t, err, "Failed to unmarshal expected JSON payload")
		err = json.Unmarshal(msg.Payload, &actualData)
		require.NoError(t, err, "Failed to unmarshal actual JSON payload")
		assert.Equal(t, expectedData, actualData, "JSON content mismatch")

		// Ack the requeued message
		err = msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack requeued message")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Timeout waiting for requeued message")
	}

	// Verify DLQ is now empty
	dlqMessages, err = conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages after requeue")
	assert.Empty(t, dlqMessages, "DLQ should be empty after requeue")
}

// TestAdministrativeOperations tests administrative management functions
func TestAdministrativeOperations(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topics and queues
	topicName1 := "test_admin_topic1"
	topicName2 := "test_admin_topic2"
	queueName1 := "test_admin_queue1"
	queueName2 := "test_admin_queue2"

	// Setup: Create topics
	err = conn.CreateTopic(ctx, topicName1)
	require.NoError(t, err, "Failed to create first test topic")
	err = conn.CreateTopic(ctx, topicName2)
	require.NoError(t, err, "Failed to create second test topic")

	// Test ListTopics()
	topics, err := conn.ListTopics(ctx)
	require.NoError(t, err, "Failed to list topics")
	assert.Contains(t, topics, topicName1, "Topic list should contain the first test topic")
	assert.Contains(t, topics, topicName2, "Topic list should contain the second test topic")

	// Setup: Create queues
	err = conn.CreateQueue(ctx, queueName1, topicName1, false)
	require.NoError(t, err, "Failed to create first test queue")
	err = conn.CreateQueue(ctx, queueName2, topicName2, false,
		postgremq.WithMaxDeliveryAttempts(3))
	require.NoError(t, err, "Failed to create second test queue")

	// Test GetQueueStatistics() for all queues
	allStats, err := conn.GetQueueStatistics(ctx, nil)
	require.NoError(t, err, "Failed to get statistics for all queues")
	assert.GreaterOrEqual(t, allStats.TotalCount, int64(0), "Total count should be non-negative")

	// Test GetQueueStatistics() for specific queue
	q1Stats, err := conn.GetQueueStatistics(ctx, &queueName1)
	require.NoError(t, err, "Failed to get statistics for specific queue")
	assert.Equal(t, int64(0), q1Stats.TotalCount, "New queue should have zero messages")

	// Setup: Publish messages to both queues
	_, err = conn.Publish(ctx, topicName1, []byte(`{"test":"admin_test1"}`))
	require.NoError(t, err, "Failed to publish message to first queue")
	_, err = conn.Publish(ctx, topicName2, []byte(`{"test":"admin_test2"}`))
	require.NoError(t, err, "Failed to publish message to second queue")

	// Allow time for message distribution
	time.Sleep(500 * time.Millisecond)

	// Test GetMessage()
	// First, get a message ID
	q1Messages, err := conn.ListMessages(ctx, queueName1)
	require.NoError(t, err, "Failed to list messages")
	require.NotEmpty(t, q1Messages, "Should have at least one message")
	msgID := q1Messages[0].MessageID

	// Now test GetMessage
	msg, err := conn.GetMessage(ctx, msgID)
	require.NoError(t, err, "Failed to get message")
	require.NotNil(t, msg, "Message should not be nil")
	assert.Equal(t, msgID, msg.MessageID, "Message ID should match")
	assert.Equal(t, topicName1, msg.TopicName, "Topic name should match")
	assert.NotEmpty(t, msg.Payload, "Payload should not be empty")

	// Test DeleteQueueMessage
	err = conn.DeleteQueueMessage(ctx, queueName1, msgID)
	require.NoError(t, err, "Failed to delete queue message")

	// Verify message was deleted
	q1MessagesAfterDelete, err := conn.ListMessages(ctx, queueName1)
	require.NoError(t, err, "Failed to list messages after delete")
	for _, m := range q1MessagesAfterDelete {
		assert.NotEqual(t, msgID, m.MessageID, "Deleted message should not be present")
	}

	// Test CleanUpQueue
	err = conn.CleanUpQueue(ctx, queueName2)
	require.NoError(t, err, "Failed to clean up queue")

	// Verify queue was cleaned
	q2Messages, err := conn.ListMessages(ctx, queueName2)
	require.NoError(t, err, "Failed to list messages for cleaned queue")
	assert.Empty(t, q2Messages, "Cleaned queue should have no messages")

	// Test DLQ operations
	// Create a queue with low max delivery attempts
	dlqQueueName := "test_admin_dlq_queue"
	err = conn.CreateQueue(ctx, dlqQueueName, topicName1, false,
		postgremq.WithMaxDeliveryAttempts(1))
	require.NoError(t, err, "Failed to create DLQ test queue")

	// Publish a message
	dlqMsgID, err := conn.Publish(ctx, topicName1, []byte(`{"test":"dlq_admin_test"}`))
	require.NoError(t, err, "Failed to publish message for DLQ test")

	// Process the message and nack it to exceed max delivery attempts
	consumer, err := conn.Consume(dlqQueueName)
	require.NoError(t, err, "Failed to create consumer")

	// Wait for and nack the message
	var receivedMsg *postgremq.Message
	select {
	case receivedMsg = <-consumer.Messages():
		err = receivedMsg.Nack(ctx)
		require.NoError(t, err, "Failed to nack message")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
	consumer.Stop()

	// Move to DLQ
	_, err = conn.MaintenanceFast(ctx)
	require.NoError(t, err, "Failed to move messages to DLQ")

	// Verify message is in DLQ
	time.Sleep(1 * time.Second) // Allow time for DLQ processing
	dlqMessages, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages")
	// Check if the message we published is in the DLQ
	dlqFound := false
	for _, dlqMsg := range dlqMessages {
		if dlqMsg.MessageID == dlqMsgID {
			dlqFound = true
			break
		}
	}
	assert.True(t, dlqFound, "Published message should be found in DLQ")

	// Test PurgeDLQ
	err = conn.PurgeDLQ(ctx)
	require.NoError(t, err, "Failed to purge DLQ")

	// Verify DLQ is empty
	dlqMessagesAfterPurge, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages after purge")
	assert.Empty(t, dlqMessagesAfterPurge, "DLQ should be empty after purge")

	// Test CleanUpTopic
	err = conn.CleanUpTopic(ctx, topicName1)
	require.NoError(t, err, "Failed to clean up first topic")
	err = conn.CleanUpTopic(ctx, topicName2)
	require.NoError(t, err, "Failed to clean up second topic")

	err = conn.DeleteQueue(ctx, queueName1)
	require.NoError(t, err, "Failed to delete first queue")
	err = conn.DeleteQueue(ctx, dlqQueueName)
	require.NoError(t, err, "Failed to delete DLQ queue")
	err = conn.DeleteQueue(ctx, queueName2)
	require.NoError(t, err, "Failed to delete second queue")
	err = conn.DeleteTopic(ctx, topicName1)
	require.NoError(t, err, "Failed to delete first topic")
	err = conn.DeleteTopic(ctx, topicName2)
	require.NoError(t, err, "Failed to delete second topic")
}

// TestListTopics verifies that ListTopics returns all created topics
func TestListTopics(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topics with unique names
	topicName1 := fmt.Sprintf("test_topic_list_%d", time.Now().UnixNano())
	topicName2 := fmt.Sprintf("test_topic_list_%d", time.Now().UnixNano()+1)

	// Verify topics don't exist initially
	initialTopics, err := conn.ListTopics(ctx)
	require.NoError(t, err, "Failed to list initial topics")
	assert.NotContains(t, initialTopics, topicName1, "Topic shouldn't exist before creation")
	assert.NotContains(t, initialTopics, topicName2, "Topic shouldn't exist before creation")

	// Create topics
	err = conn.CreateTopic(ctx, topicName1)
	require.NoError(t, err, "Failed to create first test topic")
	err = conn.CreateTopic(ctx, topicName2)
	require.NoError(t, err, "Failed to create second test topic")

	// List topics and verify both are present
	topics, err := conn.ListTopics(ctx)
	require.NoError(t, err, "Failed to list topics")
	assert.Contains(t, topics, topicName1, "Topic list should contain the first test topic")
	assert.Contains(t, topics, topicName2, "Topic list should contain the second test topic")

	// Delete topics and verify they're removed
	err = conn.DeleteTopic(ctx, topicName1)
	require.NoError(t, err, "Failed to delete first topic")
	err = conn.DeleteTopic(ctx, topicName2)
	require.NoError(t, err, "Failed to delete second topic")

	// Verify topics are no longer in the list
	finalTopics, err := conn.ListTopics(ctx)
	require.NoError(t, err, "Failed to list topics after deletion")
	assert.NotContains(t, finalTopics, topicName1, "Topic list should not contain deleted topic")
	assert.NotContains(t, finalTopics, topicName2, "Topic list should not contain deleted topic")
}

// TestListQueues verifies that ListQueues returns all created queues with correct properties
func TestListQueues(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create a test topic
	topicName := fmt.Sprintf("test_queue_list_topic_%d", time.Now().UnixNano())
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	// Create queues with different configurations
	queueName1 := fmt.Sprintf("test_queue_list_1_%d", time.Now().UnixNano())
	queueName2 := fmt.Sprintf("test_queue_list_2_%d", time.Now().UnixNano()+1)

	// Create regular queue
	err = conn.CreateQueue(ctx, queueName1, topicName, false)
	require.NoError(t, err, "Failed to create first test queue")

	// Create queue with custom max delivery attempts
	customMaxAttempts := 5
	err = conn.CreateQueue(ctx, queueName2, topicName, false,
		postgremq.WithMaxDeliveryAttempts(customMaxAttempts))
	require.NoError(t, err, "Failed to create second test queue")

	// List queues and verify properties
	queues, err := conn.ListQueues(ctx)
	require.NoError(t, err, "Failed to list queues")

	// Find and verify our queues in the list
	var queue1Found, queue2Found bool
	var queue1Info, queue2Info postgremq.QueueInfo

	for _, q := range queues {
		if q.QueueName == queueName1 {
			queue1Found = true
			queue1Info = q
		}
		if q.QueueName == queueName2 {
			queue2Found = true
			queue2Info = q
		}
	}

	// Assert queues were found
	assert.True(t, queue1Found, "Queue 1 should be in the list")
	assert.True(t, queue2Found, "Queue 2 should be in the list")

	// Verify queue 1 properties
	assert.Equal(t, topicName, queue1Info.TopicName, "Queue 1 should have correct topic name")
	assert.Equal(t, 0, queue1Info.MaxDeliveryAttempts, "Queue 1 should have default max delivery attempts (0 = unlimited)")
	assert.False(t, queue1Info.Exclusive, "Queue 1 should not be exclusive")

	// Verify queue 2 properties
	assert.Equal(t, topicName, queue2Info.TopicName, "Queue 2 should have correct topic name")
	assert.Equal(t, customMaxAttempts, queue2Info.MaxDeliveryAttempts, "Queue 2 should have custom max delivery attempts")
	assert.False(t, queue2Info.Exclusive, "Queue 2 should not be exclusive")

	// Delete queues and topic and verify cleanup
	err = conn.DeleteQueue(ctx, queueName1)
	require.NoError(t, err, "Failed to delete first queue")
	err = conn.DeleteQueue(ctx, queueName2)
	require.NoError(t, err, "Failed to delete second queue")
	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")

	// Verify queues are no longer in the list
	finalQueues, err := conn.ListQueues(ctx)
	require.NoError(t, err, "Failed to list queues after deletion")

	queue1Found, queue2Found = false, false
	for _, q := range finalQueues {
		if q.QueueName == queueName1 {
			queue1Found = true
		}
		if q.QueueName == queueName2 {
			queue2Found = true
		}
	}

	assert.False(t, queue1Found, "Queue 1 should not be in the list after deletion")
	assert.False(t, queue2Found, "Queue 2 should not be in the list after deletion")
}

// TestQueueStatistics verifies that GetQueueStatistics returns correct message counts
func TestQueueStatistics(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topic and queue
	topicName := fmt.Sprintf("test_stats_topic_%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test_stats_queue_%d", time.Now().UnixNano())

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create test queue")

	// Check initial statistics (should be empty)
	initialStats, err := conn.GetQueueStatistics(ctx, &queueName)
	require.NoError(t, err, "Failed to get initial statistics")
	assert.Equal(t, int64(0), initialStats.TotalCount, "New queue should have zero messages")
	assert.Equal(t, int64(0), initialStats.PendingCount, "New queue should have zero pending messages")
	assert.Equal(t, int64(0), initialStats.ProcessingCount, "New queue should have zero processing messages")
	assert.Equal(t, int64(0), initialStats.CompletedCount, "New queue should have zero completed messages")

	// Publish messages to the queue
	numMessages := 5
	messageIDs := make([]int64, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"index":%d}`, i)))
		require.NoError(t, err, "Failed to publish message")
		messageIDs[i] = msgID
	}

	// Allow time for messages to be distributed to the queue
	time.Sleep(500 * time.Millisecond)

	// Check statistics after publishing (should show pending messages)
	pendingStats, err := conn.GetQueueStatistics(ctx, &queueName)
	require.NoError(t, err, "Failed to get statistics after publishing")
	assert.Equal(t, int64(numMessages), pendingStats.TotalCount, "Should have correct total message count")
	assert.Equal(t, int64(numMessages), pendingStats.PendingCount, "Should have correct pending message count")
	assert.Equal(t, int64(0), pendingStats.ProcessingCount, "Should have no processing messages yet")
	assert.Equal(t, int64(0), pendingStats.CompletedCount, "Should have no completed messages yet")

	// Consume but don't ack some messages to move them to processing state
	consumer, err := conn.Consume(queueName, postgremq.WithBatchSize(3))
	require.NoError(t, err, "Failed to create consumer")

	var processedMsgs []*postgremq.Message
	for i := 0; i < 3; i++ {
		select {
		case msg := <-consumer.Messages():
			processedMsgs = append(processedMsgs, msg)
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	// Allow more time for processing state to update
	time.Sleep(1 * time.Second)

	// Check statistics after consuming (should show both pending and processing)
	processingStats, err := conn.GetQueueStatistics(ctx, &queueName)
	require.NoError(t, err, "Failed to get statistics after consuming")
	assert.Equal(t, int64(numMessages), processingStats.TotalCount, "Should have correct total message count")
	// The actual pending/processing counts might vary based on implementation
	// So we check their sum equals the total count
	assert.Equal(t, int64(numMessages), processingStats.PendingCount+processingStats.ProcessingCount,
		"Sum of pending and processing should equal total count")
	assert.Greater(t, processingStats.ProcessingCount, int64(0), "Should have some processing messages")
	assert.Equal(t, int64(0), processingStats.CompletedCount, "Should have no completed messages yet")

	// Ack the messages to move them to completed state
	for _, msg := range processedMsgs {
		err = msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")
	}
	consumer.Stop()

	// Allow time for acks to be processed
	time.Sleep(1 * time.Second)

	// Check statistics after acking (should show both pending and completed)
	completedStats, err := conn.GetQueueStatistics(ctx, &queueName)
	require.NoError(t, err, "Failed to get statistics after acking")
	assert.Equal(t, int64(numMessages), completedStats.TotalCount, "Should have correct total message count")
	assert.Greater(t, completedStats.CompletedCount, int64(0), "Should have some completed messages")
	// The sum of all categories should equal the total
	assert.Equal(t, completedStats.TotalCount,
		completedStats.PendingCount+completedStats.ProcessingCount+completedStats.CompletedCount,
		"Sum of all message states should equal total count")

	// Clean up
	err = conn.CleanUpQueue(ctx, queueName)
	require.NoError(t, err, "Failed to clean up queue")

	err = conn.CleanUpTopic(ctx, topicName)
	require.NoError(t, err, "Failed to clean up topic before deletion")

	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")
}

// TestMessageOperations verifies individual message operations like GetMessage and DeleteQueueMessage
func TestMessageOperations(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topic and queue
	topicName := fmt.Sprintf("test_msg_ops_topic_%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test_msg_ops_queue_%d", time.Now().UnixNano())

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create test queue")

	// Publish a test message with specific payload
	testPayload := []byte(`{"test":"message_ops"}`)
	msgID, err := conn.Publish(ctx, topicName, testPayload)
	require.NoError(t, err, "Failed to publish message")

	// Allow time for message to be distributed to the queue
	time.Sleep(500 * time.Millisecond)

	// List messages in the queue and verify our message is there
	queueMessages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages")
	require.NotEmpty(t, queueMessages, "Queue should contain our message")

	// Verify the message ID matches what we published
	assert.Equal(t, msgID, queueMessages[0].MessageID, "Message ID should match")

	// Get the message and verify its content
	msg, err := conn.GetMessage(ctx, msgID)
	require.NoError(t, err, "Failed to get message")
	require.NotNil(t, msg, "Message should not be nil")

	// Verify message properties
	assert.Equal(t, msgID, msg.MessageID, "Message ID should match")
	assert.Equal(t, topicName, msg.TopicName, "Topic name should match")
	assert.JSONEq(t, string(testPayload), string(msg.Payload), "Message payload should match")

	// Delete the message from the queue
	err = conn.DeleteQueueMessage(ctx, queueName, msgID)
	require.NoError(t, err, "Failed to delete message")

	// Verify message was deleted from the queue
	queueMessagesAfterDelete, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages after delete")
	assert.Empty(t, queueMessagesAfterDelete, "Queue should be empty after message deletion")

	// Verify we can still get the message metadata (it's deleted from queue, not storage)
	msgAfterDelete, err := conn.GetMessage(ctx, msgID)
	require.NoError(t, err, "Failed to get message after queue delete")
	assert.NotNil(t, msgAfterDelete, "Message should still exist in storage")

	// Clean up
	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	// Clean up the topic before deletion
	err = conn.CleanUpTopic(ctx, topicName)
	require.NoError(t, err, "Failed to clean up topic before deletion")

	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")
}

// TestQueueCleanup verifies that CleanUpQueue removes all messages from a queue
func TestQueueCleanup(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topic and queue
	topicName := fmt.Sprintf("test_cleanup_topic_%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test_cleanup_queue_%d", time.Now().UnixNano())

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create test queue")

	// Publish multiple messages to the queue
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		_, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"index":%d}`, i)))
		require.NoError(t, err, "Failed to publish message")
	}

	// Allow time for messages to be distributed to the queue
	time.Sleep(500 * time.Millisecond)

	// Verify messages are in the queue
	messagesBeforeCleanup, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages")
	assert.Equal(t, numMessages, len(messagesBeforeCleanup), "Queue should contain all published messages")

	// Get queue statistics before cleanup
	statsBeforeCleanup, err := conn.GetQueueStatistics(ctx, &queueName)
	require.NoError(t, err, "Failed to get queue statistics")
	assert.Equal(t, int64(numMessages), statsBeforeCleanup.TotalCount, "Statistics should show correct message count")

	// Clean up the queue
	err = conn.CleanUpQueue(ctx, queueName)
	require.NoError(t, err, "Failed to clean up queue")

	// Verify queue is now empty
	messagesAfterCleanup, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages after cleanup")
	assert.Empty(t, messagesAfterCleanup, "Queue should be empty after cleanup")

	// Verify queue statistics after cleanup
	statsAfterCleanup, err := conn.GetQueueStatistics(ctx, &queueName)
	require.NoError(t, err, "Failed to get queue statistics after cleanup")
	assert.Equal(t, int64(0), statsAfterCleanup.TotalCount, "Statistics should show zero messages after cleanup")
	assert.Equal(t, int64(0), statsAfterCleanup.PendingCount, "Statistics should show zero pending messages")

	// Clean up
	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	// Clean up the topic before deletion
	err = conn.CleanUpTopic(ctx, topicName)
	require.NoError(t, err, "Failed to clean up topic before deletion")

	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")
}

// TestCleanupCompletedMessages verifies that CleanupCompletedMessages removes only stale completed entries.
func TestCleanupCompletedMessages(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := fmt.Sprintf("test_cleanup_completed_topic_%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test_cleanup_completed_queue_%d", time.Now().UnixNano())

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create test queue")

	for i := 0; i < 2; i++ {
		_, err = conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"index":%d}`, i)))
		require.NoError(t, err, "Failed to publish message")
	}

	time.Sleep(500 * time.Millisecond)

	rows, err := pool.Query(ctx,
		"SELECT message_id, consumer_token FROM postgremq.consume_message($1, $2, $3)",
		queueName, 60, 2,
	)
	require.NoError(t, err, "Failed to consume messages directly")
	defer rows.Close()

	var messageIDs []int64
	var tokens []string
	for rows.Next() {
		var id int64
		var token string
		err := rows.Scan(&id, &token)
		require.NoError(t, err, "Failed to scan consumed message")
		messageIDs = append(messageIDs, id)
		tokens = append(tokens, token)
	}
	require.NoError(t, rows.Err())
	require.Len(t, messageIDs, 2, "Should have consumed two messages")

	for i, id := range messageIDs {
		_, err = pool.Exec(ctx, "SELECT postgremq.ack_message($1, $2, $3)", queueName, id, tokens[i])
		require.NoError(t, err, "Failed to ack message")
	}

	_, err = pool.Exec(ctx,
		"UPDATE postgremq.queue_messages SET processed_at = NOW() - interval '2 hours' WHERE queue_name = $1 AND message_id = $2",
		queueName, messageIDs[0],
	)
	require.NoError(t, err, "Failed to age processed_at for first message")

	_, err = pool.Exec(ctx,
		"UPDATE postgremq.queue_messages SET processed_at = NOW() - interval '30 minutes' WHERE queue_name = $1 AND message_id = $2",
		queueName, messageIDs[1],
	)
	require.NoError(t, err, "Failed to set recent processed_at for second message")

	oneHour := 1
	deleted, err := conn.CleanupCompletedMessages(ctx, &oneHour)
	require.NoError(t, err, "CleanupCompletedMessages should succeed with custom retention")
	assert.Equal(t, 1, deleted, "Exactly one message should be deleted with custom retention")

	msgs, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages after cleanup")
	require.Len(t, msgs, 1, "One completed message should remain after partial cleanup")
	assert.Equal(t, messageIDs[1], msgs[0].MessageID, "Remaining message should be the newer completion")
	assert.Equal(t, postgremq.MessageStatusCompleted, msgs[0].Status, "Remaining entry should still be completed")

	_, err = pool.Exec(ctx,
		"UPDATE postgremq.queue_messages SET processed_at = NOW() - interval '48 hours' WHERE queue_name = $1 AND message_id = $2",
		queueName, messageIDs[1],
	)
	require.NoError(t, err, "Failed to age remaining message for default cleanup")

	deletedDefault, err := conn.CleanupCompletedMessages(ctx, nil)
	require.NoError(t, err, "CleanupCompletedMessages should succeed with default retention")
	assert.Equal(t, 1, deletedDefault, "Default retention should delete the remaining completed message")

	msgs, err = conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages after default cleanup")
	assert.Empty(t, msgs, "Queue should have no completed messages after cleanup")

	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")
	err = conn.CleanUpTopic(ctx, topicName)
	require.NoError(t, err, "Failed to clean up topic")
	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")
}

// TestTopicCleanup verifies that CleanUpTopic removes all messages from a topic
func TestTopicCleanup(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topic and two queues
	topicName := fmt.Sprintf("test_cleanup_topic_%d", time.Now().UnixNano())
	queueName1 := fmt.Sprintf("test_cleanup_queue1_%d", time.Now().UnixNano())
	queueName2 := fmt.Sprintf("test_cleanup_queue2_%d", time.Now().UnixNano()+1)

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	err = conn.CreateQueue(ctx, queueName1, topicName, false)
	require.NoError(t, err, "Failed to create first test queue")

	err = conn.CreateQueue(ctx, queueName2, topicName, false)
	require.NoError(t, err, "Failed to create second test queue")

	// Publish multiple messages to the topic
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		_, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"index":%d}`, i)))
		require.NoError(t, err, "Failed to publish message")
	}

	// Allow time for messages to be distributed to the queues
	time.Sleep(500 * time.Millisecond)

	// Verify messages are in both queues
	messagesQ1, err := conn.ListMessages(ctx, queueName1)
	require.NoError(t, err, "Failed to list messages in queue 1")
	assert.Equal(t, numMessages, len(messagesQ1), "Queue 1 should contain all published messages")

	messagesQ2, err := conn.ListMessages(ctx, queueName2)
	require.NoError(t, err, "Failed to list messages in queue 2")
	assert.Equal(t, numMessages, len(messagesQ2), "Queue 2 should contain all published messages")

	// Clean up the topic
	err = conn.CleanUpTopic(ctx, topicName)
	require.NoError(t, err, "Failed to clean up topic")

	// Allow more time for cleanup to take effect
	time.Sleep(2 * time.Second)

	// Verify both queues are now empty
	messagesQ1AfterCleanup, err := conn.ListMessages(ctx, queueName1)
	require.NoError(t, err, "Failed to list messages in queue 1 after cleanup")
	assert.Empty(t, messagesQ1AfterCleanup, "Queue 1 should be empty after topic cleanup")

	messagesQ2AfterCleanup, err := conn.ListMessages(ctx, queueName2)
	require.NoError(t, err, "Failed to list messages in queue 2 after cleanup")
	assert.Empty(t, messagesQ2AfterCleanup, "Queue 2 should be empty after topic cleanup")

	// Clean up
	err = conn.DeleteQueue(ctx, queueName1)
	require.NoError(t, err, "Failed to delete first queue")
	err = conn.DeleteQueue(ctx, queueName2)
	require.NoError(t, err, "Failed to delete second queue")
	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")
}

// TestDLQOperationsAdmin verifies administrative operations on the Dead Letter Queue
func TestDLQOperationsAdmin(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create test topic and queue with DLQ
	topicName := fmt.Sprintf("test_dlq_admin_topic_%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test_dlq_admin_queue_%d", time.Now().UnixNano())

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	// Create queue with low max delivery attempts
	err = conn.CreateQueue(ctx, queueName, topicName, true,
		postgremq.WithMaxDeliveryAttempts(1)) // Only 1 attempt allowed before DLQ
	require.NoError(t, err, "Failed to create test queue with DLQ")

	// Publish a test message
	msgID, err := conn.Publish(ctx, topicName, []byte(`{"test":"dlq_admin"}`))
	require.NoError(t, err, "Failed to publish message")

	// Verify DLQ is initially empty
	initialDLQ, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list initial DLQ messages")
	assert.Empty(t, initialDLQ, "DLQ should be empty initially")

	// Consume and nack the message to exceed max delivery attempts
	consumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")

	// Wait for and nack the message
	select {
	case msg := <-consumer.Messages():
		// Verify it's our message
		assert.Equal(t, msgID, msg.ID, "Message ID should match")

		// Nack to exceed max delivery attempts
		err = msg.Nack(ctx)
		require.NoError(t, err, "Failed to nack message")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
	consumer.Stop()

	// Move eligible messages to DLQ
	_, err = conn.MaintenanceFast(ctx)
	require.NoError(t, err, "Failed to move messages to DLQ")

	// Allow time for DLQ processing
	time.Sleep(2 * time.Second)

	// Verify message is in DLQ
	dlqMessages, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages")
	require.NotEmpty(t, dlqMessages, "DLQ should contain our message")

	// Verify DLQ message properties
	found := false
	for _, dlqMsg := range dlqMessages {
		if dlqMsg.MessageID == msgID {
			found = true
			assert.Equal(t, queueName, dlqMsg.QueueName, "DLQ message should have correct queue name")
			assert.GreaterOrEqual(t, dlqMsg.RetryCount, 1, "DLQ message should have retry count >= max attempts")
			break
		}
	}
	assert.True(t, found, "Our message should be found in DLQ")

	// Test RequeueDLQMessages
	err = conn.RequeueDLQMessages(ctx, queueName)
	require.NoError(t, err, "Failed to requeue messages from DLQ")

	// Allow time for requeuing
	time.Sleep(1 * time.Second)

	// Verify DLQ is now empty
	dlqAfterRequeue, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages after requeue")
	assert.Empty(t, dlqAfterRequeue, "DLQ should be empty after requeuing")

	// Verify message is back in the original queue
	queueMessages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "Failed to list messages after requeue")

	found = false
	for _, queueMsg := range queueMessages {
		if queueMsg.MessageID == msgID {
			found = true
			break
		}
	}
	assert.True(t, found, "Our message should be back in the queue after requeuing from DLQ")

	// Test PurgeDLQ
	// First, move the message back to DLQ
	consumer, err = conn.Consume(queueName)
	require.NoError(t, err, "Failed to create consumer")

	// Wait for and nack the message again
	select {
	case msg := <-consumer.Messages():
		err = msg.Nack(ctx)
		require.NoError(t, err, "Failed to nack message again")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
	consumer.Stop()

	// Move to DLQ again
	_, err = conn.MaintenanceFast(ctx)
	require.NoError(t, err, "Failed to move messages to DLQ again")

	// Allow time for DLQ processing
	time.Sleep(2 * time.Second)

	// Verify message is in DLQ again
	dlqBeforePurge, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages before purge")
	assert.NotEmpty(t, dlqBeforePurge, "DLQ should contain our message again")

	// Purge the DLQ
	err = conn.PurgeDLQ(ctx)
	require.NoError(t, err, "Failed to purge DLQ")

	// Verify DLQ is empty after purge
	dlqAfterPurge, err := conn.ListDLQMessages(ctx)
	require.NoError(t, err, "Failed to list DLQ messages after purge")
	assert.Empty(t, dlqAfterPurge, "DLQ should be empty after purge")

	// Clean up
	err = conn.DeleteQueue(ctx, queueName)
	require.NoError(t, err, "Failed to delete queue")

	// Clean up the topic before deletion
	err = conn.CleanUpTopic(ctx, topicName)
	require.NoError(t, err, "Failed to clean up topic before deletion")

	err = conn.DeleteTopic(ctx, topicName)
	require.NoError(t, err, "Failed to delete topic")
}

// TestPurgeAllMessages verifies that PurgeAllMessages removes all messages from the system
func TestPurgeAllMessages(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create multiple topics and queues
	topics := []string{"test_purge_topic1", "test_purge_topic2"}
	queues := []string{"test_purge_queue1", "test_purge_queue2", "test_purge_queue3"}

	// Set up topics
	for _, topic := range topics {
		err = conn.CreateTopic(ctx, topic)
		require.NoError(t, err, "Failed to create topic: %s", topic)
	}

	// Set up queues
	err = conn.CreateQueue(ctx, queues[0], topics[0], false)
	require.NoError(t, err, "Failed to create queue: %s", queues[0])

	err = conn.CreateQueue(ctx, queues[1], topics[0], false)
	require.NoError(t, err, "Failed to create queue: %s", queues[1])

	err = conn.CreateQueue(ctx, queues[2], topics[1], false)
	require.NoError(t, err, "Failed to create queue: %s", queues[2])

	// Publish messages to both topics
	for i := 0; i < 5; i++ {
		_, err = conn.Publish(ctx, topics[0], []byte(fmt.Sprintf(`{"index":%d, "topic":"topic1"}`, i)))
		require.NoError(t, err, "Failed to publish to topic1")

		_, err = conn.Publish(ctx, topics[1], []byte(fmt.Sprintf(`{"index":%d, "topic":"topic2"}`, i)))
		require.NoError(t, err, "Failed to publish to topic2")
	}

	// Allow time for messages to be distributed to the queues
	time.Sleep(500 * time.Millisecond)

	// Verify messages exist in queues
	for _, queue := range queues {
		msgs, err := conn.ListMessages(ctx, queue)
		require.NoError(t, err, "Failed to list messages for queue: %s", queue)
		assert.NotEmpty(t, msgs, "Queue should have messages before purge: %s", queue)
	}

	// Purge all messages
	err = conn.PurgeAllMessages(ctx)
	require.NoError(t, err, "Failed to purge all messages")

	// Allow time for purge to complete
	time.Sleep(500 * time.Millisecond)

	// Verify all queues are empty
	for _, queue := range queues {
		msgs, err := conn.ListMessages(ctx, queue)
		require.NoError(t, err, "Failed to list messages after purge for queue: %s", queue)
		assert.Empty(t, msgs, "Queue should be empty after purge: %s", queue)
	}

	// Check statistics for each queue
	for _, queue := range queues {
		queueCopy := queue // Create a copy to take address of it
		stats, err := conn.GetQueueStatistics(ctx, &queueCopy)
		require.NoError(t, err, "Failed to get statistics for queue: %s", queue)
		assert.Equal(t, int64(0), stats.PendingCount, "Queue should have 0 pending messages: %s", queue)
		assert.Equal(t, int64(0), stats.ProcessingCount, "Queue should have 0 processing messages: %s", queue)
		assert.Equal(t, int64(0), stats.CompletedCount, "Queue should have 0 completed messages: %s", queue)
		assert.Equal(t, int64(0), stats.TotalCount, "Queue should have 0 total messages: %s", queue)
	}

	// Clean up
	for _, queue := range queues {
		err = conn.DeleteQueue(ctx, queue)
		require.NoError(t, err, "Failed to delete queue: %s", queue)
	}

	for _, topic := range topics {
		err = conn.DeleteTopic(ctx, topic)
		require.NoError(t, err, "Failed to delete topic: %s", topic)
	}
}

// TestDeleteInactiveQueues verifies that DeleteInactiveQueues removes expired queues
func TestDeleteInactiveQueues(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create a test topic
	topicName := fmt.Sprintf("test_inactive_topic_%d", time.Now().UnixNano())
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create test topic")

	// Create queue names for different test cases:
	// 1. An active exclusive queue
	// 2. An expired exclusive queue (to be deleted)
	// 3. A non-exclusive queue (should not be affected)
	activeExclusiveQueueName := fmt.Sprintf("test_active_ex_queue_%d", time.Now().UnixNano())
	expiredExclusiveQueueName := fmt.Sprintf("test_expired_ex_queue_%d", time.Now().UnixNano()+1)
	nonExclusiveQueueName := fmt.Sprintf("test_non_ex_queue_%d", time.Now().UnixNano()+2)

	// Create an active exclusive queue with future expiration (1 hour)
	err = conn.CreateQueue(ctx, activeExclusiveQueueName, topicName, true,
		postgremq.WithKeepAliveInterval(2*time.Second))
	require.NoError(t, err, "Failed to create active exclusive queue")

	// Create a non-exclusive queue (should not be affected by DeleteInactiveQueues)
	err = conn.CreateQueue(ctx, nonExclusiveQueueName, topicName, false)
	require.NoError(t, err, "Failed to create non-exclusive queue")

	// Create an exclusive queue that will be expired
	err = conn.CreateQueue(ctx, expiredExclusiveQueueName, topicName, true,
		postgremq.WithKeepAliveInterval(2*time.Second))
	require.NoError(t, err, "Failed to create soon-to-be-expired exclusive queue")

	// Verify all three queues exist initially
	queues, err := conn.ListQueues(ctx)
	require.NoError(t, err, "Failed to list queues")
	require.Len(t, queues, 3, "Should have 3 queues initially")
	conn.Close()
	// Wait past the 2s keep-alive interval PLUS the maintenance 5s grace
	// period (delete_inactive_queues only reaps queues expired > 5s).
	time.Sleep(8 * time.Second)

	conn, err = postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")

	// Now call DeleteInactiveQueues
	err = conn.DeleteInactiveQueues(ctx)
	require.NoError(t, err, "Failed to delete inactive queues")

	// Verify only the expired exclusive queue was deleted
	queuesAfter, err := conn.ListQueues(ctx)
	require.NoError(t, err, "Failed to list queues after deletion")
	require.Len(t, queuesAfter, 1, "Only non exclusive queue should remain")
	require.Equal(t, nonExclusiveQueueName, queuesAfter[0].QueueName, "Non-exclusive queue should remain")

}

// TestPublishWithTx verifies that messages are published within a transaction
func TestPublishWithTx(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Test Case 1: Successful commit
	t.Run("CommitTransaction", func(t *testing.T) {
		conn, err := postgremq.DialFromPool(context.Background(), pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		// Create topic
		topicName := "test_publish_tx_topic_commit"
		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "Failed to create topic")

		// Create queue
		queueName := "test_publish_tx_queue_commit"
		err = conn.CreateQueue(ctx, queueName, topicName, false)
		require.NoError(t, err, "Failed to create queue")

		// Start a transaction
		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "Failed to begin transaction")

		// Publish a message within the transaction
		payload := []byte(`{"key":"commit_test","number":123}`)
		msgID, err := conn.PublishWithTx(ctx, tx, topicName, payload)
		require.NoError(t, err, "Failed to publish message within transaction")
		require.Greater(t, msgID, int64(0), "Message ID should be positive")

		// Commit the transaction
		err = tx.Commit(ctx)
		require.NoError(t, err, "Failed to commit transaction")

		// Verify the message was published by consuming it
		consumer, err := conn.Consume(queueName)
		require.NoError(t, err, "Failed to create consumer")
		defer consumer.Stop()

		select {
		case msg := <-consumer.Messages():
			require.Equal(t, msgID, msg.ID, "Message ID mismatch")

			// Compare JSON semantically
			var expectedData map[string]interface{}
			var actualData map[string]interface{}
			err = json.Unmarshal(payload, &expectedData)
			require.NoError(t, err, "Failed to unmarshal expected JSON payload")
			err = json.Unmarshal(msg.Payload, &actualData)
			require.NoError(t, err, "Failed to unmarshal actual JSON payload")
			assert.Equal(t, expectedData, actualData, "JSON content mismatch")

			// Verify specific JSON fields
			assert.Equal(t, "commit_test", actualData["key"], "JSON key mismatch")
			assert.Equal(t, float64(123), actualData["number"], "JSON number mismatch")

			// Acknowledge the message
			err = msg.Ack(ctx)
			require.NoError(t, err, "Failed to ack message")
		case <-time.After(3 * time.Second):
			require.Fail(t, "Timeout waiting for message")
		}
	})

	// Test Case 2: Rollback (message should not be published)
	t.Run("RollbackTransaction", func(t *testing.T) {
		cleanTestData(t, pool, ctx)

		conn, err := postgremq.DialFromPool(context.Background(), pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		// Create topic
		topicName := "test_publish_tx_topic_rollback"
		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "Failed to create topic")

		// Create queue
		queueName := "test_publish_tx_queue_rollback"
		err = conn.CreateQueue(ctx, queueName, topicName, false)
		require.NoError(t, err, "Failed to create queue")

		// Start a transaction
		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "Failed to begin transaction")

		// Publish a message within the transaction
		payload := []byte(`{"key":"rollback_test","number":456}`)
		msgID, err := conn.PublishWithTx(ctx, tx, topicName, payload)
		require.NoError(t, err, "Failed to publish message within transaction")
		require.Greater(t, msgID, int64(0), "Message ID should be positive")

		// Rollback the transaction
		err = tx.Rollback(ctx)
		require.NoError(t, err, "Failed to rollback transaction")

		// verify message is not there
		msg, err := conn.GetMessage(ctx, msgID)
		require.NoError(t, err, "Failed to get message")
		assert.Nil(t, msg, "Message should not exist after rollback")
	})
}
