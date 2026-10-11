// Integration tests exercise concurrent processing, high‑throughput delivery,
// and recovery scenarios end‑to‑end using live PostgreSQL containers.
package postgremq_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// -----------------------------------------------------------------------
// Integration Tests - Complex Scenarios
// -----------------------------------------------------------------------

// TestConcurrentMessageProcessing verifies that concurrent consumers process messages
// exactly once under load
func TestConcurrentMessageProcessing(t *testing.T) {
	skipIfShort(t) // Skip in short test mode
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_concurrent_topic"
	queueName := "test_concurrent_queue"
	numConsumers := 5    // Number of concurrent consumers
	numMessages := 50    // Total messages to publish
	processingTime := 50 // Simulated processing time in milliseconds

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	// Create queue with visibility timeout option
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "Failed to create queue")

	// Create a map to track processed messages
	var (
		processedMsgs     = make(map[int64]int)
		processedMsgsLock sync.Mutex
		totalProcessed    atomic.Int32
	)

	// Launch consumers before publishing
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start multiple consumers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			consumerConn, err := postgremq.DialFromPool(context.Background(), pool)
			require.NoError(t, err, "Failed to create consumer connection")
			defer consumerConn.Close()

			// Each consumer uses its own Connection, so its topic cache is
			// empty for queueName. Pass the topic explicitly.
			consumer, err := consumerConn.Consume(queueName, postgremq.WithTopic(topicName))
			require.NoError(t, err, "Failed to create consumer")
			defer consumer.Stop()

			for {
				select {
				case msg := <-consumer.Messages():
					// Simulate processing time
					time.Sleep(time.Duration(processingTime) * time.Millisecond)

					// Record the processed message
					processedMsgsLock.Lock()
					processedMsgs[msg.ID] = processedMsgs[msg.ID] + 1
					processedMsgsLock.Unlock()

					// Acknowledge message
					err := msg.Ack(ctx)
					if err == nil {
						totalProcessed.Add(1)
					}
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
					// If no messages for 5 seconds and we've processed all messages, exit
					if totalProcessed.Load() >= int32(numMessages) {
						return
					}
				}
			}
		}(i)
	}

	// Give consumers time to start
	time.Sleep(200 * time.Millisecond)

	// Publish test messages
	for i := 0; i < numMessages; i++ {
		_, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"message":"test-%d"}`, i)))
		require.NoError(t, err, "Failed to publish message")
	}

	// Wait for consumers to process all messages or timeout
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer waitCancel()

	// Wait for all messages to be processed or timeout
	for totalProcessed.Load() < int32(numMessages) {
		select {
		case <-waitCtx.Done():
			t.Fatalf("Only processed %d out of %d messages before timeout", totalProcessed.Load(), numMessages)
		case <-time.After(100 * time.Millisecond):
			continue
		}
	}

	// Cancel the consumer goroutines and wait for them to finish
	cancel()
	wg.Wait()

	// Verify each message was processed exactly once
	processedMsgsLock.Lock()
	defer processedMsgsLock.Unlock()

	// Check that all messages were processed exactly once
	assert.Equal(t, numMessages, len(processedMsgs), "Not all messages were processed")

	multipleDeliveries := 0
	for msgID, count := range processedMsgs {
		if count > 1 {
			multipleDeliveries++
			t.Logf("Message %d was processed %d times", msgID, count)
		}
	}

	assert.Equal(t, 0, multipleDeliveries, "Some messages were processed multiple times")
	assert.Equal(t, int32(numMessages), totalProcessed.Load(), "Total processed count mismatch")
}

// TestHighThroughputDelivery verifies the system can handle high-throughput
// message delivery scenarios
func TestHighThroughputDelivery(t *testing.T) {
	skipIfShort(t) // Skip in short test mode
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_highthroughput_topic"
	queueName := "test_highthroughput_queue"
	batchSize := 20
	numMessages := 200
	timeout := 60 * time.Second // Generous timeout for the whole test

	// Setup topic and queue
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Track received messages
	var (
		received     = make(map[int64]bool)
		receivedLock sync.Mutex
		wg           sync.WaitGroup
	)

	// Create timeout context
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, timeout)
	defer timeoutCancel()

	// Create a separate done context to signal all consumers when finished
	doneCtx, doneCancel := context.WithCancel(context.Background())
	defer doneCancel()

	// Use multiple consumers for higher throughput
	numConsumers := 3

	// Start multiple consumers
	for i := 0; i < numConsumers; i++ {
		consumer, err := conn.Consume(queueName,
			postgremq.WithBatchSize(batchSize),
			postgremq.WithVT(10))
		require.NoError(t, err, "Failed to create consumer")
		defer consumer.Stop()

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case msg := <-consumer.Messages():
					// Process the message
					receivedLock.Lock()
					received[msg.ID] = true
					count := len(received)

					// Signal all consumers to stop if we've received all messages
					if count >= numMessages {
						doneCancel() // Signal all consumers to exit
					}

					receivedLock.Unlock()

					err := msg.Ack(ctx)
					require.NoError(t, err, "Failed to ack message")

					// Log progress periodically
					if count%100 == 0 || count == numMessages {
						t.Logf("Consumer %d: Processed %d/%d messages (%.1f%%)",
							id, count, numMessages, float64(count)/float64(numMessages)*100)
					}

				case <-timeoutCtx.Done():
					return
				case <-doneCtx.Done():
					// Exit when all messages have been processed
					return
				}
			}
		}(i)
	}

	// Publish messages
	startTime := time.Now()
	for i := 0; i < numMessages; i++ {
		_, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"index":%d}`, i)))
		require.NoError(t, err, "Failed to publish message")
	}
	t.Logf("Published %d messages", numMessages)

	// Wait for processing to complete or timeout
	completionDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(completionDone)
	}()

	select {
	case <-completionDone:
		// All consumers completed
	case <-timeoutCtx.Done():
		t.Logf("Test timed out after %v", timeout)
	}

	// Get final count and performance metrics
	receivedLock.Lock()
	finalCount := len(received)
	receivedLock.Unlock()

	duration := time.Since(startTime)
	messagesPerSecond := float64(numMessages) / duration.Seconds()
	t.Logf("Processed %d messages in %.2f seconds (%.2f msgs/sec)",
		finalCount, duration.Seconds(), messagesPerSecond)

	// Verify all messages were received
	assert.Equal(t, numMessages, finalCount, "Not all messages were received")
}

// TestRecoveryFromFailure tests the system's ability to recover from failures
func TestRecoveryFromFailure(t *testing.T) {
	skipIfShort(t) // Skip in short test mode
	// Note: Not running in parallel due to resource contention issues
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_recovery_topic"
	queueName := "test_recovery_queue"

	// Setup topic and queue
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false,
		postgremq.WithMaxDeliveryAttempts(5))
	require.NoError(t, err, "Failed to create queue")

	// Publish test messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		_, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"value":%d}`, i)))
		require.NoError(t, err, "Failed to publish message")
	}

	// First consumer that will fail to process some messages
	failingConsumer, err := conn.Consume(queueName, postgremq.WithVT(1))
	require.NoError(t, err, "Failed to create failing consumer")

	// Process half the messages successfully, nack the rest
	for i := 0; i < numMessages; i++ {
		select {
		case msg := <-failingConsumer.Messages():
			if i < numMessages/2 {
				// Successfully process half the messages
				err := msg.Ack(ctx)
				require.NoError(t, err, "Failed to ack message")
			} else {
				// Fail to process the other half
				err := msg.Nack(ctx)
				require.NoError(t, err, "Failed to nack message")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for messages")
		}
	}

	// Stop the failing consumer
	failingConsumer.Stop()

	// Verify that the nacked messages are redelivered to a new consumer
	recoveryConsumer, err := conn.Consume(queueName)
	require.NoError(t, err, "Failed to create recovery consumer")
	defer recoveryConsumer.Stop()

	// We should receive the nacked messages (half of total)
	expectedRedeliveries := numMessages / 2
	redelivered := 0

	for redelivered < expectedRedeliveries {
		select {
		case msg := <-recoveryConsumer.Messages():
			// All redelivered messages should have delivery attempt > 1
			assert.Greater(t, msg.DeliveryAttempt, 1, "Redelivered message should have increased delivery attempt")

			err := msg.Ack(ctx)
			require.NoError(t, err, "Failed to ack redelivered message")
			redelivered++

		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for redelivered messages, got %d/%d",
				redelivered, expectedRedeliveries)
		}
	}

	assert.Equal(t, expectedRedeliveries, redelivered,
		"Should have received exactly the nacked messages")
}
