// Benchmarks measure publishing throughput under different payload sizes and
// end‑to‑end publish‑consume throughput with varying consumers and batch sizes.
package postgremq_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// -----------------------------------------------------------------------
// Benchmark Tests
// -----------------------------------------------------------------------

// setupBenchConnection creates a test database connection for benchmarks
func setupBenchConnection(b *testing.B) (*pgxpool.Pool, context.Context) {
	ctx := context.Background()

	// Check for connection string in environment variable
	connString := os.Getenv("POSTGREMQ_TEST_DB_URL")
	if connString == "" {
		// Use default connection string if environment variable is not set
		connString = "postgres://postgres:postgres@localhost:5432/postgremq_test"
		b.Logf("POSTGREMQ_TEST_DB_URL not set, using default: %s", connString)
	}

	pool, err := pgxpool.New(ctx, connString)
	require.NoError(b, err, "Failed to connect to test database")

	// Clean up any leftover data
	cleanBenchData(b, pool, ctx)
	return pool, ctx
}

// cleanBenchData cleans test data for benchmarks
func cleanBenchData(b *testing.B, pool *pgxpool.Pool, ctx context.Context) {
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	if err != nil {
		b.Fatalf("Failed to create connection for cleanup: %v", err)
	}
	defer conn.Close()

	// Clean up test data
	err = conn.PurgeAllMessages(ctx)
	if err != nil {
		b.Logf("Warning: Failed to purge messages: %v", err)
	}

	err = conn.PurgeDLQ(ctx)
	if err != nil {
		b.Logf("Warning: Failed to purge DLQ: %v", err)
	}
}

// generatePayload creates a JSON payload of specified size for testing
func generatePayload(size int) []byte {
	// Create a JSON object with an array of bytes to reach the target size
	// Start with fixed overhead: {"data":[]} = ~10 bytes
	overhead := 10
	arraySize := (size - overhead) / 2 // Each element with comma is about 2 bytes

	if arraySize <= 0 {
		arraySize = 1
	}

	var payload string
	payload = `{"data":[`

	for i := 0; i < arraySize; i++ {
		if i > 0 {
			payload += ","
		}
		payload += fmt.Sprintf("%d", i%10)
	}

	payload += `]}`
	return []byte(payload)
}

// BenchmarkMessagePublishing measures the throughput of publishing messages
// with different payload sizes
func BenchmarkMessagePublishing(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	pool, ctx := setupBenchConnection(b)
	defer pool.Close()
	defer cleanBenchData(b, pool, ctx)

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(b, err, "Failed to create connection")
	defer conn.Close()

	topicName := "benchmark_publish_topic"
	err = conn.CreateTopic(ctx, topicName)
	require.NoError(b, err, "Failed to create topic")

	// Define different payload sizes to benchmark
	payloadSizes := []int{
		100,   // 100 bytes
		1024,  // 1 KB
		10240, // 10 KB
	}

	for _, size := range payloadSizes {
		// Generate a payload of the given size
		payload := generatePayload(size)

		b.Run(fmt.Sprintf("PayloadSize_%dB", size), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := conn.Publish(ctx, topicName, payload)
				if err != nil {
					b.Fatalf("Failed to publish message: %v", err)
				}
			}

			// Report throughput in MB/s
			bytesProcessed := int64(b.N) * int64(size)
			b.ReportMetric(float64(bytesProcessed)/float64(b.Elapsed().Seconds())/1024/1024, "MB/s")
		})
	}
}

// BenchmarkPublishConsume measures end-to-end throughput of publishing and consuming messages
func BenchmarkPublishConsume(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	pool, ctx := setupBenchConnection(b)
	defer pool.Close()
	defer cleanBenchData(b, pool, ctx)

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(b, err, "Failed to create connection")
	defer conn.Close()

	topicName := "benchmark_pubsub_topic"
	queueName := "benchmark_pubsub_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(b, err, "Failed to create topic")
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(b, err, "Failed to create queue")

	// Payload sizes to test
	payloadSizes := []int{100, 1024, 10240} // 100B, 1KB, 10KB

	for _, size := range payloadSizes {
		payload := generatePayload(size)

		b.Run(fmt.Sprintf("PayloadSize_%dB", size), func(b *testing.B) {
			// Create a consumer
			consumer, err := conn.Consume(queueName, postgremq.WithBatchSize(10))
			require.NoError(b, err, "Failed to create consumer")
			defer consumer.Stop()

			// Wait group to track completion
			var wg sync.WaitGroup
			wg.Add(b.N)

			// Start consumer goroutine
			receivedCount := 0
			go func() {
				for receivedCount < b.N {
					select {
					case msg := <-consumer.Messages():
						err := msg.Ack(ctx)
						if err != nil {
							b.Logf("Failed to ack message: %v", err)
						}
						receivedCount++
						wg.Done()
					case <-time.After(10 * time.Second):
						b.Logf("Timeout waiting for messages, only received %d/%d", receivedCount, b.N)
						return
					}
				}
			}()

			// Reset timer before starting publishing
			b.ResetTimer()

			// Publish messages
			for i := 0; i < b.N; i++ {
				_, err := conn.Publish(ctx, topicName, payload)
				if err != nil {
					b.Fatalf("Failed to publish message: %v", err)
				}
			}

			// Wait for all messages to be consumed
			wg.Wait()
			b.StopTimer()

			// Report throughput in messages/s and MB/s
			seconds := b.Elapsed().Seconds()
			b.ReportMetric(float64(b.N)/seconds, "msgs/s")
			bytesProcessed := int64(b.N) * int64(size)
			b.ReportMetric(float64(bytesProcessed)/seconds/1024/1024, "MB/s")
		})
	}
}

// BenchmarkConcurrentConsumers measures throughput with different numbers of concurrent consumers
func BenchmarkConcurrentConsumers(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	pool, ctx := setupBenchConnection(b)
	defer pool.Close()
	defer cleanBenchData(b, pool, ctx)

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(b, err, "Failed to create connection")
	defer conn.Close()

	topicName := "benchmark_concurrent_topic"
	queueName := "benchmark_concurrent_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(b, err, "Failed to create topic")
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(b, err, "Failed to create queue")

	// Number of consumers to test
	consumerCounts := []int{1, 2, 4, 8}
	payloadSize := 1024 // 1KB messages
	payload := generatePayload(payloadSize)

	for _, numConsumers := range consumerCounts {
		b.Run(fmt.Sprintf("Consumers_%d", numConsumers), func(b *testing.B) {
			// Create multiple connections for consumers
			consumers := make([]*postgremq.Consumer, numConsumers)
			connections := make([]*postgremq.Connection, numConsumers)

			for i := 0; i < numConsumers; i++ {
				consConn, err := postgremq.DialFromPool(context.Background(), pool)
				require.NoError(b, err, "Failed to create consumer connection")
				connections[i] = consConn

				consumer, err := consConn.Consume(queueName)
				require.NoError(b, err, "Failed to create consumer")
				consumers[i] = consumer
			}

			// Defer cleanup
			defer func() {
				for i := 0; i < numConsumers; i++ {
					consumers[i].Stop()
					connections[i].Close()
				}
			}()

			// Wait group to track message processing
			var wg sync.WaitGroup
			wg.Add(b.N)

			// Track received messages
			var receivedCount int
			var mu sync.Mutex

			// Start consumer goroutines
			for i := 0; i < numConsumers; i++ {
				go func(consumer *postgremq.Consumer) {
					for {
						select {
						case msg := <-consumer.Messages():
							// Process the message
							err := msg.Ack(ctx)
							if err != nil {
								b.Logf("Failed to ack message: %v", err)
								continue
							}

							mu.Lock()
							receivedCount++
							count := receivedCount
							mu.Unlock()

							wg.Done()

							// Exit when all messages are processed
							if count >= b.N {
								return
							}
						case <-time.After(5 * time.Second):
							// Timeout, likely all messages processed by other consumers
							return
						}
					}
				}(consumers[i])
			}

			// Reset timer before publishing
			b.ResetTimer()

			// Publish test messages
			for i := 0; i < b.N; i++ {
				_, err := conn.Publish(ctx, topicName, payload)
				if err != nil {
					b.Fatalf("Failed to publish message: %v", err)
				}
			}

			// Wait for all messages to be processed
			wg.Wait()
			b.StopTimer()

			// Report metrics
			seconds := b.Elapsed().Seconds()
			b.ReportMetric(float64(b.N)/seconds, "msgs/s")
		})
	}
}

// BenchmarkBatchProcessing measures the performance of batch message processing
func BenchmarkBatchProcessing(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	pool, ctx := setupBenchConnection(b)
	defer pool.Close()
	defer cleanBenchData(b, pool, ctx)

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(b, err, "Failed to create connection")
	defer conn.Close()

	topicName := "benchmark_batch_topic"
	queueName := "benchmark_batch_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(b, err, "Failed to create topic")
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(b, err, "Failed to create queue")

	// Test different batch sizes
	batchSizes := []int{1, 5, 10, 20, 50}
	payloadSize := 512 // 512 byte messages
	payload := generatePayload(payloadSize)

	// Fix the number of total messages for consistency
	totalMessages := 500
	if testing.Short() {
		totalMessages = 100
	}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			// Publish all messages upfront
			for i := 0; i < totalMessages; i++ {
				_, err := conn.Publish(ctx, topicName, payload)
				require.NoError(b, err, "Failed to publish message")
			}

			// Create consumer with the specific batch size
			consumer, err := conn.Consume(queueName,
				postgremq.WithBatchSize(batchSize))
			require.NoError(b, err, "Failed to create consumer")
			defer consumer.Stop()

			// Track received count
			receivedCount := 0

			// Start the timer before processing
			b.ResetTimer()

			// Process all messages
			for receivedCount < totalMessages {
				select {
				case msg := <-consumer.Messages():
					err := msg.Ack(ctx)
					require.NoError(b, err, "Failed to ack message")
					receivedCount++
				case <-time.After(10 * time.Second):
					b.Fatalf("Timeout waiting for messages, only received %d/%d",
						receivedCount, totalMessages)
				}
			}

			// Stop timer after all messages processed
			b.StopTimer()

			// Report metrics
			seconds := b.Elapsed().Seconds()
			b.ReportMetric(float64(totalMessages)/seconds, "msgs/s")
			b.ReportMetric(float64(seconds*1000)/float64(totalMessages), "ms/msg")
		})
	}
}
