// Single-loop consumer tests: head-of-line behavior under a slow /
// non-reading consumer, and Stop()/Close() shutdown races.
package postgremq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// TestConsumerStopWhenDeliveryBlocked verifies the head-of-line guarantee: when
// the caller stops reading, delivery blocks (channel buffer + outbox full) but
// the run loop still reacts to Stop() promptly. A loop that did a blocking send
// outside its select would wedge here and Stop() would hang.
func TestConsumerStopWhenDeliveryBlocked(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))
	for i := 0; i < 6; i++ {
		_, err := conn.Publish(ctx, "topic", []byte(fmt.Sprintf(`{"i":%d}`, i)))
		require.NoError(t, err)
	}

	// batchSize 2 ⇒ channel buffer 2; the loop prefetches another batch into the
	// outbox, then delivery blocks because nothing is read.
	consumer, err := conn.Consume("q", postgremq.WithBatchSize(2), postgremq.WithVT(30))
	require.NoError(t, err)
	// Give the loop time to fill the buffer + outbox and block on delivery.
	time.Sleep(300 * time.Millisecond)

	done := make(chan struct{})
	go func() { consumer.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() hung while delivery was blocked (head-of-line regression)")
	}

	// Every fetched message must be back to pending with NO delivery-attempt
	// bump (released, not nacked).
	msgs, err := conn.ListMessages(ctx, "q")
	require.NoError(t, err)
	require.Len(t, msgs, 6)
	for _, m := range msgs {
		assert.Equal(t, postgremq.MessageStatusPending, m.Status)
		assert.Equal(t, 0, m.DeliveryAttempts, "released messages must not bump delivery_attempts")
	}
}

// TestConsumerSlowReaderDeliversAll verifies that a consumer reading slowly
// (with prefetch/backpressure in play) still receives and acks every message
// exactly once.
func TestConsumerSlowReaderDeliversAll(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))
	const n = 12
	for i := 0; i < n; i++ {
		_, err := conn.Publish(ctx, "topic", []byte(fmt.Sprintf(`{"i":%d}`, i)))
		require.NoError(t, err)
	}

	consumer, err := conn.Consume("q", postgremq.WithBatchSize(3), postgremq.WithVT(30))
	require.NoError(t, err)
	defer consumer.Stop()

	seen := map[int64]bool{}
	for len(seen) < n {
		select {
		case m := <-consumer.Messages():
			seen[m.ID] = true
			time.Sleep(40 * time.Millisecond) // slow reader
			require.NoError(t, m.Ack(ctx))
		case <-time.After(10 * time.Second):
			t.Fatalf("only received %d/%d messages", len(seen), n)
		}
	}

	require.Eventually(t, func() bool {
		st, err := conn.GetQueueStatistics(ctx, ptr("q"))
		return err == nil && st.CompletedCount == n
	}, 5*time.Second, 50*time.Millisecond, "all messages should be completed")
}

// TestConsumerStopCloseRace interleaves Consumer.Stop() with Connection.Close()
// (run with -race) to shake out shutdown races between the two paths.
func TestConsumerStopCloseRace(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))
	for i := 0; i < 5; i++ {
		_, err := conn.Publish(ctx, "topic", []byte(`{}`))
		require.NoError(t, err)
	}

	consumer, err := conn.Consume("q", postgremq.WithBatchSize(3), postgremq.WithVT(30))
	require.NoError(t, err)

	// Read one to have an in-flight message during the race.
	select {
	case m := <-consumer.Messages():
		go func() { _ = m.Ack(context.Background()) }()
	case <-time.After(2 * time.Second):
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); consumer.Stop() }()
	go func() { defer wg.Done(); _ = conn.Close() }()
	wg.Wait()
}

func ptr[T any](v T) *T { return &v }
