// Queue-fatal teardown tests: when a queue a consumer depends on is gone
// (deleted out-of-band, or an exclusive queue whose keep-alive permanently
// failed), the consumer is torn down — handlers cancelled, in-flight messages
// deregistered from the extender, Messages() closed — and the reason is surfaced
// via Consumer.NotifyClose and the connection-level WithQueueFatalHandler.
package postgremq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// TestQueueFatalOnDeletedQueue verifies that deleting a (non-exclusive) queue
// out-of-band makes its consumer fatal: Messages() closes, NotifyClose delivers
// a *QueueFatalError (matching ErrQueueGone), and the connection handler fires —
// instead of the consumer retrying the fetch forever.
func TestQueueFatalOnDeletedQueue(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	fatal := make(chan string, 4)
	conn, err := postgremq.DialFromPool(context.Background(), pool,
		postgremq.WithQueueFatalHandler(func(queue string, err error) {
			assert.ErrorIs(t, err, postgremq.ErrQueueGone)
			fatal <- queue
		}))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))

	// Short checkTimeout so the consumer re-fetches promptly and hits the
	// deleted queue quickly.
	consumer, err := conn.Consume("q", postgremq.WithVT(30),
		postgremq.WithCheckTimeout(200*time.Millisecond))
	require.NoError(t, err)
	defer consumer.Stop()

	closeReason := consumer.NotifyClose(make(chan error, 1))

	// Delete the queue out from under the consumer.
	require.NoError(t, conn.DeleteQueue(ctx, "q"))

	// Messages() must close (range ends).
	select {
	case _, ok := <-consumer.Messages():
		assert.False(t, ok, "Messages() should be closed, not deliver a message")
	case <-time.After(5 * time.Second):
		t.Fatal("Messages() did not close after the queue was deleted")
	}

	// NotifyClose must deliver the typed reason.
	select {
	case err := <-closeReason:
		require.Error(t, err)
		assert.ErrorIs(t, err, postgremq.ErrQueueGone)
		var qfe *postgremq.QueueFatalError
		require.ErrorAs(t, err, &qfe)
		assert.Equal(t, "q", qfe.Queue)
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyClose did not deliver a reason")
	}

	// Connection-level handler must fire for the queue.
	select {
	case q := <-fatal:
		assert.Equal(t, "q", q)
	case <-time.After(2 * time.Second):
		t.Fatal("WithQueueFatalHandler did not fire")
	}
}

// TestQueueFatalNormalStopNoError verifies that a normal Stop closes NotifyClose
// WITHOUT an error (nil reason), distinguishing it from a fatal teardown.
func TestQueueFatalNormalStopNoError(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))

	consumer, err := conn.Consume("q", postgremq.WithVT(30))
	require.NoError(t, err)
	closeReason := consumer.NotifyClose(make(chan error, 1))

	consumer.Stop()

	select {
	case err, ok := <-closeReason:
		assert.False(t, ok, "normal Stop should close NotifyClose with no error; got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyClose was not closed after Stop")
	}
}

// TestQueueFatalTearsDownAllConsumersOnQueue verifies that a fatal queue tears
// down EVERY consumer bound to it (consumers can share a queue) and fires the
// connection handler once.
func TestQueueFatalTearsDownAllConsumersOnQueue(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	fatal := make(chan string, 8)
	conn, err := postgremq.DialFromPool(context.Background(), pool,
		postgremq.WithQueueFatalHandler(func(queue string, err error) { fatal <- queue }))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))

	c1, err := conn.Consume("q", postgremq.WithVT(30), postgremq.WithCheckTimeout(200*time.Millisecond))
	require.NoError(t, err)
	defer c1.Stop()
	c2, err := conn.Consume("q", postgremq.WithVT(30), postgremq.WithCheckTimeout(200*time.Millisecond))
	require.NoError(t, err)
	defer c2.Stop()

	close1 := c1.NotifyClose(make(chan error, 1))
	close2 := c2.NotifyClose(make(chan error, 1))

	require.NoError(t, conn.DeleteQueue(ctx, "q"))

	for i, ch := range []chan error{close1, close2} {
		select {
		case err := <-ch:
			assert.ErrorIs(t, err, postgremq.ErrQueueGone, "consumer %d", i+1)
		case <-time.After(5 * time.Second):
			t.Fatalf("consumer %d was not torn down", i+1)
		}
	}

	// Exactly one connection-level fatal for the queue (idempotent per queue).
	select {
	case q := <-fatal:
		assert.Equal(t, "q", q)
	case <-time.After(2 * time.Second):
		t.Fatal("WithQueueFatalHandler did not fire")
	}
	select {
	case q := <-fatal:
		t.Fatalf("queue-fatal fired more than once: %s", q)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestQueueFatalProducerOnlyExclusiveQueue verifies that an exclusive queue with
// NO consumer (producer-only) still surfaces a fatal via the connection handler
// when keep-alive permanently fails. Uses a spy pool to force keep-alive
// omission without waiting for real expiry.
func TestQueueFatalProducerOnlyExclusiveQueue(t *testing.T) {
	t.Parallel()
	spy := &keepAliveSpyPool{
		keptFn: func(names []string) []string { return nil }, // omit all => permanent
	}
	fatal := make(chan string, 4)
	conn, err := postgremq.DialFromPool(context.Background(), spy,
		postgremq.WithQueueFatalHandler(func(queue string, err error) {
			assert.ErrorIs(t, err, postgremq.ErrQueueGone)
			fatal <- queue
		}))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateQueue(context.Background(), "exq", "t", true,
		postgremq.WithKeepAliveInterval(200*time.Millisecond)))

	select {
	case q := <-fatal:
		assert.Equal(t, "exq", q)
	case <-time.After(3 * time.Second):
		t.Fatal("producer-only exclusive queue did not surface a queue-fatal")
	}
}

// TestQueueFatalInFlightDeregisteredFromExtender verifies the cleanup contract:
// when a queue goes fatal, an in-flight message is deregistered from the
// connection extender (its handler's StoppedCtx is cancelled) so it stops being
// auto-extended.
func TestQueueFatalInFlightDeregisteredFromExtender(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()
	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "topic"))
	require.NoError(t, conn.CreateQueue(ctx, "q", "topic", false))
	_, err = conn.Publish(ctx, "topic", []byte(`{}`))
	require.NoError(t, err)

	consumer, err := conn.Consume("q", postgremq.WithVT(30),
		postgremq.WithCheckTimeout(200*time.Millisecond))
	require.NoError(t, err)
	defer consumer.Stop()

	// Hold one message in-flight (don't settle it).
	var msg *postgremq.Message
	select {
	case msg = <-consumer.Messages():
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive the message")
	}
	require.NotNil(t, msg)

	// Delete the queue: the in-flight message's handler must be cancelled
	// (StoppedCtx fires) as part of teardown.
	require.NoError(t, conn.DeleteQueue(ctx, "q"))

	select {
	case <-msg.StoppedCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight message's StoppedCtx was not cancelled on queue-fatal")
	}

	// Settle the cancelled message (a well-behaved handler would) so the
	// consumer's drain completes — the release fails (queue gone) but complete()
	// untracks unconditionally, draining the in-flight set. Without this the
	// deferred Stop() would wait forever for the held message (existing contract).
	_ = msg.Release(context.Background())
}
