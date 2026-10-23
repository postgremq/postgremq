package postgremq_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

// waitForListen probes the given LISTEN channel with test NOTIFYs until the
// handle is woken — by a probe, or by the wake the listener sends once the
// channel's LISTEN is in place. Either proves that the listener has issued
// LISTEN on the wire and is delivering events to this handle; a probe still
// in flight wakes the handle later, so callers drain it with drainHandle.
// Without this sync, publish-and-expect-wake tests can race the asynchronous
// LISTEN registration (subscription change → interrupted wait → reconcile →
// LISTEN command).
func waitForListen(t *testing.T, pool *pgxpool.Pool, handle *postgremq.ListenerHandle, channel string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := pool.Exec(context.Background(),
			"SELECT pg_notify($1, 'probe')", channel)
		require.NoError(t, err)
		select {
		case <-handle.Wake():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("LISTEN for %q did not become active within 2s", channel)
}

// drainHandle drains pending wake events on the handle (e.g. probe events
// queued by waitForListen). Returns when no event arrives within 50ms.
func drainHandle(handle *postgremq.ListenerHandle) {
	for {
		select {
		case <-handle.Wake():
		case <-time.After(50 * time.Millisecond):
			return
		}
	}
}

// TestPerTopicNotify_CacheHit verifies that Consume against a queue created
// through this Connection does not need a DB lookup to discover the topic.
// We delete the queue between CreateQueue and Consume; if Consume hits the
// cache, it succeeds; if it falls back to DB, it would error.
func TestPerTopicNotify_CacheHit(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "TopicCacheHit"
	const queue = "QueueCacheHit"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false, postgremq.WithMaxDeliveryAttempts(0)))

	// Drop the queue at the SQL level so a DB lookup would fail. The cache
	// entry from CreateQueue lets Consume succeed regardless.
	_, err = pool.Exec(ctx, "DELETE FROM postgremq.queues WHERE name = $1", queue)
	require.NoError(t, err)

	consumer, err := conn.Consume(queue, postgremq.WithVT(30))
	require.NoError(t, err)
	consumer.Stop()
}

// TestPerTopicNotify_OutOfBandQueueNeedsWithTopic verifies that a queue that
// exists in the database but was not created through this Connection requires
// the caller to pass WithTopic — the client does not query the database to
// resolve the topic.
func TestPerTopicNotify_OutOfBandQueueNeedsWithTopic(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topic = "TopicOutOfBand"
	const queue = "QueueOutOfBand"
	_, err := pool.Exec(ctx, "SELECT postgremq.create_topic($1)", topic)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "SELECT postgremq.create_queue($1, $2, 0, false, interval '30 seconds')", queue, topic)
	require.NoError(t, err)

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	// Without WithTopic, Consume errors because the cache is empty.
	_, err = conn.Consume(queue, postgremq.WithVT(30))
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrQueueNotFound))

	// With WithTopic, it succeeds.
	consumer, err := conn.Consume(queue, postgremq.WithVT(30), postgremq.WithTopic(topic))
	require.NoError(t, err)
	consumer.Stop()
}

// TestPerTopicNotify_WithTopicOption verifies WithTopic bypasses both the
// topic cache. The queue generation is resolved from the database.
func TestPerTopicNotify_WithTopicOption(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	_, err = pool.Exec(ctx, "SELECT postgremq.create_topic('ExplicitTopic'); SELECT postgremq.create_queue('ExternalQueue','ExplicitTopic')")
	require.NoError(t, err)
	consumer, err := conn.Consume("ExternalQueue",
		postgremq.WithVT(30),
		postgremq.WithTopic("ExplicitTopic"),
	)
	require.NoError(t, err)
	consumer.Stop()
}

// TestPerTopicNotify_UnknownQueueReturnsError verifies that consuming from a
// queue that does not exist in the database (and is not in the cache) returns
// ErrQueueNotFound.
func TestPerTopicNotify_UnknownQueueReturnsError(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Consume("GhostQueue", postgremq.WithVT(30))
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrQueueNotFound),
		"expected ErrQueueNotFound, got: %v", err)
}

// TestPerTopicNotify_RefcountSharing verifies that two consumers on the same
// topic share a single physical LISTEN per channel.
func TestPerTopicNotify_RefcountSharing(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "RefcountTopic"
	const queueA = "RefcountQueueA"
	const queueB = "RefcountQueueB"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queueA, topic, false, postgremq.WithMaxDeliveryAttempts(0)))
	require.NoError(t, conn.CreateQueue(ctx, queueB, topic, false, postgremq.WithMaxDeliveryAttempts(0)))

	c1, err := conn.Consume(queueA, postgremq.WithVT(30))
	require.NoError(t, err)
	c2, err := conn.Consume(queueB, postgremq.WithVT(30))
	require.NoError(t, err)

	// Brief settle so both subscriptions have registered.
	time.Sleep(50 * time.Millisecond)

	el := conn.EventListener()
	assert.Equal(t, 2, el.SubscriberCount("pmq:t:"+topic),
		"two consumers on the same topic should share a refcount of 2")
	assert.Equal(t, 1, el.SubscriberCount("pmq:q:"+queueA))
	assert.Equal(t, 1, el.SubscriberCount("pmq:q:"+queueB))

	c1.Stop()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, el.SubscriberCount("pmq:t:"+topic),
		"after one consumer stops, the topic refcount drops to 1")
	assert.Equal(t, 0, el.SubscriberCount("pmq:q:"+queueA),
		"queueA listener should be released")

	c2.Stop()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, el.SubscriberCount("pmq:t:"+topic),
		"after both consumers stop, the topic listener is released")
}

// TestEventListener_DeliversTopicNotify verifies that a NOTIFY on a per-topic
// channel is delivered to a handle's Wake channel with the parsed message id.
// Tests the listener directly, bypassing Consumer to avoid conflating the
// listener path with Consumer's initial fetch / polling fallbacks.
func TestEventListener_DeliversTopicNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()

	const topic = "DirectTopic"
	handle := el.AddTopicListener(topic)
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:t:"+topic)
	drainHandle(handle)

	_, err = pool.Exec(ctx, "SELECT pg_notify($1, '')", "pmq:t:"+topic)
	require.NoError(t, err)

	select {
	case <-handle.Wake():
		// success — wake fired; payload is empty by contract
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive NOTIFY on per-topic channel")
	}
}

// TestEventListener_DeliversQueueNotify is the per-queue counterpart.
func TestEventListener_DeliversQueueNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()

	const queue = "DirectQueue"
	handle := el.AddQueueListener(queue)
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:q:"+queue)
	drainHandle(handle)

	_, err = pool.Exec(ctx, "SELECT pg_notify($1, '')", "pmq:q:"+queue)
	require.NoError(t, err)

	select {
	case <-handle.Wake():
		// success — wake fired; payload is empty by contract
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive NOTIFY on per-queue channel")
	}
}

// TestEventListener_PublishTriggersTopicNotify verifies that the SQL
// distribute_message trigger emits NOTIFY on `pmq:t:<topic>` and that it
// reaches a directly-attached handle. The NOTIFY carries no payload —
// the channel name alone is the wake-up signal.
func TestEventListener_PublishTriggersTopicNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "PubTopic"))

	el := conn.EventListener()
	el.Start()

	handle := el.AddTopicListener("PubTopic")
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:t:PubTopic")
	drainHandle(handle)

	_, err = conn.Publish(ctx, "PubTopic", []byte(`{}`))
	require.NoError(t, err)

	select {
	case <-handle.Wake():
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("publish_message did not produce NOTIFY on per-topic channel")
	}
}

// TestEventListener_NackTriggersPerQueueNotify verifies that the SQL
// nack_message function emits NOTIFY on `pmq:q:<queue>` (when not retiring
// to DLQ inline) carrying the affected message id.
func TestEventListener_NackTriggersPerQueueNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "NackTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "NackQueue", "NackTopic", false))

	msgID, err := conn.Publish(ctx, "NackTopic", []byte(`{}`))
	require.NoError(t, err)

	// Consume via raw SQL to grab a token without involving Consumer.
	var consumedID int64
	var token string
	err = pool.QueryRow(ctx, `
		SELECT message_id, consumer_token FROM postgremq.consume_message($1, 30)
	`, "NackQueue").Scan(&consumedID, &token)
	require.NoError(t, err)
	require.Equal(t, int64(msgID), consumedID)

	el := conn.EventListener()
	el.Start()

	handle := el.AddQueueListener("NackQueue")
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:q:NackQueue")
	drainHandle(handle)

	_, err = pool.Exec(ctx, `SELECT postgremq.nack_message($1, $2, $3, 0)`, "NackQueue", msgID, token)
	require.NoError(t, err)

	select {
	case <-handle.Wake():
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("nack_message did not produce NOTIFY on per-queue channel")
	}
}

// TestEventListener_ReleaseTriggersPerQueueNotify verifies release_message
// emits NOTIFY on `pmq:q:<queue>`.
func TestEventListener_ReleaseTriggersPerQueueNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "RelTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "RelQueue", "RelTopic", false))

	msgID, err := conn.Publish(ctx, "RelTopic", []byte(`{}`))
	require.NoError(t, err)

	var token string
	err = pool.QueryRow(ctx, `SELECT consumer_token FROM postgremq.consume_message($1, 30)`, "RelQueue").Scan(&token)
	require.NoError(t, err)

	el := conn.EventListener()
	el.Start()

	handle := el.AddQueueListener("RelQueue")
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:q:RelQueue")
	drainHandle(handle)

	_, err = pool.Exec(ctx, `SELECT postgremq.release_message($1, $2, $3)`, "RelQueue", msgID, token)
	require.NoError(t, err)

	select {
	case <-handle.Wake():
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("release_message did not produce NOTIFY on per-queue channel")
	}
}

// TestEventListener_HandleCloseClosesWake verifies that ListenerHandle.Close
// closes the Wake channel — both via Consumer.Stop() and via direct handle
// close. Previously, only EventListener.Close closed wake channels and the
// Consumer-driven path left them open.
func TestEventListener_HandleCloseClosesWake(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	conn.EventListener().Start()

	h1 := conn.EventListener().AddTopicListener("HandleCloseTopic")
	h2 := conn.EventListener().AddQueueListener("HandleCloseQueue")
	wake1 := h1.Wake()
	wake2 := h2.Wake()

	h1.Close()
	h2.Close()

	// Both wake channels must be closed after the handle's Close.
	assertChannelClosed(t, wake1, "topic handle wake should close on Close()")
	assertChannelClosed(t, wake2, "queue handle wake should close on Close()")

	// Refcounts must be back to zero.
	assert.Equal(t, 0, conn.EventListener().SubscriberCount("pmq:t:HandleCloseTopic"))
	assert.Equal(t, 0, conn.EventListener().SubscriberCount("pmq:q:HandleCloseQueue"))

	// Idempotent.
	h1.Close()
	h2.Close()
}

// TestEventListener_CloseClosesRemainingWakes verifies EventListener.Close
// closes the wake channels of any handles still registered.
func TestEventListener_CloseClosesRemainingWakes(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)

	conn.EventListener().Start()

	h := conn.EventListener().AddTopicListener("ListenerCloseTopic")
	wake := h.Wake()

	require.NoError(t, conn.Close())

	assertChannelClosed(t, wake, "EventListener.Close should close remaining handle wakes")

	// Subsequent handle.Close is a safe no-op.
	h.Close()
}

// TestEventListener_DispatchDoesNotPanicOnConcurrentClose stress-tests the
// dispatch / handle.Close interaction under the race detector. If the per-
// handle mutex were missing or used incorrectly, a send-on-closed-channel
// panic would crash the test.
func TestEventListener_DispatchDoesNotPanicOnConcurrentClose(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "RaceTopic"
	const queue = "RaceQueue"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false, postgremq.WithMaxDeliveryAttempts(0)))

	// Repeatedly add+close subscribers while the publisher is firing
	// notifications — exercises the dispatch + handle.Close race.
	stop := make(chan struct{})
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = conn.Publish(ctx, topic, []byte(`{}`))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for i := 0; i < 200; i++ {
		h := conn.EventListener().AddTopicListener(topic)
		// Drain the wake channel briefly so dispatch has somewhere to send.
		go func(h *postgremq.ListenerHandle) {
			for range h.Wake() {
			}
		}(h)
		// Race close against any in-flight dispatch.
		h.Close()
	}

	close(stop)
	<-publisherDone
}

// TestEventListener_SubscriptionChurnKeepsTheSession verifies that rapid
// Add/Remove churn, each change interrupting the session's wait for a
// notification, coalesces into reconciles that leave the session healthy:
// it is never lost, and a channel subscribed afterwards is listened to and
// receives NOTIFYs.
func TestEventListener_SubscriptionChurnKeepsTheSession(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	conn.EventListener().Start()
	steady := conn.EventListener().AddTopicListener("SteadyTopic")
	defer steady.Close()
	waitForListen(t, pool, steady, "pmq:t:SteadyTopic")
	session := listenBackend(t, pool, 0)

	const churn = 500
	for i := 0; i < churn; i++ {
		h := conn.EventListener().AddTopicListener("CoalesceTopic")
		h.Close()
	}
	require.Eventually(t, func() bool {
		return conn.EventListener().SubscriberCount("pmq:t:CoalesceTopic") == 0
	}, 2*time.Second, 10*time.Millisecond)

	h := conn.EventListener().AddTopicListener("AfterChurnTopic")
	defer h.Close()
	waitForListen(t, pool, h, "pmq:t:AfterChurnTopic")
	assertBackendAlive(t, pool, session)
}

// TestEventListener_MultipleSubscribersAllReceive verifies that two
// subscribers on the same channel both receive each NOTIFY (refcount
// sharing must not multiplex events to a single subscriber).
func TestEventListener_MultipleSubscribersAllReceive(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "FanOutTopic"
	require.NoError(t, conn.CreateTopic(ctx, topic))

	conn.EventListener().Start()

	h1 := conn.EventListener().AddTopicListener(topic)
	defer h1.Close()
	h2 := conn.EventListener().AddTopicListener(topic)
	defer h2.Close()

	// Sync both handles separately. Both share the single physical LISTEN,
	// but each handle has its own wake channel, and we want each one to
	// have already drained any probe events before the real publish fires.
	waitForListen(t, pool, h1, "pmq:t:"+topic)
	waitForListen(t, pool, h2, "pmq:t:"+topic)
	drainHandle(h1)
	drainHandle(h2)

	// Trigger one publish; both subscribers must wake.
	_, err = conn.Publish(ctx, topic, []byte(`{}`))
	require.NoError(t, err)

	receiveOne := func(ch <-chan struct{}, name string) {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not receive wake event", name)
		}
	}
	receiveOne(h1.Wake(), "subscriber 1")
	receiveOne(h2.Wake(), "subscriber 2")
}

// assertChannelClosed asserts that a wake channel has been closed (a receive
// returns immediately with ok=false). Drains any pending values first.
func assertChannelClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	deadline := time.After(1 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			// Drain pending value and continue.
		case <-deadline:
			t.Fatalf("%s: channel did not close within 1s", msg)
		}
	}
}

// TestEventListener_StartAfterCloseIsNoOp regresses a Start/Close race: a
// Connection.Consume() call that passed checkClosed could land at
// EventListener.Start() after Connection.Close() had already drained
// el.stopped and returned. Without the post-Close guard inside startOnce.Do,
// Start would replace el.stopped with a fresh open channel and launch
// goroutines that exit promptly on the cancelled ctx — leaked past Close
// (which never waited for the new stopped channel to close).
//
// The deterministic check: newEventListener seeds el.stopped with an
// already-closed channel. After Close, calling Start() must NOT replace it
// with a different channel — the same reference must come back.
func TestEventListener_StartAfterCloseIsNoOp(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)

	el := conn.EventListener()
	initialStopped := el.StoppedChan()
	require.NotNil(t, initialStopped)

	// Close the connection BEFORE any Consume — so EventListener.Start has
	// never been called. After Close, ctx is cancelled.
	require.NoError(t, conn.Close())

	// Now simulate the race: a stale Consume call (or anything else) hitting
	// Start after Close has fully run.
	el.Start()

	require.True(t, initialStopped == el.StoppedChan(),
		"Start after Close must not replace el.stopped — body should bail on cancelled ctx")

	// Sanity: the channel is still closed (so a future hypothetical Close
	// wouldn't deadlock waiting on <-el.stopped).
	select {
	case <-el.StoppedChan():
		// closed, as expected
	default:
		t.Fatal("el.stopped should remain closed after the post-Close Start no-op")
	}
}

// listenBackend returns the pid of this database's LISTEN session once it
// listens to a topic or queue channel.
func listenBackend(t *testing.T, pool *pgxpool.Pool, exclude int32) int32 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var pid int32
		err := pool.QueryRow(context.Background(),
			`SELECT pid FROM pg_stat_activity
			 WHERE datname = current_database() AND pid <> $1
			   AND (query ILIKE 'LISTEN "pmq:t:%' OR query ILIKE 'LISTEN "pmq:q:%')`,
			exclude).Scan(&pid)
		if err == nil {
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no LISTEN session within 10s")
	return 0
}

// assertBackendAlive asserts that the LISTEN session with the given pid
// still exists: a session that fails is closed and replaced.
func assertBackendAlive(t *testing.T, pool *pgxpool.Pool, pid int32) {
	t.Helper()
	var alive bool
	require.NoError(t, pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&alive))
	assert.True(t, alive, "the session was lost")
}

// TestEventListener_ReconnectWakesConsumers kills the LISTEN session and
// publishes while it is down: the NOTIFY is lost, so only the wake-up the
// listener sends once the new session listens can deliver the message
// before the consumer's 30 s poll. A round counts only if the publish
// finished within the shortest reconnect wait (half the initial 1 s
// backoff) of the kill; otherwise the NOTIFY may have reached the new
// session.
func TestEventListener_ReconnectWakesConsumers(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "GapTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "GapQueue", "GapTopic", false))
	consumer, err := conn.Consume("GapQueue", postgremq.WithCheckTimeout(30*time.Second))
	require.NoError(t, err)
	defer consumer.Stop()

	var previous int32
	for attempt, gaps := 1, 0; gaps < 2; attempt++ {
		require.LessOrEqual(t, attempt, 4, "the publish kept landing after the reconnect")
		session := listenBackend(t, pool, previous)
		// Let the initial or reconnect wake-up fetch (empty) finish first.
		time.Sleep(300 * time.Millisecond)
		killed := time.Now()
		_, err := pool.Exec(ctx, "SELECT pg_terminate_backend($1)", session)
		require.NoError(t, err)
		_, err = conn.Publish(ctx, "GapTopic", []byte(`{}`))
		require.NoError(t, err)
		inGap := time.Since(killed) < 500*time.Millisecond

		select {
		case msg := <-consumer.Messages():
			require.NoError(t, msg.Ack(ctx))
		case <-time.After(5 * time.Second):
			t.Fatalf("attempt %d: message published during the reconnect gap not delivered within 5s", attempt)
		}
		if inGap {
			gaps++
		}
		previous = session
	}
}

// TestEventListener_BackoffResetsAfterStableSession kills the LISTEN session
// three times in quick succession, then once more after it has been
// listening for 5 s, and times each reconnect from the kill until the new
// session listens. A reconnect waits at least half its backoff, so the
// third quick one takes at least 1.125 s (1 s grown ×1.5 twice). After the
// stable session the backoff starts again from 1 s: under 1.6 s with the
// reconnect itself, where a backoff that kept growing (3.375 s) would wait
// at least 1.6875 s.
func TestEventListener_BackoffResetsAfterStableSession(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	conn.EventListener().Start()
	h := conn.EventListener().AddTopicListener("BackoffTopic")
	defer h.Close()

	session := listenBackend(t, pool, 0)
	reconnect := func() time.Duration {
		t.Helper()
		killed := time.Now()
		_, err := pool.Exec(ctx, "SELECT pg_terminate_backend($1)", session)
		require.NoError(t, err)
		session = listenBackend(t, pool, session)
		return time.Since(killed)
	}
	assert.GreaterOrEqual(t, reconnect(), 500*time.Millisecond)
	assert.GreaterOrEqual(t, reconnect(), 750*time.Millisecond)
	assert.GreaterOrEqual(t, reconnect(), 1125*time.Millisecond)
	time.Sleep(6 * time.Second)
	assert.Less(t, reconnect(), 1600*time.Millisecond)
}

// consumeCounter counts the consume_message calls on a pool's connections.
type consumeCounter struct{ n atomic.Int32 }

func (c *consumeCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "consume_message") {
		c.n.Add(1)
	}
	return ctx
}

func (c *consumeCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TestEventListener_ReconnectFetchesOnce kills the LISTEN session: the new
// session listens again to both of a consumer's channels (topic and queue)
// and wakes the consumer once for the two, so it fetches once.
func TestEventListener_ReconnectFetchesOnce(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	cfg := pool.Config().Copy()
	fetches := &consumeCounter{}
	cfg.ConnConfig.Tracer = fetches
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer traced.Close()

	conn, err := postgremq.DialFromPool(ctx, traced)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "OnceTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "OnceQueue", "OnceTopic", false))
	consumer, err := conn.Consume("OnceQueue", postgremq.WithCheckTimeout(30*time.Second))
	require.NoError(t, err)
	defer consumer.Stop()

	session := listenBackend(t, pool, 0)
	for round := 1; round <= 4; round++ {
		// Let the previous fetches finish.
		time.Sleep(300 * time.Millisecond)
		before := fetches.n.Load()
		_, err := pool.Exec(ctx, "SELECT pg_terminate_backend($1)", session)
		require.NoError(t, err)
		session = listenBackend(t, pool, session)
		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, before+1, fetches.n.Load(), "round %d", round)
	}
}

// TestEventListener_NewChannelWakesItsSubscribers subscribes to a channel
// while a session is already running: once its LISTEN is in place its
// subscriber is woken without any NOTIFY on it, because a message published
// before the LISTEN is never notified. Subscribers of channels already
// listened to are not woken.
func TestEventListener_NewChannelWakesItsSubscribers(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()

	first := el.AddTopicListener("FirstTopic")
	defer first.Close()
	// The first session's wake: the channel is listened to.
	select {
	case <-first.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("the first session did not wake its subscriber")
	}

	second := el.AddTopicListener("SecondTopic")
	defer second.Close()
	select {
	case <-second.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("a newly listened channel's subscriber was not woken")
	}
	select {
	case <-first.Wake():
		t.Fatal("a channel already listened to was woken")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestEventListener_ConsumerHandleWokenOnce subscribes one handle to two
// channels, as a consumer does: once both are listened to, it is woken once,
// not once per channel, and a NOTIFY on either of them wakes it. Closing the
// listener closes its wake channel once.
func TestEventListener_ConsumerHandleWokenOnce(t *testing.T) {
	t.Parallel()
	pool, _ := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()

	h := el.AddConsumerListener("OnceTopic", "OnceQueue")
	select {
	case <-h.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("the handle was not woken once its channels were listened to")
	}
	select {
	case <-h.Wake():
		t.Fatal("the handle was woken once per channel")
	case <-time.After(200 * time.Millisecond):
	}
	for _, channel := range []string{"pmq:t:OnceTopic", "pmq:q:OnceQueue"} {
		_, err := pool.Exec(context.Background(), "SELECT pg_notify($1, '')", channel)
		require.NoError(t, err)
		select {
		case <-h.Wake():
		case <-time.After(5 * time.Second):
			t.Fatalf("a NOTIFY on %s did not wake the handle", channel)
		}
	}

	require.NoError(t, conn.Close())
	_, ok := <-h.Wake()
	assert.False(t, ok, "closing the listener closes the handle's wake channel")
	h.Close()
}

// TestEventListener_SubscribeWhilePoolExhausted subscribes to a new channel
// while every other connection of the pool is in use: the listener applies
// the change on its own connection, so the channel is listened to (and its
// subscriber woken) without needing another pool connection.
func TestEventListener_SubscribeWhilePoolExhausted(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	cfg := pool.Config().Copy()
	cfg.MaxConns = 2
	cfg.MinConns = 0
	small, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer small.Close()

	conn, err := postgremq.DialFromPool(ctx, small)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()
	first := el.AddTopicListener("FirstTopic")
	defer first.Close()
	waitForListen(t, pool, first, "pmq:t:FirstTopic")

	// The listener holds one connection, the test the other.
	held, err := small.Acquire(ctx)
	require.NoError(t, err)
	defer held.Release()
	require.Equal(t, int32(2), small.Stat().AcquiredConns())

	// NOTIFYs sent from another pool reach the new channel.
	second := el.AddTopicListener("SecondTopic")
	defer second.Close()
	waitForListen(t, pool, second, "pmq:t:SecondTopic")
}

// TestEventListener_ChurnDoesNotLoseNotifications sends NOTIFYs one at a
// time on a channel while another goroutine keeps subscribing to and
// unsubscribing from a second channel, interrupting the session's wait
// over and over: every NOTIFY still wakes its subscriber.
func TestEventListener_ChurnDoesNotLoseNotifications(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()
	handle := el.AddTopicListener("SteadyTopic")
	defer handle.Close()
	waitForListen(t, pool, handle, "pmq:t:SteadyTopic")
	drainHandle(handle)
	session := listenBackend(t, pool, 0)

	stop := make(chan struct{})
	churned := make(chan struct{})
	go func() {
		defer close(churned)
		for {
			select {
			case <-stop:
				return
			default:
			}
			el.AddTopicListener("ChurnTopic").Close()
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() {
		close(stop)
		<-churned
	}()

	for i := range 50 {
		_, err := pool.Exec(ctx, "SELECT pg_notify('pmq:t:SteadyTopic', '')")
		require.NoError(t, err)
		select {
		case <-handle.Wake():
		case <-time.After(2 * time.Second):
			t.Fatalf("NOTIFY %d did not wake its subscriber", i)
		}
	}
	assertBackendAlive(t, pool, session)
}

// TestEventListener_SubscribeWithCancelRequestHandler runs the connection on
// a pool whose connections cancel a context by sending the server a cancel
// request and only then, after a long delay, a deadline: a subscription
// change must still be applied at once, without waiting for that delay.
func TestEventListener_SubscribeWithCancelRequestHandler(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	cfg := pool.Config().Copy()
	cfg.ConnConfig.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: pgConn, DeadlineDelay: 2 * time.Second}
	}
	custom, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer custom.Close()

	conn, err := postgremq.DialFromPool(ctx, custom)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()
	first := el.AddTopicListener("FirstTopic")
	defer first.Close()
	select {
	case <-first.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("the first session did not wake its subscriber")
	}
	session := listenBackend(t, pool, 0)
	// Let the session block in its wait, so the change has to interrupt it.
	time.Sleep(100 * time.Millisecond)

	second := el.AddTopicListener("SecondTopic")
	defer second.Close()
	select {
	case <-second.Wake():
	case <-time.After(time.Second):
		t.Fatal("a subscription change waited for the pool's cancel handling")
	}
	waitForListen(t, pool, second, "pmq:t:SecondTopic")
	assertBackendAlive(t, pool, session)
}
