package postgremq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"postgremq.dev/postgremq-go"
)

// Two consumers (on separate connections) share a queue carrying interleaved
// grouped and ungrouped traffic. Every group must be delivered in sequence
// order with no message delivered twice.
func TestMessageGroups_OrderHoldsWithTwoConsumers(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic, queue = "group_order_topic", "group_order_queue"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false))

	groups := []string{"g0", "g1", "g2", "g3"}
	plan := make([]string, 0, 120)
	for i := 0; i < 100; i++ {
		plan = append(plan, groups[i%len(groups)])
	}
	for i := 0; i < 20; i++ {
		plan = append(plan, "")
	}
	rand.Shuffle(len(plan), func(i, j int) { plan[i], plan[j] = plan[j], plan[i] })

	var (
		mu       sync.Mutex
		received = map[string][]int64{} // group -> seqs in delivery order
		seen     = map[int64]int{}
		total    int
		done     = make(chan struct{})
	)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := postgremq.DialFromPool(context.Background(), pool)
			if !assert.NoError(t, err) {
				return
			}
			defer c.Close()
			consumer, err := c.Consume(queue, postgremq.WithTopic(topic), postgremq.WithBatchSize(3),
				postgremq.WithCheckTimeout(200*time.Millisecond))
			if !assert.NoError(t, err) {
				return
			}
			defer consumer.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case msg, ok := <-consumer.Messages():
					if !ok {
						return
					}
					// Record before acking: the successor is not claimable
					// until this ack commits, so the record order is the
					// delivery order.
					mu.Lock()
					received[msg.GroupKey] = append(received[msg.GroupKey], msg.GroupSeq)
					seen[msg.ID]++
					mu.Unlock()
					time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
					if !assert.NoError(t, msg.Ack(ctx)) {
						return
					}
					mu.Lock()
					total++
					if total == len(plan) {
						close(done)
					}
					mu.Unlock()
				}
			}
		}()
	}

	for i, g := range plan {
		payload := json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))
		var opts []postgremq.PublishOption
		if g != "" {
			opts = append(opts, postgremq.WithGroupKey(g))
		}
		_, err := conn.Publish(ctx, topic, payload, opts...)
		require.NoError(t, err)
	}

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("timed out: %d/%d acked", total, len(plan))
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		assert.Equal(t, 1, n, "message %d delivered %d times", id, n)
	}
	for _, g := range groups {
		want := make([]int64, 25)
		for i := range want {
			want[i] = int64(i + 1)
		}
		assert.Equal(t, want, received[g], "group %s delivered out of order", g)
	}
	assert.Len(t, received[""], 20)
	for _, seq := range received[""] {
		assert.Zero(t, seq, "ungrouped message must have GroupSeq 0")
	}
}

// Acking a group's head wakes its successor through NOTIFY: with a long poll
// interval and a head lease far in the future, the successor must still be
// delivered promptly after the ack.
func TestMessageGroups_AckWakesSuccessor(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic, queue = "group_wake_topic", "group_wake_queue"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false))
	for i := 0; i < 2; i++ {
		_, err := conn.Publish(ctx, topic, json.RawMessage(`{}`), postgremq.WithGroupKey("session-1"))
		require.NoError(t, err)
	}

	consumer, err := conn.Consume(queue, postgremq.WithBatchSize(5), postgremq.WithVT(60),
		postgremq.WithCheckTimeout(30*time.Second))
	require.NoError(t, err)
	defer consumer.Stop()

	var head *postgremq.Message
	select {
	case head = <-consumer.Messages():
	case <-time.After(5 * time.Second):
		t.Fatal("head not delivered")
	}
	assert.Equal(t, "session-1", head.GroupKey)
	assert.Equal(t, int64(1), head.GroupSeq)

	select {
	case m := <-consumer.Messages():
		// Settle both before failing so the deferred Stop can drain.
		_ = m.Ack(ctx)
		_ = head.Ack(ctx)
		t.Fatalf("successor %d delivered while its head is leased", m.GroupSeq)
	case <-time.After(500 * time.Millisecond):
	}

	ackedAt := time.Now()
	require.NoError(t, head.Ack(ctx))
	select {
	case succ := <-consumer.Messages():
		assert.Equal(t, int64(2), succ.GroupSeq)
		assert.Less(t, time.Since(ackedAt), 5*time.Second)
		require.NoError(t, succ.Ack(ctx))
	case <-time.After(10 * time.Second):
		t.Fatal("successor not delivered after the head was acked")
	}
}

func TestMessageGroups_PublishOptionsAndInspection(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(context.Background(), pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic, queue = "group_opts_topic", "group_opts_queue"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false))

	first, err := conn.Publish(ctx, topic, json.RawMessage(`{"n":1}`), postgremq.WithGroupKey("A"))
	require.NoError(t, err)

	// Transactional publish, combined with a delivery delay.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit
	second, err := conn.PublishWithTx(ctx, tx, topic, json.RawMessage(`{"n":2}`),
		postgremq.WithGroupKey("A"), postgremq.WithDeliverAfter(time.Now().Add(-time.Second)))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	ungrouped, err := conn.Publish(ctx, topic, json.RawMessage(`{"n":3}`))
	require.NoError(t, err)

	_, err = conn.Publish(ctx, topic, json.RawMessage(`{}`), postgremq.WithGroupKey(""))
	assert.ErrorIs(t, err, postgremq.ErrValidation)

	pm, err := conn.GetMessage(ctx, second)
	require.NoError(t, err)
	require.NotNil(t, pm)
	assert.Equal(t, "A", pm.GroupKey)
	assert.Equal(t, int64(2), pm.GroupSeq)

	list, err := conn.ListMessages(ctx, queue)
	require.NoError(t, err)
	byID := map[int64]postgremq.QueueMessage{}
	for _, m := range list {
		byID[m.MessageID] = m
	}
	assert.Equal(t, "A", byID[first].GroupKey)
	assert.Equal(t, int64(1), byID[first].GroupSeq)
	assert.Equal(t, int64(2), byID[second].GroupSeq)
	assert.Equal(t, "", byID[ungrouped].GroupKey)
	assert.Zero(t, byID[ungrouped].GroupSeq)
}
