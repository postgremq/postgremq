// Package postgremq provides a Go client for PostgreMQ, a message queue that
// runs inside PostgreSQL.
//
// # Overview
//
// Messages are published to topics and copied to every queue subscribed to the
// topic. Consumers claim messages under a visibility timeout (a lease) instead
// of a lock: an unsettled message becomes visible again when its lease expires.
// Because the queue lives in your database, you can publish and acknowledge in
// the same transaction as your application writes.
//
// # Core Concepts
//
// Visibility timeout (VT): a claimed message is invisible to other consumers
// until its visibility timeout expires, similar to Amazon SQS. By default the
// client extends the lease of every claimed message until it is settled.
//
// Queue types:
//   - Non-exclusive: persistent queues.
//   - Exclusive: queues with a lease that the creating Connection renews in the
//     background. If renewals stop, the queue expires, and maintenance deletes it
//     unless it has DLQ entries.
//
// Dead letter queue (DLQ): with WithMaxDeliveryAttempts(n), a message whose
// n-th delivery is nacked moves to the DLQ for inspection and requeueing; if the
// n-th delivery's lease expires unsettled, MaintenanceFast moves it. The default
// 0 means unlimited attempts and no DLQ.
//
// # Basic Usage
//
// Create a connection and set up a topic and a queue:
//
//	ctx := context.Background()
//	cfg, err := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/app")
//	if err != nil {
//		log.Fatal(err)
//	}
//	conn, err := postgremq.Dial(ctx, cfg)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer conn.Close()
//
//	// Both calls are idempotent.
//	if err := conn.CreateTopic(ctx, "orders"); err != nil {
//		log.Fatal(err)
//	}
//	if err := conn.CreateQueue(ctx, "orders-processor", "orders", false); err != nil {
//		log.Fatal(err)
//	}
//
// Import it as:
//
//	import "postgremq.dev/postgremq-go" // package postgremq
//
// # Publishing Messages
//
// Publish a JSON payload, optionally with delayed delivery:
//
//	payload := json.RawMessage(`{"order_id": 12345, "amount": 99.99}`)
//	messageID, err := conn.Publish(ctx, "orders", payload)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Not visible to consumers for 5 minutes.
//	delayedID, err := conn.Publish(ctx, "orders", payload,
//		postgremq.WithDeliverAfter(time.Now().Add(5*time.Minute)))
//
// WithGroupKey publishes into a message group: within each queue, a group's
// messages are delivered one at a time, in publish commit order.
//
// # Consuming Messages
//
// Consume needs the queue's topic: create the queue on the same Connection
// (CreateQueue is idempotent) or pass WithTopic. Range over Messages() and
// settle every message:
//
//	consumer, err := conn.Consume("orders-processor",
//		postgremq.WithBatchSize(10),
//		postgremq.WithVT(30))
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer consumer.Stop()
//
//	for msg := range consumer.Messages() {
//		var order map[string]any
//		if err := json.Unmarshal(msg.Payload, &order); err != nil {
//			log.Printf("invalid message: %v", err)
//			_ = msg.Nack(ctx) // redeliver now
//			continue
//		}
//		if err := processOrder(msg.StoppedCtx, order); err != nil {
//			// Redeliver after one minute.
//			_ = msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(time.Minute)))
//			continue
//		}
//		_ = msg.Ack(ctx)
//	}
//
// ConsumeHandler runs a function per message on its own goroutine, limited by
// WithMaxInFlight. A handler that returns without settling has its message
// acked, or nacked if its context was cancelled; a panic is recovered and the
// message is nacked.
//
// # Settling Messages
//
//   - Ack: mark the message completed.
//   - Nack: return the message for redelivery, immediately or at WithDelayUntil.
//     The attempt counts; on the final allowed attempt the message moves to the
//     DLQ.
//   - Release: return the message immediately without counting the attempt
//     (use it for work that was never started).
//   - AckWithTx: Ack inside the caller's transaction.
//
// Only the first settle call on a Message runs SQL; later calls return
// ErrLeaseLost. Delivery is at least once, so make side effects idempotent.
//
// Acknowledging inside a transaction:
//
//	func processWithTx(ctx context.Context, pool *pgxpool.Pool, msg *postgremq.Message) error {
//		tx, err := pool.Begin(ctx)
//		if err != nil {
//			return err
//		}
//		defer tx.Rollback(ctx) // no-op after Commit
//
//		if _, err := tx.Exec(ctx, "INSERT INTO app.orders (id, data) VALUES ($1, $2)",
//			msg.ID, msg.Payload); err != nil {
//			return err
//		}
//		if err := msg.AckWithTx(ctx, tx); err != nil {
//			return err
//		}
//		return tx.Commit(ctx)
//	}
//
// The caller owns the transaction; the client never begins, commits, rolls back
// or retries it. If the transaction rolls back, the message is redelivered when
// its lease expires.
//
// # Auto-Extension
//
// Auto-extension runs once per Connection: one background goroutine extends the
// due leases of every consumer, across all queues, in one batched call per tick
// (at most WithExtenderBatchSize messages per call).
//
// A message is extended once WithExtensionThreshold (default 0.5) of its
// remaining lease has elapsed, and each extension sets the deadline to now plus
// the consumer's WithVT. With WithVT(60), the first extension happens about 30
// seconds after the claim. If an extension finds that the lease was lost, the
// client cancels the message's StoppedCtx.
//
// Disable auto-extension to manage leases yourself (WithVT is then required):
//
//	consumer, err := conn.Consume("queue-name",
//		postgremq.WithVT(60),
//		postgremq.WithNoAutoExtension())
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for msg := range consumer.Messages() {
//		if _, err := msg.SetVT(ctx, 60); err != nil { // deadline = now + 60 s
//			log.Printf("extend: %v", err)
//		}
//		// ... process and settle the message ...
//	}
//
// # Shutdown Behavior
//
// Connection.Close performs a graceful shutdown:
//  1. Publish, PublishWithTx, CreateTopic, CreateQueue, Consume and
//     ConsumeHandler start returning ErrConnectionClosed. Other methods keep
//     working until Close finishes.
//  2. The LISTEN/NOTIFY listener stops and every consumer stops fetching.
//  3. Buffered messages that were never delivered to application code are
//     released without counting the attempt, and the StoppedCtx of delivered
//     messages is cancelled.
//  4. Close waits for delivered messages to be acked, nacked or released,
//     bounded by WithShutdownTimeout if set. Auto-extension and keep-alive keep
//     running meanwhile.
//  5. The background goroutines stop, and the pool is closed if the Connection
//     owns it (Dial, not DialFromPool).
//
// Consumer.Stop runs the same drain for one consumer: it stops fetching, closes
// the Messages() channel, releases buffered messages, cancels the StoppedCtx of
// delivered messages, and waits, without a timeout, until they are settled.
//
// Bound the drain with a shutdown timeout:
//
//	conn, err := postgremq.Dial(ctx, cfg,
//		postgremq.WithShutdownTimeout(30*time.Second))
//
// A worker that reacts to shutdown:
//
//	func worker(ctx context.Context, conn *postgremq.Connection) error {
//		consumer, err := conn.Consume("work-queue", postgremq.WithVT(60))
//		if err != nil {
//			return err
//		}
//		defer consumer.Stop()
//
//		for msg := range consumer.Messages() {
//			if msg.StoppedCtx.Err() != nil {
//				// Stopping: hand the message back without counting the attempt.
//				_ = msg.Release(context.WithoutCancel(ctx))
//				continue
//			}
//			if err := processWithContext(msg.StoppedCtx, msg.Payload); err != nil {
//				_ = msg.Nack(ctx)
//			} else {
//				_ = msg.Ack(ctx)
//			}
//		}
//		return nil
//	}
//
// # Retry Configuration
//
// Non-transactional calls retry transient database errors (serialization
// failures, deadlocks, lock-not-available, connection errors and server
// restarts) with exponential backoff. Publish retries only serialization
// failures and deadlocks, which guarantee a rollback. Configure the policy:
//
//	conn, err := postgremq.Dial(ctx, cfg,
//		postgremq.WithRetryConfig(postgremq.RetryConfig{
//			MaxAttempts:       5,
//			InitialBackoff:    100 * time.Millisecond,
//			MaxBackoff:        2 * time.Second,
//			BackoffMultiplier: 2.0,
//		}))
//
// Or disable retries:
//
//	conn, err := postgremq.Dial(ctx, cfg, postgremq.WithoutRetries())
//
// PublishWithTx and AckWithTx never retry, since the caller controls the
// transaction. A consumer's fetch is never retried in place; the consumer
// tries again on its next fetch.
//
// # Exclusive Queues and Keep-Alive
//
// CreateQueue with exclusive = true registers the queue with the Connection's
// keep-alive goroutine, which renews it about every half interval until
// DeleteQueue on this Connection, a queue-fatal teardown, or Close:
//
//	// Exclusive queue with a 5-minute lease.
//	err := conn.CreateQueue(ctx, "temp-queue", "events", true,
//		postgremq.WithKeepAliveInterval(5*time.Minute))
//
// If the process exits, the queue expires about 5 minutes after the last
// renewal. An expired queue receives no messages and serves no consumers;
// MaintenanceFast or DeleteInactiveQueues deletes it.
//
// # Queue-Fatal Teardown
//
// When a queue a consumer depends on is gone (deleted, replaced, or an expired
// exclusive queue), the consumer is torn down: Messages() closes and the reason,
// a *QueueFatalError matching errors.Is(err, ErrQueueGone), is delivered through
// Consumer.NotifyClose and the connection-wide WithQueueFatalHandler.
//
// # Error Handling
//
// Use errors.Is with the sentinels:
//   - ErrConnectionClosed: the Connection is closing or closed.
//   - ErrLeaseLost: the delivery is no longer owned by this consumer (expired
//     and reclaimed, token mismatch, or already settled).
//   - ErrQueueNotFound: missing topic or queue, or an expired exclusive queue.
//   - ErrValidation: the request was rejected (invalid name, parameter mismatch,
//     and similar).
//   - ErrQueueGone: a consumer's queue is gone (see above).
//
// Check settle errors:
//
//	if err := msg.Ack(ctx); err != nil {
//		switch {
//		case errors.Is(err, postgremq.ErrLeaseLost):
//			// Another consumer may have the message; it will be processed again.
//		case errors.Is(err, postgremq.ErrConnectionClosed):
//			// The Connection has shut down.
//		default:
//			log.Printf("ack failed: %v", err)
//		}
//	}
//
// # Performance Considerations
//
// Batch size: larger batches reduce database round-trips but increase memory use
// and claim more messages ahead of processing. The default is 10, and the
// Messages() channel buffers one batch.
//
//	consumer, err := conn.Consume("queue", postgremq.WithBatchSize(100))
//
// Visibility timeout: the default is 30 seconds. With auto-extension it bounds
// how long a crashed consumer's messages stay invisible; without it, it must
// exceed the processing time.
//
// Check timeout: the longest wait between fetches when no LISTEN/NOTIFY event
// arrives. The default is 10 seconds.
//
//	consumer, err := conn.Consume("queue", postgremq.WithCheckTimeout(5*time.Second))
//
// # Concurrency
//
// Connection methods are safe to call from multiple goroutines. Each Consumer
// has one goroutine that owns its state (each fetch runs on a short-lived helper
// goroutine); the Connection runs one LISTEN session for all consumers and one
// goroutine each for auto-extension and keep-alive.
//
// Several consumers can consume from the same queue: each claim locks the rows
// it takes with FOR UPDATE SKIP LOCKED, so a message is claimed by one consumer
// at a time.
package postgremq
