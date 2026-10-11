package postgremq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Stoppable is implemented by consumers that can be stopped during shutdown.
type Stoppable interface {
	Stop()
}

// Connection is a client handle to the PostgreMQ schema running in a
// PostgreSQL database. It owns a connection Pool (unless created with
// DialFromPool), manages consumers and an event listener (LISTEN/NOTIFY), and
// exposes high‑level queue APIs (publish/consume/ack/nack/etc).
//
// Concurrency:
//   - Connection methods are safe to call from multiple goroutines unless
//     otherwise stated.
//   - Each Consumer created from a Connection runs internal goroutines for
//     fetching messages and (optionally) auto‑extending visibility timeouts.
type Connection struct {
	meterProvider       metric.MeterProvider
	metrics             *clientMetrics
	pool                Pool
	ownPool             bool // true if we created the pool and should close it
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.RWMutex
	shutdownTimeout     time.Duration
	consumers           []Stoppable
	eventListener       *EventListener
	eventListenerDoOnce sync.Once
	logger              LevelLogger
	onQueueFatal        func(queue string, err error) // fired when a queue becomes fatal (gone)
	fatalQueues         sync.Map                      // set of queues already declared fatal (dedupe queueFatal)
	extenderBatchSize   int                           // per-tick cap on messages extended in one set_vt_batch_multi call
	// Connection-level background actors (see actor.go). Both run on their own
	// context and are stopped LAST in Close().
	keepAlive    *actor[*kaEntry, string, kaResult]
	extender     *actor[*extEntry, extKey, extResult]
	retryConfig  RetryConfig
	closedFlag   chan struct{}
	drainingFlag chan struct{}
	ioCtx        context.Context
	ioCancel     context.CancelFunc
	closeOnce    sync.Once
	// topicCache maps queue name -> topic name. Populated by CreateQueue
	// in this session, and also by Consume when WithTopic is provided
	// explicitly. Consume avoids any SQL round-trip when the entry is
	// present; queues that exist in the database but were not created via
	// this Connection (and are not passed WithTopic) error from Consume
	// with ErrQueueNotFound.
	topicCache       sync.Map
	queueGenerations sync.Map
}

// Dial creates a new Connection, building an underlying pgxpool.Pool from the
// provided pgx PoolConfig.
//
// The returned Connection owns the pool and will Close() it during shutdown.
//
// Dial reads postgremq.info() and returns a *CompatibilityError (matching
// ErrIncompatibleSchema) when the installation's protocol major is not one
// of SupportedProtocolMajors(), or when discovery is missing and the database
// needs a PostgreMQ installation or upgrade.
//
// ctx is bootstrap-only: it bounds pgxpool.NewWithConfig and the protocol
// check at construction time and is not retained by the returned Connection. Cancelling it after Dial
// returns has no effect — call Close() to shut down the Connection.
func Dial(ctx context.Context, config *pgxpool.Config, opts ...ConnectionOption) (*Connection, error) {
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	if err := checkProtocol(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return newConnection(ctx, pool, true, opts...)
}

// DialFromPool creates a new Connection using an existing Pool implementation
// (typically *pgxpool.Pool). The Connection does not own the pool and will not
// close it on Connection.Close().
//
// ctx bounds the protocol check (see Dial) and is not retained. Lifetime is
// owned by Close().
func DialFromPool(ctx context.Context, pool Pool, opts ...ConnectionOption) (*Connection, error) {
	if err := checkProtocol(ctx, pool); err != nil {
		return nil, err
	}
	return newConnection(ctx, pool, false, opts...)
}

func newConnection(_ context.Context, pool Pool, ownPool bool, opts ...ConnectionOption) (*Connection, error) {
	// Connection lifetime is owned by Close(), not by the caller's ctx.
	// Deriving Connection.ctx from a user-supplied ctx (which we used to
	// do) violated the canonical Go pattern (see pgxpool.New, amqp.Dial,
	// sql.Open): the ctx parameter limits the bootstrap call, but the
	// returned long-lived resource is managed via its own Close()/Stop()
	// methods. The user ctx is therefore intentionally ignored here —
	// it's used only by Dial for the pgxpool bootstrap call.
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		pool:                pool,
		ownPool:             ownPool,
		ctx:                 ctx,
		cancel:              cancel,
		consumers:           nil,
		logger:              NoopLogger{},
		retryConfig:         defaultRetryConfig(),
		closedFlag:          make(chan struct{}),
		drainingFlag:        make(chan struct{}),
		eventListenerDoOnce: sync.Once{},
		extenderBatchSize:   defaultExtenderBatchSize, // overridable via WithExtenderBatchSize
	}
	for _, opt := range opts {
		opt(conn)
	}
	if err := validateConnectionOptions(conn); err != nil {
		cancel()
		if ownPool {
			pool.Close()
		}
		return nil, err
	}
	var metricsErr error
	conn.metrics, metricsErr = newClientMetrics(conn.meterProvider)
	if metricsErr != nil {
		cancel()
		if ownPool {
			pool.Close()
		}
		return nil, fmt.Errorf("initialize metrics: %w", metricsErr)
	}
	conn.ioCtx, conn.ioCancel = context.WithCancel(context.Background())
	conn.eventListener = newEventListener(ctx, pool, conn.logger)

	// Start the connection-level background actors. One idle goroutine each (vs.
	// lazy start) is the simpler, race-free choice — they sit blocked on their
	// select with no work until the first register. Built AFTER the options loop
	// so the extender's batch cap is known and the keep-alive actor never races a
	// write to onKeepAliveFailure. Each owns its own context (not conn.ctx) and
	// is stopped LAST in Close() so it outlives the consumer drain.
	conn.keepAlive = newActor[*kaEntry, string, kaResult](newKeepAliveScheduler(conn), 64, 64)
	conn.extender = newActor[*extEntry, extKey, extResult](newExtScheduler(conn, conn.extenderBatchSize), 256, 256)
	conn.keepAlive.start()
	conn.extender.start()

	return conn, nil
}

// Close implements graceful shutdown for the Connection and all derived
// Consumers.
//
// Behavior:
//   - Cancels internal context and stops the LISTEN/NOTIFY event listener.
//   - Signals all Consumers to Stop and (optionally) waits up to the configured
//     shutdown timeout for in‑flight messages to finish (Ack/Nack/Release).
//   - Stops keep‑alive background loops for exclusive queues.
//   - Closes the underlying pool if the Connection owns it.
//
// Side effects:
//   - Messages buffered by a Consumer but not yet delivered are released back
//     to the queue without incrementing delivery attempts.
func (c *Connection) Close() error {
	// Guard the entire teardown with a sync.Once so concurrent callers (e.g. a
	// deferred Close racing a signal handler) can't both close(c.closedFlag) —
	// which panics "close of closed channel" — or run the non-idempotent
	// teardown twice. The first caller performs the shutdown; any concurrent or
	// later caller returns nil cleanly.
	c.closeOnce.Do(func() {
		c.mu.Lock()
		close(c.drainingFlag)
		consumers := append([]Stoppable(nil), c.consumers...)
		c.mu.Unlock()
		var drainTimer *time.Timer
		if c.shutdownTimeout > 0 {
			drainTimer = time.AfterFunc(c.shutdownTimeout, c.ioCancel)
			defer drainTimer.Stop()
		}
		c.cancel()

		// Stop the event listener first. This closes every consumer's wake
		// channels, so consumers stop pulling new messages immediately while
		// they finish processing whatever they already prefetched. Their
		// Stop() calls below then become a clean drain.
		c.eventListener.Close()

		// Stop all consumers and wait for in-flight messages to complete.
		var wg sync.WaitGroup
		for _, consumer := range consumers {
			wg.Add(1)
			go func(cons Stoppable) {
				defer wg.Done()
				cons.Stop()
			}(consumer)
		}

		// Wait for consumers with timeout if specified
		if c.shutdownTimeout > 0 {
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				// All consumers finished gracefully
			case <-c.ioCtx.Done():

				c.logger.Warnf("Exceeded timeout for consumers to finish. Will shutdown connection")
			}
		} else {
			wg.Wait()
		}

		// Stop the connection-level background actors. Both intentionally
		// outlive the consumer drain and are
		// stopped here, after consumers have drained, in any order.
		c.ioCancel()
		close(c.closedFlag)
		c.keepAlive.stop()
		c.extender.stop()

		// Close pool if we own it
		if c.ownPool {
			c.pool.Close()
		}
	})
	return nil
}

// CreateTopic creates a topic if it does not already exist (idempotent).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic to create.
//
// Returns an error if the operation fails due to database issues.
// The operation uses the configured retry policy for transient errors.
func (c *Connection) CreateTopic(ctx context.Context, topic string) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.create_topic($1)", topic)
		if err != nil {
			return mapPgError(fmt.Errorf("failed to create topic: %w", err))
		}
		return nil
	})
}

// CreateQueue creates a queue subscribed to a topic.
//
// Parameters:
//   - name: queue name.
//   - topic: existing topic name.
//   - exclusive: when true, the queue is temporary and must be kept alive by
//     clients (see WithKeepAliveInterval). When false, the queue is persistent.
//   - opts: queue options (max delivery attempts, keep‑alive interval seconds).
//
// Side effects:
//   - If exclusive, a background keep‑alive loop is started to extend the
//     queue's expiration while the Connection is alive.
func (c *Connection) CreateQueue(ctx context.Context, name, topic string, exclusive bool, opts ...QueueOption) error {
	if err := c.checkClosed(); err != nil {
		return err
	}

	options := defaultQueueOptions()
	for _, opt := range opts {
		opt(&options)
	}

	if options.keepAliveInterval < time.Millisecond {
		return fmt.Errorf("keep alive interval must be at least 1ms")
	}
	var generation string
	err := c.withRetry(ctx, func(ctx context.Context) error {
		err := c.pool.QueryRow(ctx,
			"SELECT postgremq.create_queue($1, $2, $3, $4, $5 * interval '1 ms')",
			name,                                                       // p_queue_name
			topic,                                                      // p_topic_name
			options.maxDeliveryAttempts,                                // p_max_attempts
			exclusive,                                                  // p_exclusive
			options.keepAliveInterval.Milliseconds()).Scan(&generation) // p_keep_alive_interval (ms scaled to INTERVAL)
		if err != nil {
			return mapPgError(fmt.Errorf("failed to create queue: %w", err))
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueGenerations.Store(name, generation)
	c.topicCache.Store(name, topic)
	// Recreating a queue clears any prior fatal mark so it can be consumed (and,
	// if it dies again, declared fatal) anew.
	c.fatalQueues.Delete(name)
	if exclusive {
		c.keepAliveRegister(name, options.keepAliveInterval.Milliseconds(), generation)
	}
	return nil
}

// resolveTopic returns the topic that owns the given queue. The lookup order
// is: explicit override (from WithTopic) -> in-process cache. If neither is
// available, the caller must either pass WithTopic or have populated the
// cache via CreateQueue first; we don't query the database for this.
func (c *Connection) resolveTopic(queue, explicit string) (string, error) {
	if explicit != "" {
		c.topicCache.Store(queue, explicit)
		return explicit, nil
	}
	if v, ok := c.topicCache.Load(queue); ok {
		return v.(string), nil
	}
	return "", fmt.Errorf("%w: %q (provide WithTopic or call CreateQueue first)", ErrQueueNotFound, queue)
}

// Publish publishes a message to a topic and returns its message ID.
//
// The message is immediately inserted into the messages table and distributed
// to all queues subscribed to the topic via the after_message_insert trigger.
// Use WithDeliverAfter option to delay initial visibility.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic (must already exist).
//   - payload: JSON-encoded message payload.
//   - opts: Optional publish options (e.g., WithDeliverAfter for delayed delivery).
//
// Returns the generated message ID on success, or an error if:
//   - The topic does not exist.
//   - The payload is not valid JSON.
//   - Database operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	payload := json.RawMessage(`{"order_id": 123}`)
//	id, err := conn.Publish(ctx, "orders", payload)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (c *Connection) Publish(ctx context.Context, topic string, payload json.RawMessage, opts ...PublishOption) (int64, error) {
	return c.executePublish(ctx, c.pool, topic, payload, true, opts...)
}

// PublishWithTx publishes a message within an existing transaction and returns
// the message ID.
//
// This method allows publishing messages atomically with other database operations
// within the same transaction. It does NOT use the internal retry policy since
// transaction boundaries are controlled by the caller.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - tx: Active database transaction (implements Tx interface).
//   - topic: Name of the topic (must already exist).
//   - payload: JSON-encoded message payload.
//   - opts: Optional publish options (e.g., WithDeliverAfter for delayed delivery).
//
// Returns the generated message ID on success, or an error if the operation fails.
//
// Side effects:
//   - The after_message_insert trigger will run within the transaction,
//     distributing the message to queues only if the transaction commits.
//
// Example:
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//	id, err := conn.PublishWithTx(ctx, tx, "orders", payload)
//	if err != nil {
//	    return err
//	}
//	// ... other DB operations ...
//	tx.Commit(ctx)
func (c *Connection) PublishWithTx(ctx context.Context, tx Tx, topic string, payload json.RawMessage, opts ...PublishOption) (int64, error) {
	return c.executePublish(ctx, tx, topic, payload, false, opts...)
}

// Consume starts a Consumer for the provided queue name.
//
// Behavior:
//   - Returns immediately with a Consumer whose Messages() channel yields
//     messages as they are fetched.
//   - Auto‑extension: Unless disabled via WithNoAutoExtension, the Consumer
//     automatically extends visibility timeouts for in‑flight messages around
//     halfway through their vt.
//   - Backpressure: Messages are fetched in batches (WithBatchSize) and new
//     fetches are driven by LISTEN/NOTIFY and polling (WithCheckTimeout).
//
// Consumer lifetime is owned by Stop and Close. An uncached queue generation
// is resolved with a bounded bootstrap query before registration.
//
// Shutdown:
//   - Consumer.Stop() releases buffered messages that were not delivered to the
//     client yet (no attempt), and waits for in‑flight messages to complete or
//     be released.
func (c *Connection) Consume(queue string, opts ...ConsumeOption) (*Consumer, error) {
	options := defaultConsumeOptions()
	for _, opt := range opts {
		opt(&options)
	}
	topic, err := c.resolveTopic(queue, options.topic)
	if err != nil {
		return nil, err
	}
	options.topic = topic
	options.generation, err = c.resolveQueueGeneration(queue)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// checkClosed under the write lock closes the TOCTOU window with
	// Close: either Close has already set closedFlag (we return here) or
	// Close hasn't started yet (and our append happens before Close's
	// snapshot, so Stop() will reach this consumer). Without this guard
	// a post-Close Consume would register listener handles, append to a
	// slice that's already been snapshotted, and leak both.
	if err := c.checkClosed(); err != nil {
		return nil, err
	}
	topicHandle := c.eventListener.AddTopicListener(topic)
	queueHandle := c.eventListener.AddQueueListener(queue)
	consumer, err := newConsumerFromOptions(c.ctx, c, c.logger, queue, topicHandle, queueHandle, options)
	if err != nil {
		topicHandle.Close()
		queueHandle.Close()
		return nil, err
	}
	c.consumers = append(c.consumers, consumer)
	// start() must run synchronously: each sub-loop Add(1)s the consumer's
	// WaitGroup, and a caller that immediately calls Stop() (which Wait()s)
	// would race with the Adds if start() were launched in a goroutine.
	consumer.start()

	// starting event listener if not already started
	c.eventListenerDoOnce.Do(func() {
		c.eventListener.Start()
	})

	return consumer, nil
}

// ConsumeHandler creates a handler-based consumer for the specified queue.
//
// The handler function is called for each message. The handler should call
// msg.Ack() or msg.Nack() to acknowledge or reject the message. If the handler
// returns without calling either, the message is automatically acked. If the
// handler panics, the message is automatically nacked.
//
// Each handler invocation receives msg.StoppedCtx, which is cancelled when
// the consumer begins shutting down (HandlerConsumer.Stop or
// Connection.Close). Handlers should check ctx.Done() and return promptly.
//
// No ctx parameter: this constructor performs no I/O. Consumer lifetime is
// owned by HandlerConsumer.Stop() and Connection.Close().
//
// Example:
//
//	hc, err := conn.ConsumeHandler("orders",
//	    func(ctx context.Context, msg *postgremq.Message) {
//	        select {
//	        case <-ctx.Done():
//	            msg.Nack(ctx)
//	            return
//	        default:
//	        }
//	        if err := processOrder(msg.Payload); err != nil {
//	            msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(5*time.Second)))
//	            return
//	        }
//	        msg.Ack(ctx)
//	    },
//	    postgremq.WithVT(60),
//	    postgremq.WithMaxInFlight(10),
//	)
func (c *Connection) ConsumeHandler(
	queue string,
	handler MessageHandler,
	opts ...HandlerConsumeOption,
) (*HandlerConsumer, error) {
	// Parse and validate options
	options := defaultHandlerConsumeOptions()
	for _, opt := range opts {
		opt.apply(&options)
	}
	if err := validateHandlerConsumeOptions(&options); err != nil {
		return nil, err
	}

	topic, err := c.resolveTopic(queue, options.topic)
	if err != nil {
		return nil, err
	}
	options.topic = topic
	options.generation, err = c.resolveQueueGeneration(queue)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// See the comment in Consume — checkClosed under the write lock
	// closes the TOCTOU with Close.
	if err := c.checkClosed(); err != nil {
		return nil, err
	}

	topicHandle := c.eventListener.AddTopicListener(topic)
	queueHandle := c.eventListener.AddQueueListener(queue)
	consumer, err := newConsumerFromOptions(c.ctx, c, c.logger, queue, topicHandle, queueHandle, options.consumeOptions)
	if err != nil {
		topicHandle.Close()
		queueHandle.Close()
		return nil, err
	}

	hc := newHandlerConsumer(c, consumer, handler, c.logger, options)

	c.consumers = append(c.consumers, hc)

	c.eventListenerDoOnce.Do(func() {
		c.eventListener.Start()
	})

	// Run synchronously for the same reason as Consume: start()'s sub-loops
	// Add(1) the consumer's WaitGroup, and a fast Stop() would race.
	hc.start()

	return hc, nil
}

// fatalConsumer is implemented by both *Consumer and *HandlerConsumer so the
// connection can tear down every consumer bound to a now-gone queue.
type fatalConsumer interface {
	Stoppable
	queueName() string
	queueGeneration() string
	fatal(err error)
}

// queueFatal handles a queue that has become unrecoverably gone — deleted out of
// band (a consume returns PMQ02), or an exclusive queue whose keep-alive
// permanently failed (omitted from extend_queue_keep_alive_multi = gone or
// non-exclusive). It is idempotent per queue. It stops keeping the queue alive,
// tears down every consumer bound to it (their normal drain cancels handlers and
// deregisters in-flight messages from the extender), then fires the queue-fatal
// handler — the only signal for a producer-only exclusive queue with no consumer.
//
// Callers invoke it as `go c.queueFatal(...)`: it runs off the actor / consumer
// loops, so a blocking user handler can't stall them. consumer.fatal only
// cancels (non-blocking) — we don't wait for the drain here; each consumer's
// NotifyClose delivers the after-cleanup signal once it finishes draining.
func (c *Connection) queueFatal(queue string, cause error, generations ...string) {
	c.mu.RLock()
	if len(generations) > 0 {
		if current, ok := c.queueGenerations.Load(queue); ok && current != generations[0] {
			c.mu.RUnlock()
			return
		}
	}
	if _, dup := c.fatalQueues.LoadOrStore(queue, struct{}{}); dup {
		c.mu.RUnlock()
		return
	}
	wrapped := &QueueFatalError{Queue: queue, Err: cause}

	// Stop keeping a dead queue alive (no-op if not exclusive / not registered).
	c.keepAliveDeregister(queue)

	// Tear down every consumer on this queue (consumers can share a queue).
	consumers := make([]Stoppable, len(c.consumers))
	copy(consumers, c.consumers)
	c.mu.RUnlock()
	for _, cons := range consumers {
		if fc, ok := cons.(fatalConsumer); ok && fc.queueName() == queue && (len(generations) == 0 || fc.queueGeneration() == generations[0]) {
			fc.fatal(wrapped)
		}
	}

	// Queue-level signal (also the only signal for producer-only queues).
	if c.onQueueFatal != nil {
		c.onQueueFatal(queue, wrapped)
	} else {
		c.logger.Errorf("Queue %s is gone: %v", queue, wrapped)
	}
}

// MultiExtension identifies one message to extend in a cross-queue batch
// visibility-timeout operation (SetVTBatchMulti). It carries the queue and a
// per-message VT, so one call can extend every in-flight message a Connection
// owns across all of its queues.
type MultiExtension struct {
	// Queue is the queue the message belongs to.
	Queue string
	// ID is the message ID to extend.
	ID int64
	// Token is the consumer token that currently owns the message.
	Token string
	// VTSec is the new visibility timeout in seconds.
	VTSec int
}

// MultiLock reports an extension or a busy row. Correlate by Queue, ID and
// Token. Busy means retry within the previously confirmed deadline; VT is zero.
type MultiLock struct {
	// Queue is the queue the extended message belongs to.
	Queue string
	// ID is the message ID that was extended.
	ID int64
	// VT is the new visibility timeout expiration timestamp.
	VT    time.Time
	Token string
	Busy  bool
}

// SetVTBatchMulti extends visibility timeouts for a batch of in-flight messages
// spanning any number of queues, in a single database round-trip. It is the
// connection-level auto-extension primitive (the extender actor coalesces every
// consumer's due extensions into one call per tick).
//
// Returns the messages actually extended; a requested (Queue, ID) absent from
// the result lost its lease server-side (status no longer 'processing', vt
// already passed, or token mismatch) — correlate by the COMPOSITE (Queue, ID)
// identity (including Token), never message_id alone.
//
// The operation uses the configured retry policy: extension is idempotent.
func (c *Connection) SetVTBatchMulti(ctx context.Context, exts []MultiExtension) (result []MultiLock, resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "extend_batch", "", false)
	defer func() { finishMetric(resultErr) }()
	if len(exts) == 0 {
		return nil, nil
	}

	queues := make([]string, len(exts))
	ids := make([]int64, len(exts))
	tokens := make([]string, len(exts))
	vts := make([]int32, len(exts)) // p_vts is INTEGER[]
	for i, e := range exts {
		if e.VTSec < 0 || int64(e.VTSec) > 2147483647 {
			return nil, fmt.Errorf("visibility timeout must be between 0 and 2147483647")
		}
		queues[i] = e.Queue
		ids[i] = e.ID
		tokens[i] = e.Token
		vts[i] = int32(e.VTSec)
	}

	var locks []MultiLock
	err := c.withRetry(ctx, func(ctx context.Context) error {
		locks = locks[:0]
		rows, err := c.pool.Query(ctx,
			"SELECT queue_name, message_id, vt, consumer_token, outcome FROM postgremq.set_vt_batch_multi($1, $2, $3, $4)",
			queues, ids, tokens, vts)
		if err != nil {
			return mapPgError(fmt.Errorf("failed to set message visibility timeout (multi): %w", err))
		}
		defer rows.Close()
		for rows.Next() {
			var l MultiLock
			var vt *time.Time
			var outcome string
			if err := rows.Scan(&l.Queue, &l.ID, &vt, &l.Token, &outcome); err != nil {
				return mapPgError(err)
			}
			if vt != nil {
				l.VT = *vt
			}
			l.Busy = outcome == "busy"
			locks = append(locks, l)
		}
		return mapPgError(rows.Err())
	})
	return locks, err
}

// // Database methods
// executePublish handles the actual execution of publish SQL with the given transaction
func (c *Connection) executePublish(ctx context.Context, tx Tx, topic string, payload json.RawMessage, retry bool, opts ...PublishOption) (id int64, resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "publish", topic, !retry)
	defer func() { finishMetric(resultErr) }()
	if err := c.checkClosed(); err != nil {
		return 0, err
	}

	options := &publishOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Optional arguments use named notation so each keeps its SQL default
	// when omitted.
	query := "SELECT postgremq.publish_message($1, $2"
	args := []any{topic, payload}
	if options.deliverAfter != nil {
		args = append(args, *options.deliverAfter)
		query += fmt.Sprintf(", p_deliver_after => $%d", len(args))
	}
	if options.groupKey != nil {
		args = append(args, *options.groupKey)
		query += fmt.Sprintf(", p_group_key => $%d", len(args))
	}
	query += ")"

	publish := func(ctx context.Context) (messageID int64, err error) {
		defer func() { c.metrics.recordSent(ctx, topic, !retry, err) }()
		err = tx.QueryRow(ctx, query, args...).Scan(&messageID)
		return messageID, err
	}
	var messageID int64
	var err error
	if retry {
		err = c.withRetryPolicy(ctx, func(ctx context.Context) error {
			var err error
			messageID, err = publish(ctx)
			return err
		}, isAbortedTransaction)
	} else {
		messageID, err = publish(ctx)
	}
	if err != nil {
		return 0, mapPgError(fmt.Errorf("failed to publish message: %w", err))
	}
	return messageID, nil
}

func (c *Connection) consumeMessages(ctx context.Context, queue string, limit int, vt int, generations ...string) (result []*Message, resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "consume", queue, false)
	defer func() { finishMetric(resultErr) }()
	defer func() { c.metrics.recordConsumed(ctx, queue, result, resultErr) }()
	if c.isClosed() {
		return nil, ErrConnectionClosed
	}

	// Deliberately NOT wrapped in withRetry: consume_message has a side
	// effect (it transitions matched rows to status='processing' with a
	// new consumer_token and vt). If pool.Query succeeds and rows.Scan or
	// rows.Err fails partway through (08-class network drop while reading
	// the result stream), retrying would re-execute consume_message and
	// claim a SECOND batch — orphaning the first batch in 'processing'
	// until vt expires. The fetch loop in Consumer.startMessageLoop
	// already retries the next tick on error, which is the correct
	// recovery path: don't double-consume, let vt expiry redeliver the
	// stranded batch.
	var generation *string
	if len(generations) > 0 && generations[0] != "" {
		generation = &generations[0]
	}
	rows, err := c.pool.Query(ctx,
		"SELECT message_id, payload, consumer_token, delivery_attempts, vt, published_at, group_key, group_seq FROM postgremq.consume_message($1, $2, $3, $4)",
		queue, vt, limit, generation)
	if err != nil {
		return nil, mapPgError(fmt.Errorf("failed to consume messages: %w", err))
	}
	defer rows.Close()

	var messages []*Message
	for rows.Next() {
		var (
			id               int64
			payload          json.RawMessage
			consumerToken    string
			deliveryAttempts int
			vt               time.Time
			publishedAt      time.Time
			groupKey         *string
			groupSeq         *int64
		)
		if err := rows.Scan(&id, &payload, &consumerToken, &deliveryAttempts, &vt, &publishedAt, &groupKey, &groupSeq); err != nil {
			// Return successfully-scanned messages alongside the error.
			// They're already claimed server-side; the caller can deliver
			// them while still seeing the error and scheduling a retry.
			return messages, mapPgError(fmt.Errorf("failed to scan message: %w", err))
		}

		// Create a new Message and ensure all internal fields are initialized.
		msg := &Message{
			ID:              id,
			Payload:         payload,
			consumerToken:   consumerToken,
			DeliveryAttempt: deliveryAttempts,
			PublishedAt:     publishedAt,
			conn:            c,     // Set connection so methods like Ack() will work.
			queue:           queue, // Save the originating queue name.
			VT:              vt,
		}
		if groupKey != nil && groupSeq != nil {
			msg.GroupKey, msg.GroupSeq = *groupKey, *groupSeq
		}
		messages = append(messages, msg)
	}
	return messages, mapPgError(rows.Err())
}

func (c *Connection) ackMessage(ctx context.Context, queue string, messageID int64, consumerToken string) (resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "ack", queue, false)
	defer func() { finishMetric(resultErr) }()
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx,
			"SELECT postgremq.ack_message($1, $2, $3)",
			queue, messageID, consumerToken)
		if err != nil {
			return mapPgError(fmt.Errorf("failed to ack message: %w", err))
		}
		return nil
	})
}

// ackMessageWithTx acknowledges a message within an existing transaction
func (c *Connection) ackMessageWithTx(ctx context.Context, tx Tx, queue string, messageID int64, consumerToken string) (resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "ack", queue, true)
	defer func() { finishMetric(resultErr) }()
	if c.isClosed() {
		return ErrConnectionClosed
	}

	_, err := tx.Exec(ctx,
		"SELECT postgremq.ack_message($1, $2, $3)",
		queue, messageID, consumerToken)
	if err != nil {
		return mapPgError(fmt.Errorf("failed to ack message within transaction: %w", err))
	}
	return nil
}

func (c *Connection) releaseMessage(ctx context.Context, queue string, messageID int64, consumerToken string) (resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "release", queue, false)
	defer func() { finishMetric(resultErr) }()
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx,
			"SELECT postgremq.release_message($1, $2, $3)",
			queue, messageID, consumerToken)
		if err != nil {
			return mapPgError(fmt.Errorf("failed to release message: %w", err))
		}
		return nil
	})
}

func (c *Connection) nackMessage(ctx context.Context, queue string, messageID int64, consumerToken string, delayUntil *time.Time) (resultErr error) {
	finishMetric := c.metrics.startOperation(ctx, "nack", queue, false)
	defer func() { finishMetric(resultErr) }()
	return c.withRetry(ctx, func(ctx context.Context) error {
		var err error
		if delayUntil != nil && !delayUntil.IsZero() {
			_, err = c.pool.Exec(ctx,
				"SELECT postgremq.nack_message($1, $2, $3, $4)",
				queue, messageID, consumerToken, *delayUntil)
		} else {
			_, err = c.pool.Exec(ctx,
				"SELECT postgremq.nack_message($1, $2, $3)",
				queue, messageID, consumerToken)
		}
		if err != nil {
			return mapPgError(fmt.Errorf("failed to nack message: %w", err))
		}
		return nil
	})
}

// isClosed returns true if the connection is stopped
func (c *Connection) isClosed() bool {
	select {
	case <-c.closedFlag:
		return true
	default:
		return false
	}
}

// checkClosed returns an error if the connection is stopped
func (c *Connection) checkClosed() error {
	select {
	case <-c.drainingFlag:
		return ErrConnectionClosed
	default:
	}
	if c.isClosed() {
		return ErrConnectionClosed
	}
	return nil
}

// QueueStatistics contains message counts for a queue or aggregate statistics.
//
// Returned by GetQueueStatistics to provide insight into queue depth and
// processing state.
type QueueStatistics struct {
	// PendingCount is the number of messages with status='pending' (ready for consumption).
	PendingCount int64
	// ProcessingCount is the number of messages with status='processing' (currently being processed).
	ProcessingCount int64
	// CompletedCount is the number of messages with status='completed' (successfully processed).
	CompletedCount int64
	// TotalCount is the sum of pending + processing + completed messages.
	TotalCount int64
}

// QueueInfo contains configuration and metadata for a queue.
//
// Returned by ListQueues to show all queues and their settings.
type QueueInfo struct {
	// QueueName is the unique name of the queue.
	QueueName string
	// TopicName is the name of the topic this queue subscribes to.
	TopicName string
	// MaxDeliveryAttempts is the maximum number of delivery attempts before
	// moving a message to the DLQ. 0 means unlimited retries.
	MaxDeliveryAttempts int
	// Exclusive indicates whether this is an exclusive (temporary) queue.
	// Exclusive queues are deleted when their keep-alive expires.
	Exclusive bool
	// KeepAliveUntil is the timestamp when an exclusive queue will expire.
	// Nil for non-exclusive queues.
	KeepAliveUntil *time.Time
}

// DLQMessage represents a message that has been moved to the Dead Letter Queue.
//
// Returned by ListDLQMessages to show messages that exceeded their max
// delivery attempts and can be inspected or requeued.
type DLQMessage struct {
	// QueueName is the name of the original queue.
	QueueName string
	// MessageID is the ID of the failed message.
	MessageID int64
	// RetryCount is the number of delivery attempts that were made.
	RetryCount int
	// PublishedAt is when the message was originally published.
	PublishedAt time.Time
}

// MaintenanceCounters carries the counters returned by MaintenanceFast.
type MaintenanceCounters struct {
	// RetiredToDLQ counts crashed-final-attempt rows moved into
	// dead_letter_queue. nack_message already retires final attempts inline
	// when a handler completes normally; this only catches rows whose
	// consumer crashed mid-handler before acking/nacking.
	RetiredToDLQ int64
	// InactiveQueuesDropped counts exclusive queues whose keep_alive_until
	// expired.
	InactiveQueuesDropped int64
}

// MaintenanceFast bundles the latency-sensitive maintenance routines into one
// call: retire crashed-final-attempt rows to DLQ + reap expired exclusive
// queues. Run on a 30-60 second cron (≤ ½ × the shortest keep_alive_interval
// so dead exclusive queues are reaped within ~1.5× their interval).
//
// cleanup_completed_messages stays separate — it's a latency-tolerant bulk
// DELETE governed by retention policy, not freshness.
func (c *Connection) MaintenanceFast(ctx context.Context) (MaintenanceCounters, error) {
	var counters MaintenanceCounters
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT retired_to_dlq, inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()").
			Scan(&counters.RetiredToDLQ, &counters.InactiveQueuesDropped)
	})
	return counters, err
}

// ListTopics returns all available topics in the system.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns a slice of topic names sorted alphabetically, or an error if
// the operation fails.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) ListTopics(ctx context.Context) ([]string, error) {
	var topics []string
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx, "SELECT topic FROM postgremq.list_topics()")
		if err != nil {
			return err
		}
		defer rows.Close()

		topics = make([]string, 0)
		for rows.Next() {
			var topic string
			if err := rows.Scan(&topic); err != nil {
				return err
			}
			topics = append(topics, topic)
		}
		return rows.Err()
	})
	return topics, err
}

// ListQueues returns all available queues with their configuration details.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns a slice of QueueInfo containing queue name, topic name,
// max delivery attempts, exclusive status, and keep-alive expiration (if applicable).
// Queues are sorted alphabetically by name.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var queues []QueueInfo
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT queue_name, topic_name, max_delivery_attempts, exclusive, keep_alive_until FROM postgremq.list_queues()")
		if err != nil {
			return err
		}
		defer rows.Close()

		queues = make([]QueueInfo, 0)
		for rows.Next() {
			var q QueueInfo
			if err := rows.Scan(&q.QueueName, &q.TopicName, &q.MaxDeliveryAttempts, &q.Exclusive, &q.KeepAliveUntil); err != nil {
				return err
			}
			queues = append(queues, q)
		}
		return rows.Err()
	})
	return queues, err
}

// GetQueueStatistics returns message statistics for a specified queue or all queues.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queueName: Pointer to queue name, or nil to get statistics for all queues combined.
//
// Returns QueueStatistics containing counts of pending, processing, completed,
// and total messages. If queueName is nil, returns aggregate statistics across
// all queues.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	// Get stats for specific queue
//	queueName := "orders-queue"
//	stats, _ := conn.GetQueueStatistics(ctx, &queueName)
//
//	// Get stats for all queues
//	allStats, _ := conn.GetQueueStatistics(ctx, nil)
func (c *Connection) GetQueueStatistics(ctx context.Context, queueName *string) (*QueueStatistics, error) {
	var stats QueueStatistics
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT pending_count, processing_count, completed_count, total_count FROM postgremq.get_queue_statistics($1)",
			queueName).Scan(&stats.PendingCount, &stats.ProcessingCount, &stats.CompletedCount, &stats.TotalCount)
	})
	return &stats, err
}

// ListDLQMessages returns all messages currently in the Dead Letter Queue (DLQ).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns a slice of DLQMessage containing queue name, message ID, retry count,
// and published timestamp. Messages are sorted by published timestamp.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) ListDLQMessages(ctx context.Context) ([]DLQMessage, error) {
	var messages []DLQMessage
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT queue_name, message_id, retry_count, published_at FROM postgremq.list_dlq_messages()")
		if err != nil {
			return err
		}
		defer rows.Close()

		messages = make([]DLQMessage, 0)
		for rows.Next() {
			var msg DLQMessage
			if err := rows.Scan(&msg.QueueName, &msg.MessageID, &msg.RetryCount, &msg.PublishedAt); err != nil {
				return err
			}
			messages = append(messages, msg)
		}
		return rows.Err()
	})
	return messages, err
}

// RequeueDLQMessages moves messages from the DLQ back to their original queue.
//
// Messages are moved from postgremq.dead_letter_queue back to queue_messages with:
//   - status set to 'pending'
//   - delivery_attempts reset to 0
//   - vt set to NOW() (immediately visible)
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queueName: Name of the queue to requeue messages for.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Use this when you've fixed the issue causing messages to fail and want to
// retry processing them.
func (c *Connection) RequeueDLQMessages(ctx context.Context, queueName string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.requeue_dlq_messages($1)", queueName)
		return err
	})
}

// PurgeDLQ removes all messages from the Dead Letter Queue (DLQ).
//
// This is a destructive operation that permanently deletes all DLQ entries.
// The underlying message records in the messages table are not deleted.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Use with caution - this operation cannot be undone.
func (c *Connection) PurgeDLQ(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.purge_dlq()")
		return err
	})
}

// PurgeAllMessages removes all messages from the entire system.
//
// This is a destructive operation that deletes:
//   - All entries from postgremq.dead_letter_queue
//   - All entries from postgremq.queue_messages
//   - All entries from postgremq.messages
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Use with extreme caution - this operation cannot be undone. Typically only
// used in testing or emergency cleanup scenarios.
func (c *Connection) PurgeAllMessages(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.purge_all_messages()")
		return err
	})
}

// DeleteTopic deletes a topic from the system.
//
// The topic cannot be deleted if any messages are associated with it. Use
// CleanUpTopic to remove messages first if necessary.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic to delete.
//
// Returns an error if:
//   - Messages exist for the topic (use CleanUpTopic first).
//   - The operation fails due to database issues.
//
// Side effects:
//   - Due to ON DELETE CASCADE, all queues subscribed to this topic will also
//     be deleted, along with their queue_messages entries.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) DeleteTopic(ctx context.Context, topic string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.delete_topic($1)", topic)
		return mapPgError(err)
	})
}

// DeleteQueue deletes a queue from the system.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue to delete.
//
// Returns an error if the operation fails.
//
// Side effects:
//   - Due to ON DELETE CASCADE, all queue_messages entries for this queue
//     will be automatically deleted.
//   - The underlying messages in the messages table are NOT deleted.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) DeleteQueue(ctx context.Context, queue string) error {
	err := c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.delete_queue($1)", queue)
		return err
	})
	if err == nil {
		c.keepAliveDeregister(queue)
		c.queueGenerations.Delete(queue)
		// Drop the cached topic mapping so a future Consume on a recreated
		// queue (possibly with a different topic) doesn't subscribe to the
		// stale topic channel.
		c.topicCache.Delete(queue)
	}
	return err
}

// DeleteQueueMessage deletes a specific message from a queue.
//
// This removes the queue_messages entry for the specified message and queue.
// The underlying message in the messages table is NOT deleted.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue.
//   - messageID: ID of the message to delete.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) DeleteQueueMessage(ctx context.Context, queue string, messageID int64) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.delete_queue_message($1, $2)", queue, messageID)
		return err
	})
}

// CleanUpQueue removes all messages from a queue.
//
// This deletes all queue_messages entries for the specified queue but does NOT
// delete the queue itself or the underlying messages in the messages table.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue to clean up.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) CleanUpQueue(ctx context.Context, queue string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.clean_up_queue($1)", queue)
		return err
	})
}

// CleanUpTopic removes all messages associated with a topic.
//
// This deletes all messages from the messages table for the specified topic.
// Due to ON DELETE CASCADE, this also removes all related queue_messages entries.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic to clean up.
//
// Returns an error if the operation fails.
//
// Side effects:
//   - All messages for this topic are permanently deleted.
//   - All queue_messages entries for these messages are automatically deleted.
//
// The operation uses the configured retry policy for transient errors.
//
// This operation is typically required before calling DeleteTopic.
func (c *Connection) CleanUpTopic(ctx context.Context, topic string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.clean_up_topic($1)", topic)
		return err
	})
}

// DeleteInactiveQueues deletes exclusive queues with expired keep-alive timestamps.
// A queue is considered inactive if it is marked as exclusive and its
// keep_alive_until timestamp is either NULL or has already expired.
// This is useful for automatically cleaning up client-specific queues that are
// no longer being used.
func (c *Connection) DeleteInactiveQueues(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT postgremq.delete_inactive_queues()")
		return err
	})
}

// CleanupCompletedMessages removes completed messages older than the provided retention window.
//
// This function should be called periodically (e.g., via cron or scheduled task)
// to prevent unbounded growth of the queue_messages table.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - olderThanHours: Pointer to retention period in hours, or nil to use the
//     default (24 hours defined in the SQL function).
//
// Returns the number of completed messages deleted, or an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	// Delete messages completed more than 7 days ago
//	hours := 168
//	deleted, _ := conn.CleanupCompletedMessages(ctx, &hours)
//
//	// Use default retention (24 hours)
//	deleted, _ := conn.CleanupCompletedMessages(ctx, nil)
func (c *Connection) CleanupCompletedMessages(ctx context.Context, olderThanHours *int) (int, error) {
	var deleted int
	err := c.withRetry(ctx, func(ctx context.Context) error {
		var row pgx.Row
		if olderThanHours != nil {
			row = c.pool.QueryRow(ctx, "SELECT postgremq.cleanup_completed_messages($1)", *olderThanHours)
		} else {
			row = c.pool.QueryRow(ctx, "SELECT postgremq.cleanup_completed_messages()")
		}
		return row.Scan(&deleted)
	})
	return deleted, err
}

// QueueMessage represents message metadata without the payload.
//
// Returned by ListMessages for administrative views and monitoring.
// The payload is excluded for efficiency when listing many messages.
type QueueMessage struct {
	// MessageID is the unique message identifier.
	MessageID int64
	// Status is the current processing status ('pending', 'processing', or 'completed').
	Status string
	// PublishedAt is when the message was first published to the topic.
	PublishedAt time.Time
	// DeliveryAttempts is the number of times this message has been consumed.
	DeliveryAttempts int
	// VT is the visibility timeout expiration timestamp (when the message
	// becomes visible again if not acknowledged).
	VT time.Time
	// ProcessedAt is when the message was completed (nil if not yet completed).
	ProcessedAt *time.Time
	// GroupKey is the message's group (empty if ungrouped).
	GroupKey string
	// GroupSeq is the message's position in its group (0 if ungrouped).
	GroupSeq int64
}

// PublishedMessage represents a complete message including its payload.
//
// Returned by GetMessage to retrieve the full message data for inspection.
type PublishedMessage struct {
	// MessageID is the unique message identifier.
	MessageID int64
	// TopicName is the name of the topic this message was published to.
	TopicName string
	// Payload is the JSON-encoded message payload.
	Payload []byte
	// PublishedAt is when the message was first published to the topic.
	PublishedAt time.Time
	// GroupKey is the message's group (empty if ungrouped).
	GroupKey string
	// GroupSeq is the message's position in its group (0 if ungrouped).
	GroupSeq int64
}

// ListMessages lists all messages in a queue without consuming them.
//
// This is a read-only operation that does NOT change message state or acquire locks.
// The returned messages exclude the payload field for efficiency.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queueName: Name of the queue to list messages from.
//
// Returns a slice of QueueMessage containing message metadata (ID, status,
// published timestamp, delivery attempts, visibility timeout, processed timestamp).
// Messages are sorted by published timestamp.
//
// The operation uses the configured retry policy for transient errors.
//
// Use this for debugging, monitoring, or admin interfaces.
func (c *Connection) ListMessages(ctx context.Context, queueName string) ([]QueueMessage, error) {
	var messages []QueueMessage
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT message_id, status, published_at, delivery_attempts, vt, processed_at, coalesce(group_key, ''), coalesce(group_seq, 0) FROM postgremq.list_messages($1)",
			queueName)
		if err != nil {
			return err
		}
		defer rows.Close()

		messages = make([]QueueMessage, 0)
		for rows.Next() {
			var msg QueueMessage
			if err := rows.Scan(
				&msg.MessageID,
				&msg.Status,
				&msg.PublishedAt,
				&msg.DeliveryAttempts,
				&msg.VT,
				&msg.ProcessedAt,
				&msg.GroupKey,
				&msg.GroupSeq,
			); err != nil {
				return err
			}
			messages = append(messages, msg)
		}
		return rows.Err()
	})
	return messages, err
}

// GetMessage retrieves a single message by ID without consuming it.
//
// This is a read-only operation that returns the complete message including
// its payload. It does NOT change message state.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - messageID: ID of the message to retrieve.
//
// Returns a pointer to PublishedMessage containing the message ID, topic name,
// payload, and published timestamp. Returns nil if the message doesn't exist.
//
// The operation uses the configured retry policy for transient errors.
//
// Use this for debugging or inspecting message payloads.
func (c *Connection) GetMessage(ctx context.Context, messageID int64) (*PublishedMessage, error) {
	var msg PublishedMessage
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT message_id, topic_name, payload, published_at, coalesce(group_key, ''), coalesce(group_seq, 0) FROM postgremq.get_message($1)",
			messageID).Scan(&msg.MessageID, &msg.TopicName, &msg.Payload, &msg.PublishedAt, &msg.GroupKey, &msg.GroupSeq)
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &msg, nil
}

func (c *Connection) getNextVisibleTime(ctx context.Context, queue string) (time.Time, error) {
	if err := c.checkClosed(); err != nil {
		return time.Time{}, err
	}

	nextTime := time.Time{}
	err := c.withRetry(ctx, func(ctx context.Context) error {
		var t sql.NullTime
		err := c.pool.QueryRow(ctx,
			"SELECT postgremq.get_next_visible_time($1)",
			queue).Scan(&t)
		if err != nil {
			return fmt.Errorf("failed to get next visible time: %w", err)
		}
		if t.Valid {
			nextTime = t.Time
		}
		return nil
	})
	return nextTime, err
}

func (c *Connection) unregisterConsumer(consumer Stoppable) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, current := range c.consumers {
		if current == consumer {
			c.consumers = append(c.consumers[:i], c.consumers[i+1:]...)
			return
		}
	}
}

// Bind each consumer once, before taking the connection's registration lock.
func (c *Connection) resolveQueueGeneration(queue string) (string, error) {
	if err := c.checkClosed(); err != nil {
		return "", err
	}
	if cached, ok := c.queueGenerations.Load(queue); ok {
		return cached.(string), nil
	}
	ctx, cancel := context.WithTimeout(c.ioCtx, time.Second)
	defer cancel()
	var generation string
	err := c.pool.QueryRow(ctx, "SELECT generation::text FROM postgremq.queues WHERE name=$1", queue).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrQueueNotFound
	}
	return generation, mapPgError(err)
}
