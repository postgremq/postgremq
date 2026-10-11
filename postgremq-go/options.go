package postgremq

import (
	"fmt"
	"math"
	"time"
)

// ConnectionOption configures a Connection at construction time.
//
// Connection options are passed to Dial to customize the behavior of
// the connection, such as shutdown timeout, retry policy, and logging.
type ConnectionOption func(*Connection)

// WithShutdownTimeout sets how long Connection.Close waits for all Consumers
// to finish before continuing shutdown.
//
// When Connection.Close() is called, it waits for consumers to stop and
// in-flight messages to be processed. This timeout applies to the entire
// shutdown process.
//
// Parameters:
//   - d: Maximum duration to wait. Zero means wait indefinitely (default).
//
// If the timeout expires, shutdown continues anyway, potentially leaving
// some messages in 'processing' state (they will become visible again when
// their visibility timeout expires).
func WithShutdownTimeout(d time.Duration) ConnectionOption {
	return func(c *Connection) {
		c.shutdownTimeout = d
	}
}

// WithRetryConfig sets the retry policy for transient database errors.
//
// The retry policy is used by all Connection methods except those with
// "WithTx" suffix (PublishWithTx, AckWithTx), since transaction boundaries
// are controlled by the caller for those methods.
//
// Parameters:
//   - config: RetryConfig struct specifying max attempts, backoff parameters, etc.
//
// The default retry policy is:
//   - MaxAttempts: 3
//   - InitialBackoff: 100ms
//   - MaxBackoff: 2s
//   - BackoffMultiplier: 2.0
func WithRetryConfig(config RetryConfig) ConnectionOption {
	return func(c *Connection) {
		c.retryConfig = RetryConfig{
			Disabled:          config.Disabled,
			MaxAttempts:       config.MaxAttempts,
			InitialBackoff:    config.InitialBackoff,
			MaxBackoff:        config.MaxBackoff,
			BackoffMultiplier: config.BackoffMultiplier,
		}
	}
}

// WithoutRetries disables the retry policy for this Connection.
//
// When retries are disabled, operations fail immediately on the first error
// without any retry attempts. This can be useful for:
//   - Testing failure scenarios
//   - Applications that implement their own retry logic
//   - Strict latency requirements where retries are unacceptable
func WithoutRetries() ConnectionOption {
	return func(c *Connection) {
		c.retryConfig.Disabled = true
	}
}

func validateConnectionOptions(options *Connection) error {
	if options.shutdownTimeout < 0 {
		return fmt.Errorf("shutdown timeout must be positive")
	}
	if options.extenderBatchSize <= 0 {
		return fmt.Errorf("extender batch size must be positive")
	}
	if options.retryConfig.Disabled {
		return nil
	}
	if options.retryConfig.MaxAttempts <= 0 {
		return fmt.Errorf("max attempts must be positive")
	}
	if options.retryConfig.InitialBackoff <= 0 {
		return fmt.Errorf("initial backoff must be positive")
	}
	if options.retryConfig.MaxBackoff <= 0 {
		return fmt.Errorf("max backoff must be positive")
	}
	if math.IsNaN(options.retryConfig.BackoffMultiplier) || math.IsInf(options.retryConfig.BackoffMultiplier, 0) || options.retryConfig.BackoffMultiplier <= 0 {
		return fmt.Errorf("backoff multiplier must be positive")
	}
	return nil
}

// WithQueueFatalHandler installs a callback invoked when a queue becomes fatal
// for this connection — it is gone and any consumers on it have been torn down.
//
// This fires when the queue is deleted out-of-band (a consume returns PMQ02) or
// when an exclusive queue's keep-alive permanently fails (omitted from
// extend_queue_keep_alive_multi's result = gone/non-exclusive). The error is a
// *QueueFatalError (matching errors.Is(err, ErrQueueGone)). It is the queue-level
// signal; consumers also learn via Consumer.NotifyClose. This handler is the only
// signal for a producer-only exclusive queue that has no consumer.
//
// The handler runs on its own goroutine, so it may block; it must be safe to
// call concurrently with other Connection operations. Without a handler, fatal
// queues are logged.
//
// Mirrors the TS client's onQueueFatal / 'queueFatal' event.
func WithQueueFatalHandler(fn func(queue string, err error)) ConnectionOption {
	return func(c *Connection) {
		c.onQueueFatal = fn
	}
}

// WithLogger installs a Logger (Printf-style) adapter used for warnings and errors.
//
// Parameters:
//   - logger: Logger interface with a single Printf method.
//
// The logger is used for:
//   - Warning messages (keep-alive failures, notification listener issues)
//   - Error messages (unexpected failures during background operations)
//   - Debug messages (when enabled)
func WithLogger(logger Logger) ConnectionOption {
	return func(c *Connection) {
		c.logger = &levelLogAdapter{logger: logger}
	}
}

// WithLevelLogger installs a leveled logger that supports Debug, Info, Warn, and Error methods.
//
// Parameters:
//   - logger: LevelLogger interface with Debug, Info, Warn, and Error methods.
//
// This provides more granular control over log levels compared to WithLogger.
func WithLevelLogger(logger LevelLogger) ConnectionOption {
	return func(c *Connection) {
		c.logger = logger
	}
}

// queueOptions holds configuration for queue creation.
type queueOptions struct {
	maxDeliveryAttempts int
	keepAliveInterval   time.Duration
}

// QueueOption configures queue creation parameters.
//
// Queue options are passed to CreateQueue to customize the queue's behavior,
// such as max delivery attempts and keep-alive interval for exclusive queues.
type QueueOption func(*queueOptions)

func defaultQueueOptions() queueOptions {
	return queueOptions{
		maxDeliveryAttempts: 0,
		keepAliveInterval:   5 * time.Minute,
	}
}

// WithMaxDeliveryAttempts sets the maximum delivery attempts for a queue.
//
// When a message's delivery_attempts reaches this limit, it is moved to the
// Dead Letter Queue (DLQ) inline by nack_message on the final attempt.
// MaintenanceFast also recovers any crashed-final-attempt rows.
//
// Parameters:
//   - n: Maximum delivery attempts (0 for unlimited retries).
//
// Default is 0 (unlimited) — matches the SQL function's default and the
// TS client. Pick a finite value to enable the dead-letter queue path.
//
// Messages increment delivery_attempts each time they are consumed. If a
// message is Nack'd or released due to visibility timeout expiration, it
// remains eligible for redelivery until max_delivery_attempts is reached.
func WithMaxDeliveryAttempts(n int) QueueOption {
	return func(o *queueOptions) {
		o.maxDeliveryAttempts = n
	}
}

// WithKeepAliveInterval sets the keep-alive interval for exclusive queues.
//
// Exclusive queues are automatically deleted when their keep_alive_until
// timestamp expires. consume_message refreshes keep_alive_until on every
// call (so an actively-polling consumer keeps its queue alive for free),
// and the Connection runs a background timer that extends keep_alive_until
// every (interval / 2) regardless. The interval should comfortably exceed
// the longest expected gap between consume calls or message-handler runs.
//
// Parameters:
//   - d: Keep-alive interval. Default is 5 minutes.
//
// If the Connection closes or crashes, the queue is deleted after the
// interval elapses. Only applicable to exclusive queues.
func WithKeepAliveInterval(d time.Duration) QueueOption {
	return func(o *queueOptions) {
		o.keepAliveInterval = d
	}
}

// consumeOptions holds configuration for message consumption.
type consumeOptions struct {
	generation         string
	batchSize          int
	checkTimeout       time.Duration
	vt                 int
	noAutoExtension    bool
	extensionThreshold float64
	topic              string
}

// ConsumeOption configures consumer behavior.
//
// Consumer options are passed to Consume to customize fetching, visibility
// timeout, and auto-extension behavior.
//
// ConsumeOption also implements HandlerConsumeOption, so options like WithVT
// can be passed to both Consume and ConsumeHandler.
type ConsumeOption func(*consumeOptions)

// apply implements HandlerConsumeOption for ConsumeOption.
func (o ConsumeOption) apply(opts *handlerConsumeOptions) {
	o(&opts.consumeOptions)
}

// handlerConsumeOptions extends consumeOptions with handler-specific settings.
type handlerConsumeOptions struct {
	consumeOptions
	maxInFlight int // 0 means unlimited
}

// HandlerConsumeOption configures handler consumer behavior.
//
// Both ConsumeOption (like WithVT, WithBatchSize) and handler-specific options
// (like WithMaxInFlight) implement this interface.
type HandlerConsumeOption interface {
	apply(*handlerConsumeOptions)
}

// handlerConsumeOption is for options that only apply to handler consumers.
type handlerConsumeOption func(*handlerConsumeOptions)

func (o handlerConsumeOption) apply(opts *handlerConsumeOptions) {
	o(opts)
}

// WithMaxInFlight sets the maximum number of concurrent handler executions.
// Default is 0 (unlimited - handlers run as fast as messages arrive).
// Only applies to ConsumeHandler, ignored by Consume.
func WithMaxInFlight(n int) HandlerConsumeOption {
	return handlerConsumeOption(func(o *handlerConsumeOptions) {
		o.maxInFlight = n
	})
}

func defaultConsumeOptions() consumeOptions {
	return consumeOptions{
		batchSize:          10,
		checkTimeout:       10 * time.Second,
		vt:                 0,
		noAutoExtension:    false,
		extensionThreshold: 0.5,
	}
}

func validateConsumeOptions(options *consumeOptions) error {
	if options.checkTimeout <= 0 {
		return fmt.Errorf("check timeout must be positive")
	}
	if options.vt == 0 && options.noAutoExtension {
		return fmt.Errorf("no auto extension is not supported without lock timeout")
	}
	if options.batchSize <= 0 || int64(options.batchSize) > 2147483647 {
		return fmt.Errorf("batch size must be positive")
	}
	if options.vt < 0 || int64(options.vt) > 2147483647 {
		return fmt.Errorf("lock timeout must be positive")
	}
	if math.IsNaN(options.extensionThreshold) || options.extensionThreshold <= 0 || options.extensionThreshold >= 1 {
		return fmt.Errorf("extension threshold must be in (0, 1)")
	}
	return nil
}

func defaultHandlerConsumeOptions() handlerConsumeOptions {
	return handlerConsumeOptions{
		consumeOptions: defaultConsumeOptions(),
		maxInFlight:    0, // 0 means unlimited
	}
}

func validateHandlerConsumeOptions(options *handlerConsumeOptions) error {
	if err := validateConsumeOptions(&options.consumeOptions); err != nil {
		return err
	}
	if options.maxInFlight < 0 {
		return fmt.Errorf("maxInFlight must be non-negative")
	}
	return nil
}

// WithBatchSize sets how many messages to fetch per batch.
//
// Larger batch sizes reduce database round-trips but increase memory usage
// and time to first message. The Messages() channel is buffered to this size.
//
// Parameters:
//   - n: Number of messages per batch. Default is 10. Must be > 0.
func WithBatchSize(n int) ConsumeOption {
	return func(o *consumeOptions) {
		o.batchSize = n
	}
}

// WithCheckTimeout sets the maximum polling interval when no notifications are received.
//
// The consumer uses PostgreSQL LISTEN/NOTIFY for real-time notification when
// new messages arrive. However, notifications can be missed in some failure
// scenarios. This timeout ensures the consumer wakes up periodically to check
// for messages even if no notification was received.
//
// Parameters:
//   - d: Maximum wait time between checks. Default is 10 seconds. Must be > 0.
//
// Shorter timeouts increase database load but reduce latency in notification
// failure scenarios.
func WithCheckTimeout(d time.Duration) ConsumeOption {
	return func(o *consumeOptions) {
		o.checkTimeout = d
	}
}

// WithVT sets the visibility timeout (seconds) for consumed messages.
//
// When a message is consumed, it becomes invisible to other consumers for
// the visibility timeout duration. This prevents multiple consumers from
// processing the same message simultaneously.
//
// Parameters:
//   - seconds: Visibility timeout in seconds. Default is 0 (must be set). Must be >= 0.
//
// The visibility timeout should be longer than typical message processing time.
// If auto-extension is enabled (default), messages are automatically extended
// once the extension threshold elapses (default 50% — see WithExtensionThreshold)
// to prevent expiration during processing.
//
// If a message's visibility timeout expires before being acknowledged, it
// becomes visible to other consumers again.
func WithVT(seconds int) ConsumeOption {
	return func(o *consumeOptions) {
		o.vt = seconds
	}
}

// WithNoAutoExtension disables automatic visibility timeout extension.
//
// By default, the Consumer automatically extends visibility timeouts for
// in-flight messages once the extension threshold elapses (default 50% —
// see WithExtensionThreshold) to prevent them from becoming visible to other
// consumers while still being processed.
//
// When auto-extension is disabled, the application is responsible for calling
// Message.SetVT() to extend the visibility timeout if processing takes longer
// than expected.
//
// Requires a non-zero VT (set via WithVT).
//
// Use this option if:
//   - You want fine-grained control over extension timing
//   - You know message processing always completes within the VT period
//   - You want to minimize database operations
func WithNoAutoExtension() ConsumeOption {
	return func(o *consumeOptions) {
		o.noAutoExtension = true
	}
}

// WithTopic provides the topic name for the consumed queue, bypassing the
// internal queue->topic cache and any database lookup.
//
// Use this when the caller already knows the topic and wants to skip the
// resolution round-trip — typically in code paths that consume from a queue
// without having created it through this Connection.
func WithTopic(topic string) ConsumeOption {
	return func(o *consumeOptions) {
		o.topic = topic
	}
}

// WithExtenderBatchSize limits how many in-flight messages the connection-level
// auto-extension actor extends in a single set_vt_batch_multi call per tick.
//
// Auto-extension is connection-level: one actor coalesces the due extensions of
// every consumer on the Connection into one batched round-trip. This bounds the
// statement size of that call (correctness does not depend on it — the SQL's
// global ordering makes any batch deadlock-safe).
//
// Parameters:
//   - size: Maximum messages per extension batch. Default is 100. Must be > 0.
//
// Larger batch sizes reduce database round-trips at the cost of larger
// statements; the actor simply pops more entries per tick (the rest follow on
// the next tick).
func WithExtenderBatchSize(size int) ConnectionOption {
	return func(c *Connection) {
		c.extenderBatchSize = size
	}
}

// WithExtensionThreshold sets when auto-extension fires, expressed as the
// fraction of the visibility timeout that must elapse before extension.
//
// Default is 0.5 — extend at the halfway point. Lower values extend
// earlier (more headroom, more DB calls); higher values extend later
// (fewer DB calls, less headroom for slow handlers / network blips).
//
// Parameters:
//   - threshold: Fraction in (0, 1). 0.5 = 50% of VT elapsed.
//
// Mirrors the TS client's autoExtension.extensionThreshold option.
// Only applicable when auto-extension is enabled (default).
func WithExtensionThreshold(threshold float64) ConsumeOption {
	return func(o *consumeOptions) {
		o.extensionThreshold = threshold
	}
}

// MessageOption configures message acknowledgment operations.
//
// Message options are passed to Message.Nack to customize behavior such as
// delayed redelivery.
type MessageOption func(*messageOptions)

type messageOptions struct {
	delayUntil *time.Time
}

// WithDelayUntil sets the absolute time when a Nacked message should become visible again.
//
// This allows implementing exponential backoff or scheduled retries for
// failed messages.
//
// Parameters:
//   - t: Absolute timestamp when the message should become visible.
//
// If not specified, Nack makes the message immediately visible for redelivery.
//
// Example:
//
//	// Retry after 5 minutes
//	msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(5*time.Minute)))
//
//	// Exponential backoff
//	delay := time.Duration(math.Pow(2, float64(msg.DeliveryAttempts))) * time.Second
//	msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(delay)))
func WithDelayUntil(t time.Time) MessageOption {
	return func(o *messageOptions) {
		o.delayUntil = &t
	}
}

// PublishOption configures message publishing behavior.
//
// Publish options are passed to Publish or PublishWithTx to customize
// behavior such as delayed delivery.
type PublishOption func(*publishOptions)

type publishOptions struct {
	deliverAfter *time.Time
	groupKey     *string
}

// WithDeliverAfter sets the time when a newly published message should become visible.
//
// This allows scheduling messages for future delivery. The message is inserted
// into the database immediately but remains invisible to consumers until the
// specified time.
//
// Parameters:
//   - t: Absolute timestamp when the message should become visible.
//
// If not specified, the message is immediately visible for consumption.
//
// Example:
//
//	// Deliver in 1 hour
//	conn.Publish(ctx, "topic", payload, postgremq.WithDeliverAfter(time.Now().Add(time.Hour)))
//
//	// Schedule for specific time
//	scheduleTime := time.Date(2025, 12, 25, 9, 0, 0, 0, time.UTC)
//	conn.Publish(ctx, "topic", payload, postgremq.WithDeliverAfter(scheduleTime))
func WithDeliverAfter(t time.Time) PublishOption {
	return func(o *publishOptions) {
		o.deliverAfter = &t
	}
}

// WithGroupKey publishes the message into a message group.
//
// Within one queue, deliveries of one group are claimed strictly in publish
// commit order, one at a time: a message is not delivered while an earlier
// message of its group is pending or being processed (head-of-line blocking,
// as in SQS FIFO). A head nacked with a delay blocks its group until the delay
// passes; a head that exhausts its delivery attempts moves to the dead letter
// queue, which unblocks the group. A consume batch holds at most one message
// per group. Messages without a group key are unordered and never blocked.
//
// Publishers of one group are serialised by the database until their
// transaction commits, so a group should be a session, an order or an
// account — never a hot key shared by unrelated work. A transaction that
// publishes to several groups should take them in a consistent order to
// avoid deadlocks.
//
// The key must be 1..255 characters; an empty key is rejected with
// ErrValidation (omit the option to publish ungrouped).
//
// Example:
//
//	conn.Publish(ctx, "session-events", payload, postgremq.WithGroupKey(sessionID))
func WithGroupKey(key string) PublishOption {
	return func(o *publishOptions) {
		o.groupKey = &key
	}
}
