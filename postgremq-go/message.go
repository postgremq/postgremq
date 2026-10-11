package postgremq

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// MessageStatus values represent the possible states of a message in the queue.
// These values match the status values stored in the queue_messages table.
const (
	MessageStatusPending    = "pending"    // Initial state when message is added to queue
	MessageStatusProcessing = "processing" // Message is being processed by a consumer
	MessageStatusCompleted  = "completed"  // Message has been successfully processed
)

// Message represents a single queue message fetched by a Consumer.
//
// Only the first Ack, Nack, Release or AckWithTx performs SQL. Additional
// terminal calls return ErrLeaseLost. The first call also returns an error
// if the underlying server state no longer matches (e.g., token
// mismatch or message no longer in processing state).
type Message struct {
	// ID is the unique message identifier.
	ID int64
	// Payload is the JSON-encoded message payload.
	Payload json.RawMessage
	// PublishedAt is when the message was first published to the topic.
	PublishedAt time.Time
	// DeliveryAttempt is the number of times this message has been consumed
	// (including the current attempt).
	DeliveryAttempt int
	// VT is the visibility timeout expiration timestamp. If the message is not
	// acknowledged by this time, it becomes visible to other consumers.
	VT time.Time
	// GroupKey is the message group the message was published into (see
	// WithGroupKey); empty for an ungrouped message.
	GroupKey string
	// GroupSeq is the message's dense, 1-based position in its group, in
	// publish commit order; 0 for an ungrouped message.
	GroupSeq int64

	// StoppedCtx is a context that is cancelled when Consumer.Stop() or
	// Connection.Close() is called. Applications can monitor this context
	// to detect shutdown; nack unfinished attempted work, or ack completed work.
	StoppedCtx context.Context

	// internal fields
	queue         string
	consumerToken string

	conn              *Connection
	onComplete        func(*Message)
	settlementStarted atomic.Bool
	cancel            context.CancelFunc
	trackingID        string
	vtMu              sync.RWMutex // Protects VT field from concurrent read/write
}

// Ack acknowledges the message, marking it as successfully processed.
//
// This method updates the database to mark the message as completed, preventing
// further redelivery. It should be called after successful message processing.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if:
//   - The consumer token doesn't match (message was already processed or stolen).
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects:
//   - Sets queue_messages.status = 'completed'
//   - Clears the consumer_token field
//   - Records processed_at timestamp
//   - Removes the message from the consumer's internal tracking
//
// A Message instance executes only one terminal operation. Repeated calls
// return ErrLeaseLost.
//
// The operation uses the configured retry policy for transient errors.
func (m *Message) Ack(ctx context.Context) error {
	return m.settle(func() error { return m.conn.ackMessage(ctx, m.queue, m.ID, m.consumerToken) })
}

// AckWithTx acknowledges the message within an existing database transaction.
//
// This allows acknowledging the message atomically with other database operations,
// ensuring all-or-nothing semantics. If the transaction is rolled back, the message
// remains in 'processing' state.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - tx: Active database transaction (implements Tx interface).
//
// Returns an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects (only if transaction commits):
//   - Sets queue_messages.status = 'completed'
//   - Clears the consumer_token field
//   - Records processed_at timestamp
//
// This method does NOT use the retry policy since transaction boundaries
// are controlled by the caller.
//
// Example:
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//	_, err := tx.Exec(ctx, "INSERT INTO orders (...) VALUES (...)")
//	if err != nil {
//	    return err
//	}
//	if err := msg.AckWithTx(ctx, tx); err != nil {
//	    return err
//	}
//	return tx.Commit(ctx)
func (m *Message) AckWithTx(ctx context.Context, tx Tx) error {
	// Completion transfers transaction responsibility to the caller. On
	// rollback the delivery becomes available again when its lease expires.
	return m.settle(func() error { return m.conn.ackMessageWithTx(ctx, tx, m.queue, m.ID, m.consumerToken) })
}

// Nack negatively acknowledges the message, returning it to the queue for redelivery.
//
// This should be called when message processing fails but the message should be
// retried. The delivery_attempts counter is incremented by the server when the
// message was originally consumed, so calling Nack does not increment it further.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - opts: Optional message options (use WithDelayUntil to delay redelivery).
//
// Returns an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects:
//   - Sets queue_messages.status = 'pending'
//   - Sets vt to the specified delay time (or NOW() if no delay)
//   - Clears the consumer_token field
//   - Emits a NOTIFY event on the per-queue channel `pmq:q:<queue>`
//   - Removes the message from the consumer's internal tracking
//
// If the message has reached max_delivery_attempts, this Nack retires it to
// the DLQ inline (no maintenance call required).
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	// Immediate retry
//	err := msg.Nack(ctx)
//
//	// Retry after 5 minutes
//	err := msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(5*time.Minute)))
func (m *Message) Nack(ctx context.Context, opts ...MessageOption) error {
	options := &messageOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return m.settle(func() error { return m.conn.nackMessage(ctx, m.queue, m.ID, m.consumerToken, options.delayUntil) })
}

// Release returns the message back to the queue WITHOUT incrementing delivery attempts.
//
// This method should be used when a message was fetched but never actually
// attempted to be processed (e.g., buffered but not consumed by the application,
// or consumer is shutting down). This differs from Nack, which assumes the
// message was attempted but failed.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects:
//   - Sets queue_messages.status = 'pending'
//   - Sets vt = NOW() (immediately visible)
//   - Clears the consumer_token field
//   - DECREMENTS delivery_attempts by 1 (to undo the increment from consume)
//   - Emits a NOTIFY event on the per-queue channel `pmq:q:<queue>`
//   - Removes the message from the consumer's internal tracking
//
// The operation uses the configured retry policy for transient errors.
//
// This method is automatically called by Consumer.Stop() for buffered messages
// that haven't been delivered to the application.
func (m *Message) Release(ctx context.Context) error {
	return m.settle(func() error { return m.conn.releaseMessage(ctx, m.queue, m.ID, m.consumerToken) })
}

// SetVT extends the visibility timeout for this in-flight message.
//
// This allows the consumer to extend the processing time for a message that is
// taking longer than expected to process, preventing it from becoming visible
// to other consumers.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - vt: New visibility timeout in seconds from now.
//
// Returns the new visibility timeout expiration timestamp, or an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The visibility timeout has already expired.
//   - The database operation fails.
//
// Side effects:
//   - Updates queue_messages.vt to NOW() + vt seconds
//   - Updates the Message.VT field with the new expiration time
//
// The operation uses the configured retry policy for transient errors.
//
// Note: This method is typically not needed when auto-extension is enabled (default).
// The Consumer automatically extends visibility timeouts for in-flight messages
// once the extension threshold elapses (default 50% — see WithExtensionThreshold).
//
// Example:
//
//	// Manually extend visibility timeout by 60 seconds
//	newVT, err := msg.SetVT(ctx, 60)
//	if err != nil {
//	    log.Printf("Failed to extend VT: %v", err)
//	}
func (m *Message) SetVT(ctx context.Context, vt int) (result time.Time, resultErr error) {
	finishMetric := m.conn.metrics.startOperation(ctx, "extend", m.queue, false)
	defer func() { finishMetric(resultErr) }()
	if m.conn.isClosed() {
		return time.Time{}, ErrConnectionClosed
	}

	var newVT time.Time
	err := m.conn.withRetry(ctx, func(ctx context.Context) error {
		return m.conn.pool.QueryRow(ctx,
			"SELECT postgremq.set_vt($1, $2, $3, $4)",
			m.queue, m.ID, m.consumerToken, vt).Scan(&newVT)
	})
	if err != nil {
		return time.Time{}, mapPgError(err)
	}

	m.vtMu.Lock()
	m.VT = newVT
	m.vtMu.Unlock()
	return newVT, nil
}

// GetVT returns the current visibility timeout in a thread-safe manner.
//
// This method should be used instead of directly accessing the VT field when
// auto-extension is enabled, as the VT field may be updated concurrently by
// the auto-extension goroutine.
//
// For performance reasons, you may choose to access the VT field directly if
// you know auto-extension is disabled or if occasional stale reads are acceptable.
func (m *Message) GetVT() time.Time {
	m.vtMu.RLock()
	defer m.vtMu.RUnlock()
	return m.VT
}

// ConsumerToken returns the consumer token associated with this message.
//
// The consumer token is a unique identifier generated when the message is consumed
// (by the consume_message SQL function). It is used to verify ownership when
// acknowledging, nacking, releasing, or extending the message.
//
// The token is a UUID generated by PostgreSQL for each claim.
//
// Returns the consumer token as a string.
//
// This method is primarily useful for debugging or logging purposes.
func (m *Message) ConsumerToken() string {
	return m.consumerToken
}

func (m *Message) setVT(vt time.Time) { m.vtMu.Lock(); m.VT = vt; m.vtMu.Unlock() }

// The first terminal operation owns both SQL settlement and completion.
// Concurrent/repeated terminal calls return ErrLeaseLost immediately, without
// running SQL or removing the winning operation's tracking.
func (m *Message) settle(operation func() error) error {
	if !m.settlementStarted.CompareAndSwap(false, true) {
		return ErrLeaseLost
	}
	// Only the owner completes tracking, after SQL finishes. Completion is
	// unconditional: a failed settlement must also stop renewal and let the
	// consumer drain. Retries within operation retain ownership until they end.
	if m.onComplete != nil {
		defer m.onComplete(m)
	}
	return operation()
}
