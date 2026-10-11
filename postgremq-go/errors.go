package postgremq

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE codes raised by the PostgreMQ SQL functions. Clients should
// match on the code via mapPgError, never on the human-readable text.
const (
	// ErrCodeLeaseLost is raised by ack/nack/release/set_vt when
	// the consumer's lease is no longer valid: vt expired, token mismatch, or
	// the message is no longer in the 'processing' state.
	ErrCodeLeaseLost = "PMQ01"

	// ErrCodeQueueNotFound is raised when a referenced queue or topic does
	// not exist (or is an expired exclusive queue).
	ErrCodeQueueNotFound = "PMQ02"

	// ErrCodeValidation is raised on caller-rejection paths: input validation
	// failures (negative timeouts, p_limit <= 0, mismatched batch arrays,
	// names outside [A-Za-z0-9_:.\-]) AND state-precondition failures
	// (exclusive-queue duplicate, delete_topic on a non-empty topic). The
	// common thread is "the request was rejected before mutating state."
	ErrCodeValidation = "PMQ03"
)

// Error sentinels. Application code uses errors.Is to discriminate.
//
//   - ErrConnectionClosed: returned by any operation attempted after the
//     Connection has begun shutting down or has been closed.
//   - ErrLeaseLost: another consumer may have processed the message; the
//     handler should be idempotent or treat the operation as a no-op.
//   - ErrQueueNotFound: the queue (or topic) doesn't exist. Returned by
//     resolveTopic for an unknown queue (client-side) AND by mapPgError for
//     PMQ02 server-side errors (e.g. publish to a non-existent topic).
//   - ErrValidation: the request was rejected before any state change. Covers
//     input validation (bad arguments) AND state preconditions (e.g. delete
//     a topic that still has messages, create a duplicate exclusive queue).
//   - ErrQueueGone: a queue a consumer depends on is gone — it was deleted, or
//     (for an exclusive queue) its keep-alive permanently failed (omitted from
//     extend_queue_keep_alive_multi's result = gone/non-exclusive). It is the
//     single sentinel for a fatal, unrecoverable queue condition; consumers on
//     the queue are torn down and the reason is delivered as a *QueueFatalError
//     (which matches errors.Is(err, ErrQueueGone)) via Consumer.NotifyClose and
//     the connection-level WithQueueFatalHandler.
var (
	ErrConnectionClosed = errors.New("postgremq: connection is stopped")
	ErrLeaseLost        = errors.New("postgremq: lease lost")
	ErrQueueNotFound    = errors.New("postgremq: queue not found")
	ErrValidation       = errors.New("postgremq: validation error")
	ErrQueueGone        = errors.New("postgremq: queue is gone")
)

// QueueFatalError is the reason a consumer was torn down: the queue it depends
// on is gone. It always matches errors.Is(err, ErrQueueGone) regardless of the
// underlying cause (a keep-alive permanent failure, or a consume that returned
// PMQ02/ErrQueueNotFound because the queue was deleted out-of-band).
type QueueFatalError struct {
	// Queue is the queue that is gone.
	Queue string
	// Err is the underlying cause (e.g. ErrQueueGone or an ErrQueueNotFound chain).
	Err error
}

func (e *QueueFatalError) Error() string {
	return fmt.Sprintf("postgremq: queue %q is gone: %v", e.Queue, e.Err)
}

// Unwrap exposes the underlying cause so errors.Is/As reach it (e.g.
// ErrQueueNotFound when the queue was deleted out-of-band).
func (e *QueueFatalError) Unwrap() error { return e.Err }

// Is makes every QueueFatalError match ErrQueueGone, so applications have one
// sentinel to check no matter which detector fired.
func (e *QueueFatalError) Is(target error) bool { return target == ErrQueueGone }

// mapPgError inspects err and, if it carries a PostgreMQ SQLSTATE, wraps it
// with the matching sentinel error so callers can use errors.Is. Errors that
// are not pgconn.PgError or that carry an unrelated SQLSTATE are returned
// unchanged. nil pass-through.
//
// Both the typed sentinel and the original error are preserved in the wrap
// chain via fmt.Errorf's multi-%w (Go 1.20+). That keeps `errors.As(err,
// &pgErr)` working for callers that want the SQLSTATE, and — critically —
// keeps the retry policy's `IsRetryableError` accurate: it inspects the
// underlying *pgconn.PgError to decide whether to retry.
func mapPgError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch pgErr.Code {
	case ErrCodeLeaseLost:
		return fmt.Errorf("%w: %w", ErrLeaseLost, err)
	case ErrCodeQueueNotFound:
		return fmt.Errorf("%w: %w", ErrQueueNotFound, err)
	case ErrCodeValidation:
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}
	return err
}
