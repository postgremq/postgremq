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
	// the consumer's lease is no longer valid: token mismatch, or the message
	// is no longer in the 'processing' state. Settlement is fenced by the
	// delivery token only: an expired vt alone does not fail ack/nack/release,
	// but set_vt also fails once vt has expired.
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
//   - ErrDirtySchema: Migrate found the recorded schema version marked dirty:
//     a migration failed partway and the schema needs manual repair. The
//     error is a *DirtySchemaError, which carries the version.
//   - ErrUnknownSchemaVersion: Migrate found a recorded schema version below
//     its latest migration that is none of its migrations, so it knows no
//     upgrade from it. The error is an *UnknownSchemaVersionError.
//   - ErrMigrationFailed: a migration failed and its version is left dirty
//     (rolled back when the database reported the error). The error is a
//     *MigrationError, with the database error in its chain.
var (
	ErrConnectionClosed     = errors.New("postgremq: connection is stopped")
	ErrLeaseLost            = errors.New("postgremq: lease lost")
	ErrQueueNotFound        = errors.New("postgremq: queue not found")
	ErrValidation           = errors.New("postgremq: validation error")
	ErrQueueGone            = errors.New("postgremq: queue is gone")
	ErrDirtySchema          = errors.New("postgremq: database schema is dirty")
	ErrUnknownSchemaVersion = errors.New("postgremq: unknown database schema version")
	ErrMigrationFailed      = errors.New("postgremq: migration failed")
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

// DirtySchemaError is returned by Migrate when the recorded schema version is
// marked dirty: its migration failed partway, so the schema is in an unknown
// state. Fix the schema by hand, then clear the flag in
// postgremq.postgremq_migrations (see the CLI's README, "Dirty state"). It
// matches errors.Is(err, ErrDirtySchema).
type DirtySchemaError struct {
	// Version is the schema version whose migration did not finish.
	Version uint
}

func (e *DirtySchemaError) Error() string {
	return fmt.Sprintf("postgremq: database schema is dirty at version %d; a migration failed partway and needs manual repair", e.Version)
}

// Is makes every DirtySchemaError match ErrDirtySchema.
func (e *DirtySchemaError) Is(target error) bool { return target == ErrDirtySchema }

// UnknownSchemaVersionError is returned by Migrate when the recorded schema
// version is below this client's latest migration but is none of its
// migrations, so no upgrade from it is known. Nothing is applied. It matches
// errors.Is(err, ErrUnknownSchemaVersion).
type UnknownSchemaVersionError struct {
	// Version is the recorded schema version.
	Version uint
}

func (e *UnknownSchemaVersionError) Error() string {
	return fmt.Sprintf("postgremq: database schema version %d (mq %s) is not one of this client's migrations, so no upgrade from it is known",
		e.Version, formatSchemaVersion(uint64(e.Version)))
}

// Is makes every UnknownSchemaVersionError match ErrUnknownSchemaVersion.
func (e *UnknownSchemaVersionError) Is(target error) bool { return target == ErrUnknownSchemaVersion }

// MigrationError is returned by Migrate when a migration fails. Its version
// is left dirty (Migrate then returns a *DirtySchemaError until it is
// repaired). The migration runs in one transaction: when the database reports
// the error (Err is a *pgconn.PgError), it was rolled back; after a
// connection failure its outcome is unknown, so inspect the schema before
// repairing. It matches errors.Is(err, ErrMigrationFailed).
type MigrationError struct {
	// Version is the schema version of the migration that failed.
	Version uint
	// Line and Column locate the error in the migration's file, from 1, when
	// PostgreSQL reports a position (it does for errors in the file's
	// statements, including a function's definition); otherwise both are 0.
	// They are also 0 when the position cannot be mapped to the file: with a
	// multibyte database or client encoding other than UTF8 (such as EUC_JP)
	// the server counts other characters.
	Line, Column int
	// Err is the database error.
	Err error
}

// Error gives the migration, the position and the database error with its
// detail, hint, the failing query inside a function and its context.
func (e *MigrationError) Error() string {
	at := ""
	if e.Line > 0 {
		at = fmt.Sprintf(" at line %d, column %d", e.Line, e.Column)
	}
	extra := ""
	var pgErr *pgconn.PgError
	if errors.As(e.Err, &pgErr) {
		for _, field := range []struct{ name, value string }{
			{"detail", pgErr.Detail}, {"hint", pgErr.Hint}, {"query", pgErr.InternalQuery}, {"context", pgErr.Where},
		} {
			if field.value != "" {
				extra += fmt.Sprintf("; %s: %s", field.name, field.value)
			}
		}
	}
	return fmt.Sprintf("postgremq: migration %s (schema version %d) failed%s: %v%s",
		formatSchemaVersion(uint64(e.Version)), e.Version, at, e.Err, extra)
}

// Unwrap exposes the database error.
func (e *MigrationError) Unwrap() error { return e.Err }

// Is makes every MigrationError match ErrMigrationFailed.
func (e *MigrationError) Is(target error) bool { return target == ErrMigrationFailed }

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
