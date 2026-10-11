package postgremq

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// RetryConfig holds configuration for automatic retry of transient database errors.
//
// The client automatically retries operations that fail due to:
//   - Serialization failures (40001) and deadlock detection (40P01)
//   - Connection failures (PostgreSQL error class 08)
//   - Admin shutdown / crash shutdown / cannot-connect-now (57P01/57P02/57P03)
//
// Retry uses exponential backoff. Publish retries only aborted transactions.
type RetryConfig struct {
	// Disabled turns off retry logic entirely. When true, operations fail immediately on error.
	Disabled bool

	// MaxAttempts is the maximum number of attempts (including the initial attempt).
	// For example, MaxAttempts=3 means: 1 initial attempt + up to 2 retries.
	MaxAttempts int

	// InitialBackoff is the wait time before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff caps the backoff duration. Even with exponential growth, backoff
	// will never exceed this value.
	MaxBackoff time.Duration

	// BackoffMultiplier is applied to the previous backoff to calculate the next wait time.
	// For example, with InitialBackoff=100ms and BackoffMultiplier=2.0:
	//   Retry 1: wait 100ms
	//   Retry 2: wait 200ms
	//   Retry 3: wait 400ms (capped by MaxBackoff)
	BackoffMultiplier float64
}

func defaultRetryConfig() RetryConfig {
	return RetryConfig{
		Disabled:          false,
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// IsRetryableError determines if an error represents a transient database failure
// that should trigger a retry.
//
// Retryable PostgreSQL SQLSTATE codes:
//   - 40001 (serialization_failure): Transaction conflicts.
//   - 40P01 (deadlock_detected): Deadlocks that may resolve on retry.
//   - Class 08 (connection_exception): Network failures, broken connections.
//   - 57P01 (admin_shutdown): Operator restarted the server (graceful).
//   - 57P02 (crash_shutdown): Server crashed and is restarting.
//   - 57P03 (cannot_connect_now): Server is starting up / in recovery.
//
// Returns true if the operation should be retried, false otherwise. The error
// must unwrap to a *pgconn.PgError; we deliberately do NOT fall back to
// string-matching the error text — the previous fallback `strings.Contains
// (errStr, "08")` matched timestamps ("08:00:00"), file paths, and any
// other place where "08" appeared, producing spurious retries on permanent
// failures.
func IsRetryableError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "40001", "40P01", "55P03":
		return true
	case "57P01", "57P02", "57P03":
		return true
	}
	return strings.HasPrefix(pgErr.Code, "08")
}

// withRetry executes the given operation with retries
// It checks whether the connection is stopped (via isClosed) both before and during retries.
func (c *Connection) withRetry(ctx context.Context, operation func(context.Context) error) error {
	return c.withRetryPolicy(ctx, operation, IsRetryableError)
}
func isAbortedTransaction(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}
func (c *Connection) withRetryPolicy(ctx context.Context, operation func(context.Context) error, retryable func(error) bool) error {
	if c.isClosed() {
		return ErrConnectionClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if c.ioCtx != nil {
		stop := context.AfterFunc(c.ioCtx, cancel)
		defer stop()
	}

	if c.retryConfig.Disabled {
		return operation(ctx)
	}

	var lastErr error
	backoff := c.retryConfig.InitialBackoff

	for attempt := 0; attempt < c.retryConfig.MaxAttempts; attempt++ {
		// If the connection is stopped mid-retry, return immediately.
		if c.isClosed() {
			return ErrConnectionClosed
		}

		err := operation(ctx)
		if err == nil {
			return nil
		}

		if !retryable(err) {
			return err // Non-retryable error, return immediately
		}

		lastErr = err
		c.logger.Warnf("Retryable error occurred (attempt %d/%d): %v",
			attempt+1, c.retryConfig.MaxAttempts, err)

		// Check if we should make another attempt
		if attempt == c.retryConfig.MaxAttempts-1 {
			break
		}

		// Wait before next attempt, but respect context cancellation
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}

		// Increase backoff for next attempt
		backoff = time.Duration(float64(backoff) * c.retryConfig.BackoffMultiplier)
		if backoff > c.retryConfig.MaxBackoff {
			backoff = c.retryConfig.MaxBackoff
		}
	}

	return lastErr
}
