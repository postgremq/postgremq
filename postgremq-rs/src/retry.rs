//! Retry with exponential backoff for transient database errors.

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::{sqlstate, sqlstate_of};
use crate::options::RetryConfig;

/// Transient SQLSTATEs: aborted transactions, lock contention, connection
/// exceptions (class `08`) and server restarts.
pub(crate) fn is_transient(err: &sqlx::Error) -> bool {
    sqlstate_of(err).is_some_and(|code| {
        matches!(
            code.as_ref(),
            sqlstate::SERIALIZATION_FAILURE
                | sqlstate::DEADLOCK_DETECTED
                | sqlstate::LOCK_NOT_AVAILABLE
                | sqlstate::ADMIN_SHUTDOWN
                | sqlstate::CRASH_SHUTDOWN
                | sqlstate::CANNOT_CONNECT_NOW
        ) || code.starts_with(sqlstate::CONNECTION_EXCEPTION_CLASS)
    })
}

/// Failures worth retrying for an operation that is safe to repeat
/// (token-fenced settlement, lease and queue heartbeats, idempotent
/// declarations and admin calls): transient SQLSTATEs, plus a dropped
/// connection (e.g. a pooled socket the server or a load balancer closed
/// while idle), which a fresh connection overcomes. Pool exhaustion
/// (`PoolTimedOut`) is not retried: it already waited the acquire timeout.
/// A repeated settlement whose first attempt did commit fails with
/// `LeaseLost`; that stays an error.
pub(crate) fn is_retryable(err: &sqlx::Error) -> bool {
    is_transient(err) || matches!(err, sqlx::Error::Io(_))
}

/// Failures that prove a mutation rolled back: an aborted transaction or a
/// lock that was not available. Connection exceptions (class `08`, e.g.
/// `08007` transaction_resolution_unknown) and shutdowns (`57P0x`) can arrive
/// after a commit, so destructive mutations never retry them.
pub(crate) fn is_rolled_back(err: &sqlx::Error) -> bool {
    sqlstate_of(err).is_some_and(|code| {
        matches!(
            code.as_ref(),
            sqlstate::SERIALIZATION_FAILURE
                | sqlstate::DEADLOCK_DETECTED
                | sqlstate::LOCK_NOT_AVAILABLE
        )
    })
}

/// Only an aborted transaction proves a publish did not commit.
pub(crate) fn is_aborted_transaction(err: &sqlx::Error) -> bool {
    sqlstate_of(err).is_some_and(|code| {
        matches!(
            code.as_ref(),
            sqlstate::SERIALIZATION_FAILURE | sqlstate::DEADLOCK_DETECTED
        )
    })
}

/// Runs `op` until it succeeds, fails with an error `retryable` rejects, or the
/// policy's attempts are used up. Backoff waits end early when `abort` is
/// cancelled (forced shutdown), returning the last error.
pub(crate) async fn with_retry<T, F, Fut>(
    config: &RetryConfig,
    abort: &CancellationToken,
    retryable: fn(&sqlx::Error) -> bool,
    mut op: F,
) -> Result<T, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    let mut backoff = config.initial_backoff;
    let mut attempt: u32 = 1;
    loop {
        let err = match op().await {
            Ok(value) => return Ok(value),
            Err(err) => err,
        };
        if attempt >= config.max_attempts.get() || !retryable(&err) {
            return Err(err);
        }
        tracing::warn!(
            attempt,
            max_attempts = config.max_attempts.get(),
            error = &err as &dyn std::error::Error,
            "retrying transient database error"
        );
        tokio::select! {
            () = abort.cancelled() => return Err(err),
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = next_backoff(backoff, config);
        attempt = attempt.saturating_add(1);
    }
}

fn next_backoff(current: Duration, config: &RetryConfig) -> Duration {
    Duration::try_from_secs_f64(current.as_secs_f64() * config.multiplier)
        .map_or(config.max_backoff, |next| next.min(config.max_backoff))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_by_the_multiplier_and_is_capped() {
        let config = RetryConfig::default();
        let first = next_backoff(Duration::from_millis(100), &config);
        assert_eq!(first, Duration::from_millis(200));
        assert_eq!(
            next_backoff(Duration::from_secs(5), &config),
            config.max_backoff
        );
    }

    #[tokio::test]
    async fn non_retryable_errors_return_after_one_attempt() {
        let mut calls = 0_u32;
        let result: Result<(), _> = with_retry(
            &RetryConfig::default(),
            &CancellationToken::new(),
            |_| false,
            || {
                calls += 1;
                async { Err(sqlx::Error::PoolTimedOut) }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn retryable_errors_use_every_attempt() {
        let mut calls = 0_u32;
        let config = RetryConfig::default().initial_backoff(Duration::from_millis(1));
        let result: Result<(), _> = with_retry(
            &config,
            &CancellationToken::new(),
            |_| true,
            || {
                calls += 1;
                async { Err(sqlx::Error::PoolTimedOut) }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls, 3);
    }
}
