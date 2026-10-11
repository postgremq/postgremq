//! Optional client metrics: the shared client instrumentation contract,
//! version 1, in `docs/observability.md` (the same instruments, attributes
//! and recording boundaries as the Go and TypeScript clients).
//!
//! Enabled by the `otel` cargo feature plus
//! [`ConnectionOptions::meter_provider`](crate::ConnectionOptions::meter_provider).
//! The crate depends on the OpenTelemetry API only: the application owns the
//! SDK, readers, exporters and their shutdown. Without the feature the
//! [`noop`] backend's zero-sized recorders take the same calls.

use crate::error::{sqlstate, sqlstate_of};

#[cfg(not(feature = "otel"))]
mod noop;
#[cfg(feature = "otel")]
mod otel;

#[cfg(not(feature = "otel"))]
pub(crate) use noop::Metrics;
#[cfg(feature = "otel")]
pub(crate) use otel::{CONTRACT_VERSION, Metrics, SCOPE};

/// A logical connection operation, measured once including pool wait and
/// internal retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    Publish,
    Consume,
    Ack,
    Nack,
    Release,
    Extend,
    ExtendBatch,
    KeepAlive,
}

/// The bounded `error.type` category of a driver error (one SQL attempt).
pub(crate) fn sqlx_error_type(err: &sqlx::Error) -> &'static str {
    if let sqlx::Error::Io(io) = err
        && io.kind() == std::io::ErrorKind::TimedOut
    {
        return "deadline_exceeded";
    }
    match err {
        sqlx::Error::PoolClosed => return "connection_closed",
        // The pool's acquire timeout.
        sqlx::Error::PoolTimedOut => return "deadline_exceeded",
        _ => {}
    }
    match sqlstate_of(err).as_deref() {
        Some(sqlstate::LEASE_LOST) => "lease_lost",
        Some(sqlstate::QUEUE_NOT_FOUND) => "queue_not_found",
        Some(sqlstate::VALIDATION) => "validation",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_errors_map_to_the_bounded_categories() {
        assert_eq!(
            sqlx_error_type(&sqlx::Error::Io(std::io::ErrorKind::TimedOut.into())),
            "deadline_exceeded"
        );
        assert_eq!(
            sqlx_error_type(&sqlx::Error::PoolClosed),
            "connection_closed"
        );
        assert_eq!(
            sqlx_error_type(&sqlx::Error::PoolTimedOut),
            "deadline_exceeded"
        );
        assert_eq!(sqlx_error_type(&sqlx::Error::RowNotFound), "other");
    }

    #[test]
    fn recorders_without_a_provider_are_inert() {
        let metrics = Metrics::default();
        metrics
            .operation(Operation::Publish, Some("t"), false)
            .finish(None);
        drop(metrics.operation(Operation::Consume, Some("q"), false));
        metrics.send_attempt("t", false).finish(None);
        drop(metrics.send_attempt("t", false));
        metrics.consumed("q", 1, 1);
        metrics.handler("q").finish(Some("handler_error"));
        metrics.renewal_lost("q");
    }
}
