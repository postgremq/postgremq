//! The recorders without the `otel` feature: zero-sized, recording nothing.

use super::Operation;
use crate::error::Error;

/// The connection's (absent) instruments.
#[derive(Debug, Clone, Default)]
pub(crate) struct Metrics {}

impl Metrics {
    pub(crate) const fn operation(
        &self,
        _operation: Operation,
        _destination: Option<&str>,
        _transaction: bool,
    ) -> OperationTimer {
        OperationTimer
    }

    pub(crate) const fn send_attempt(&self, _topic: &str, _transaction: bool) -> SendAttempt {
        SendAttempt
    }

    pub(crate) const fn consumed(&self, _queue: &str, _first: u64, _redelivered: u64) {}

    pub(crate) const fn handler(&self, _queue: &str) -> HandlerTimer {
        HandlerTimer
    }

    pub(crate) const fn renewal_lost(&self, _queue: &str) {}
}

/// See the `otel` backend.
#[derive(Debug)]
#[must_use = "finish the timer with the operation's outcome"]
pub(crate) struct OperationTimer;

impl OperationTimer {
    pub(crate) const fn finish(self, _error: Option<&'static str>) {}

    pub(crate) const fn finish_with<T>(self, _result: &Result<T, Error>) {}
}

/// See the `otel` backend.
#[derive(Debug)]
#[must_use = "finish the attempt with its outcome"]
pub(crate) struct SendAttempt;

impl SendAttempt {
    pub(crate) const fn finish(self, _error: Option<&'static str>) {}
}

/// See the `otel` backend.
#[derive(Debug)]
#[must_use = "finish the timer when the callback returns"]
pub(crate) struct HandlerTimer;

impl HandlerTimer {
    pub(crate) const fn finish(self, _error: Option<&'static str>) {}
}
