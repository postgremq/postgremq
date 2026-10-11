//! The recorders with the `otel` feature.

use std::sync::Arc;
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use super::{Operation, sqlx_error_type};
use crate::error::{Error, ErrorKind};

/// Instrumentation scope name and contract version.
pub(crate) const SCOPE: &str = "postgremq";
pub(crate) const CONTRACT_VERSION: &str = "1";

/// Histogram bucket boundaries (seconds) shared by every client.
const BOUNDARIES: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

const fn name(operation: Operation) -> &'static str {
    match operation {
        Operation::Publish => "publish",
        Operation::Consume => "consume",
        Operation::Ack => "ack",
        Operation::Nack => "nack",
        Operation::Release => "release",
        Operation::Extend => "extend",
        Operation::ExtendBatch => "extend_batch",
        Operation::KeepAlive => "keep_alive",
    }
}

const fn kind(operation: Operation) -> &'static str {
    match operation {
        Operation::Publish => "send",
        Operation::Consume => "receive",
        Operation::Ack | Operation::Nack | Operation::Release => "settle",
        Operation::Extend => "extend",
        Operation::ExtendBatch => "extend_batch",
        Operation::KeepAlive => "keep_alive",
    }
}

/// The bounded `error.type` category of a crate error.
pub(crate) fn error_type(err: &Error) -> &'static str {
    match err.kind() {
        ErrorKind::LeaseLost => "lease_lost",
        ErrorKind::QueueNotFound | ErrorKind::QueueGone => "queue_not_found",
        ErrorKind::Validation => "validation",
        ErrorKind::Closed => "connection_closed",
        ErrorKind::Busy | ErrorKind::Payload | ErrorKind::DirtySchema | ErrorKind::Incompatible => {
            "other"
        }
        ErrorKind::Sqlx => match err {
            Error::Sqlx(sqlx) => sqlx_error_type(sqlx),
            _ => "other",
        },
    }
}

/// The connection's instruments (cheap to clone; inert without a provider).
#[derive(Clone, Default)]
pub(crate) struct Metrics {
    instruments: Option<Arc<Instruments>>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("enabled", &self.instruments.is_some())
            .finish()
    }
}

struct Instruments {
    operation: Histogram<f64>,
    process: Histogram<f64>,
    sent: Counter<u64>,
    consumed: Counter<u64>,
    active: UpDownCounter<i64>,
    renewal_lost: Counter<u64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        let histogram = |name: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_boundaries(BOUNDARIES.to_vec())
                .build()
        };
        Self {
            operation: histogram("messaging.client.operation.duration"),
            process: histogram("messaging.process.duration"),
            sent: meter
                .u64_counter("messaging.client.sent.messages")
                .with_unit("{message}")
                .build(),
            consumed: meter
                .u64_counter("messaging.client.consumed.messages")
                .with_unit("{message}")
                .build(),
            active: meter
                .i64_up_down_counter("postgremq.client.handlers.active")
                .with_unit("{handler}")
                .build(),
            renewal_lost: meter
                .u64_counter("postgremq.client.renewal.lost")
                .with_unit("{message}")
                .build(),
        }
    }
}

/// `messaging.system`, `messaging.operation.name`, `messaging.operation.type`
/// and (when given) `messaging.destination.name`.
fn common(name: &'static str, kind: &'static str, destination: Option<&str>) -> Vec<KeyValue> {
    let mut attributes = Vec::with_capacity(7);
    attributes.push(KeyValue::new("messaging.system", "postgremq"));
    attributes.push(KeyValue::new("messaging.operation.name", name));
    attributes.push(KeyValue::new("messaging.operation.type", kind));
    if let Some(destination) = destination {
        attributes.push(KeyValue::new(
            "messaging.destination.name",
            destination.to_owned(),
        ));
    }
    attributes
}

impl Metrics {
    /// Instruments from the application's meter, when one was supplied.
    pub(crate) fn new(meter: Option<&Meter>) -> Self {
        Self {
            instruments: meter.map(|meter| Arc::new(Instruments::new(meter))),
        }
    }

    /// Starts timing one logical operation. `transaction` is true for a
    /// caller-owned transaction (it says nothing about commit).
    pub(crate) fn operation(
        &self,
        operation: Operation,
        destination: Option<&str>,
        transaction: bool,
    ) -> OperationTimer {
        OperationTimer {
            state: self.instruments.as_ref().map(|instruments| {
                let mut attributes = common(name(operation), kind(operation), destination);
                attributes.push(KeyValue::new("postgremq.transaction", transaction));
                (Arc::clone(instruments), Instant::now(), attributes)
            }),
        }
    }

    /// Starts one publish SQL attempt: it counts once it ends, failed or
    /// not — also if it is dropped mid-query (`cancelled`), since the
    /// statement may still have been applied.
    pub(crate) fn send_attempt(&self, topic: &str, transaction: bool) -> SendAttempt {
        SendAttempt {
            state: self.instruments.as_ref().map(|instruments| {
                let mut attributes = common("publish", "send", Some(topic));
                attributes.push(KeyValue::new("postgremq.transaction", transaction));
                (Arc::clone(instruments), attributes)
            }),
        }
    }

    /// Counts deliveries decoded from one consume result.
    pub(crate) fn consumed(&self, queue: &str, first: u64, redelivered: u64) {
        if let Some(instruments) = &self.instruments {
            for (count, again) in [(first, false), (redelivered, true)] {
                if count > 0 {
                    let mut attributes = common("consume", "receive", Some(queue));
                    attributes.push(KeyValue::new("postgremq.redelivered", again));
                    instruments.consumed.add(count, &attributes);
                }
            }
        }
    }

    /// Starts one handler callback: counted active until it finishes.
    pub(crate) fn handler(&self, queue: &str) -> HandlerTimer {
        HandlerTimer {
            state: self.instruments.as_ref().map(|instruments| {
                let attributes = common("process", "process", Some(queue));
                instruments.active.add(1, &attributes);
                (Arc::clone(instruments), Instant::now(), attributes)
            }),
        }
    }

    /// The automatic renewal retired a tracked delivery that lost its lease.
    pub(crate) fn renewal_lost(&self, queue: &str) {
        if let Some(instruments) = &self.instruments {
            instruments.renewal_lost.add(
                1,
                &[
                    KeyValue::new("messaging.system", "postgremq"),
                    KeyValue::new("messaging.destination.name", queue.to_owned()),
                ],
            );
        }
    }
}

/// Times one logical operation. Dropped without [`finish`](Self::finish)
/// (the caller dropped the operation's future), it records `cancelled`.
#[must_use = "finish the timer with the operation's outcome"]
pub(crate) struct OperationTimer {
    state: Option<(Arc<Instruments>, Instant, Vec<KeyValue>)>,
}

impl std::fmt::Debug for OperationTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationTimer").finish_non_exhaustive()
    }
}

impl OperationTimer {
    /// Records the operation with its outcome's `error.type`, if any.
    pub(crate) fn finish(mut self, error: Option<&'static str>) {
        self.record(error);
    }

    /// Records the outcome of `result`.
    pub(crate) fn finish_with<T>(self, result: &Result<T, Error>) {
        self.finish(result.as_ref().err().map(error_type));
    }

    fn record(&mut self, error: Option<&'static str>) {
        if let Some((instruments, started, mut attributes)) = self.state.take() {
            if let Some(error) = error {
                attributes.push(KeyValue::new("error.type", error));
            }
            instruments
                .operation
                .record(started.elapsed().as_secs_f64(), &attributes);
        }
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        self.record(Some("cancelled"));
    }
}

/// One publish SQL attempt; see [`Metrics::send_attempt`].
#[must_use = "finish the attempt with its outcome"]
pub(crate) struct SendAttempt {
    state: Option<(Arc<Instruments>, Vec<KeyValue>)>,
}

impl std::fmt::Debug for SendAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendAttempt").finish_non_exhaustive()
    }
}

impl SendAttempt {
    /// Counts the attempt with its `error.type`, if it failed.
    pub(crate) fn finish(mut self, error: Option<&'static str>) {
        self.record(error);
    }

    fn record(&mut self, error: Option<&'static str>) {
        if let Some((instruments, mut attributes)) = self.state.take() {
            if let Some(error) = error {
                attributes.push(KeyValue::new("error.type", error));
            }
            instruments.sent.add(1, &attributes);
        }
    }
}

impl Drop for SendAttempt {
    fn drop(&mut self) {
        self.record(Some("cancelled"));
    }
}

/// Times one handler callback; see [`Metrics::handler`].
#[must_use = "finish the timer when the callback returns"]
pub(crate) struct HandlerTimer {
    state: Option<(Arc<Instruments>, Instant, Vec<KeyValue>)>,
}

impl std::fmt::Debug for HandlerTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerTimer").finish_non_exhaustive()
    }
}

impl HandlerTimer {
    /// The callback returned: `handler_error` for an error or panic,
    /// `cancelled` when it returned after its stop token was cancelled.
    pub(crate) fn finish(mut self, error: Option<&'static str>) {
        self.record(error);
    }

    fn record(&mut self, error: Option<&'static str>) {
        if let Some((instruments, started, mut attributes)) = self.state.take() {
            instruments.active.add(-1, &attributes);
            if let Some(error) = error {
                attributes.push(KeyValue::new("error.type", error));
            }
            instruments
                .process
                .record(started.elapsed().as_secs_f64(), &attributes);
        }
    }
}

impl Drop for HandlerTimer {
    fn drop(&mut self) {
        // Unfinished: the callback panicked (unwinding through here) or its
        // task was aborted. Either way the active count stays balanced.
        self.record(Some(if std::thread::panicking() {
            "handler_error"
        } else {
            "cancelled"
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_map_to_the_bounded_categories() {
        assert_eq!(error_type(&Error::already_settled()), "lease_lost");
        assert_eq!(error_type(&Error::invalid("x")), "validation");
        assert_eq!(error_type(&Error::Closed), "connection_closed");
        assert_eq!(
            error_type(&Error::QueueGone {
                queue: Arc::from("q")
            }),
            "queue_not_found"
        );
        assert_eq!(
            error_type(&Error::Sqlx(sqlx::Error::Io(
                std::io::ErrorKind::TimedOut.into()
            ))),
            "deadline_exceeded"
        );
        assert_eq!(error_type(&Error::Sqlx(sqlx::Error::RowNotFound)), "other");
    }
}
