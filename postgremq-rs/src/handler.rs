//! Handler-based consumption.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::connection::Connection;
use crate::consumer::{Consumer, ConsumerShared};
use crate::delivery::Delivery;
use crate::error::{Error, Result};
use crate::options::ConsumeOptions;

/// The error type a handler may return.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// A running handler-based consumer from
/// [`Connection::consume_with_handler`].
#[must_use = "dropping a HandlerConsumer stops it"]
pub struct HandlerConsumer {
    shared: Arc<ConsumerShared>,
    done: CancellationToken,
    reason: Arc<std::sync::OnceLock<Error>>,
}

impl HandlerConsumer {
    /// The queue this consumer reads.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.shared.queue
    }

    /// Stops fetching, cancels running handlers' [`stopped`](Delivery::stopped)
    /// tokens and waits for every handler to finish and its delivery to
    /// settle.
    pub async fn stop(&self) {
        self.shared.shutdown.cancel();
        self.done.cancelled().await;
    }

    /// Waits until the consumer has stopped (by [`stop`](Self::stop), the
    /// connection closing, or its queue being gone) and returns
    /// [`Error::QueueGone`] in the last case.
    pub async fn closed(&self) -> Option<&Error> {
        self.done.cancelled().await;
        self.reason.get()
    }
}

impl Drop for HandlerConsumer {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
    }
}

impl fmt::Debug for HandlerConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerConsumer")
            .field("queue", &self.shared.queue)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// Consumes `queue` by calling `handler` for each delivery, with at most
    /// `max_in_flight` handlers running at once.
    ///
    /// `None` is unbounded, as the Go client's `WithMaxInFlight(0)`: every
    /// claimed delivery gets a handler task at once, so in-flight work (and
    /// the leases being renewed) grows with the backlog. Prefer `Some(n)` in
    /// production.
    ///
    /// Settlement rules (as in the Go client): a handler may settle the
    /// delivery itself. If it returns without settling, the delivery is
    /// acked when the handler returned `Ok` and its
    /// [`stopped`](Delivery::stopped) token is still live; it is nacked when
    /// the handler returned `Err`, panicked, or returned after cancellation.
    /// (Panics are caught only with `panic = "unwind"`; with `panic = "abort"`
    /// the process ends and the lease expires.)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
    /// use std::num::NonZeroUsize;
    /// use postgremq::{ConsumeOptions, Delivery};
    ///
    /// let consumer = conn
    ///     .consume_with_handler(
    ///         "billing",
    ///         ConsumeOptions::default(),
    ///         NonZeroUsize::new(8),
    ///         |delivery: Delivery| async move {
    ///             let order: serde_json::Value = delivery.payload_as()?;
    ///             // ... `?` on failure nacks; returning Ok acks ...
    ///             let _ = order;
    ///             Ok(())
    ///         },
    ///     )
    ///     .await?;
    /// // ... later:
    /// consumer.stop().await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// As for [`consume`](Self::consume).
    pub async fn consume_with_handler<F, Fut>(
        &self,
        queue: &str,
        options: ConsumeOptions,
        max_in_flight: Option<NonZeroUsize>,
        handler: F,
    ) -> Result<HandlerConsumer>
    where
        F: Fn(Delivery) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        let done = CancellationToken::new();
        let shared = self.start_consumer(queue, options, Some(&done)).await?;
        let consumer = Consumer::new(Arc::clone(&shared));
        let reason = Arc::new(std::sync::OnceLock::new());
        tokio::spawn(
            dispatch(
                consumer,
                Arc::clone(&shared),
                Arc::new(handler),
                max_in_flight
                    .map(|limit| Arc::new(Semaphore::new(limit.get().min(Semaphore::MAX_PERMITS)))),
                done.clone(),
                Arc::clone(&reason),
            )
            .instrument(tracing::info_span!("postgremq.handler", queue = %queue)),
        );
        Ok(HandlerConsumer {
            shared,
            done,
            reason,
        })
    }
}

async fn dispatch<F, Fut>(
    mut consumer: Consumer,
    shared: Arc<ConsumerShared>,
    handler: Arc<F>,
    slots: Option<Arc<Semaphore>>,
    done: CancellationToken,
    reason: Arc<std::sync::OnceLock<Error>>,
) where
    F: Fn(Delivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    // Signals completion on return and if this task panics, so `stop` and
    // `closed` never wait on a dead task.
    let _done = done.drop_guard();
    let mut running = JoinSet::new();
    // Each running handler's `stopped` token: a handler that already settled
    // is no longer tracked by the consumer, but must still learn about the
    // shutdown.
    let mut tokens: HashMap<tokio::task::Id, CancellationToken> = HashMap::new();
    loop {
        // Reap finished supervisors so the set does not grow.
        while let Some(result) = running.try_join_next_with_id() {
            reap(&mut tokens, result);
        }
        let slot = match &slots {
            Some(slots) => tokio::select! {
                biased;
                () = shared.shutdown.cancelled() => break,
                slot = Arc::clone(slots).acquire_owned() => match slot {
                    Ok(slot) => Some(slot),
                    Err(_closed) => break,
                },
            },
            None => None,
        };
        let delivery = match consumer.next().await {
            Some(Ok(delivery)) => delivery,
            Some(Err(err)) => {
                // Only QueueGone ends a stream with an error.
                let _first = reason.set(err);
                break;
            }
            None => break,
        };
        let span = tracing::info_span!(
            "postgremq.handle",
            message_id = %delivery.message_id(),
            attempt = delivery.delivery_attempts()
        );
        let handler = Arc::clone(&handler);
        let stopped = delivery.stopped();
        let task = running.spawn(
            async move {
                run_handler(handler, delivery).await;
                drop(slot);
            }
            .instrument(span),
        );
        tokens.insert(task.id(), stopped);
    }
    // Stopped while waiting for a slot, the stream's final QueueGone was
    // never read.
    if shared.fatal.load(Ordering::Acquire) {
        let _first = reason.set(Error::QueueGone {
            queue: Arc::clone(&shared.queue),
        });
    }
    // Stopping: every handler still running learns about it.
    for token in tokens.values() {
        token.cancel();
    }
    while let Some(result) = running.join_next_with_id().await {
        reap(&mut tokens, result);
    }
    consumer.stop().await;
}

fn reap(
    tokens: &mut HashMap<tokio::task::Id, CancellationToken>,
    result: Result<(tokio::task::Id, ()), tokio::task::JoinError>,
) {
    match result {
        Ok((id, ())) => {
            tokens.remove(&id);
        }
        Err(err) => {
            tokens.remove(&err.id());
            tracing::error!(
                error = &err as &dyn std::error::Error,
                "handler supervisor failed"
            );
        }
    }
}

async fn run_handler<F, Fut>(handler: Arc<F>, delivery: Delivery)
where
    F: Fn(Delivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    let tracker = delivery.share();
    let message_id = delivery.message_id();
    let metrics = delivery.inner.conn.metrics.clone();
    let queue = Arc::clone(&delivery.inner.key.queue);
    let stopped = delivery.stopped();
    // The handler is called *inside* a nested task, so a panic — while
    // building its future or while running it — becomes a JoinError here.
    let task = tokio::spawn(
        async move {
            // Processing is the callback alone, measured inside its task from
            // invocation to return (a panic is recorded as the timer
            // unwinds); automatic settlement is its own operation.
            let processing = metrics.handler(&queue);
            let result = handler(delivery).await;
            processing.finish(match &result {
                Err(_) => Some("handler_error"),
                Ok(()) if stopped.is_cancelled() => Some("cancelled"),
                Ok(()) => None,
            });
            result
        }
        .in_current_span(),
    );
    let failed = match task.await {
        Ok(Ok(())) => false,
        Ok(Err(err)) => {
            tracing::warn!(%message_id, error = &*err as &dyn std::error::Error, "handler failed");
            true
        }
        Err(err) => {
            tracing::error!(%message_id, error = &err as &dyn std::error::Error, "handler panicked");
            true
        }
    };
    if tracker.is_settled() {
        return;
    }
    // Cancellation does not establish success: an unsettled handler that
    // returned after its token was cancelled is retried.
    let settled = if failed || tracker.stopped().is_cancelled() {
        tracker.nack(None).await
    } else {
        tracker.ack().await
    };
    if let Err(err) = settled {
        tracing::warn!(%message_id, error = &err as &dyn std::error::Error, "auto-settlement failed");
    }
}
