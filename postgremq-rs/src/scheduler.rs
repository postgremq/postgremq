//! The connection-level background scheduler shared by lease renewal and
//! queue keep-alive.
//!
//! A schedule lives behind a `std::sync::Mutex` so registration and
//! deregistration are synchronous (callable from `Drop`) and never wait on
//! I/O. One task per scheduler drives the schedule: when work is due it claims a
//! batch, runs the batched SQL call on a separate task (so registrations stay
//! responsive during I/O), and folds the outcome back in. At most one flush is
//! in flight; the lock is never held across an `.await`.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::sync::lock;

/// A schedule driven by [`spawn_scheduler`]. All methods run with the lock held
/// and must not block.
pub(crate) trait Schedule: Send + 'static {
    /// A claimed batch, handed to the flush task.
    type Batch: Send + 'static;
    /// What a batch claimed, to hand back if its flush task dies.
    type Claim: Send + 'static;
    /// One flush's outcome.
    type Outcome: Send + 'static;
    /// Something the schedule gave up on; reported to the scheduler's hook after
    /// the lock is released.
    type Lost: Send + 'static;

    /// When the next work is due; `None` when idle.
    fn earliest(&self) -> Option<Instant>;
    /// Claims the work due at `now`; `None` when nothing is due.
    fn collect_due(&mut self, now: Instant) -> Option<Self::Batch>;
    /// The claim a batch holds.
    fn claim_of(batch: &Self::Batch) -> Self::Claim;
    /// Returns a claim whose flush task failed (panicked or was aborted
    /// unexpectedly): the work is retried instead of staying in flight.
    fn abandon(&mut self, claim: Self::Claim);
    /// Folds a flush outcome back in.
    fn apply(&mut self, outcome: Self::Outcome, lost: &mut Vec<Self::Lost>);
}

/// The `outcome` column of the heartbeat functions
/// (`set_vt_batch_multi`, `extend_queue_keep_alive_multi`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Heartbeat {
    /// Renewed; the row carries the new deadline.
    Extended,
    /// The row lock was contended; retry within the confirmed lease.
    Busy,
}

impl Heartbeat {
    /// Parses the column. An unknown value is a protocol error, so the batch
    /// is treated as unknown and retried within the confirmed lease.
    pub(crate) fn parse(outcome: &str) -> Result<Self, sqlx::Error> {
        match outcome {
            "extended" => Ok(Self::Extended),
            "busy" => Ok(Self::Busy),
            other => Err(sqlx::Error::Protocol(format!(
                "unknown heartbeat outcome {other:?}"
            ))),
        }
    }
}

/// A running scheduler: its shared schedule plus the handles to wake and stop it.
#[derive(Debug)]
pub(crate) struct Scheduler<S> {
    pub(crate) state: Arc<Mutex<S>>,
    wake: Arc<Notify>,
    stop: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl<S: Schedule> Scheduler<S> {
    /// Re-evaluates the schedule (call after registering earlier work).
    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Tells the scheduler to stop without waiting for it.
    pub(crate) fn signal_stop(&self) {
        self.stop.cancel();
    }

    /// Stops the scheduler and waits for it; an in-flight flush is abandoned.
    pub(crate) async fn stop(&self) {
        self.stop.cancel();
        let task = lock(&self.task).take();
        if let Some(task) = task
            && let Err(err) = task.await
        {
            tracing::error!(
                error = &err as &dyn std::error::Error,
                "scheduler task failed"
            );
        }
    }
}

impl<S> Drop for Scheduler<S> {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// Spawns a scheduler over `schedule`. `flush` runs each claimed batch;
/// `on_lost` is called for everything an outcome gave up on.
pub(crate) fn spawn_scheduler<S, F, Fut, L>(
    name: &'static str,
    schedule: S,
    flush: F,
    on_lost: L,
) -> Scheduler<S>
where
    S: Schedule,
    F: Fn(S::Batch) -> Fut + Send + 'static,
    Fut: Future<Output = S::Outcome> + Send + 'static,
    L: Fn(S::Lost) + Send + 'static,
{
    let state = Arc::new(Mutex::new(schedule));
    let wake = Arc::new(Notify::new());
    let stop = CancellationToken::new();
    let task = tokio::spawn(
        run(
            Arc::clone(&state),
            Arc::clone(&wake),
            stop.clone(),
            flush,
            on_lost,
        )
        .instrument(tracing::info_span!("postgremq.scheduler", scheduler = name)),
    );
    Scheduler {
        state,
        wake,
        stop,
        task: Mutex::new(Some(task)),
    }
}

async fn run<S, F, Fut, L>(
    state: Arc<Mutex<S>>,
    wake: Arc<Notify>,
    stop: CancellationToken,
    flush: F,
    on_lost: L,
) where
    S: Schedule,
    F: Fn(S::Batch) -> Fut,
    Fut: Future<Output = S::Outcome> + Send + 'static,
    L: Fn(S::Lost),
{
    let mut flushing: Option<(JoinHandle<S::Outcome>, S::Claim)> = None;
    let mut lost = Vec::new();
    loop {
        // While a flush is in flight the timer stays disarmed: its outcome
        // re-arms it, so ticks cannot pile up overlapping calls.
        let due = if flushing.is_none() {
            lock(&state).earliest()
        } else {
            None
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            outcome = join(&mut flushing) => {
                let claim = flushing.take().map(|(_, claim)| claim);
                match outcome {
                    Ok(outcome) => lock(&state).apply(outcome, &mut lost),
                    Err(err) => {
                        tracing::error!(error = &err as &dyn std::error::Error, "flush task failed");
                        if let Some(claim) = claim {
                            lock(&state).abandon(claim);
                        }
                    }
                }
                for item in lost.drain(..) {
                    on_lost(item);
                }
            }
            () = wake.notified() => {}
            () = sleep_until(due) => {
                let batch = lock(&state).collect_due(Instant::now());
                if let Some(batch) = batch {
                    let claim = S::claim_of(&batch);
                    flushing = Some((tokio::spawn(flush(batch).in_current_span()), claim));
                }
            }
        }
    }
    if let Some((task, _claim)) = flushing {
        task.abort();
        // Cancellation is the expected result here; the outcome is moot.
        let _abandoned = task.await;
    }
}

/// Awaits an optional task; pending forever when there is none.
async fn join<T, C>(task: &mut Option<(JoinHandle<T>, C)>) -> Result<T, tokio::task::JoinError> {
    match task {
        Some((task, _)) => task.await,
        None => std::future::pending().await,
    }
}

/// Sleeps until `at`; pending forever when there is no deadline.
async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
