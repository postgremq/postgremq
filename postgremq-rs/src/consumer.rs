//! Pull consumption: [`Consumer`] yields [`Delivery`]s from one queue.
//!
//! One task per consumer owns fetching, in-flight tracking and the drain. The
//! claimed-but-unread deliveries sit in a small buffer shared with the
//! [`Consumer`] handle (not a channel), so on shutdown the task can release
//! exactly those deliveries that were never handed out.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use futures_core::stream::FusedStream;
use sqlx::Connection as _;
use sqlx::PgConnection;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::checkout::Checkout;
use crate::connection::{FatalSource, Inner};
use crate::delivery::{Delivery, DeliveryInner};
use crate::error::{Error, ErrorKind, Result};
use crate::listener::Subscription;
use crate::metrics::Operation;
use crate::options::ConsumeSettings;
use crate::renewal::{Confirmation, DeliveryKey, LeaseSink, Registration, SharedLease};
use crate::sync::lock;
use crate::types::{Generation, MessageId, from_unix_micros, millis_until};

/// Retry delay after a failed fetch.
const FETCH_ERROR_RETRY: Duration = Duration::from_secs(1);
/// Floor for refetching when the next-visible hint is already due (a row that
/// is visible but locked by another transaction): avoids a tight loop.
const DUE_HINT_BACKOFF: Duration = Duration::from_millis(100);
/// Bound for the read-only next-visible lookup.
const NEXT_VISIBLE_TIMEOUT: Duration = Duration::from_secs(2);
/// Client-side margin beyond the server-side claim timeout.
const CLAIM_SAFETY_MARGIN: Duration = Duration::from_secs(10);
/// Bound for releasing the never-delivered messages of one batch.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(1);

/// State shared by a [`Consumer`] handle, its task and its deliveries.
pub(crate) struct ConsumerShared {
    pub(crate) queue: Arc<str>,
    pub(crate) generation: Generation,
    buffer: Mutex<Buffer>,
    /// A fetch is allowed while fewer than this many deliveries are buffered
    /// (the batch size): the next batch is claimed while the current one is
    /// being handed out, as the Go client does, and at most about two
    /// batches are ever buffered.
    prefetch_below: usize,
    /// Task → reader: an item was buffered, or the buffer closed.
    readable: Arc<Notify>,
    /// Reader → task: the buffer dropped below `prefetch_below`.
    taken: Notify,
    /// Deliveries handed out or buffered and not yet settled.
    inflight: Mutex<HashMap<DeliveryKey, CancellationToken>>,
    /// Delivery → task: an in-flight delivery settled.
    settled: Notify,
    /// Stop request.
    pub(crate) shutdown: CancellationToken,
    /// Cancelled when the task has exited.
    pub(crate) finished: CancellationToken,
    /// The queue is gone; the stream ends with [`Error::QueueGone`].
    pub(crate) fatal: AtomicBool,
}

#[derive(Default)]
struct Buffer {
    items: VecDeque<Delivery>,
    closed: bool,
}

enum Popped {
    Item(Delivery),
    Empty,
    Closed,
}

impl ConsumerShared {
    pub(crate) fn new(
        queue: Arc<str>,
        generation: Generation,
        prefetch_below: NonZeroUsize,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            queue,
            generation,
            buffer: Mutex::new(Buffer::default()),
            prefetch_below: prefetch_below.get(),
            readable: Arc::new(Notify::new()),
            taken: Notify::new(),
            inflight: Mutex::new(HashMap::new()),
            settled: Notify::new(),
            shutdown,
            finished: CancellationToken::new(),
            fatal: AtomicBool::new(false),
        }
    }

    /// Stops tracking a delivery (it settled). Idempotent.
    pub(crate) fn untrack(&self, key: &DeliveryKey) {
        if lock(&self.inflight).remove(key).is_some() {
            self.settled.notify_one();
        }
    }

    /// Marks the consumer's queue gone and stops the consumer. Deliveries
    /// still buffered are discarded rather than handed out: their rows are
    /// gone, so they could only produce work that cannot be acknowledged.
    /// Dropping them ends their tracking; no release is attempted. `fatal` is
    /// set first, so a reader that finds the buffer closed reports
    /// [`Error::QueueGone`] exactly once.
    pub(crate) fn fail(&self) {
        self.fatal.store(true, Ordering::Release);
        drop(self.close_buffer());
        self.shutdown.cancel();
    }

    fn pop(&self) -> Popped {
        let mut buffer = lock(&self.buffer);
        match buffer.items.pop_front() {
            Some(delivery) => {
                if buffer.items.len() < self.prefetch_below {
                    self.taken.notify_one();
                }
                Popped::Item(delivery)
            }
            None if buffer.closed => Popped::Closed,
            None => Popped::Empty,
        }
    }

    fn wants_prefetch(&self) -> bool {
        lock(&self.buffer).items.len() < self.prefetch_below
    }

    /// Buffers a delivery for the reader; hands it back if the buffer was
    /// closed meanwhile (e.g. by [`fail`](Self::fail) from another task).
    /// The check and the append share one lock.
    fn push(&self, delivery: Delivery) -> Option<Delivery> {
        {
            let mut buffer = lock(&self.buffer);
            if buffer.closed {
                return Some(delivery);
            }
            buffer.items.push_back(delivery);
        }
        self.readable.notify_one();
        None
    }

    /// Closes the buffer and returns what was never handed out.
    fn close_buffer(&self) -> Vec<Delivery> {
        let drained: Vec<Delivery> = {
            let mut buffer = lock(&self.buffer);
            buffer.closed = true;
            buffer.items.drain(..).collect()
        };
        self.readable.notify_one();
        drained
    }

    fn inflight_is_empty(&self) -> bool {
        lock(&self.inflight).is_empty()
    }
}

/// Receives deliveries from one queue.
///
/// Use it as a [`Stream`] or call [`next`](Self::next). The stream ends when
/// the consumer stops; if it stopped because the queue is gone, the last item
/// is [`Error::QueueGone`] (deliveries still buffered at that point are
/// discarded, not handed out). Dropping the consumer stops it (buffered,
/// never-delivered messages are released), but only
/// [`stop`](Self::stop) waits for the drain.
#[must_use = "dropping a Consumer stops it"]
pub struct Consumer {
    shared: Arc<ConsumerShared>,
    wait: Option<Pin<Box<OwnedNotified>>>,
    reported: bool,
    /// The stream has returned `None`.
    terminated: bool,
}

impl Consumer {
    pub(crate) fn new(shared: Arc<ConsumerShared>) -> Self {
        Self {
            shared,
            wait: None,
            reported: false,
            terminated: false,
        }
    }

    /// The next delivery; `None` once the consumer has stopped.
    ///
    /// Cancel-safe: dropping the future (e.g. in `tokio::select!`) loses no
    /// delivery.
    pub async fn next(&mut self) -> Option<Result<Delivery>> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    /// The queue this consumer reads.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.shared.queue
    }

    /// The queue generation this consumer is bound to.
    #[must_use]
    pub fn generation(&self) -> Generation {
        self.shared.generation
    }

    /// Stops fetching, releases buffered never-delivered messages, cancels
    /// the [`stopped`](Delivery::stopped) token of every delivery still in
    /// flight, and waits until those are settled (or the connection's
    /// shutdown deadline abandons them). Lease renewal continues meanwhile.
    ///
    /// Do not await this while holding unsettled deliveries in the same task.
    pub async fn stop(&self) {
        self.shared.shutdown.cancel();
        self.shared.finished.cancelled().await;
    }

    fn end(&mut self) -> Option<Result<Delivery>> {
        if self.shared.fatal.load(Ordering::Acquire) && !self.reported {
            self.reported = true;
            return Some(Err(Error::QueueGone {
                queue: Arc::clone(&self.shared.queue),
            }));
        }
        None
    }
}

impl Stream for Consumer {
    type Item = Result<Delivery>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.shared.pop() {
                Popped::Item(delivery) => {
                    self.wait = None;
                    return Poll::Ready(Some(Ok(delivery)));
                }
                Popped::Closed => {
                    let end = self.end();
                    self.terminated = end.is_none();
                    return Poll::Ready(end);
                }
                Popped::Empty => {}
            }
            let readable = Arc::clone(&self.shared.readable);
            let wait = self
                .wait
                .get_or_insert_with(|| Box::pin(readable.notified_owned()));
            if wait.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.wait = None;
        }
    }
}

impl FusedStream for Consumer {
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
    }
}

impl fmt::Debug for Consumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consumer")
            .field("queue", &self.shared.queue)
            .field("generation", &self.shared.generation)
            .finish_non_exhaustive()
    }
}

/// One claimed row.
struct Claimed {
    /// When the claim was sent; leases are measured from here.
    sent: Instant,
    id: i64,
    payload: serde_json::Value,
    token: String,
    attempts: u32,
    vt_micros: i64,
    published_micros: i64,
    group: Option<(String, i64)>,
    lease: Duration,
}

/// When to fetch next.
enum NextFetch {
    Now,
    In(Duration),
    Idle,
}

enum FetchOutcome {
    Claimed(Vec<Claimed>, NextFetch),
    QueueGone(Error),
    Failed,
}

/// The consumer task's exit sequence (also run when the task panics).
struct TaskExit {
    conn: Arc<Inner>,
    shared: Arc<ConsumerShared>,
}

impl Drop for TaskExit {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!(queue = %self.shared.queue, "consumer task panicked");
        }
        // Wakes a reader parked on an empty buffer and ends its stream;
        // anything still buffered (only after a panic) expires.
        drop(self.shared.close_buffer());
        // After a panic, deliveries may still be in flight: stop renewing
        // them and cancel their handlers (their leases expire). Empty on a
        // normal exit.
        let orphaned: Vec<(DeliveryKey, CancellationToken)> =
            lock(&self.shared.inflight).drain().collect();
        for (key, token) in orphaned {
            token.cancel();
            self.conn.stop_renewal(&key);
        }
        self.conn.unregister_consumer(&self.shared);
        self.shared.finished.cancel();
    }
}

/// The consumer task.
pub(crate) struct Task {
    pub(crate) conn: Arc<Inner>,
    pub(crate) shared: Arc<ConsumerShared>,
    pub(crate) settings: ConsumeSettings,
    pub(crate) topic_wake: Subscription,
    pub(crate) queue_wake: Subscription,
}

impl Task {
    pub(crate) async fn run(self) {
        let shared = Arc::clone(&self.shared);
        // Ends the stream, unregisters and signals completion on return *and*
        // if this task panics: no reader, `stop` or `close` waits on a dead
        // task.
        let _exit = TaskExit {
            conn: Arc::clone(&self.conn),
            shared: Arc::clone(&shared),
        };
        let mut fetching: Option<JoinHandle<FetchOutcome>> = None;
        let mut fetch_after = Instant::now();
        let mut shutting = false;
        let mut abandoned = false;
        let mut cancelled_inflight = false;
        loop {
            if shutting {
                // Running handlers learn about the shutdown first; then the
                // never-delivered rows are released (concurrently, bounded).
                if !cancelled_inflight {
                    for token in lock(&shared.inflight).values() {
                        token.cancel();
                    }
                    cancelled_inflight = true;
                }
                release_undelivered(shared.close_buffer(), &self.conn.io).await;
                if fetching.is_none() && shared.inflight_is_empty() {
                    break;
                }
            }
            let idle = !shutting && fetching.is_none();
            let due = (idle && shared.wants_prefetch())
                .then(|| fetch_after.min(Instant::now() + self.settings.check_timeout));
            tokio::select! {
                biased;
                () = self.conn.io.cancelled(), if !abandoned => {
                    // The shutdown deadline passed: abandon in-flight work.
                    // Its leases expire normally; attempts are not reduced. A
                    // claim still in flight is abandoned too (its rows, if it
                    // committed, expire).
                    if let Some(fetch) = fetching.take() {
                        fetch.abort();
                    }
                    let abandoned_now: Vec<(DeliveryKey, CancellationToken)> =
                        lock(&shared.inflight).drain().collect();
                    for (key, token) in abandoned_now {
                        token.cancel();
                        self.conn.stop_renewal(&key);
                    }
                    abandoned = true;
                    shutting = true;
                }
                () = shared.shutdown.cancelled(), if !shutting => shutting = true,
                outcome = join(&mut fetching) => {
                    fetching = None;
                    match outcome {
                        Ok(outcome) => {
                            fetch_after = self.on_fetched(outcome, shutting).await;
                            if shared.shutdown.is_cancelled() {
                                shutting = true;
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = &err as &dyn std::error::Error, "fetch task failed");
                            fetch_after = Instant::now() + FETCH_ERROR_RETRY;
                        }
                    }
                }
                () = shared.settled.notified() => {}
                () = shared.taken.notified(), if !shutting => {}
                () = self.topic_wake.notified(), if idle => fetch_after = Instant::now(),
                () = self.queue_wake.notified(), if idle => fetch_after = Instant::now(),
                () = sleep_until(due) => {
                    fetching = Some(tokio::spawn(
                        fetch(
                            Arc::clone(&self.conn),
                            Arc::clone(&shared.queue),
                            shared.generation,
                            self.settings,
                        )
                        .in_current_span(),
                    ));
                }
            }
        }
    }

    /// Books a fetch's deliveries and returns when to fetch next.
    async fn on_fetched(&self, outcome: FetchOutcome, shutting: bool) -> Instant {
        let now = Instant::now();
        match outcome {
            FetchOutcome::Claimed(rows, next) => {
                // Claimed after shutdown began: never delivered, so never
                // renewed either.
                let late = shutting || self.shared.shutdown.is_cancelled();
                let mut registrations = Vec::new();
                let deliveries: Vec<Delivery> = rows
                    .into_iter()
                    .map(|row| {
                        let (delivery, registration) = self.book(row, !late);
                        registrations.extend(registration);
                        delivery
                    })
                    .collect();
                // The whole fetch under one lock of the renewal schedule.
                self.conn.register_renewals(registrations);
                if late {
                    // Claimed after shutdown began: never delivered.
                    release_undelivered(deliveries, &self.conn.io).await;
                } else {
                    let rejected: Vec<Delivery> = deliveries
                        .into_iter()
                        .filter_map(|delivery| self.shared.push(delivery))
                        .collect();
                    // The buffer closed concurrently (the queue is gone):
                    // never hand these out.
                    release_undelivered(rejected, &self.conn.io).await;
                }
                match next {
                    NextFetch::Now => now,
                    // The loop polls at `check_timeout` anyway; capping also
                    // keeps a far-future hint from overflowing `Instant`.
                    NextFetch::In(delay) => now + delay.min(self.settings.check_timeout),
                    NextFetch::Idle => now + self.settings.check_timeout,
                }
            }
            FetchOutcome::QueueGone(err) => {
                tracing::debug!(
                    queue = %self.shared.queue,
                    error = &err as &dyn std::error::Error,
                    "queue is gone; stopping consumer"
                );
                self.shared.fail();
                self.conn.queue_fatal(
                    &self.shared.queue,
                    self.shared.generation,
                    FatalSource::Consumer,
                );
                now
            }
            FetchOutcome::Failed => now + FETCH_ERROR_RETRY,
        }
    }

    /// Turns a claimed row into a tracked delivery, and — when `renew` and
    /// `auto_extend` — the registration that renews it.
    fn book(&self, row: Claimed, renew: bool) -> (Delivery, Option<Registration>) {
        let key = DeliveryKey {
            queue: Arc::clone(&self.shared.queue),
            id: MessageId::new(row.id),
            token: Arc::from(row.token),
        };
        let stopped = CancellationToken::new();
        let lease = Arc::new(SharedLease::new(row.vt_micros));
        lock(&self.shared.inflight).insert(key.clone(), stopped.clone());
        let registration = (renew && self.settings.auto_extend).then(|| Registration {
            key: key.clone(),
            vt_secs: self.settings.vt_secs,
            threshold: self.settings.threshold,
            lease: Confirmation {
                sent: row.sent,
                remaining: row.lease,
            },
            sink: LeaseSink {
                view: Arc::clone(&lease),
                stopped: stopped.clone(),
            },
        });
        let delivery = Delivery {
            inner: Arc::new(DeliveryInner {
                conn: Arc::clone(&self.conn),
                key,
                payload: row.payload,
                group: row.group,
                attempts: row.attempts,
                published_at: from_unix_micros(row.published_micros),
                lease,
                settled: AtomicBool::new(false),
                stopped,
                consumer: Arc::clone(&self.shared),
            }),
        };
        (delivery, registration)
    }
}

/// Releases deliveries that were claimed but never handed to the
/// application: concurrently, within [`RELEASE_TIMEOUT`] overall, and not past
/// the shutdown deadline. Whatever is not released expires.
async fn release_undelivered(deliveries: Vec<Delivery>, deadline: &CancellationToken) {
    if deliveries.is_empty() {
        return;
    }
    let mut releases = tokio::task::JoinSet::new();
    for delivery in deliveries {
        releases.spawn(
            async move {
                if let Err(err) = delivery.release().await {
                    tracing::warn!(
                        message_id = %delivery.message_id(),
                        error = &err as &dyn std::error::Error,
                        "releasing an undelivered message failed; its lease will expire"
                    );
                }
            }
            .in_current_span(),
        );
    }
    let all = async { while releases.join_next().await.is_some() {} };
    let finished = tokio::select! {
        () = deadline.cancelled() => false,
        result = tokio::time::timeout(RELEASE_TIMEOUT, all) => result.is_ok(),
    };
    if !finished {
        tracing::warn!("releasing undelivered messages timed out; their leases will expire");
        releases.abort_all();
    }
}

/// Claims a batch, then decides when to fetch next.
///
/// A claim is not idempotent, so it is never retried and never cancelled
/// from the client: a dropped claim could still commit, leasing rows to
/// nobody. Instead the server bounds it with `statement_timeout` inside its
/// own transaction, so a slow claim ends in a definite rollback.
async fn fetch(
    conn: Arc<Inner>,
    queue: Arc<str>,
    generation: Generation,
    settings: ConsumeSettings,
) -> FetchOutcome {
    // Only a hung connection reaches this net (the server bounds the claim
    // first); a claim abandoned here may have committed, so its rows expire.
    let net = claim_timeout(settings) + CLAIM_SAFETY_MARGIN;
    // One `consume` operation per claim, empty claims included.
    let timer = conn
        .metrics
        .operation(Operation::Consume, Some(&queue), false);
    let claimed = tokio::time::timeout(net, claim(&conn, &queue, generation, settings)).await;
    let Ok(claimed) = claimed else {
        timer.finish(Some("deadline_exceeded"));
        tracing::warn!(queue = %queue, "claim did not return; its outcome is unknown");
        return FetchOutcome::Failed;
    };
    // A server-side cancellation (SQLSTATE 57014, from the claim's own
    // `statement_timeout` or an operator) cannot be told apart: like any
    // database error it counts as `other`, as in Go. Only the client's own
    // net above is known to be a deadline.
    timer.finish_with(&claimed);
    if let Ok(deliveries) = &claimed {
        let redelivered = deliveries.iter().filter(|row| row.attempts > 1).count();
        let first = deliveries.len() - redelivered;
        conn.metrics.consumed(
            &queue,
            u64::try_from(first).unwrap_or(u64::MAX),
            u64::try_from(redelivered).unwrap_or(u64::MAX),
        );
    }
    let rows = match claimed {
        Ok(rows) => rows,
        Err(err) if err.kind() == ErrorKind::QueueNotFound => {
            return FetchOutcome::QueueGone(err);
        }
        Err(err) => {
            tracing::warn!(queue = %queue, error = &err as &dyn std::error::Error, "fetch failed");
            return FetchOutcome::Failed;
        }
    };
    let next = match i32::try_from(rows.len()) {
        Ok(n) if n >= settings.batch => NextFetch::Now,
        Ok(0) => next_visible(&conn, &queue).await,
        _ => NextFetch::Idle,
    };
    FetchOutcome::Claimed(rows, next)
}

/// Server-side bound for one claim: half the lease, between 1 and 30 seconds.
fn claim_timeout(settings: ConsumeSettings) -> Duration {
    Duration::from_millis(u64::from(settings.vt_secs.unsigned_abs()) * 500)
        .clamp(Duration::from_secs(1), Duration::from_secs(30))
}

async fn claim(
    conn: &Inner,
    queue: &str,
    generation: Generation,
    settings: ConsumeSettings,
) -> Result<Vec<Claimed>> {
    // Checked out explicitly: if the safety net abandons the claim, the
    // connection is closed rather than pinged back into the pool.
    let mut checkout = Checkout::acquire(&conn.pool).await?;
    let claimed = claim_on(checkout.conn()?, queue, generation, settings).await;
    // Completed, even with an error (e.g. the statement timeout): the
    // connection is healthy and goes back to the pool. sqlx rolls an aborted
    // transaction back on release (the return-to-pool ping flushes the queued
    // `ROLLBACK`); `a_blocked_claim_is_bounded_by_the_server_and_recovers`
    // settles on the same pool afterwards, so an upgrade that changed this
    // would fail it.
    checkout.release();
    let (rows, sent) = claimed?;
    // A row that cannot be decoded (only possible on a schema mismatch) is
    // skipped and its lease left to expire; the rest of the batch is still
    // delivered, since the server already claimed it.
    let decoded = rows.iter().map(|row| -> Result<Claimed, sqlx::Error> {
        let group_key: Option<String> = row.try_get("group_key")?;
        let group_seq: Option<i64> = row.try_get("group_seq")?;
        let attempts: i32 = row.try_get("delivery_attempts")?;
        Ok(Claimed {
            sent,
            id: row.try_get("message_id")?,
            payload: row.try_get("payload")?,
            token: row.try_get("consumer_token")?,
            attempts: u32::try_from(attempts).map_err(|err| sqlx::Error::Decode(Box::new(err)))?,
            vt_micros: row.try_get("vt_us")?,
            published_micros: row.try_get("published_us")?,
            group: group_key.zip(group_seq),
            lease: millis_until(row.try_get("lease_ms")?),
        })
    });
    Ok(decoded
        .filter_map(|claimed| {
            claimed
                .map_err(|err| {
                    tracing::error!(
                        queue = %queue,
                        error = &err as &dyn std::error::Error,
                        "undecodable claimed row skipped; its lease will expire"
                    );
                })
                .ok()
        })
        .collect())
}

/// Runs the claim transaction; returns its rows and when it was sent.
async fn claim_on(
    conn: &mut PgConnection,
    queue: &str,
    generation: Generation,
    settings: ConsumeSettings,
) -> Result<(Vec<PgRow>, Instant), sqlx::Error> {
    // One round trip opens the transaction and bounds it. The value is an
    // integer we computed, so the dynamic statement is safe.
    let mut tx = conn
        .begin_with(sqlx::AssertSqlSafe(format!(
            "BEGIN; SET LOCAL statement_timeout = {}",
            claim_timeout(settings).as_millis()
        )))
        .await?;
    // After the pool wait and setup: the lease is measured from here.
    let sent = Instant::now();
    let rows = sqlx::query(
        "SELECT message_id, payload, consumer_token::text AS consumer_token, delivery_attempts, \
                (extract(epoch FROM vt) * 1000000)::int8 AS vt_us, \
                (extract(epoch FROM published_at) * 1000000)::int8 AS published_us, \
                group_key::text AS group_key, group_seq, \
                (extract(epoch FROM vt - clock_timestamp()) * 1000)::int8 AS lease_ms \
         FROM postgremq.consume_message($1, $2, $3, $4)",
    )
    .bind(queue)
    .bind(settings.vt_secs)
    .bind(settings.batch)
    .bind(generation.get())
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((rows, sent))
}

/// After an empty claim: wait for the next visible row (blocked group
/// successors are excluded server-side), but never spin on an already-due
/// hint — that row is locked by another transaction.
async fn next_visible(conn: &Inner, queue: &str) -> NextFetch {
    // A read-only lookup: safe to abandon if it hangs (the checkout then
    // closes the connection instead of returning it to the pool).
    let lookup = async {
        let mut checkout = Checkout::acquire(&conn.pool).await?;
        let result = sqlx::query_scalar(
            "SELECT (extract(epoch FROM postgremq.get_next_visible_time($1) - clock_timestamp()) * 1000)::int8",
        )
        .bind(queue)
        .fetch_one(checkout.conn()?)
        .await;
        checkout.release();
        result
    };
    let Ok(result) = tokio::time::timeout(NEXT_VISIBLE_TIMEOUT, lookup).await else {
        tracing::warn!(queue = %queue, "next-visible lookup timed out");
        return NextFetch::In(FETCH_ERROR_RETRY);
    };
    let result: Result<Option<i64>, sqlx::Error> = result;
    match result {
        Ok(Some(millis)) if millis <= 0 => NextFetch::In(DUE_HINT_BACKOFF),
        Ok(Some(millis)) => NextFetch::In(millis_until(millis)),
        Ok(None) => NextFetch::Idle,
        Err(err) => {
            tracing::warn!(queue = %queue, error = &err as &dyn std::error::Error, "next-visible lookup failed");
            NextFetch::In(FETCH_ERROR_RETRY)
        }
    }
}

/// Awaits an optional task; pending forever when there is none.
async fn join<T>(task: &mut Option<JoinHandle<T>>) -> Result<T, tokio::task::JoinError> {
    match task {
        Some(task) => task.await,
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
