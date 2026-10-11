//! Connection-level lease renewal: one schedule extends every in-flight
//! delivery of every consumer with one `set_vt_batch_multi` call per tick.
//!
//! A delivery is `(queue, message_id, consumer_token)`: the same message ID is
//! distributed to every queue on a topic, and a token fences a superseded
//! lease. The live map owns registration; the heap only orders pending
//! renewals (stale heap items are skipped by epoch). A renewal that comes back
//! `busy`, or fails in transport, is retried until the last *confirmed*
//! deadline; only an omitted row (or running out of confirmed lease) means the
//! lease is lost, which cancels the delivery's token.
//!
//! Manual extensions (`Delivery::extend`) are independent of this schedule,
//! as in the Go and TypeScript clients: renewal keeps going from its
//! own last confirmation (resetting the lease to `vt_secs`).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::Row as _;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::checkout::Checkout;
use crate::metrics::{Metrics, Operation, sqlx_error_type};
use crate::options::RetryConfig;
use crate::retry::{is_retryable, with_retry};
use crate::scheduler::{Heartbeat, Schedule};
use crate::types::{MessageId, millis_until};

/// Retry delay after a `busy` outcome.
const BUSY_RETRY: Duration = Duration::from_millis(100);
/// The least time between a confirmation and the next renewal.
const MIN_RENEWAL: Duration = Duration::from_millis(100);
/// Retry delay after a transport or database error.
const ERROR_RETRY: Duration = Duration::from_secs(1);
/// Upper bound for one renewal round trip.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

/// The identity of one delivery. (`Ord` only breaks heap ties; epochs are
/// unique.)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DeliveryKey {
    pub(crate) queue: Arc<str>,
    pub(crate) id: MessageId,
    pub(crate) token: Arc<str>,
}

/// Where renewal results go: the delivery's lease view and its stop token.
#[derive(Debug, Clone)]
pub(crate) struct LeaseSink {
    /// The delivery's published lease deadline.
    pub(crate) view: Arc<SharedLease>,
    /// Cancelled when the lease is lost.
    pub(crate) stopped: CancellationToken,
}

/// A delivery's lease as shared by the delivery and the renewal schedule: the
/// deadline the application sees (`Delivery::vt`) — the last one confirmed by
/// a renewal or a manual extension, whichever was recorded last — and whether
/// the delivery has finished.
///
/// Finishing a delivery only sets [`retired`](Self::retire), without taking
/// the schedule's lock: the schedule drops a retired entry when it next comes
/// due, or sweeps them all once they pile up — checked on every registration
/// and every look at the schedule (see [`RenewalSchedule::sweep`]).
#[derive(Debug)]
pub(crate) struct SharedLease {
    vt_micros: AtomicI64,
    retired: AtomicBool,
    /// A settlement has started: a renewal that finds the row gone is not a
    /// lost lease.
    settling: AtomicBool,
}

impl SharedLease {
    /// The view of a fresh claim.
    pub(crate) fn new(vt_micros: i64) -> Self {
        Self {
            vt_micros: AtomicI64::new(vt_micros),
            retired: AtomicBool::new(false),
            settling: AtomicBool::new(false),
        }
    }

    /// A settlement of the delivery has started.
    pub(crate) fn begin_settling(&self) {
        self.settling.store(true, Ordering::Release);
    }

    /// Ends renewal of the delivery. Returns `true` the first time.
    pub(crate) fn retire(&self) -> bool {
        !self.retired.swap(true, Ordering::AcqRel)
    }

    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    /// The last confirmed deadline (Unix µs).
    pub(crate) fn vt_micros(&self) -> i64 {
        self.vt_micros.load(Ordering::Acquire)
    }

    /// Records a confirmed deadline.
    pub(crate) fn publish(&self, vt_micros: i64) {
        self.vt_micros.store(vt_micros, Ordering::Release);
    }
}

#[derive(Debug)]
struct Entry {
    vt_secs: i32,
    threshold: f64,
    /// The last confirmed lease deadline.
    expires_at: Instant,
    /// When the confirmation behind `expires_at` was sent (to derive the
    /// renewal time).
    confirmed_at: Instant,
    epoch: u64,
    in_flight: bool,
    sink: LeaseSink,
}

/// The lease-renewal schedule.
#[derive(Debug)]
pub(crate) struct RenewalSchedule {
    live: HashMap<DeliveryKey, Entry>,
    heap: BinaryHeap<Reverse<(Instant, u64, DeliveryKey)>>,
    next_epoch: u64,
    batch_cap: usize,
    /// Entries retired since the last sweep (counted lock-free by the
    /// deliveries; see [`SharedLease`]).
    retired: Arc<AtomicUsize>,
}

/// One delivery to register (see [`RenewalSchedule::register`]).
#[derive(Debug)]
pub(crate) struct Registration {
    pub(crate) key: DeliveryKey,
    pub(crate) vt_secs: i32,
    pub(crate) threshold: f64,
    pub(crate) lease: Confirmation,
    pub(crate) sink: LeaseSink,
}

/// One delivery claimed for renewal.
#[derive(Debug)]
pub(crate) struct Item {
    key: DeliveryKey,
    epoch: u64,
    vt_secs: i32,
    expires_at: Instant,
}

/// One renewal row returned by `set_vt_batch_multi`.
#[derive(Debug)]
struct Renewal {
    busy: bool,
    remaining: Option<Duration>,
    vt_micros: Option<i64>,
}

/// What a flush learned about its batch.
#[derive(Debug)]
enum Renewals {
    /// The call returned; an omitted delivery has lost its lease.
    Known(HashMap<DeliveryKey, Renewal>),
    /// Transport/database failure: nothing is known about the batch.
    Unknown,
}

/// One flush's outcome.
#[derive(Debug)]
pub(crate) struct Outcome {
    items: Vec<Item>,
    /// When the request was sent. The server measures remaining leases at a
    /// later instant, so `sent + remaining` never overstates a deadline.
    sent: Instant,
    renewals: Renewals,
}

/// A server-confirmed lease: the request was sent at `sent` (after pool
/// acquisition) and `remaining` was left when the server applied it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Confirmation {
    pub(crate) sent: Instant,
    pub(crate) remaining: Duration,
}

impl RenewalSchedule {
    pub(crate) fn new(batch_cap: NonZeroUsize) -> Self {
        Self {
            live: HashMap::new(),
            heap: BinaryHeap::new(),
            next_epoch: 0,
            batch_cap: batch_cap.get(),
            retired: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The counter deliveries bump when they retire a registered lease.
    pub(crate) fn retired_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.retired)
    }

    /// Registers deliveries (a fetch's batch, under one lock) with the
    /// leases their claim confirmed.
    pub(crate) fn register(&mut self, registrations: impl IntoIterator<Item = Registration>) {
        // Every fetch sweeps if due: settled entries are bounded by the
        // deliveries still in flight, not by how long until the next renewal.
        self.sweep();
        for registration in registrations {
            self.register_one(registration);
        }
    }

    fn register_one(&mut self, registration: Registration) {
        let Registration {
            key,
            vt_secs,
            threshold,
            lease,
            sink,
        } = registration;
        let Confirmation { sent, remaining } = lease;
        let epoch = self.bump_epoch();
        self.heap.push(Reverse((
            sent + scaled(remaining, threshold),
            epoch,
            key.clone(),
        )));
        self.live.insert(
            key,
            Entry {
                vt_secs,
                threshold,
                expires_at: sent + remaining,
                confirmed_at: sent,
                epoch,
                in_flight: false,
                sink,
            },
        );
    }

    /// The renewal time implied by an entry's latest confirmation.
    fn renewal_at(entry: &Entry) -> Instant {
        let remaining = entry
            .expires_at
            .saturating_duration_since(entry.confirmed_at);
        entry.confirmed_at + scaled(remaining, entry.threshold)
    }

    /// Drops every retired entry once they make up a large share of the
    /// schedule (amortised O(1) per retirement).
    fn sweep(&mut self) {
        let retired = self.retired.load(Ordering::Acquire);
        if retired > self.live.len() / 2 + 64 {
            // Subtracted, not reset: retirements counted meanwhile stay.
            self.retired.fetch_sub(retired, Ordering::AcqRel);
            self.live.retain(|_, entry| !entry.sink.view.is_retired());
            self.compact();
        }
    }

    /// Stops renewing a delivery at once (shutdown and panic paths; a normal
    /// settlement only retires its lease).
    pub(crate) fn deregister(&mut self, key: &DeliveryKey) {
        if self.live.remove(key).is_some() {
            self.compact();
        }
    }

    /// Stale heap slots (deregistered or rescheduled deliveries) are skipped
    /// when popped, but deliveries settled long before their renewal is due
    /// would pile up between pops: drop them once they dominate the heap.
    fn compact(&mut self) {
        if self.heap.len() > self.live.len().saturating_mul(2).saturating_add(64) {
            let live = &self.live;
            self.heap.retain(|Reverse((_, epoch, key))| {
                live.get(key).is_some_and(|entry| entry.epoch == *epoch)
            });
        }
    }

    fn bump_epoch(&mut self) -> u64 {
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.next_epoch
    }

    fn schedule(&mut self, key: DeliveryKey, at: Instant) {
        let epoch = self.bump_epoch();
        if let Some(entry) = self.live.get_mut(&key) {
            entry.epoch = epoch;
            entry.in_flight = false;
            self.heap.push(Reverse((at, epoch, key)));
            self.compact();
        }
    }
}

/// `lease × threshold`, falling back to half the lease if the product is not
/// representable, and never less than [`MIN_RENEWAL`] (or half the lease, if
/// shorter): a tiny threshold must not renew back to back.
fn scaled(lease: Duration, threshold: f64) -> Duration {
    Duration::try_from_secs_f64(lease.as_secs_f64() * threshold)
        .unwrap_or(lease / 2)
        .max(MIN_RENEWAL.min(lease / 2))
}

impl Schedule for RenewalSchedule {
    type Batch = Vec<Item>;
    type Claim = Vec<(DeliveryKey, u64)>;
    type Outcome = Outcome;
    type Lost = LostLease;

    fn earliest(&self) -> Option<Instant> {
        self.heap.peek().map(|Reverse((at, _, _))| *at)
    }

    fn collect_due(&mut self, now: Instant) -> Option<Vec<Item>> {
        self.sweep();
        let mut batch = Vec::new();
        while batch.len() < self.batch_cap {
            let Some(Reverse((at, _, _))) = self.heap.peek() else {
                break;
            };
            if *at > now {
                break;
            }
            let Some(Reverse((_, epoch, key))) = self.heap.pop() else {
                break;
            };
            // Skip heap items superseded by a later schedule or deregistration.
            let Some(entry) = self.live.get_mut(&key) else {
                continue;
            };
            if entry.epoch != epoch || entry.in_flight {
                continue;
            }
            if entry.sink.view.is_retired() {
                self.live.remove(&key);
                continue;
            }
            entry.in_flight = true;
            batch.push(Item {
                key,
                epoch,
                vt_secs: entry.vt_secs,
                expires_at: entry.expires_at,
            });
        }
        (!batch.is_empty()).then_some(batch)
    }

    fn claim_of(batch: &Vec<Item>) -> Self::Claim {
        batch
            .iter()
            .map(|item| (item.key.clone(), item.epoch))
            .collect()
    }

    fn abandon(&mut self, claim: Self::Claim) {
        let now = Instant::now();
        for (key, epoch) in claim {
            let retry_at = match self.live.get(&key) {
                Some(entry) if entry.epoch == epoch => {
                    (now + ERROR_RETRY).min(entry.expires_at).max(now)
                }
                _ => continue,
            };
            self.schedule(key, retry_at);
        }
    }

    fn apply(&mut self, outcome: Outcome, lost: &mut Vec<LostLease>) {
        let now = Instant::now();
        let sent = outcome.sent;
        for item in outcome.items {
            // A delivery deregistered while its renewal was in flight is gone;
            // the result must not resurrect it.
            let Some(entry) = self.live.get_mut(&item.key) else {
                continue;
            };
            if entry.epoch != item.epoch {
                continue;
            }
            // Finished while its renewal was in flight: nothing to report.
            if entry.sink.view.is_retired() {
                self.live.remove(&item.key);
                continue;
            }
            let expires_at = entry.expires_at;
            // Keys were built when decoding, off the lock: no allocation here.
            let renewal = match &outcome.renewals {
                Renewals::Known(rows) => Some(rows.get(&item.key)),
                Renewals::Unknown => None,
            };
            match renewal {
                Some(Some(Renewal {
                    busy: false,
                    remaining: Some(remaining),
                    vt_micros,
                })) => {
                    entry.confirmed_at = sent;
                    entry.expires_at = sent + *remaining;
                    if let Some(vt) = vt_micros {
                        entry.sink.view.publish(*vt);
                    }
                    let at = Self::renewal_at(entry);
                    self.schedule(item.key, at);
                }
                Some(Some(Renewal { busy: true, .. })) | None if expires_at > now => {
                    let delay = if renewal.is_none() {
                        ERROR_RETRY
                    } else {
                        BUSY_RETRY
                    };
                    self.schedule(item.key, (now + delay).min(expires_at));
                }
                _ => {
                    // Omitted (or out of confirmed lease): the lease is lost —
                    // unless the delivery is being settled, which removes
                    // the row on purpose.
                    if let Some(entry) = self.live.remove(&item.key)
                        && !entry.sink.view.settling.load(Ordering::Acquire)
                    {
                        tracing::warn!(
                            queue = %item.key.queue,
                            message_id = %item.key.id,
                            "delivery lease lost"
                        );
                        lost.push(LostLease {
                            queue: Arc::clone(&item.key.queue),
                            sink: entry.sink,
                        });
                    }
                }
            }
        }
    }
}

/// Runs one renewal batch.
pub(crate) async fn flush(
    pool: PgPool,
    retry: RetryConfig,
    abort: CancellationToken,
    metrics: Metrics,
    items: Vec<Item>,
) -> Outcome {
    // One sample per batch, including retries and batches with no eligible
    // rows.
    let timer = metrics.operation(Operation::ExtendBatch, None, false);
    let started = Instant::now();
    let deadline = items
        .iter()
        .map(|item| item.expires_at)
        .fold(started + FLUSH_TIMEOUT, Instant::min);
    let mut queues = Vec::with_capacity(items.len());
    let mut ids = Vec::with_capacity(items.len());
    let mut tokens = Vec::with_capacity(items.len());
    let mut vts = Vec::with_capacity(items.len());
    for item in &items {
        queues.push(item.key.queue.to_string());
        ids.push(item.key.id.get());
        tokens.push(item.key.token.to_string());
        vts.push(item.vt_secs);
    }
    // `sent` is taken per attempt after a connection is acquired, so pool
    // waits and retries never shorten a confirmed deadline.
    let call = with_retry(&retry, &abort, is_retryable, || {
        let (pool, queues, ids, tokens, vts) = (&pool, &queues, &ids, &tokens, &vts);
        async move {
            // Abandoned on timeout: the connection is closed, not pinged
            // back into the pool.
            let mut checkout = Checkout::acquire(pool).await?;
            let sent = Instant::now();
            let rows = sqlx::query(
                "SELECT queue_name::text AS queue_name, message_id, consumer_token::text AS consumer_token, \
                        outcome, \
                        (extract(epoch FROM vt - clock_timestamp()) * 1000)::int8 AS remaining_ms, \
                        (extract(epoch FROM vt) * 1000000)::int8 AS vt_us \
                 FROM postgremq.set_vt_batch_multi($1::varchar[], $2::int8[], $3::varchar[], $4::int4[])",
            )
            .bind(queues)
            .bind(ids)
            .bind(tokens)
            .bind(vts)
            .fetch_all(checkout.conn()?)
            .await;
            checkout.release();
            Ok((rows?, sent))
        }
    });
    // Dropping a renewal mid-flight is harmless: extending is idempotent, and
    // an unknown outcome is retried within the confirmed lease.
    // Checked first: nothing is sent once the shutdown deadline passed.
    let (rows, mut error) = tokio::select! {
        biased;
        () = abort.cancelled() => (Err(None), Some("cancelled")),
        result = tokio::time::timeout_at(deadline, call) => match result {
            Ok(Ok(rows)) => (Ok(rows), None),
            Ok(Err(err)) => {
                let category = sqlx_error_type(&err);
                (Err(Some(err)), Some(category))
            }
            Err(_elapsed) => (Err(None), Some("deadline_exceeded")),
        },
    };
    let (sent, renewals) = match rows {
        Ok((rows, sent)) => (
            sent,
            decode(&rows).unwrap_or_else(|err| {
                tracing::warn!(
                    error = &err as &dyn std::error::Error,
                    "undecodable renewal result"
                );
                error = Some("other");
                Renewals::Unknown
            }),
        ),
        Err(err) => {
            if let Some(err) = err {
                tracing::warn!(
                    error = &err as &dyn std::error::Error,
                    "lease renewal failed"
                );
            } else {
                tracing::warn!("lease renewal timed out");
            }
            (started, Renewals::Unknown)
        }
    };
    timer.finish(error);
    Outcome {
        items,
        sent,
        renewals,
    }
}

fn decode(rows: &[sqlx::postgres::PgRow]) -> Result<Renewals, sqlx::Error> {
    let mut renewals = HashMap::with_capacity(rows.len());
    for row in rows {
        let busy = Heartbeat::parse(row.try_get("outcome")?)? == Heartbeat::Busy;
        let remaining: Option<i64> = row.try_get("remaining_ms")?;
        let queue: String = row.try_get("queue_name")?;
        let token: String = row.try_get("consumer_token")?;
        renewals.insert(
            DeliveryKey {
                queue: Arc::from(queue),
                id: MessageId::new(row.try_get("message_id")?),
                token: Arc::from(token),
            },
            Renewal {
                busy,
                remaining: remaining.map(millis_until),
                vt_micros: row.try_get("vt_us")?,
            },
        );
    }
    Ok(Renewals::Known(renewals))
}

/// A delivery whose lease the renewal found lost (handled after the
/// schedule's lock is released).
#[derive(Debug)]
pub(crate) struct LostLease {
    pub(crate) queue: Arc<str>,
    pub(crate) sink: LeaseSink,
}

/// Cancels a lost delivery's token, after recording the loss.
pub(crate) fn on_lost(metrics: &Metrics, lost: LostLease) {
    metrics.renewal_lost(&lost.queue);
    lost.sink.stopped.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: i64) -> DeliveryKey {
        DeliveryKey {
            queue: Arc::from("q"),
            id: MessageId::new(id),
            token: Arc::from("t"),
        }
    }

    fn sink() -> LeaseSink {
        LeaseSink {
            view: Arc::new(SharedLease::new(0)),
            stopped: CancellationToken::new(),
        }
    }

    fn confirmed(remaining: Duration) -> Confirmation {
        Confirmation {
            sent: Instant::now(),
            remaining,
        }
    }

    fn register(
        state: &mut RenewalSchedule,
        key: DeliveryKey,
        vt_secs: i32,
        threshold: f64,
        lease: Confirmation,
        sink: LeaseSink,
    ) {
        state.register([Registration {
            key,
            vt_secs,
            threshold,
            lease,
            sink,
        }]);
    }

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn renewal_map(id: i64, renewal: Renewal) -> HashMap<DeliveryKey, Renewal> {
        HashMap::from([(key(id), renewal)])
    }

    #[tokio::test(start_paused = true)]
    async fn a_retired_delivery_is_dropped_when_due_without_a_renewal() {
        let mut state = RenewalSchedule::new(cap(10));
        let sink = sink();
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink.clone(),
        );
        assert!(sink.view.retire());
        assert!(!sink.view.retire(), "only the first retirement counts");
        tokio::time::advance(Duration::from_secs(15)).await;
        assert!(state.collect_due(Instant::now()).is_none());
        assert!(state.live.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retired_deliveries_are_swept_in_bulk() {
        let mut state = RenewalSchedule::new(cap(10));
        let counter = state.retired_counter();
        let sinks: Vec<LeaseSink> = (0..1_000).map(|_| sink()).collect();
        for (id, sink) in (0..).zip(&sinks) {
            register(
                &mut state,
                key(id),
                30,
                0.5,
                confirmed(Duration::from_secs(30)),
                sink.clone(),
            );
        }
        for sink in &sinks[..900] {
            if sink.view.retire() {
                counter.fetch_add(1, Ordering::AcqRel);
            }
        }
        // Long before any renewal is due, a look at the schedule sweeps.
        assert!(state.collect_due(Instant::now()).is_none());
        assert_eq!(state.live.len(), 100);
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_delivery_finished_during_its_renewal_is_dropped_silently() {
        let mut state = RenewalSchedule::new(cap(10));
        let sink = sink();
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink.clone(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        // Acked meanwhile: the server omits the row, which is not a loss.
        sink.view.retire();
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Known(HashMap::new()),
            },
            &mut lost,
        );
        assert!(lost.is_empty());
        assert!(!sink.stopped.is_cancelled());
        assert!(state.live.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn settled_deliveries_do_not_accumulate_before_any_renewal_is_due() {
        // A long lease: no renewal is due for 150 s, yet settled deliveries
        // must not pile up meanwhile.
        let mut state = RenewalSchedule::new(cap(10));
        let counter = state.retired_counter();
        register(
            &mut state,
            key(0),
            300,
            0.5,
            confirmed(Duration::from_secs(300)),
            sink(),
        );
        for id in 1..10_000 {
            let sink = sink();
            register(
                &mut state,
                key(id),
                300,
                0.5,
                confirmed(Duration::from_secs(300)),
                sink.clone(),
            );
            // Settled at once, as `retire_lease` does.
            if sink.view.retire() {
                counter.fetch_add(1, Ordering::AcqRel);
            }
        }
        assert!(state.live.len() <= 2 * 64 + 4, "{}", state.live.len());
        assert!(state.heap.len() <= 2 * state.live.len() + 64 + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_omitted_row_while_settling_is_not_a_lost_lease() {
        let mut state = RenewalSchedule::new(cap(10));
        let sink = sink();
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink.clone(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        // The ack committed while the renewal was in flight; it has not
        // retired the lease yet.
        sink.view.begin_settling();
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Known(HashMap::new()),
            },
            &mut lost,
        );
        assert!(lost.is_empty());
        assert!(state.live.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn due_entries_are_claimed_once_until_their_outcome_is_applied() {
        let mut state = RenewalSchedule::new(cap(10));
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink(),
        );
        assert!(state.collect_due(Instant::now()).is_none());

        tokio::time::advance(Duration::from_secs(15)).await;
        let batch = state.collect_due(Instant::now()).expect("due");
        assert_eq!(batch.len(), 1);
        assert!(state.collect_due(Instant::now()).is_none(), "in flight");
    }

    #[tokio::test(start_paused = true)]
    async fn an_extended_lease_is_rescheduled_at_the_threshold() {
        let mut state = RenewalSchedule::new(cap(10));
        let sink = sink();
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink.clone(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        let renewals = renewal_map(
            1,
            Renewal {
                busy: false,
                remaining: Some(Duration::from_secs(30)),
                vt_micros: Some(30_000_042),
            },
        );
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Known(renewals),
            },
            &mut lost,
        );
        assert!(lost.is_empty());
        assert_eq!(sink.view.vt_micros(), 30_000_042);
        assert_eq!(
            state.earliest(),
            Some(Instant::now() + Duration::from_secs(15))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_transport_error_retries_within_the_confirmed_lease() {
        let mut state = RenewalSchedule::new(cap(10));
        let sink = sink();
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink.clone(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Unknown,
            },
            &mut lost,
        );
        assert!(lost.is_empty(), "a transport error is not lease loss");
        assert!(!sink.stopped.is_cancelled());
        assert_eq!(state.earliest(), Some(Instant::now() + ERROR_RETRY));
    }

    #[tokio::test(start_paused = true)]
    async fn errors_past_the_confirmed_deadline_lose_the_lease() {
        let mut state = RenewalSchedule::new(cap(10));
        register(
            &mut state,
            key(1),
            2,
            0.5,
            confirmed(Duration::from_secs(2)),
            sink(),
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        tokio::time::advance(Duration::from_secs(2)).await;
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Unknown,
            },
            &mut lost,
        );
        assert_eq!(lost.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_omitted_row_loses_the_lease() {
        let mut state = RenewalSchedule::new(cap(10));
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Known(HashMap::new()),
            },
            &mut lost,
        );
        assert_eq!(lost.len(), 1);
        assert!(state.earliest().is_none() || state.collect_due(Instant::now()).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_result_for_a_deregistered_delivery_is_ignored() {
        let mut state = RenewalSchedule::new(cap(10));
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        state.deregister(&key(1));
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                renewals: Renewals::Known(HashMap::new()),
            },
            &mut lost,
        );
        assert!(lost.is_empty());
        assert!(state.live.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn batches_are_capped() {
        let mut state = RenewalSchedule::new(cap(2));
        for id in 0..5 {
            register(
                &mut state,
                key(id),
                30,
                0.5,
                confirmed(Duration::from_secs(30)),
                sink(),
            );
        }
        tokio::time::advance(Duration::from_secs(15)).await;
        assert_eq!(state.collect_due(Instant::now()).map(|b| b.len()), Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn an_abandoned_claim_is_retried() {
        let mut state = RenewalSchedule::new(cap(10));
        register(
            &mut state,
            key(1),
            30,
            0.5,
            confirmed(Duration::from_secs(30)),
            sink(),
        );
        tokio::time::advance(Duration::from_secs(15)).await;
        let batch = state.collect_due(Instant::now()).expect("due");
        state.abandon(RenewalSchedule::claim_of(&batch));
        assert_eq!(state.earliest(), Some(Instant::now() + ERROR_RETRY));
    }
}
