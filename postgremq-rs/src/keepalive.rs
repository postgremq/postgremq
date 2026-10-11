//! Connection-level keep-alive for exclusive queues: one schedule renews every
//! exclusive queue this connection declared, with one
//! `extend_queue_keep_alive_multi` call per tick.
//!
//! Renewal is bound to the queue generation, so an old registration cannot
//! keep a replacement queue alive. A queue omitted from a successful result is
//! gone (deleted, expired, or replaced): that is permanent and makes the queue
//! fatal. `busy` and transport errors retry until the last confirmed deadline.

use std::collections::HashMap;
use std::sync::Arc;
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
use crate::types::{Generation, duration_millis, millis_until};

/// Retry delay after a `busy` outcome or an error.
const RETRY: Duration = Duration::from_millis(100);
/// Upper bound for one keep-alive round trip.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct Entry {
    generation: Generation,
    interval: Duration,
    next_at: Instant,
    expires_at: Instant,
    epoch: u64,
    in_flight: bool,
}

/// The keep-alive schedule.
#[derive(Debug, Default)]
pub(crate) struct KeepAliveSchedule {
    entries: HashMap<Arc<str>, Entry>,
    next_epoch: u64,
}

/// One queue claimed for renewal.
#[derive(Debug)]
pub(crate) struct Item {
    queue: Arc<str>,
    generation: Generation,
    interval: Duration,
    epoch: u64,
    expires_at: Instant,
}

#[derive(Debug)]
struct Lease {
    busy: bool,
    remaining: Option<Duration>,
}

/// What a flush learned about its batch.
#[derive(Debug)]
enum Leases {
    /// The call returned; an omitted queue is gone.
    Known(HashMap<String, Lease>),
    /// Transport/database failure: nothing is known about the batch.
    Unknown,
}

/// One flush's outcome.
#[derive(Debug)]
pub(crate) struct Outcome {
    items: Vec<Item>,
    /// When the request was sent (deadlines are measured from here, which
    /// never overstates them).
    sent: Instant,
    leases: Leases,
}

/// A queue whose keep-alive failed permanently.
#[derive(Debug)]
pub(crate) struct LostQueue {
    pub(crate) queue: Arc<str>,
    pub(crate) generation: Generation,
}

impl KeepAliveSchedule {
    /// Keeps `queue` alive; replaces any previous registration (a fresh
    /// declaration renews the lease and fences older results). `sent` is when
    /// the declaration that set the lease was sent.
    pub(crate) fn register(
        &mut self,
        queue: Arc<str>,
        generation: Generation,
        interval: Duration,
        sent: Instant,
    ) {
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.entries.insert(
            queue,
            Entry {
                generation,
                interval,
                next_at: sent + interval / 2,
                expires_at: sent + interval,
                epoch: self.next_epoch,
                in_flight: false,
            },
        );
    }

    /// Stops keeping `queue` alive only if it is registered as `generation`:
    /// a stale signal must not stop a replacement queue's keep-alive.
    /// Returns whether it was kept alive.
    pub(crate) fn deregister_generation(&mut self, queue: &str, generation: Generation) -> bool {
        if self
            .entries
            .get(queue)
            .is_some_and(|entry| entry.generation == generation)
        {
            self.entries.remove(queue);
            return true;
        }
        false
    }
}

impl Schedule for KeepAliveSchedule {
    type Batch = Vec<Item>;
    type Claim = Vec<(Arc<str>, u64)>;
    type Outcome = Outcome;
    type Lost = LostQueue;

    fn earliest(&self) -> Option<Instant> {
        self.entries
            .values()
            .filter(|entry| !entry.in_flight)
            .map(|entry| entry.next_at)
            .min()
    }

    fn collect_due(&mut self, now: Instant) -> Option<Vec<Item>> {
        let batch: Vec<Item> = self
            .entries
            .iter_mut()
            .filter(|(_, entry)| !entry.in_flight && entry.next_at <= now)
            .map(|(queue, entry)| {
                entry.in_flight = true;
                Item {
                    queue: Arc::clone(queue),
                    generation: entry.generation,
                    interval: entry.interval,
                    epoch: entry.epoch,
                    expires_at: entry.expires_at,
                }
            })
            .collect();
        (!batch.is_empty()).then_some(batch)
    }

    fn claim_of(batch: &Vec<Item>) -> Self::Claim {
        batch
            .iter()
            .map(|item| (Arc::clone(&item.queue), item.epoch))
            .collect()
    }

    fn abandon(&mut self, claim: Self::Claim) {
        let now = Instant::now();
        for (queue, epoch) in claim {
            if let Some(entry) = self.entries.get_mut(&queue)
                && entry.epoch == epoch
            {
                entry.in_flight = false;
                entry.next_at = (now + RETRY).min(entry.expires_at).max(now);
            }
        }
    }

    fn apply(&mut self, outcome: Outcome, lost: &mut Vec<LostQueue>) {
        let now = Instant::now();
        let sent = outcome.sent;
        for item in outcome.items {
            let Some(entry) = self.entries.get_mut(&item.queue) else {
                continue;
            };
            if entry.epoch != item.epoch {
                continue;
            }
            entry.in_flight = false;
            let lease = match &outcome.leases {
                Leases::Known(leases) => Some(leases.get(item.queue.as_ref())),
                Leases::Unknown => None,
            };
            match lease {
                Some(Some(Lease {
                    busy: false,
                    remaining: Some(remaining),
                })) => {
                    entry.expires_at = sent + *remaining;
                    entry.next_at = sent + *remaining / 2;
                }
                Some(Some(Lease { busy: true, .. })) | None if item.expires_at > now => {
                    entry.next_at = (now + RETRY).min(item.expires_at);
                }
                _ => {
                    self.entries.remove(&item.queue);
                    tracing::warn!(queue = %item.queue, "exclusive queue keep-alive failed permanently");
                    lost.push(LostQueue {
                        queue: item.queue,
                        generation: item.generation,
                    });
                }
            }
        }
    }
}

/// Runs one keep-alive batch.
pub(crate) async fn flush(
    pool: PgPool,
    retry: RetryConfig,
    abort: CancellationToken,
    metrics: Metrics,
    items: Vec<Item>,
) -> Outcome {
    // One sample per batch, including retries and batches with no eligible
    // rows.
    let timer = metrics.operation(Operation::KeepAlive, None, false);
    let started = Instant::now();
    let deadline = items
        .iter()
        .map(|item| item.expires_at)
        .fold(started + FLUSH_TIMEOUT, Instant::min);
    let queues: Vec<String> = items.iter().map(|item| item.queue.to_string()).collect();
    let intervals: Vec<i64> = items
        .iter()
        .map(|item| duration_millis(item.interval))
        .collect();
    let generations: Vec<uuid::Uuid> = items.iter().map(|item| item.generation.get()).collect();
    // `sent` is taken per attempt after a connection is acquired, so pool
    // waits and retries never shorten a confirmed deadline.
    let call = with_retry(&retry, &abort, is_retryable, || {
        let (pool, queues, intervals, generations) = (&pool, &queues, &intervals, &generations);
        async move {
            // Abandoned on timeout: the connection is closed, not pinged
            // back into the pool.
            let mut checkout = Checkout::acquire(pool).await?;
            let sent = Instant::now();
            let rows = sqlx::query(
                "SELECT queue_name::text AS queue_name, outcome, \
                        (extract(epoch FROM keep_alive_until - clock_timestamp()) * 1000)::int8 AS remaining_ms \
                 FROM postgremq.extend_queue_keep_alive_multi($1::varchar[], $2::int8[], $3::uuid[])",
            )
            .bind(queues)
            .bind(intervals)
            .bind(generations)
            .fetch_all(checkout.conn()?)
            .await;
            checkout.release();
            Ok((rows?, sent))
        }
    });
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
    let (sent, leases) = match rows {
        Ok((rows, sent)) => (
            sent,
            decode(&rows).unwrap_or_else(|err| {
                tracing::warn!(
                    error = &err as &dyn std::error::Error,
                    "undecodable keep-alive result"
                );
                error = Some("other");
                Leases::Unknown
            }),
        ),
        Err(err) => {
            if let Some(err) = err {
                tracing::warn!(
                    error = &err as &dyn std::error::Error,
                    "queue keep-alive failed"
                );
            } else {
                tracing::warn!("queue keep-alive timed out");
            }
            (started, Leases::Unknown)
        }
    };
    timer.finish(error);
    Outcome {
        items,
        sent,
        leases,
    }
}

fn decode(rows: &[sqlx::postgres::PgRow]) -> Result<Leases, sqlx::Error> {
    let mut leases = HashMap::with_capacity(rows.len());
    for row in rows {
        let busy = Heartbeat::parse(row.try_get("outcome")?)? == Heartbeat::Busy;
        let remaining: Option<i64> = row.try_get("remaining_ms")?;
        leases.insert(
            row.try_get("queue_name")?,
            Lease {
                busy,
                remaining: remaining.map(millis_until),
            },
        );
    }
    Ok(Leases::Known(leases))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation() -> Generation {
        Generation::new(uuid::Uuid::nil())
    }

    #[tokio::test(start_paused = true)]
    async fn renews_at_half_the_interval_and_reschedules_from_the_server_deadline() {
        let mut state = KeepAliveSchedule::default();
        state.register(
            Arc::from("q"),
            generation(),
            Duration::from_secs(10),
            Instant::now(),
        );
        assert_eq!(
            state.earliest(),
            Some(Instant::now() + Duration::from_secs(5))
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        let leases = HashMap::from([(
            "q".to_owned(),
            Lease {
                busy: false,
                remaining: Some(Duration::from_secs(10)),
            },
        )]);
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                leases: Leases::Known(leases),
            },
            &mut lost,
        );
        assert!(lost.is_empty());
        assert_eq!(
            state.earliest(),
            Some(Instant::now() + Duration::from_secs(5))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_omitted_queue_is_lost_but_a_transport_error_is_retried() {
        let mut state = KeepAliveSchedule::default();
        state.register(
            Arc::from("q"),
            generation(),
            Duration::from_secs(10),
            Instant::now(),
        );
        tokio::time::advance(Duration::from_secs(5)).await;

        let items = state.collect_due(Instant::now()).expect("due");
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                leases: Leases::Unknown,
            },
            &mut lost,
        );
        assert!(lost.is_empty());

        tokio::time::advance(RETRY).await;
        let items = state.collect_due(Instant::now()).expect("retry due");
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                leases: Leases::Known(HashMap::new()),
            },
            &mut lost,
        );
        assert_eq!(lost.len(), 1);
        assert!(state.earliest().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_re_registration_fences_the_in_flight_result() {
        let mut state = KeepAliveSchedule::default();
        state.register(
            Arc::from("q"),
            generation(),
            Duration::from_secs(10),
            Instant::now(),
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        let items = state.collect_due(Instant::now()).expect("due");
        state.register(
            Arc::from("q"),
            generation(),
            Duration::from_secs(10),
            Instant::now(),
        );
        let mut lost = Vec::new();
        state.apply(
            Outcome {
                items,
                sent: Instant::now(),
                leases: Leases::Known(HashMap::new()),
            },
            &mut lost,
        );
        assert!(
            lost.is_empty(),
            "the stale result must not kill the new registration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_due_queue_is_renewed_in_one_batch() {
        let mut state = KeepAliveSchedule::default();
        for queue in ["a", "b", "c"] {
            state.register(
                Arc::from(queue),
                generation(),
                Duration::from_secs(10),
                Instant::now(),
            );
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        let batch = state.collect_due(Instant::now()).expect("due");
        assert_eq!(batch.len(), 3, "one extend_queue_keep_alive_multi call");
        assert!(state.collect_due(Instant::now()).is_none(), "all in flight");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_generation_does_not_deregister_a_replacement() {
        let mut state = KeepAliveSchedule::default();
        let old = Generation::new(uuid::Uuid::from_u128(1));
        let new = Generation::new(uuid::Uuid::from_u128(2));
        state.register(Arc::from("q"), new, Duration::from_secs(10), Instant::now());
        state.deregister_generation("q", old);
        assert!(
            state.earliest().is_some(),
            "the replacement keeps its keep-alive"
        );
        state.deregister_generation("q", new);
        assert!(state.earliest().is_none());
    }
}
