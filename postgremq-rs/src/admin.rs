//! Maintenance and inspection passthroughs.

use std::num::NonZeroU32;
use std::time::SystemTime;

use sqlx::Row as _;

use crate::checkout::checked_out;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::types::{MAX_HORIZON, MessageId, from_unix_micros};

/// Counters returned by [`Connection::maintenance_fast`].
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::MaintenanceCounters { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MaintenanceCounters {
    /// Crashed final attempts retired to the dead letter queue.
    pub retired_to_dlq: i64,
    /// Expired exclusive queues deleted.
    pub inactive_queues_dropped: i64,
}

/// A queue as listed by [`Connection::list_queues`].
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::QueueInfo { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct QueueInfo {
    /// Queue name.
    pub name: String,
    /// The topic it subscribes to.
    pub topic: String,
    /// Attempts before dead-lettering; `0` is unlimited.
    pub max_delivery_attempts: u32,
    /// Whether the queue expires unless kept alive.
    pub exclusive: bool,
    /// An exclusive queue's current expiry.
    pub keep_alive_until: Option<SystemTime>,
}

/// Message counts from [`Connection::queue_statistics`].
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::QueueStatistics { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct QueueStatistics {
    /// Waiting (including delayed) deliveries.
    pub pending: i64,
    /// Leased deliveries.
    pub processing: i64,
    /// Completed deliveries still retained.
    pub completed: i64,
    /// The sum of the three.
    pub total: i64,
}

/// A dead-lettered delivery from [`Connection::list_dlq`].
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::DlqMessage { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DlqMessage {
    /// The queue it was retired from.
    pub queue: String,
    /// The message ID.
    pub message_id: MessageId,
    /// Attempts made before retirement.
    pub retry_count: u32,
    /// When it was moved to the dead letter queue.
    pub dead_lettered_at: SystemTime,
}

/// Decodes a count the schema keeps non-negative.
fn non_negative_count(value: Option<i32>) -> Result<u32, sqlx::Error> {
    u32::try_from(value.unwrap_or(0)).map_err(|err| sqlx::Error::Decode(Box::new(err)))
}

/// An age in hours, at most [`MAX_HORIZON`].
fn horizon_hours(hours: u32) -> Result<i32> {
    if u64::from(hours) > MAX_HORIZON.as_secs() / 3600 {
        return Err(Error::invalid("older_than_hours must be at most 100 years"));
    }
    non_negative(hours, "older_than_hours too large")
}

fn non_negative(value: u32, what: &'static str) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::invalid(what))
}

impl Connection {
    /// Runs `pmq_maintenance_fast()`: retires crashed final attempts to the
    /// dead letter queue and deletes expired exclusive queues. Schedule it
    /// frequently (e.g. every second).
    ///
    /// # Errors
    ///
    /// Database errors after retries.
    pub async fn maintenance_fast(&self) -> Result<MaintenanceCounters> {
        let inner = &self.inner;
        let (retired_to_dlq, inactive_queues_dropped): (i64, i64) = inner
            .retry_mutation(|| checked_out(&inner.pool, async |conn| {
                sqlx::query_as(
                    "SELECT retired_to_dlq, inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()",
                )
                .fetch_one(conn).await
            }))
            .await?;
        Ok(MaintenanceCounters {
            retired_to_dlq,
            inactive_queues_dropped,
        })
    }

    /// Deletes up to `batch` completed deliveries older than `older_than_hours`
    /// (and then unreferenced payloads). Returns the deliveries deleted.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for out-of-range arguments; database errors.
    pub async fn cleanup_completed_messages(
        &self,
        older_than_hours: u32,
        batch: NonZeroU32,
    ) -> Result<i64> {
        let inner = &self.inner;
        let hours = horizon_hours(older_than_hours)?;
        let batch = non_negative(batch.get(), "batch too large")?;
        let deleted: i32 = inner
            .retry_mutation(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query_scalar("SELECT postgremq.cleanup_completed_messages($1, $2)")
                        .bind(hours)
                        .bind(batch)
                        .fetch_one(conn)
                        .await
                })
            })
            .await?;
        Ok(i64::from(deleted))
    }

    /// Deletes up to `batch` payloads older than `older_than_hours` that no
    /// queue or dead letter entry references (and prunes their empty message
    /// groups). Returns the payloads deleted.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for out-of-range arguments; database errors.
    pub async fn cleanup_unreferenced_messages(
        &self,
        older_than_hours: u32,
        batch: NonZeroU32,
    ) -> Result<i64> {
        let inner = &self.inner;
        let hours = horizon_hours(older_than_hours)?;
        let batch = non_negative(batch.get(), "batch too large")?;
        let deleted: i32 = inner
            .retry_mutation(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query_scalar("SELECT postgremq.cleanup_unreferenced_messages($1, $2)")
                        .bind(hours)
                        .bind(batch)
                        .fetch_one(conn)
                        .await
                })
            })
            .await?;
        Ok(i64::from(deleted))
    }

    /// Lists all queues.
    ///
    /// # Errors
    ///
    /// Database errors after retries.
    pub async fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        let inner = &self.inner;
        let rows = inner
            .retry(|| checked_out(&inner.pool, async |conn| {
                sqlx::query(
                    "SELECT queue_name::text AS name, topic_name::text AS topic, max_delivery_attempts, \
                            exclusive, (extract(epoch FROM keep_alive_until) * 1000000)::int8 AS until_us \
                     FROM postgremq.list_queues()",
                )
                .fetch_all(conn).await
            }))
            .await?;
        rows.iter()
            .map(|row| {
                let until: Option<i64> = row.try_get("until_us")?;
                Ok(QueueInfo {
                    name: row.try_get("name")?,
                    topic: row.try_get("topic")?,
                    max_delivery_attempts: non_negative_count(
                        row.try_get("max_delivery_attempts")?,
                    )?,
                    exclusive: row.try_get("exclusive")?,
                    keep_alive_until: until.map(from_unix_micros),
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)
    }

    /// Message counts for `queue`, or across all queues when `None`.
    ///
    /// # Errors
    ///
    /// Database errors after retries.
    pub async fn queue_statistics(&self, queue: Option<&str>) -> Result<QueueStatistics> {
        let inner = &self.inner;
        let (pending, processing, completed, total): (i64, i64, i64, i64) = inner
            .retry(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query_as(
                        "SELECT pending_count, processing_count, completed_count, total_count \
                     FROM postgremq.get_queue_statistics($1)",
                    )
                    .bind(queue)
                    .fetch_one(conn)
                    .await
                })
            })
            .await?;
        Ok(QueueStatistics {
            pending,
            processing,
            completed,
            total,
        })
    }

    /// Lists dead-lettered deliveries, oldest first.
    ///
    /// # Errors
    ///
    /// Database errors after retries.
    pub async fn list_dlq(&self) -> Result<Vec<DlqMessage>> {
        let inner = &self.inner;
        let rows = inner
            .retry(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query(
                        "SELECT queue_name::text AS queue, message_id, retry_count, \
                            (extract(epoch FROM published_at) * 1000000)::int8 AS at_us \
                     FROM postgremq.list_dlq_messages()",
                    )
                    .fetch_all(conn)
                    .await
                })
            })
            .await?;
        rows.iter()
            .map(|row| {
                Ok(DlqMessage {
                    queue: row.try_get("queue")?,
                    message_id: MessageId::new(row.try_get("message_id")?),
                    retry_count: non_negative_count(row.try_get("retry_count")?)?,
                    dead_lettered_at: from_unix_micros(row.try_get("at_us")?),
                })
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(Error::from)
    }

    /// Returns `queue`'s dead-lettered deliveries to it with reset attempts. A
    /// grouped message re-enters as the head of its group.
    ///
    /// # Errors
    ///
    /// Database errors after retries.
    pub async fn requeue_dlq(&self, queue: &str) -> Result<()> {
        let inner = &self.inner;
        inner
            .retry_mutation(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query("SELECT postgremq.requeue_dlq_messages($1)")
                        .bind(queue)
                        .execute(conn)
                        .await
                })
            })
            .await?;
        Ok(())
    }

    /// Deletes every dead letter queue entry.
    ///
    /// # Errors
    ///
    /// Database errors after retries.
    pub async fn purge_dlq(&self) -> Result<()> {
        let inner = &self.inner;
        inner
            .retry_mutation(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query("SELECT postgremq.purge_dlq()")
                        .execute(conn)
                        .await
                })
            })
            .await?;
        Ok(())
    }

    /// Deletes a queue and its deliveries. Its consumers end with
    /// [`Error::QueueGone`] at their next fetch.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] if the queue has dead-lettered entries; database
    /// errors after retries.
    pub async fn delete_queue(&self, queue: &str) -> Result<()> {
        let inner = &self.inner;
        // A keep-alive renewal racing the delete would see the queue gone:
        // that is this deletion, not a failure.
        // One declaration or deletion of a name at a time (see
        // `Inner::lifecycle_lock`).
        let _lifecycle = inner.lifecycle_lock(queue).await;
        let deleting = inner.deleting(queue);
        let deleted = inner
            .retry_mutation(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query("SELECT postgremq.delete_queue($1)")
                        .bind(queue)
                        .execute(conn)
                        .await
                })
            })
            .await;
        deleted?;
        deleting.succeeded();
        Ok(())
    }

    /// Deletes a topic without messages.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] if the topic still has messages; database errors.
    pub async fn delete_topic(&self, topic: &str) -> Result<()> {
        let inner = &self.inner;
        inner
            .retry_mutation(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query("SELECT postgremq.delete_topic($1)")
                        .bind(topic)
                        .execute(conn)
                        .await
                })
            })
            .await?;
        Ok(())
    }
}
