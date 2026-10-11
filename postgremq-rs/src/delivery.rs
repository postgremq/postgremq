//! A claimed message and its settlement.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use serde::de::DeserializeOwned;
use sqlx::PgConnection;
use tokio_util::sync::CancellationToken;

use crate::connection::Inner;
use crate::consumer::ConsumerShared;
use crate::error::{Error, Result};
use crate::renewal::{DeliveryKey, SharedLease};
use crate::types::{MAX_HORIZON, MessageId, from_unix_micros};

/// A message claimed from a queue, with its lease.
///
/// A delivery is settled at most once: the first of [`ack`](Self::ack),
/// [`ack_tx`](Self::ack_tx), [`nack`](Self::nack) or
/// [`release`](Self::release) owns the outcome, and any later settlement
/// returns [`Error::LeaseLost`] without touching the database. Settlement
/// ends the delivery's tracking and lease renewal even when the SQL fails (the
/// lease is then left to expire, and the message is redelivered).
///
/// Dropping a delivery without settling it abandons it: its lease is no
/// longer renewed and expires, and the message is redelivered.
///
/// While the consumer runs with `auto_extend`, the lease is renewed in the
/// background; if renewal finds the lease lost, or the consumer is shutting
/// down, [`stopped`](Self::stopped) is cancelled. Handlers should watch it and
/// stop work whose result could no longer be acknowledged.
///
/// # Cancel safety
///
/// The settlement futures ([`ack`](Self::ack), [`ack_tx`](Self::ack_tx),
/// [`nack`](Self::nack), [`release`](Self::release)) are not cancel-safe:
/// the first one claims the settlement before its SQL runs. Dropping it (for
/// example when it loses a `tokio::select!` race) leaves the outcome unknown,
/// ends lease renewal, and makes later settlements return
/// [`Error::LeaseLost`]; if nothing committed, the message is redelivered
/// once its lease expires. [`extend`](Self::extend) may be dropped freely
/// (whether the extension was applied is then unknown).
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # async fn example(consumer: &mut postgremq::Consumer) {
/// // Discarding a delivery without settling it is reported.
/// consumer.next().await.unwrap().unwrap();
/// # }
/// ```
#[must_use = "dropping an unsettled delivery abandons it: its lease expires and it is redelivered"]
pub struct Delivery {
    pub(crate) inner: Arc<DeliveryInner>,
}

pub(crate) struct DeliveryInner {
    pub(crate) conn: Arc<Inner>,
    pub(crate) key: DeliveryKey,
    pub(crate) payload: serde_json::Value,
    pub(crate) group: Option<(String, i64)>,
    pub(crate) attempts: u32,
    pub(crate) published_at: SystemTime,
    /// The published lease deadline.
    pub(crate) lease: Arc<SharedLease>,
    pub(crate) settled: AtomicBool,
    pub(crate) stopped: CancellationToken,
    pub(crate) consumer: Arc<ConsumerShared>,
}

impl DeliveryInner {
    /// Ends tracking and renewal. Idempotent.
    pub(crate) fn complete(&self) {
        // Lock-free: the renewal scheduler drops the entry later.
        self.conn.retire_lease(&self.lease);
        self.consumer.untrack(&self.key);
    }
}

/// Dropping every handle of an unsettled delivery abandons it: renewal and
/// tracking end (so a stopping consumer does not wait for it) and its lease
/// expires, after which the message is redelivered.
impl Drop for DeliveryInner {
    fn drop(&mut self) {
        self.complete();
    }
}

/// Completes the delivery when the settlement ends — including when the
/// settling future is dropped before the SQL returns.
struct CompleteOnDrop<'a>(&'a DeliveryInner);

impl Drop for CompleteOnDrop<'_> {
    fn drop(&mut self) {
        self.0.complete();
    }
}

impl Delivery {
    /// The queue the message was claimed from.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.inner.key.queue
    }

    /// The message ID.
    #[must_use]
    pub fn message_id(&self) -> MessageId {
        self.inner.key.id
    }

    /// The JSON payload.
    #[must_use]
    pub fn payload(&self) -> &serde_json::Value {
        &self.inner.payload
    }

    /// Deserializes the payload.
    ///
    /// # Errors
    ///
    /// [`Error::Payload`] if the payload does not match `T`.
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T> {
        T::deserialize(&self.inner.payload).map_err(Error::Payload)
    }

    /// The message group, if the message was published with one.
    #[must_use]
    pub fn group_key(&self) -> Option<&str> {
        self.inner.group.as_ref().map(|(key, _)| key.as_str())
    }

    /// The message's position in its group (dense, 1-based, in publish commit
    /// order), if it is grouped.
    #[must_use]
    pub fn group_seq(&self) -> Option<i64> {
        self.inner.group.as_ref().map(|(_, seq)| *seq)
    }

    /// How many times the message has been claimed, including this claim.
    #[must_use]
    pub fn delivery_attempts(&self) -> u32 {
        self.inner.attempts
    }

    /// When the message was published (distributed to the queue).
    #[must_use]
    pub fn published_at(&self) -> SystemTime {
        self.inner.published_at
    }

    /// The current lease deadline, as last confirmed by the server (by a
    /// renewal or [`extend`](Self::extend), whichever was recorded last).
    #[must_use]
    pub fn vt(&self) -> SystemTime {
        from_unix_micros(self.inner.lease.vt_micros())
    }

    /// Whether a settlement has started.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.inner.settled.load(Ordering::Acquire)
    }

    /// Cancelled when the lease is lost or the consumer is shutting down.
    #[must_use]
    pub fn stopped(&self) -> CancellationToken {
        self.inner.stopped.clone()
    }

    /// Marks the message completed.
    ///
    /// # Errors
    ///
    /// [`Error::LeaseLost`] if the delivery was already settled or its lease
    /// is gone; [`Error::Closed`] after the connection closed (or if the shutdown
    /// deadline abandoned the call, with an unknown outcome); other database
    /// errors after the retry policy is exhausted.
    pub async fn ack(&self) -> Result<()> {
        self.settle(|| self.inner.conn.ack(&self.inner.key)).await
    }

    /// Marks the message completed inside the caller's transaction.
    ///
    /// Renewal ends with this call (once the statement completes): if the
    /// transaction rolls back, the delivery is left to expire and is
    /// redelivered after its lease. Never retried —
    /// the caller owns the transaction. `&mut tx` for a [`sqlx::Transaction`]
    /// coerces to the connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(conn: postgremq::Connection, delivery: postgremq::Delivery) -> postgremq::Result<()> {
    /// let mut tx = conn.pool().begin().await?;
    /// postgremq::sqlx::query("UPDATE app.orders SET billed = true WHERE id = 7")
    ///     .execute(&mut *tx)
    ///     .await?;
    /// delivery.ack_tx(&mut tx).await?;
    /// tx.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::LeaseLost`] if the delivery was already settled or its lease
    /// is gone; [`Error::Closed`] after the connection closed; other database
    /// errors. Any error that carries a database source (including a
    /// server-reported `LeaseLost`) leaves the caller's transaction aborted;
    /// the source-less "already settled" `LeaseLost` and `Closed` run no SQL.
    pub async fn ack_tx(&self, conn: &mut PgConnection) -> Result<()> {
        self.settle(move || self.inner.conn.ack_tx(conn, &self.inner.key))
            .await
    }

    /// Returns the message for another attempt, after `delay` if given. On the
    /// queue's final attempt the message is retired to the dead letter queue.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(delivery: postgremq::Delivery) -> postgremq::Result<()> {
    /// use std::time::Duration;
    ///
    /// // Retry in 30 seconds (the delay is measured by the database's clock).
    /// delivery.nack(Some(Duration::from_secs(30))).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] (leaving the delivery unsettled) for a delay over
    /// about 100 years; otherwise as for [`ack`](Self::ack).
    pub async fn nack(&self, delay: Option<Duration>) -> Result<()> {
        // Checked before the settlement is claimed, so a bad delay leaves the
        // delivery unsettled.
        if delay.is_some_and(|delay| delay > MAX_HORIZON) {
            return Err(Error::invalid("nack delay must be at most 100 years"));
        }
        self.settle(|| self.inner.conn.nack(&self.inner.key, delay))
            .await
    }

    /// Returns the message without counting this claim as an attempt (for
    /// work that was never started).
    ///
    /// # Errors
    ///
    /// As for [`ack`](Self::ack).
    pub async fn release(&self) -> Result<()> {
        self.settle(|| self.inner.conn.release(&self.inner.key))
            .await
    }

    /// Extends the lease to `secs` from now and returns the new deadline
    /// (also shown by [`vt`](Self::vt)). Usually unnecessary with
    /// `auto_extend`.
    ///
    /// As in the Go and TypeScript clients, this is independent of automatic
    /// renewal: the consumer's renewals keep their own schedule and reset the
    /// lease to `vt_secs`, so a longer extension lasts only until the next
    /// renewal and a shorter one is not seen by it. With `auto_extend`, keeping
    /// manual extensions consistent with renewal is the caller's
    /// responsibility.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(delivery: postgremq::Delivery) -> postgremq::Result<()> {
    /// use std::num::NonZeroU32;
    ///
    /// let five_minutes = NonZeroU32::new(300).expect("non-zero");
    /// let deadline = delivery.extend(five_minutes).await?;
    /// # let _ = deadline;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] if `secs` exceeds `i32::MAX`;
    /// [`Error::LeaseLost`] if the lease is gone or the delivery settled;
    /// [`Error::Busy`] if the row is momentarily locked (retry within the
    /// lease); [`Error::Closed`] after the connection closed (or if the shutdown
    /// deadline abandoned the call).
    pub async fn extend(&self, secs: NonZeroU32) -> Result<SystemTime> {
        if self.is_settled() {
            return Err(Error::already_settled());
        }
        let secs =
            i32::try_from(secs.get()).map_err(|_| Error::invalid("secs must fit a SQL integer"))?;
        let vt_micros = self.inner.conn.set_vt(&self.inner.key, secs).await?;
        self.inner.lease.publish(vt_micros);
        Ok(from_unix_micros(vt_micros))
    }

    /// Claims the settlement, then runs `operation` (built only after the
    /// claim, so its future is held once and a lost claim builds none).
    async fn settle<F>(&self, operation: impl FnOnce() -> F) -> Result<()>
    where
        F: Future<Output = Result<()>>,
    {
        if self
            .inner
            .settled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::already_settled());
        }
        self.inner.lease.begin_settling();
        let _complete = CompleteOnDrop(&self.inner);
        operation().await
    }

    /// A second handle to the same delivery (crate-internal: the handler
    /// supervisor settles after the handler returns).
    pub(crate) fn share(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("queue", &self.queue())
            .field("message_id", &self.message_id())
            .field("group_seq", &self.group_seq())
            .field("delivery_attempts", &self.delivery_attempts())
            .field("settled", &self.is_settled())
            .finish_non_exhaustive()
    }
}
