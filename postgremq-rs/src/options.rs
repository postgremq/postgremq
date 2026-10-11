//! Option structs for connections, queues, publishing and consuming.
//!
//! Every option struct is `#[non_exhaustive]`: start from `Default` and use the
//! chainable setters, e.g. `ConsumeOptions::default().batch_size(n)`.

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::error::Error;
use crate::types::{Generation, MAX_HORIZON};

/// Upper bound for [`ConsumeOptions::check_timeout`].
const MAX_CHECK_TIMEOUT: Duration = Duration::from_secs(24 * 3600);
/// Lower bound for [`ConsumeOptions::check_timeout`]: polling faster only
/// loads the database.
const MIN_CHECK_TIMEOUT: Duration = Duration::from_millis(10);

// Panic-free non-zero defaults: `MIN` (1) plus a constant.
const DEFAULT_MAX_ATTEMPTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(2); // 3
const DEFAULT_BATCH_SIZE: NonZeroU32 = NonZeroU32::MIN.saturating_add(9); // 10
const DEFAULT_VT_SECS: NonZeroU32 = NonZeroU32::MIN.saturating_add(29); // 30
const DEFAULT_RENEWAL_BATCH: NonZeroUsize = NonZeroUsize::MIN.saturating_add(99); // 100

/// Retry policy for transient database errors.
///
/// Operations that are safe to repeat (settlement, extension, heartbeats,
/// declarations, reads) retry SQLSTATEs `40001`, `40P01`, `55P03`, class `08`
/// and `57P01`/`57P02`/`57P03`, plus a dropped connection. Destructive admin
/// mutations (delete, purge, requeue, cleanup, maintenance) retry only
/// failures that prove a rollback (`40001`, `40P01`, `55P03`). [`Connection::publish`](crate::Connection::publish) retries only
/// `40001`/`40P01` (an aborted transaction is the only proof a publish did
/// not commit), and a claim is never retried. Methods taking a caller's connection
/// (`publish_tx`, `ack_tx`) never retry: the caller owns the transaction.
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::RetryConfig { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RetryConfig {
    /// Total attempts including the first; `1` disables retries.
    pub max_attempts: NonZeroU32,
    /// Wait before the first retry; positive when `max_attempts > 1`.
    pub initial_backoff: Duration,
    /// Upper bound for the wait between attempts.
    pub max_backoff: Duration,
    /// Factor applied to the wait after each retry.
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
            multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// A policy that never retries.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_attempts: NonZeroU32::MIN,
            ..Self::default()
        }
    }

    /// Sets [`max_attempts`](Self::max_attempts).
    #[must_use]
    pub fn max_attempts(mut self, attempts: NonZeroU32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Sets [`initial_backoff`](Self::initial_backoff).
    #[must_use]
    pub fn initial_backoff(mut self, backoff: Duration) -> Self {
        self.initial_backoff = backoff;
        self
    }

    /// Sets [`max_backoff`](Self::max_backoff).
    #[must_use]
    pub fn max_backoff(mut self, backoff: Duration) -> Self {
        self.max_backoff = backoff;
        self
    }

    /// Sets [`multiplier`](Self::multiplier).
    #[must_use]
    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !self.multiplier.is_finite() || self.multiplier < 1.0 {
            return Err(Error::invalid("retry multiplier must be finite and >= 1"));
        }
        if self.initial_backoff.is_zero() && self.max_attempts.get() > 1 {
            // A zero wait would retry a dropped connection back to back.
            return Err(Error::invalid(
                "retry initial_backoff must be positive when retrying",
            ));
        }
        if self.initial_backoff > self.max_backoff {
            return Err(Error::invalid(
                "retry initial_backoff must not exceed max_backoff",
            ));
        }
        Ok(())
    }
}

/// Called when a queue becomes fatal (see [`Error::QueueGone`]). It runs on
/// Tokio's blocking thread pool, unordered with the affected consumers'
/// streams ending; a panic in it is logged.
pub type QueueFatalHook = Arc<dyn Fn(&str, &Error) + Send + Sync>;

/// Options for [`Connection::connect`](crate::Connection::connect) and
/// [`Connection::from_pool`](crate::Connection::from_pool).
///
/// Diagnostics are emitted through [`tracing`] (target `postgremq`); install a
/// subscriber in the application to collect them.
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::ConnectionOptions { ..unimplemented!() };
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub struct ConnectionOptions {
    /// How long [`Connection::close`](crate::Connection::close) waits for
    /// in-flight deliveries to settle before abandoning them (their leases
    /// then expire normally). `None` waits indefinitely. The remaining
    /// cleanup steps share what is left of this time; past it, each gets at
    /// most 50ms, so `close` returns within about 250ms of the deadline.
    pub shutdown_timeout: Option<Duration>,
    /// Retry policy for transient database errors.
    pub retry: RetryConfig,
    /// Upper bound on deliveries extended by one `set_vt_batch_multi` call.
    pub renewal_batch_size: NonZeroUsize,
    /// Use `LISTEN`/`NOTIFY` wake-ups. When `false`, consumers rely on
    /// polling every `check_timeout` alone (e.g. behind a transaction-pooling
    /// proxy that cannot carry a `LISTEN` session).
    pub notifications: bool,
    /// Called once when a queue becomes fatal. This is the only signal for an
    /// exclusive queue without consumers. Without a hook the event is logged.
    pub on_queue_fatal: Option<QueueFatalHook>,
    /// The meter client metrics are recorded with; see
    /// [`meter_provider`](Self::meter_provider).
    #[cfg(feature = "otel")]
    pub(crate) meter: Option<opentelemetry::metrics::Meter>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            shutdown_timeout: None,
            retry: RetryConfig::default(),
            renewal_batch_size: DEFAULT_RENEWAL_BATCH,
            notifications: true,
            on_queue_fatal: None,
            #[cfg(feature = "otel")]
            meter: None,
        }
    }
}

impl fmt::Debug for ConnectionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ConnectionOptions");
        let debug = debug
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("retry", &self.retry)
            .field("renewal_batch_size", &self.renewal_batch_size)
            .field("notifications", &self.notifications)
            .field(
                "on_queue_fatal",
                &self.on_queue_fatal.as_ref().map(|_| ".."),
            );
        #[cfg(feature = "otel")]
        let debug = debug.field("metrics", &self.meter.is_some());
        debug.finish()
    }
}

impl ConnectionOptions {
    /// Sets [`shutdown_timeout`](Self::shutdown_timeout).
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    /// Sets [`retry`](Self::retry).
    #[must_use]
    pub fn retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Sets [`renewal_batch_size`](Self::renewal_batch_size).
    #[must_use]
    pub fn renewal_batch_size(mut self, size: NonZeroUsize) -> Self {
        self.renewal_batch_size = size;
        self
    }

    /// Sets [`notifications`](Self::notifications).
    #[must_use]
    pub fn notifications(mut self, enabled: bool) -> Self {
        self.notifications = enabled;
        self
    }

    /// Enables client metrics (requires the `otel` feature), recorded through
    /// `provider` under the instrumentation scope `postgremq`, version `1`:
    /// the shared contract in `docs/observability.md`.
    ///
    /// Without this, no metrics are recorded, even if a global provider is
    /// set; pass `opentelemetry::global::meter_provider()` to use that one.
    /// The application owns the provider: close the connection, then flush
    /// and shut the provider down.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(provider: opentelemetry_sdk::metrics::SdkMeterProvider)
    /// #     -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use postgremq::{Connection, ConnectionOptions};
    ///
    /// let conn = Connection::connect(
    ///     "postgres://localhost/app",
    ///     ConnectionOptions::default().meter_provider(&provider),
    /// )
    /// .await?;
    /// // ... publish and consume ...
    /// conn.close().await;
    /// // The SDK's flush blocks: run it off the async workers.
    /// tokio::task::spawn_blocking(move || provider.force_flush()).await??;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "otel")]
    #[cfg_attr(docsrs, doc(cfg(feature = "otel")))]
    #[must_use]
    pub fn meter_provider<P>(mut self, provider: &P) -> Self
    where
        P: opentelemetry::metrics::MeterProvider + ?Sized,
    {
        let scope = opentelemetry::InstrumentationScope::builder(crate::metrics::SCOPE)
            .with_version(crate::metrics::CONTRACT_VERSION)
            .build();
        self.meter = Some(provider.meter_with_scope(scope));
        self
    }

    /// Sets [`on_queue_fatal`](Self::on_queue_fatal).
    #[must_use]
    pub fn on_queue_fatal(mut self, hook: impl Fn(&str, &Error) + Send + Sync + 'static) -> Self {
        self.on_queue_fatal = Some(Arc::new(hook));
        self
    }
}

/// Options for [`Connection::create_queue`](crate::Connection::create_queue).
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::QueueOptions { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct QueueOptions {
    /// Attempts before a delivery is retired to the dead letter queue; `0`
    /// retries forever.
    pub max_delivery_attempts: u32,
    /// An exclusive queue expires unless kept alive; this connection keeps it
    /// alive while it is open.
    pub exclusive: bool,
    /// The exclusive queue's lease, renewed at half this interval; in
    /// `[1s, 30 days]`.
    pub keep_alive_interval: Duration,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            max_delivery_attempts: 0,
            exclusive: false,
            keep_alive_interval: Duration::from_secs(300),
        }
    }
}

impl QueueOptions {
    /// Sets [`max_delivery_attempts`](Self::max_delivery_attempts).
    #[must_use]
    pub fn max_delivery_attempts(mut self, attempts: u32) -> Self {
        self.max_delivery_attempts = attempts;
        self
    }

    /// Sets [`exclusive`](Self::exclusive).
    #[must_use]
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    /// Sets [`keep_alive_interval`](Self::keep_alive_interval).
    #[must_use]
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }
}

/// Options for [`Connection::publish`](crate::Connection::publish) and
/// [`Connection::publish_tx`](crate::Connection::publish_tx).
///
/// Not constructible outside the crate (fields may be added):
///
/// ```compile_fail,E0639
/// let _ = postgremq::PublishOptions { ..unimplemented!() };
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PublishOptions {
    /// The message stays invisible until this time (at most about 100
    /// years ahead).
    pub deliver_after: Option<SystemTime>,
    /// Message group (1..=255 characters). Within each queue a group is
    /// delivered strictly in publish commit order, one delivery at a time;
    /// see the crate documentation. `None` publishes an ungrouped message.
    pub group_key: Option<String>,
}

impl PublishOptions {
    /// Sets [`deliver_after`](Self::deliver_after).
    #[must_use]
    pub fn deliver_after(mut self, at: SystemTime) -> Self {
        self.deliver_after = Some(at);
        self
    }

    /// Sets [`group_key`](Self::group_key).
    #[must_use]
    pub fn group_key(mut self, key: impl Into<String>) -> Self {
        self.group_key = Some(key.into());
        self
    }

    /// Rejects what the server could not represent. (The group key is
    /// validated by the server.)
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let too_far = self.deliver_after.is_some_and(|at| {
            at.duration_since(SystemTime::now())
                .is_ok_and(|ahead| ahead > MAX_HORIZON)
        });
        if too_far {
            return Err(Error::invalid(
                "deliver_after must be at most 100 years ahead",
            ));
        }
        Ok(())
    }
}

/// Options for [`Connection::consume`](crate::Connection::consume) and
/// [`Connection::consume_with_handler`](crate::Connection::consume_with_handler).
///
/// Build from [`Default`] with the setters; a struct literal does not compile
/// outside the crate, so new options can be added compatibly:
///
/// ```compile_fail,E0639
/// let options = postgremq::ConsumeOptions {
///     batch_size: std::num::NonZeroU32::MIN,
///     ..Default::default()
/// };
/// ```
///
/// Setters return the updated options; discarding them is a mistake the
/// compiler reports:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// let options = postgremq::ConsumeOptions::default();
/// options.clone().auto_extend(false);
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConsumeOptions {
    /// Deliveries claimed per fetch. The consumer prefetches the next batch
    /// while handing out the current one, so up to about twice this many
    /// deliveries can be leased but not yet handed out. With message groups,
    /// a buffered group head blocks its group for every consumer until it is
    /// processed: keep batches small when groups are slow-moving.
    pub batch_size: NonZeroU32,
    /// Lease (visibility timeout) per claim, in seconds.
    pub vt_secs: NonZeroU32,
    /// Poll interval when no notification arrives (the correctness fallback
    /// for `LISTEN`/`NOTIFY`); in `[10ms, 24h]`.
    pub check_timeout: Duration,
    /// Renew leases of in-flight deliveries automatically.
    pub auto_extend: bool,
    /// Fraction of the remaining lease after which a renewal is sent; in
    /// `(0, 1)`. Renewals are never sent less than 100ms (or half the lease,
    /// if shorter) after the previous confirmation.
    pub extension_threshold: f64,
    /// Bind to this queue incarnation; by default the current one is used.
    pub generation: Option<Generation>,
}

impl Default for ConsumeOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            vt_secs: DEFAULT_VT_SECS,
            check_timeout: Duration::from_secs(10),
            auto_extend: true,
            extension_threshold: 0.5,
            generation: None,
        }
    }
}

impl ConsumeOptions {
    /// Sets [`batch_size`](Self::batch_size).
    #[must_use]
    pub fn batch_size(mut self, size: NonZeroU32) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets [`vt_secs`](Self::vt_secs).
    #[must_use]
    pub fn vt_secs(mut self, secs: NonZeroU32) -> Self {
        self.vt_secs = secs;
        self
    }

    /// Sets [`check_timeout`](Self::check_timeout).
    #[must_use]
    pub fn check_timeout(mut self, timeout: Duration) -> Self {
        self.check_timeout = timeout;
        self
    }

    /// Sets [`auto_extend`](Self::auto_extend).
    #[must_use]
    pub fn auto_extend(mut self, enabled: bool) -> Self {
        self.auto_extend = enabled;
        self
    }

    /// Sets [`extension_threshold`](Self::extension_threshold).
    #[must_use]
    pub fn extension_threshold(mut self, threshold: f64) -> Self {
        self.extension_threshold = threshold;
        self
    }

    /// Sets [`generation`](Self::generation).
    #[must_use]
    pub fn generation(mut self, generation: Generation) -> Self {
        self.generation = Some(generation);
        self
    }

    /// Validates the options into the values the consumer runs with.
    pub(crate) fn validate(&self) -> Result<ConsumeSettings, Error> {
        let batch = i32::try_from(self.batch_size.get())
            .map_err(|_| Error::invalid("batch_size must fit a SQL integer"))?;
        let vt_secs = i32::try_from(self.vt_secs.get())
            .map_err(|_| Error::invalid("vt_secs must fit a SQL integer"))?;
        if self.check_timeout < MIN_CHECK_TIMEOUT || self.check_timeout > MAX_CHECK_TIMEOUT {
            return Err(Error::invalid("check_timeout must be in [10ms, 24h]"));
        }
        let threshold = self.extension_threshold;
        if !threshold.is_finite() || threshold <= 0.0 || threshold >= 1.0 {
            return Err(Error::invalid("extension_threshold must be in (0, 1)"));
        }
        Ok(ConsumeSettings {
            batch,
            prefetch: NonZeroUsize::try_from(self.batch_size)
                .map_err(|_| Error::invalid("batch_size must fit usize"))?,
            vt_secs,
            check_timeout: self.check_timeout,
            auto_extend: self.auto_extend,
            threshold,
        })
    }
}

/// Validated [`ConsumeOptions`], in the types the SQL calls take.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConsumeSettings {
    /// Positive.
    pub(crate) batch: i32,
    /// The batch size, as the consumer's prefetch threshold.
    pub(crate) prefetch: NonZeroUsize,
    /// Positive.
    pub(crate) vt_secs: i32,
    /// Positive.
    pub(crate) check_timeout: std::time::Duration,
    pub(crate) auto_extend: bool,
    /// In `(0, 1)`.
    pub(crate) threshold: f64,
}
