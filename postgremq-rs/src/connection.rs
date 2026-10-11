//! [`Connection`]: the client handle.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;
use sqlx::{PgConnection, PgExecutor, PgPool};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::checkout::checked_out;
use crate::consumer::{Consumer, ConsumerShared, Task};
use crate::error::{Error, Result};
use crate::keepalive::{self, KeepAliveSchedule};
use crate::listener::Listener;
use crate::metrics::{Metrics, Operation, sqlx_error_type};
use crate::options::{ConnectionOptions, ConsumeOptions, PublishOptions, QueueOptions};
use crate::renewal::{self, DeliveryKey, Registration, RenewalSchedule, SharedLease};
use crate::retry::{is_aborted_transaction, is_retryable, is_rolled_back, with_retry};
use crate::scheduler::{Scheduler, spawn_scheduler};
use crate::sync::lock;
use crate::types::{Generation, MessageId, duration_millis, to_unix_micros};

/// How long [`Connection::close`] waits for consumers to exit after the
/// shutdown deadline abandoned their work.
const ABANDON_GRACE: Duration = Duration::from_secs(1);
/// Idle connections of an owned pool are closed after this, before typical
/// load-balancer and pooler idle cut-offs.
const IDLE_TIMEOUT: Duration = Duration::from_secs(180);
/// Lower bound for [`QueueOptions::keep_alive_interval`]: a shorter lease
/// cannot reliably survive one renewal round trip.
const MIN_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(1);
/// Upper bound for [`QueueOptions::keep_alive_interval`].
const MAX_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30 * 24 * 3600);
/// The least time a shutdown step gets once the deadline has passed.
const MIN_SHUTDOWN_STEP: Duration = Duration::from_millis(50);
/// Bound for stopping the `LISTEN` session during `close`.
const LISTENER_STOP: Duration = Duration::from_secs(1);
/// How long a deleted incarnation's keep-alive loss is recognised as that
/// deletion (far longer than a keep-alive flush can take).
const DELETED_RETENTION: Duration = Duration::from_secs(60);
/// Bound for the queue lookup when a consumer starts (as in the Go client).
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(1);
/// Bound for stopping the renewal and keep-alive schedulers during `close`.
const SCHEDULER_STOP: Duration = Duration::from_secs(2);
/// Bound for closing an owned pool at the end of `close`.
const POOL_CLOSE: Duration = Duration::from_secs(5);

/// A client handle to a PostgreMQ installation.
///
/// Cheap to clone; clones share one pool, one `LISTEN` session, one
/// lease-renewal scheduler and one keep-alive scheduler. Call
/// [`close`](Self::close) for a graceful shutdown. Without it, the background tasks stop (without
/// draining) once the last `Connection`, consumer and delivery are dropped.
#[derive(Clone)]
pub struct Connection {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) pool: PgPool,
    owns_pool: bool,
    /// The runtime the connection was created on: queue-fatal hooks are
    /// dispatched there even when signalled outside a runtime context.
    pub(crate) runtime: tokio::runtime::Handle,
    /// Client metrics (a no-op unless enabled).
    pub(crate) metrics: Metrics,
    pub(crate) options: ConnectionOptions,
    /// Cancelled when `close` begins: new work is rejected and consumers stop.
    pub(crate) draining: CancellationToken,
    /// Cancelled at the shutdown deadline (or the end of `close`): background
    /// I/O is abandoned.
    pub(crate) io: CancellationToken,
    closed: AtomicBool,
    /// Set by the first `close`: cancelled when the shutdown task finishes.
    closing: Mutex<Option<CancellationToken>>,
    registry: Mutex<Registry>,
    /// Per queue name, serializes this connection's declarations and
    /// deletions (see [`Inner::lifecycle_lock`]).
    lifecycle_locks: Mutex<HashMap<String, LifecycleLock>>,
    listener: Listener,
    renewals: Scheduler<RenewalSchedule>,
    /// Shared with the renewal schedule: deliveries bump it when they retire (see
    /// [`RenewalSchedule`]'s sweep).
    retired_leases: Arc<std::sync::atomic::AtomicUsize>,
    keepalive: Scheduler<KeepAliveSchedule>,
}

#[derive(Default)]
struct Registry {
    consumers: Vec<Arc<ConsumerShared>>,
    /// Queues declared or consumed through this connection.
    queues: HashMap<String, QueueMeta>,
    /// Completion tokens of handler consumers (cancelled when every handler
    /// has returned): `close` waits for them too.
    handlers: Vec<CancellationToken>,
    /// Queues this connection is deleting.
    deleting: HashMap<String, Deletion>,
    /// Queue incarnations already declared fatal (signalled once).
    fatal: HashSet<(String, Generation)>,
    /// Kept-alive incarnations this connection deleted, with when: a late
    /// keep-alive loss of one is that deletion, not a failure (its consumers
    /// still stop). Kept for [`DELETED_RETENTION`], well past any keep-alive
    /// round trip that could still report it.
    deleted: HashMap<(String, Generation), Instant>,
}

/// Finishes `close` on return *and* if the shutdown task panics: the
/// connection ends up closed (new work rejected, background I/O abandoned)
/// either way, and every `close` caller is released.
struct ShutdownExit {
    inner: Arc<Inner>,
    done: CancellationToken,
}

impl Drop for ShutdownExit {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!("connection shutdown panicked; background I/O abandoned");
        }
        self.inner.draining.cancel();
        self.inner.io.cancel();
        self.inner.closed.store(true, Ordering::Release);
        self.done.cancel();
    }
}

/// Who detected that a queue is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FatalSource {
    /// A consumer's claim found its queue incarnation missing (`PMQ02`).
    Consumer,
    /// The keep-alive scheduler found an exclusive queue gone.
    KeepAlive,
}

/// See [`Inner::lifecycle_lock`]. The per-name lock is removed once nobody
/// holds or waits for it.
pub(crate) struct LifecycleGuard<'a> {
    inner: &'a Inner,
    queue: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for LifecycleGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut locks = lock(&self.inner.lifecycle_locks);
        if let Some(entry) = locks.get_mut(&self.queue) {
            entry.users = entry.users.saturating_sub(1);
            if entry.users == 0 {
                locks.remove(&self.queue);
            }
        }
    }
}

/// One queue name's lifecycle lock and how many operations hold or wait for
/// it.
#[derive(Default)]
struct LifecycleLock {
    mutex: Arc<tokio::sync::Mutex<()>>,
    users: usize,
}

/// `delete_queue` calls in progress for one queue.
#[derive(Default)]
struct Deletion {
    calls: usize,
    /// The incarnation being deleted (as this connection declared it);
    /// only its keep-alive loss is held back, never a replacement's.
    target: Option<Generation>,
    /// A keep-alive loss held back meanwhile.
    suppressed: Option<Generation>,
    /// One of the calls deleted the queue: the loss was that deletion.
    deleted: bool,
}

/// See [`Inner::deleting`]. When the last concurrent `delete_queue` of a
/// queue ends and none of them deleted it (each failed or was cancelled), a
/// keep-alive loss held back meanwhile was not a deletion, and is signalled.
pub(crate) struct Deleting<'a> {
    inner: &'a Inner,
    queue: &'a str,
    /// The incarnation this connection knew when the delete began, and
    /// whether this connection kept it alive.
    target: Option<(Generation, bool)>,
}

impl Deleting<'_> {
    /// The queue was deleted: a loss held back meanwhile was this deletion.
    /// The connection forgets that incarnation only — a replacement declared
    /// while the response was in flight keeps its registration and
    /// keep-alive.
    pub(crate) fn succeeded(self) {
        let mut registry = lock(&self.inner.registry);
        if let Some(deletion) = registry.deleting.get_mut(self.queue) {
            deletion.deleted = true;
        }
        if let Some((target, exclusive)) = self.target {
            if registry
                .queues
                .get(self.queue)
                .is_some_and(|meta| meta.generation == target)
            {
                registry.queues.remove(self.queue);
            }
            lock(&self.inner.keepalive.state).deregister_generation(self.queue, target);
            if exclusive {
                // A keep-alive loss of the deleted incarnation — even one the
                // keep-alive scheduler already applied and is about to
                // dispatch — is this deletion: not reported (its consumers
                // still stop).
                let now = Instant::now();
                registry
                    .deleted
                    .retain(|_, at| now.saturating_duration_since(*at) < DELETED_RETENTION);
                registry
                    .deleted
                    .insert((self.queue.to_owned(), target), now);
            }
        }
        drop(registry);
    }
}

impl Drop for Deleting<'_> {
    fn drop(&mut self) {
        let unexplained = {
            let mut registry = lock(&self.inner.registry);
            let Some(deletion) = registry.deleting.get_mut(self.queue) else {
                return;
            };
            deletion.calls = deletion.calls.saturating_sub(1);
            if deletion.calls > 0 {
                return;
            }
            // The hold-back ends under this lock: a later loss is not held.
            registry
                .deleting
                .remove(self.queue)
                .filter(|deletion| !deletion.deleted)
                .and_then(|deletion| deletion.suppressed)
        };
        // Synchronous state changes; the hook goes to the connection's
        // runtime, so this works wherever the delete future is dropped.
        if let Some(generation) = unexplained {
            self.inner
                .queue_fatal(self.queue, generation, FatalSource::KeepAlive);
        }
    }
}

#[derive(Clone)]
struct QueueMeta {
    topic: String,
    generation: Generation,
    /// Declared exclusive by this connection (kept alive by it).
    exclusive: bool,
}

/// Fails, rather than letting sqlx panic, outside a Tokio runtime.
fn require_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current()
        .map(drop)
        .map_err(|_| Error::invalid("a Connection must be created within a Tokio runtime"))
}

impl Connection {
    /// Connects with a new pool that the connection owns (and closes in
    /// [`close`](Self::close)).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> postgremq::Result<()> {
    /// use postgremq::{Connection, ConnectionOptions};
    ///
    /// let conn = Connection::connect("postgres://localhost/app", ConnectionOptions::default()).await?;
    /// // ... publish and consume ...
    /// conn.close().await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for invalid options; [`Error::Sqlx`] if the pool
    /// cannot connect; [`Error::Incompatible`] if the database's
    /// `postgremq.info()` reports a protocol major not in
    /// [`SUPPORTED_PROTOCOL_MAJORS`](crate::SUPPORTED_PROTOCOL_MAJORS), or is
    /// missing (the database needs a PostgreMQ installation or upgrade).
    pub async fn connect(url: &str, options: ConnectionOptions) -> Result<Self> {
        options.retry.validate()?;
        require_runtime()?;
        // No ping before every checkout: it doubles the round trips of every
        // claim and settlement. Instead idle connections are recycled before
        // typical load-balancer/pooler idle cut-offs (~4-6 min), and an
        // operation that is safe to repeat retries a dropped connection.
        let pool = PgPoolOptions::new()
            .test_before_acquire(false)
            .idle_timeout(IDLE_TIMEOUT)
            .connect(url)
            .await?;
        if let Err(err) = crate::protocol::check(&pool).await {
            pool.close().await;
            return Err(err);
        }
        Self::build(pool, true, options)
    }

    /// Uses an existing pool, which the connection does not close. Must be
    /// called within a Tokio runtime.
    ///
    /// Every claim and settlement checks out a connection; sqlx's default
    /// `test_before_acquire(true)` adds a ping round trip to each, which
    /// roughly halves consume throughput. Consider
    /// [`PgPoolOptions::test_before_acquire(false)`](sqlx::postgres::PgPoolOptions::test_before_acquire)
    /// together with an
    /// [`idle_timeout`](sqlx::postgres::PgPoolOptions::idle_timeout) below
    /// your network's idle cut-off (this is what [`connect`](Self::connect)
    /// does); operations that are safe to repeat retry a dropped connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> postgremq::Result<()> {
    /// use postgremq::sqlx::postgres::PgPoolOptions;
    /// use postgremq::{Connection, ConnectionOptions};
    ///
    /// let pool = PgPoolOptions::new()
    ///     .test_before_acquire(false)
    ///     .idle_timeout(std::time::Duration::from_secs(180))
    ///     .connect("postgres://localhost/app")
    ///     .await?;
    /// let conn = Connection::from_pool(pool, ConnectionOptions::default()).await?;
    /// # let _ = conn;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for invalid options or outside a Tokio runtime;
    /// [`Error::Incompatible`] as for [`connect`](Self::connect). The pool is
    /// consumed even on error; pass a clone to keep using it (e.g. to call
    /// [`migrate`](crate::migrate) after `Error::Incompatible`);
    /// [`Error::Sqlx`] if the protocol check cannot reach the database.
    pub async fn from_pool(pool: PgPool, options: ConnectionOptions) -> Result<Self> {
        options.retry.validate()?;
        require_runtime()?;
        crate::protocol::check(&pool).await?;
        Self::build(pool, false, options)
    }

    fn build(pool: PgPool, owns_pool: bool, options: ConnectionOptions) -> Result<Self> {
        options.retry.validate()?;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return Err(Error::invalid(
                "a Connection must be created within a Tokio runtime",
            ));
        };
        let io = CancellationToken::new();
        #[cfg(feature = "otel")]
        let metrics = Metrics::new(options.meter.as_ref());
        #[cfg(not(feature = "otel"))]
        let metrics = Metrics::default();
        let inner = Arc::new_cyclic(|weak: &Weak<Inner>| {
            let schedule = RenewalSchedule::new(options.renewal_batch_size);
            let retired_leases = schedule.retired_counter();
            let renewals = {
                let (pool, retry, io) = (pool.clone(), options.retry.clone(), io.clone());
                let (flushes, losses) = (metrics.clone(), metrics.clone());
                spawn_scheduler(
                    "renewal",
                    schedule,
                    move |batch| {
                        renewal::flush(
                            pool.clone(),
                            retry.clone(),
                            io.clone(),
                            flushes.clone(),
                            batch,
                        )
                    },
                    // After the schedule's lock is released.
                    move |lost| renewal::on_lost(&losses, lost),
                )
            };
            let keepalive = {
                let (pool, retry, io) = (pool.clone(), options.retry.clone(), io.clone());
                let metrics = metrics.clone();
                let weak = weak.clone();
                spawn_scheduler(
                    "keepalive",
                    KeepAliveSchedule::default(),
                    move |batch| {
                        keepalive::flush(
                            pool.clone(),
                            retry.clone(),
                            io.clone(),
                            metrics.clone(),
                            batch,
                        )
                    },
                    move |lost: keepalive::LostQueue| {
                        if let Some(inner) = weak.upgrade() {
                            inner.queue_fatal(&lost.queue, lost.generation, FatalSource::KeepAlive);
                        }
                    },
                )
            };
            Inner {
                listener: Listener::start(&pool, options.notifications),
                pool,
                owns_pool,
                runtime,
                metrics,
                options,
                draining: CancellationToken::new(),
                io,
                closed: AtomicBool::new(false),
                closing: Mutex::new(None),
                registry: Mutex::new(Registry::default()),
                lifecycle_locks: Mutex::new(HashMap::new()),
                renewals,
                retired_leases,
                keepalive,
            }
        });
        Ok(Self { inner })
    }

    /// The underlying pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    /// Creates a topic (idempotent).
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for an invalid name; [`Error::Closed`] once
    /// closing; database errors after retries.
    pub async fn create_topic(&self, name: &str) -> Result<()> {
        let inner = &self.inner;
        inner.check_open()?;
        inner
            .retry(|| {
                checked_out(&inner.pool, async |conn| {
                    sqlx::query("SELECT postgremq.create_topic($1)")
                        .bind(name)
                        .execute(conn)
                        .await
                })
            })
            .await?;
        Ok(())
    }

    /// Creates a queue subscribed to `topic` and returns its generation.
    /// Re-declaring an existing queue with identical options returns its
    /// generation; different options are a validation error. An exclusive
    /// queue is kept alive by this connection while it is open.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
    /// use std::time::Duration;
    /// use postgremq::QueueOptions;
    ///
    /// conn.create_topic("orders").await?;
    /// conn.create_queue("billing", "orders", QueueOptions::default().max_delivery_attempts(5))
    ///     .await?;
    /// // A temporary queue that lives while this connection keeps it alive:
    /// conn.create_queue(
    ///     "orders-audit-tmp",
    ///     "orders",
    ///     QueueOptions::default()
    ///         .exclusive(true)
    ///         .keep_alive_interval(Duration::from_secs(30)),
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::QueueNotFound`] if the topic does not exist or an exclusive
    /// queue of that name expired; [`Error::Validation`] for invalid or
    /// conflicting options; [`Error::Closed`] once closing.
    pub async fn create_queue(
        &self,
        name: &str,
        topic: &str,
        options: QueueOptions,
    ) -> Result<Generation> {
        let inner = &self.inner;
        inner.check_open()?;
        // Declarations and deletions of one name run one at a time, so the
        // registration below follows the database's order.
        let _lifecycle = inner.lifecycle_lock(name).await;
        let max_attempts = i32::try_from(options.max_delivery_attempts)
            .map_err(|_| Error::invalid("max_delivery_attempts must fit a SQL integer"))?;
        if options.keep_alive_interval < MIN_KEEP_ALIVE_INTERVAL
            || options.keep_alive_interval > MAX_KEEP_ALIVE_INTERVAL
        {
            return Err(Error::invalid(
                "keep_alive_interval must be in [1s, 30 days]",
            ));
        }
        let interval_ms = duration_millis(options.keep_alive_interval);
        // `sent` is taken per attempt after a connection is acquired: the
        // keep-alive deadline is measured from it.
        let (generation, sent): (uuid::Uuid, Instant) = inner
            .retry(|| {
                checked_out(&inner.pool, async |conn| {
                    let sent = Instant::now();
                    let generation = sqlx::query_scalar(
                    "SELECT postgremq.create_queue($1, $2, $3, $4, $5 * interval '1 millisecond')",
                )
                .bind(name)
                .bind(topic)
                .bind(max_attempts)
                .bind(options.exclusive)
                .bind(interval_ms)
                .fetch_one(&mut *conn)
                .await?;
                    Ok((generation, sent))
                })
            })
            .await?;
        let generation = Generation::new(generation);
        let mut registry = lock(&inner.registry);
        registry.queues.insert(
            name.to_owned(),
            QueueMeta {
                topic: topic.to_owned(),
                generation,
                exclusive: options.exclusive,
            },
        );
        // A re-created incarnation can be consumed (and fail) anew; a late
        // signal for an older one stays deduplicated.
        registry
            .fatal
            .retain(|(queue, known)| queue != name || *known != generation);
        if options.exclusive {
            // Under the registry lock (registry, then keep-alive: the only
            // nesting order), so the two are updated as one.
            lock(&inner.keepalive.state).register(
                Arc::from(name),
                generation,
                options.keep_alive_interval,
                sent,
            );
        }
        drop(registry);
        if options.exclusive {
            inner.keepalive.wake();
        }
        Ok(generation)
    }

    /// Publishes `payload` to `topic` and returns the message ID.
    ///
    /// Retried only on SQLSTATE `40001`/`40P01` (an aborted transaction is the
    /// only proof that nothing was published). Any other failure, including a
    /// disconnect, has an unknown outcome and is returned as is.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
    /// use std::time::{Duration, SystemTime};
    /// use postgremq::PublishOptions;
    ///
    /// let order = serde_json::json!({ "id": 7, "total": 42 });
    /// conn.publish("orders", &order, PublishOptions::default()).await?;
    ///
    /// // Delivered in publish order with the other messages of customer 17:
    /// conn.publish("orders", &order, PublishOptions::default().group_key("customer-17"))
    ///     .await?;
    ///
    /// // Invisible for a minute:
    /// let later = SystemTime::now() + Duration::from_secs(60);
    /// conn.publish("orders", &order, PublishOptions::default().deliver_after(later))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Payload`] if `payload` cannot be serialized;
    /// [`Error::QueueNotFound`] if the topic does not exist;
    /// [`Error::Validation`] for an invalid group key or a `deliver_after`
    /// too far ahead; [`Error::Closed`] once
    /// closing; other database errors.
    // Not an `async fn`: the payload is serialized before the future is
    // created, so the future is `Send` even when `T` is not `Sync`.
    pub fn publish<'a, T: Serialize + ?Sized>(
        &'a self,
        topic: &'a str,
        payload: &T,
        options: PublishOptions,
    ) -> impl Future<Output = Result<MessageId>> + Send + use<'a, T> {
        let inner = &self.inner;
        let prepared = inner
            .check_open()
            .and_then(|()| options.validate())
            .and_then(|()| to_json(payload));
        async move {
            let timer = inner
                .metrics
                .operation(Operation::Publish, Some(topic), false);
            let published = async {
                let payload = prepared?;
                // Re-checked when first polled: the future may outlive `close`.
                inner.check_open()?;
                let id = inner
                    .bounded(with_retry(
                        &inner.options.retry,
                        &inner.io,
                        is_aborted_transaction,
                        || {
                            checked_out(&inner.pool, async |conn| {
                                // Each attempt counts, failed or not, and
                                // also if dropped mid-query.
                                let counted = inner.metrics.send_attempt(topic, false);
                                let attempt = publish_on(conn, topic, &payload, &options).await;
                                counted.finish(attempt.as_ref().err().map(sqlx_error_type));
                                attempt
                            })
                        },
                    ))
                    .await?;
                Ok(MessageId::new(id))
            }
            .await;
            timer.finish_with(&published);
            published
        }
    }

    /// Publishes inside the caller's transaction (`&mut tx` for a
    /// [`sqlx::Transaction`] coerces to the connection); the message exists
    /// only if that transaction commits. Never retried: the caller owns the
    /// transaction.
    ///
    /// # Errors
    ///
    /// As for [`publish`](Self::publish); a database error aborts the
    /// caller's transaction.
    pub fn publish_tx<'a, T: Serialize + ?Sized>(
        &'a self,
        conn: &'a mut PgConnection,
        topic: &'a str,
        payload: &T,
        options: PublishOptions,
    ) -> impl Future<Output = Result<MessageId>> + Send + use<'a, T> {
        let inner = &self.inner;
        let prepared = inner
            .check_open()
            .and_then(|()| options.validate())
            .and_then(|()| to_json(payload));
        async move {
            let timer = inner
                .metrics
                .operation(Operation::Publish, Some(topic), true);
            let published = async {
                let payload = prepared?;
                // Re-checked when first polled: the future may outlive `close`.
                inner.check_open()?;
                let counted = inner.metrics.send_attempt(topic, true);
                let attempt = publish_on(conn, topic, &payload, &options).await;
                counted.finish(attempt.as_ref().err().map(sqlx_error_type));
                Ok(MessageId::new(attempt?))
            }
            .await;
            timer.finish_with(&published);
            published
        }
    }

    /// Starts a consumer on `queue`.
    ///
    /// The consumer binds to [`ConsumeOptions::generation`], else to the
    /// generation this connection declared, else to the queue's current one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
    /// use std::num::NonZeroU32;
    /// use postgremq::ConsumeOptions;
    ///
    /// let mut consumer = conn
    ///     .consume("billing", ConsumeOptions::default().batch_size(NonZeroU32::MIN))
    ///     .await?;
    /// while let Some(delivery) = consumer.next().await {
    ///     let delivery = delivery?; // only `QueueGone` ends the stream with an error
    ///     let order: serde_json::Value = delivery.payload_as()?;
    ///     // ... process `order` ...
    ///     # let _ = order;
    ///     delivery.ack().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] for invalid options; [`Error::QueueNotFound`] if
    /// the queue (or the requested generation) does not exist;
    /// [`Error::Closed`] once closing.
    pub async fn consume(&self, queue: &str, options: ConsumeOptions) -> Result<Consumer> {
        let shared = self.start_consumer(queue, options, None).await?;
        Ok(Consumer::new(shared))
    }

    /// Starts a consumer task. `handlers_done` (for a handler consumer) is
    /// cancelled once its last handler returned; `close` waits for it too.
    pub(crate) async fn start_consumer(
        &self,
        queue: &str,
        options: ConsumeOptions,
        handlers_done: Option<&CancellationToken>,
    ) -> Result<Arc<ConsumerShared>> {
        let inner = &self.inner;
        inner.check_open()?;
        let settings = options.validate()?;
        let meta = inner.resolve_queue(queue, options.generation).await?;

        let shared = {
            let mut registry = lock(&inner.registry);
            // Checked under the registry lock: `close` either sees this
            // consumer in its snapshot or we see it draining.
            inner.check_open()?;
            let shared = Arc::new(ConsumerShared::new(
                Arc::from(queue),
                meta.generation,
                settings.prefetch,
                inner.draining.child_token(),
            ));
            registry.consumers.push(Arc::clone(&shared));
            if let Some(done) = handlers_done {
                registry.handlers.retain(|done| !done.is_cancelled());
                registry.handlers.push(done.clone());
            }
            shared
        };
        let task = Task {
            conn: Arc::clone(inner),
            shared: Arc::clone(&shared),
            settings,
            topic_wake: inner.listener.subscribe(format!("pmq:t:{}", meta.topic)),
            queue_wake: inner.listener.subscribe(format!("pmq:q:{queue}")),
        };
        tokio::spawn(
            task.run()
                .instrument(tracing::info_span!("postgremq.consumer", queue = %queue)),
        );
        Ok(shared)
    }

    /// Gracefully shuts down. Concurrent and repeated calls await the same
    /// shutdown.
    ///
    /// 1. New publishes, consumers and declarations are rejected.
    /// 2. Consumers stop fetching, release buffered never-delivered messages
    ///    and cancel the [`stopped`](crate::Delivery::stopped) token of
    ///    in-flight deliveries.
    /// 3. Settlement, lease renewal and queue keep-alive continue until every
    ///    in-flight delivery settles or
    ///    [`shutdown_timeout`](ConnectionOptions::shutdown_timeout) passes
    ///    (abandoned leases then expire normally).
    /// 4. The background tasks stop last, and an owned pool is closed.
    ///
    /// The shutdown runs on its own task: cancelling a `close` call does not
    /// cancel the shutdown.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> postgremq::Result<()> {
    /// use std::time::Duration;
    /// use postgremq::{Connection, ConnectionOptions};
    ///
    /// let conn = Connection::connect(
    ///     "postgres://localhost/app",
    ///     ConnectionOptions::default().shutdown_timeout(Duration::from_secs(30)),
    /// )
    /// .await?;
    /// // ... on SIGTERM: in-flight work gets up to 30s to settle.
    /// conn.close().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&self) {
        // Decide under the lock; spawn after releasing it.
        let (done, first) = self.inner.closing_token();
        if first {
            let exit = ShutdownExit {
                inner: Arc::clone(&self.inner),
                done: done.clone(),
            };
            tokio::spawn(
                async move {
                    exit.inner.shutdown().await;
                    drop(exit);
                }
                .in_current_span(),
            );
        }
        done.cancelled().await;
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("options", &self.inner.options)
            .field("closed", &self.inner.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// Serializes a payload for publishing.
fn to_json<T: Serialize + ?Sized>(payload: &T) -> Result<serde_json::Value> {
    serde_json::to_value(payload).map_err(Error::Payload)
}

/// Runs `publish_message` on any executor.
async fn publish_on<'e, E: PgExecutor<'e>>(
    executor: E,
    topic: &str,
    payload: &serde_json::Value,
    options: &PublishOptions,
) -> Result<i64, sqlx::Error> {
    // Named notation lets each optional argument keep its SQL default.
    const BASE: &str = "SELECT postgremq.publish_message($1, $2)";
    const DELAYED: &str = "SELECT postgremq.publish_message($1, $2, \
         p_deliver_after => 'epoch'::timestamptz + $3 * interval '1 microsecond')";
    const GROUPED: &str = "SELECT postgremq.publish_message($1, $2, p_group_key => $3)";
    const DELAYED_GROUPED: &str = "SELECT postgremq.publish_message($1, $2, \
         p_deliver_after => 'epoch'::timestamptz + $3 * interval '1 microsecond', \
         p_group_key => $4)";
    // Any time in the past means "visible now"; clamping keeps one before
    // PostgreSQL's timestamp range from being an error.
    let deliver_after = options.deliver_after.map(|at| to_unix_micros(at).max(0));
    let group_key = options.group_key.as_deref();
    let query = match (deliver_after, group_key) {
        (None, None) => sqlx::query_scalar(BASE).bind(topic).bind(Json(payload)),
        (Some(at), None) => sqlx::query_scalar(DELAYED)
            .bind(topic)
            .bind(Json(payload))
            .bind(at),
        (None, Some(key)) => sqlx::query_scalar(GROUPED)
            .bind(topic)
            .bind(Json(payload))
            .bind(key),
        (Some(at), Some(key)) => sqlx::query_scalar(DELAYED_GROUPED)
            .bind(topic)
            .bind(Json(payload))
            .bind(at)
            .bind(key),
    };
    query.fetch_one(executor).await
}

impl Inner {
    /// The token released when shutdown finishes, and whether this call is
    /// the first `close` (which starts the shutdown).
    fn closing_token(&self) -> (CancellationToken, bool) {
        let mut closing = lock(&self.closing);
        let first = closing.is_none();
        (
            closing.get_or_insert_with(CancellationToken::new).clone(),
            first,
        )
    }

    /// Rejects new work once `close` began.
    pub(crate) fn check_open(&self) -> Result<()> {
        if self.draining.is_cancelled() || self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    /// Rejects settlement once `close` finished draining.
    fn check_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    /// Runs `op` under the transient-error retry policy. At the forced
    /// shutdown deadline the operation is abandoned ([`Error::Closed`]): its
    /// outcome is then unknown, as with any interrupted statement.
    pub(crate) async fn retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, sqlx::Error>>,
    {
        self.bounded(with_retry(&self.options.retry, &self.io, is_retryable, op))
            .await
    }

    /// Runs a destructive or counting mutation (delete, purge, requeue,
    /// cleanup, maintenance) under the retry policy, but only for failures
    /// that prove a rollback (`40001`, `40P01`, `55P03`). A dropped connection,
    /// a connection exception or a shutdown may follow a commit, and replaying
    /// it could, for example, delete a queue recreated in between. (Stricter
    /// than the Go client, which also retries classes `08`/`57P0x` here.)
    pub(crate) async fn retry_mutation<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, sqlx::Error>>,
    {
        self.bounded(with_retry(
            &self.options.retry,
            &self.io,
            is_rolled_back,
            op,
        ))
        .await
    }

    /// Abandons `operation` at the forced shutdown deadline.
    /// Rejected once the connection closed; cancellation is checked before
    /// the operation is polled, so nothing is sent after `close`.
    pub(crate) async fn bounded<T>(
        &self,
        operation: impl Future<Output = Result<T, sqlx::Error>>,
    ) -> Result<T> {
        self.check_not_closed()?;
        tokio::select! {
            biased;
            () = self.io.cancelled() => Err(Error::Closed),
            result = operation => Ok(result?),
        }
    }

    async fn resolve_queue(&self, queue: &str, requested: Option<Generation>) -> Result<QueueMeta> {
        let cached = lock(&self.registry).queues.get(queue).cloned();
        // Bounded as in the Go client: a locked metadata table must not hold
        // consumer startups (and their pool connections) indefinitely.
        let lookup = self.retry(|| {
            checked_out(&self.pool, async |conn| {
                sqlx::query_as(
                    "SELECT topic_name::text, generation FROM postgremq.queues WHERE name = $1",
                )
                .bind(queue)
                .fetch_optional(conn)
                .await
            })
        });
        let row: Option<(String, uuid::Uuid)> = tokio::time::timeout(LOOKUP_TIMEOUT, lookup)
            .await
            .map_err(|_elapsed| {
                Error::Sqlx(sqlx::Error::Io(std::io::ErrorKind::TimedOut.into()))
            })??;
        let Some((topic, current)) = row else {
            return Err(Error::QueueNotFound {
                message: format!("queue {queue:?} does not exist").into(),
                source: None,
            });
        };
        let generation = requested
            .or(cached.as_ref().map(|meta| meta.generation))
            .unwrap_or(Generation::new(current));
        if generation.get() != current {
            return Err(Error::QueueNotFound {
                message: format!(
                    "queue {queue:?} generation {generation} no longer exists (the queue \
                     was recreated as {current}); declare it again with create_queue or \
                     pass ConsumeOptions::generation to bind the new incarnation"
                )
                .into(),
                source: None,
            });
        }
        Ok(QueueMeta {
            topic,
            generation,
            exclusive: cached.is_some_and(|meta| meta.exclusive),
        })
    }

    /// Registers a fetch's deliveries for renewal, under one lock.
    pub(crate) fn register_renewals(&self, registrations: Vec<Registration>) {
        if registrations.is_empty() {
            return;
        }
        lock(&self.renewals.state).register(registrations);
        self.renewals.wake();
    }

    /// Ends renewal of a finished delivery without taking the renewal schedule's
    /// lock (see [`SharedLease`]).
    pub(crate) fn retire_lease(&self, lease: &SharedLease) {
        if lease.retire() {
            self.retired_leases.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn stop_renewal(&self, key: &DeliveryKey) {
        lock(&self.renewals.state).deregister(key);
    }

    pub(crate) fn unregister_consumer(&self, shared: &Arc<ConsumerShared>) {
        lock(&self.registry)
            .consumers
            .retain(|consumer| !Arc::ptr_eq(consumer, shared));
    }

    /// A queue incarnation became unrecoverably gone. Idempotent per
    /// incarnation. A keep-alive signal for a generation other than the one
    /// this connection declared is stale and ignored; a consumer's own
    /// signal is authoritative for the generation it is bound to.
    pub(crate) fn queue_fatal(&self, queue: &str, generation: Generation, source: FatalSource) {
        let (consumers, stale, intentional) = {
            let mut registry = lock(&self.registry);
            // A signal for an incarnation other than the one this connection
            // declared (it was replaced): as in Go, it is not reported to the
            // hook. Its consumers still stop.
            let stale = registry
                .queues
                .get(queue)
                .is_some_and(|meta| meta.generation != generation);
            if source == FatalSource::KeepAlive {
                if stale {
                    return;
                }
                // Presumably this connection's own delete; held back in case
                // that delete fails.
                if let Some(deletion) = registry.deleting.get_mut(queue)
                    && deletion.target == Some(generation)
                {
                    deletion.suppressed = Some(generation);
                    return;
                }
            }
            if !registry.fatal.insert((queue.to_owned(), generation)) {
                return;
            }
            let consumers: Vec<Arc<ConsumerShared>> = registry
                .consumers
                .iter()
                .filter(|consumer| &*consumer.queue == queue && consumer.generation == generation)
                .cloned()
                .collect();
            let intentional = source == FatalSource::KeepAlive
                && registry
                    .deleted
                    .contains_key(&(queue.to_owned(), generation));
            drop(registry);
            (consumers, stale, intentional)
        };
        // Generation-qualified: a replacement queue keeps its keep-alive.
        lock(&self.keepalive.state).deregister_generation(queue, generation);
        for consumer in consumers {
            consumer.fail();
        }
        if stale || intentional {
            tracing::debug!(queue = %queue, "replaced or deleted queue incarnation is gone");
            return;
        }
        let queue: Arc<str> = Arc::from(queue);
        let Some(hook) = self.options.on_queue_fatal.clone() else {
            tracing::error!(queue = %queue, "queue is gone");
            return;
        };
        // The hook is application code: run it off the scheduler and consumer
        // tasks, so a slow or panicking hook cannot stall renewals.
        self.runtime.spawn(async move {
            let hook_queue = Arc::clone(&queue);
            let call = tokio::task::spawn_blocking(move || {
                let err = Error::QueueGone {
                    queue: Arc::clone(&hook_queue),
                };
                hook(&hook_queue, &err);
            });
            if let Err(err) = call.await {
                tracing::error!(
                    queue = %queue,
                    error = &err as &dyn std::error::Error,
                    "on_queue_fatal hook failed"
                );
            }
        });
    }

    /// Serializes this connection's declarations and deletions of `queue`:
    /// each holds the guard from before its SQL until its local registration
    /// is updated, so concurrent ones apply in the database's order. (A
    /// caller that drops one mid-statement leaves its outcome unknown.)
    pub(crate) async fn lifecycle_lock(&self, queue: &str) -> LifecycleGuard<'_> {
        // Counted as a user before waiting, and uncounted by the guard's
        // drop — also if this future is cancelled while waiting.
        let mutex = {
            let mut locks = lock(&self.lifecycle_locks);
            let entry = locks.entry(queue.to_owned()).or_default();
            entry.users += 1;
            Arc::clone(&entry.mutex)
        };
        let mut guard = LifecycleGuard {
            inner: self,
            queue: queue.to_owned(),
            guard: None,
        };
        guard.guard = Some(mutex.lock_owned().await);
        guard
    }

    /// Marks `queue` as being deleted by this connection until the guard
    /// drops: keep-alive omissions of it meanwhile are not fatal.
    pub(crate) fn deleting<'a>(&'a self, queue: &'a str) -> Deleting<'a> {
        let mut registry = lock(&self.registry);
        let target = registry
            .queues
            .get(queue)
            .map(|meta| (meta.generation, meta.exclusive));
        let deletion = registry.deleting.entry(queue.to_owned()).or_default();
        deletion.calls += 1;
        deletion.target = deletion.target.or(target.map(|(generation, _)| generation));
        drop(registry);
        Deleting {
            inner: self,
            queue,
            target,
        }
    }

    /// Runs one settlement or extension as a measured logical operation
    /// (one sample, retries included; nothing if never polled).
    ///
    /// `run` builds the operation's future only once the timer started, so
    /// the future is held once (not also as an argument).
    async fn measured<T, F>(
        &self,
        operation: Operation,
        key: &DeliveryKey,
        transaction: bool,
        run: impl FnOnce() -> F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let timer = self
            .metrics
            .operation(operation, Some(&key.queue), transaction);
        let result = run().await;
        timer.finish_with(&result);
        result
    }

    pub(crate) async fn ack(&self, key: &DeliveryKey) -> Result<()> {
        self.measured(Operation::Ack, key, false, || self.ack_sql(key))
            .await
    }

    pub(crate) async fn ack_tx(&self, conn: &mut PgConnection, key: &DeliveryKey) -> Result<()> {
        self.measured(Operation::Ack, key, true, move || {
            self.ack_tx_sql(conn, key)
        })
        .await
    }

    pub(crate) async fn nack(&self, key: &DeliveryKey, delay: Option<Duration>) -> Result<()> {
        self.measured(Operation::Nack, key, false, || self.nack_sql(key, delay))
            .await
    }

    pub(crate) async fn release(&self, key: &DeliveryKey) -> Result<()> {
        self.measured(Operation::Release, key, false, || self.release_sql(key))
            .await
    }

    /// Extends a lease and returns the new deadline (Unix µs). Independent
    /// of automatic renewal, as in the Go and TypeScript clients.
    pub(crate) async fn set_vt(&self, key: &DeliveryKey, secs: i32) -> Result<i64> {
        self.measured(Operation::Extend, key, false, || self.set_vt_sql(key, secs))
            .await
    }

    async fn ack_sql(&self, key: &DeliveryKey) -> Result<()> {
        self.check_not_closed()?;
        self.retry(|| {
            checked_out(&self.pool, async |conn| {
                sqlx::query("SELECT postgremq.ack_message($1, $2, $3)")
                    .bind(&*key.queue)
                    .bind(key.id.get())
                    .bind(&*key.token)
                    .execute(conn)
                    .await
            })
        })
        .await?;
        Ok(())
    }

    async fn ack_tx_sql(&self, conn: &mut PgConnection, key: &DeliveryKey) -> Result<()> {
        self.check_not_closed()?;
        sqlx::query("SELECT postgremq.ack_message($1, $2, $3)")
            .bind(&*key.queue)
            .bind(key.id.get())
            .bind(&*key.token)
            .execute(conn)
            .await?;
        Ok(())
    }

    async fn nack_sql(&self, key: &DeliveryKey, delay: Option<Duration>) -> Result<()> {
        self.check_not_closed()?;
        match delay.filter(|delay| !delay.is_zero()) {
            // The delay is applied with the server's clock.
            Some(delay) => {
                let delay_ms = duration_millis(delay);
                self.retry(|| {
                    checked_out(&self.pool, async |conn| {
                        sqlx::query(
                            "SELECT postgremq.nack_message($1, $2, $3, \
                         clock_timestamp() + $4 * interval '1 millisecond')",
                        )
                        .bind(&*key.queue)
                        .bind(key.id.get())
                        .bind(&*key.token)
                        .bind(delay_ms)
                        .execute(conn)
                        .await
                    })
                })
                .await?;
            }
            None => {
                self.retry(|| {
                    checked_out(&self.pool, async |conn| {
                        sqlx::query("SELECT postgremq.nack_message($1, $2, $3)")
                            .bind(&*key.queue)
                            .bind(key.id.get())
                            .bind(&*key.token)
                            .execute(conn)
                            .await
                    })
                })
                .await?;
            }
        }
        Ok(())
    }

    async fn release_sql(&self, key: &DeliveryKey) -> Result<()> {
        self.check_not_closed()?;
        self.retry(|| {
            checked_out(&self.pool, async |conn| {
                sqlx::query("SELECT postgremq.release_message($1, $2, $3)")
                    .bind(&*key.queue)
                    .bind(key.id.get())
                    .bind(&*key.token)
                    .execute(conn)
                    .await
            })
        })
        .await?;
        Ok(())
    }

    async fn set_vt_sql(&self, key: &DeliveryKey, secs: i32) -> Result<i64> {
        self.check_not_closed()?;
        let vt_micros = self
            .retry(|| {
                checked_out(&self.pool, async |conn| {
                    sqlx::query_scalar(
                        "SELECT (extract(epoch FROM postgremq.set_vt($1, $2, $3, $4)) \
                                 * 1000000)::int8",
                    )
                    .bind(&*key.queue)
                    .bind(key.id.get())
                    .bind(&*key.token)
                    .bind(secs)
                    .fetch_one(conn)
                    .await
                })
            })
            .await?;
        Ok(vt_micros)
    }

    async fn shutdown(&self) {
        // One overall deadline bounds every step (an unrepresentably long
        // timeout means none). Each later step gets at most what is left, but
        // never less than a brief moment to stop cleanly.
        let deadline = self
            .options
            .shutdown_timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        let budget = |cap: Duration| match deadline {
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .clamp(MIN_SHUTDOWN_STEP, cap.max(MIN_SHUTDOWN_STEP)),
            None => cap,
        };
        self.draining.cancel();
        let (consumers, handlers) = {
            let registry = lock(&self.registry);
            (registry.consumers.clone(), registry.handlers.clone())
        };
        // No more wake-ups: consumers drain what they already hold. A
        // black-holed LISTEN socket must not hold up the drain.
        if tokio::time::timeout(budget(LISTENER_STOP), self.listener.stop())
            .await
            .is_err()
        {
            tracing::warn!("notification listener did not stop in time");
        }
        for consumer in &consumers {
            consumer.shutdown.cancel();
        }
        // Consumers drain their in-flight deliveries; handler consumers also
        // wait for handlers still running after settling.
        let drained = async {
            for consumer in &consumers {
                consumer.finished.cancelled().await;
            }
            for handler in &handlers {
                handler.cancelled().await;
            }
        };
        match deadline {
            Some(deadline) => {
                if tokio::time::timeout_at(deadline, drained).await.is_err() {
                    tracing::warn!("shutdown timeout passed; abandoning in-flight deliveries");
                }
            }
            None => drained.await,
        }
        self.io.cancel();
        let exited = async {
            for consumer in &consumers {
                consumer.finished.cancelled().await;
            }
        };
        if tokio::time::timeout(budget(ABANDON_GRACE), exited)
            .await
            .is_err()
        {
            tracing::warn!("consumers did not exit after their work was abandoned");
        }
        self.closed.store(true, Ordering::Release);
        // Both are told to stop before either is awaited, so a slow one
        // cannot leave the other running; a flush in progress is bounded by
        // its own timeout, and the budget caps the wait for it.
        self.renewals.signal_stop();
        self.keepalive.signal_stop();
        if tokio::time::timeout(budget(SCHEDULER_STOP), async {
            self.renewals.stop().await;
            self.keepalive.stop().await;
        })
        .await
        .is_err()
        {
            tracing::warn!("background tasks did not stop in time");
        }
        if self.owns_pool
            && tokio::time::timeout(budget(POOL_CLOSE), self.pool.close())
                .await
                .is_err()
        {
            tracing::warn!("pool connections did not close in time");
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.draining.cancel();
        self.io.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// A connection whose pool never connects (nothing here reaches the
    /// database) and whose queue-fatal hook counts calls.
    fn connection() -> (Connection, Arc<AtomicUsize>) {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://127.0.0.1:1/none")
            .unwrap();
        let hooks = Arc::new(AtomicUsize::new(0));
        let options = ConnectionOptions::default().on_queue_fatal({
            let hooks = Arc::clone(&hooks);
            move |_, _| {
                hooks.fetch_add(1, Ordering::SeqCst);
            }
        });
        (Connection::build(pool, false, options).unwrap(), hooks)
    }

    fn declare(inner: &Inner, queue: &str) -> Generation {
        declare_as(inner, queue, false)
    }

    fn declare_as(inner: &Inner, queue: &str, exclusive: bool) -> Generation {
        let generation = Generation::new(uuid::Uuid::new_v4());
        lock(&inner.registry).queues.insert(
            queue.to_owned(),
            QueueMeta {
                topic: "t".to_owned(),
                generation,
                exclusive,
            },
        );
        generation
    }

    /// Declares `queue` as exclusive (kept alive; the long interval keeps
    /// the scheduler from flushing during the test).
    fn declare_exclusive(inner: &Inner, queue: &str) -> Generation {
        let generation = declare_as(inner, queue, true);
        lock(&inner.keepalive.state).register(
            Arc::from(queue),
            generation,
            Duration::from_secs(600),
            Instant::now(),
        );
        generation
    }

    /// Waits (up to 5 s) for the hook to have run at least `expected` times,
    /// then lets any surplus call land, and reads the count.
    async fn hook_calls(hooks: &AtomicUsize, expected: usize) -> usize {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while hooks.load(Ordering::SeqCst) < expected && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        hooks.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn a_keep_alive_loss_during_a_successful_delete_is_not_signalled() {
        let (conn, hooks) = connection();
        let generation = declare(&conn.inner, "q");
        let deleting = conn.inner.deleting("q");
        conn.inner
            .queue_fatal("q", generation, FatalSource::KeepAlive);
        deleting.succeeded();
        assert_eq!(hook_calls(&hooks, 0).await, 0);
    }

    #[tokio::test]
    async fn a_keep_alive_loss_during_a_failed_delete_is_signalled_once_it_ends() {
        let (conn, hooks) = connection();
        let generation = declare(&conn.inner, "q");
        let deleting = conn.inner.deleting("q");
        conn.inner
            .queue_fatal("q", generation, FatalSource::KeepAlive);
        assert_eq!(hook_calls(&hooks, 0).await, 0, "held back while deleting");
        drop(deleting);
        assert_eq!(hook_calls(&hooks, 1).await, 1);
    }

    #[tokio::test]
    async fn a_replacement_incarnations_loss_is_not_held_back_by_a_delete() {
        let (conn, hooks) = connection();
        declare(&conn.inner, "q");
        let deleting = conn.inner.deleting("q");
        // Re-declared while the delete's response is in flight.
        let replacement = declare(&conn.inner, "q");
        conn.inner
            .queue_fatal("q", replacement, FatalSource::KeepAlive);
        assert_eq!(hook_calls(&hooks, 1).await, 1);
        deleting.succeeded();
    }

    #[tokio::test]
    async fn a_loss_reported_after_a_successful_delete_ended_is_not_signalled() {
        let (conn, hooks) = connection();
        let generation = declare_exclusive(&conn.inner, "q");
        conn.inner.deleting("q").succeeded();
        // The keep-alive saw the deletion; its result is dispatched late.
        conn.inner
            .queue_fatal("q", generation, FatalSource::KeepAlive);
        assert_eq!(hook_calls(&hooks, 0).await, 0);
    }

    #[tokio::test]
    async fn lifecycle_operations_on_one_name_run_one_at_a_time() {
        let (conn, _hooks) = connection();
        let first = conn.inner.lifecycle_lock("q").await;
        // Another name is independent.
        drop(conn.inner.lifecycle_lock("other").await);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), conn.inner.lifecycle_lock("q"))
                .await
                .is_err(),
            "the second operation waits"
        );
        drop(first);
        drop(conn.inner.lifecycle_lock("q").await);
        assert!(
            lock(&conn.inner.lifecycle_locks).is_empty(),
            "unused locks are removed"
        );
    }

    #[tokio::test]
    async fn a_loss_already_applied_by_keep_alive_before_the_delete_ended_is_not_signalled() {
        let (conn, hooks) = connection();
        let generation = declare_exclusive(&conn.inner, "q");
        let deleting = conn.inner.deleting("q");
        // The keep-alive saw the queue gone and dropped its entry; its loss
        // is queued but not dispatched yet.
        assert!(lock(&conn.inner.keepalive.state).deregister_generation("q", generation));
        deleting.succeeded();
        conn.inner
            .queue_fatal("q", generation, FatalSource::KeepAlive);
        assert_eq!(hook_calls(&hooks, 0).await, 0);
    }

    #[tokio::test]
    async fn a_waiter_cancelled_after_the_owner_left_does_not_strand_the_lock() {
        let (conn, _hooks) = connection();
        let owner = conn.inner.lifecycle_lock("q").await;
        let mut waiter = Box::pin(conn.inner.lifecycle_lock("q"));
        assert!(futures_poll_once(waiter.as_mut()).await.is_none(), "waits");
        drop(owner);
        // Cancelled before it resumes to take the lock.
        drop(waiter);
        assert!(lock(&conn.inner.lifecycle_locks).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn deleted_incarnations_are_forgotten_after_their_retention() {
        let (conn, _hooks) = connection();
        declare_exclusive(&conn.inner, "a");
        conn.inner.deleting("a").succeeded();
        tokio::time::advance(DELETED_RETENTION + Duration::from_secs(1)).await;
        declare_exclusive(&conn.inner, "b");
        conn.inner.deleting("b").succeeded();
        let deleted = &lock(&conn.inner.registry).deleted;
        assert_eq!(deleted.len(), 1);
        assert!(deleted.keys().all(|(queue, _)| queue == "b"));
    }

    /// Polls `future` once: its output if ready.
    async fn futures_poll_once<F: Future + Unpin>(mut future: F) -> Option<F::Output> {
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match std::pin::Pin::new(&mut future).poll(cx) {
                std::task::Poll::Ready(output) => Some(output),
                std::task::Poll::Pending => None,
            })
        })
        .await
    }

    #[tokio::test]
    async fn a_replaced_incarnation_gone_is_not_reported_to_the_hook() {
        let (conn, hooks) = connection();
        let old = declare(&conn.inner, "q");
        declare(&conn.inner, "q");
        conn.inner.queue_fatal("q", old, FatalSource::Consumer);
        assert_eq!(hook_calls(&hooks, 0).await, 0);
    }
}
