//! The connection's `LISTEN` session.
//!
//! One task owns a dedicated connection (its own single-connection pool built
//! from the main pool's connect options — never a connection borrowed from the
//! shared pool, and never through a transaction-pooling proxy's transaction).
//! Subscriptions are reference counted per channel (`pmq:t:<topic>`,
//! `pmq:q:<queue>`); the task reconciles the channels it `LISTEN`s to with the
//! subscribed set. Each subscriber owns a [`Notify`]: a wake-up stores a permit
//! when nobody is waiting, so a notification that arrives while a consumer is
//! busy fetching is not lost.
//!
//! Notifications are hints. Once a new session's `LISTEN`s are in place all
//! subscribers are woken, since notifications sent while the session was down
//! are gone; likewise a channel's subscribers are woken when it is first
//! listened to (a message published before that is never notified). Polling
//! every `check_timeout` remains the correctness fallback. The task is started
//! once, connects when the first channel is subscribed, and retries with
//! bounded backoff for the connection's lifetime.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::sync::lock;

const MIN_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Channel → subscribers.
#[derive(Debug, Default)]
struct Shared {
    subscribers: Mutex<HashMap<String, Vec<Weak<Notify>>>>,
    /// Signalled when the subscribed channel set changes.
    changed: Notify,
}

impl Shared {
    fn wake(&self, channel: &str) {
        let subscribers = lock(&self.subscribers);
        for notify in subscribers.get(channel).into_iter().flatten() {
            if let Some(notify) = notify.upgrade() {
                notify.notify_one();
            }
        }
    }

    fn wake_all(&self) {
        let subscribers = lock(&self.subscribers);
        for notify in subscribers.values().flatten() {
            if let Some(notify) = notify.upgrade() {
                notify.notify_one();
            }
        }
    }

    fn channels(&self) -> HashSet<String> {
        lock(&self.subscribers).keys().cloned().collect()
    }
}

/// The connection's notification listener.
#[derive(Debug)]
pub(crate) struct Listener {
    shared: Option<Arc<Shared>>,
    stop: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    pool: Option<PgPool>,
}

/// A subscription to one channel; dropping it unsubscribes.
#[derive(Debug)]
pub(crate) struct Subscription {
    channel: String,
    notify: Arc<Notify>,
    shared: Option<Arc<Shared>>,
}

impl Subscription {
    /// Resolves at the next wake-up (immediately if one is stored).
    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let Some(shared) = &self.shared else {
            return;
        };
        let mut subscribers = lock(&shared.subscribers);
        let Some(list) = subscribers.get_mut(&self.channel) else {
            return;
        };
        list.retain(|weak| {
            weak.upgrade()
                .is_some_and(|n| !Arc::ptr_eq(&n, &self.notify))
        });
        if list.is_empty() {
            subscribers.remove(&self.channel);
            drop(subscribers);
            shared.changed.notify_one();
        }
    }
}

impl Listener {
    /// Starts the listener task, or a no-op listener when `enabled` is false.
    pub(crate) fn start(main_pool: &PgPool, enabled: bool) -> Self {
        let stop = CancellationToken::new();
        if !enabled {
            return Self {
                shared: None,
                stop,
                task: Mutex::new(None),
                pool: None,
            };
        }
        let options = (*main_pool.connect_options()).clone();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_lazy_with(options);
        let shared = Arc::new(Shared::default());
        let task = tokio::spawn(
            run(pool.clone(), Arc::clone(&shared), stop.clone())
                .instrument(tracing::info_span!("postgremq.listener")),
        );
        Self {
            shared: Some(shared),
            stop,
            task: Mutex::new(Some(task)),
            pool: Some(pool),
        }
    }

    /// Subscribes to `channel`.
    pub(crate) fn subscribe(&self, channel: String) -> Subscription {
        let notify = Arc::new(Notify::new());
        if let Some(shared) = &self.shared {
            let mut subscribers = lock(&shared.subscribers);
            let list = subscribers.entry(channel.clone()).or_default();
            list.push(Arc::downgrade(&notify));
            let first = list.len() == 1;
            drop(subscribers);
            if first {
                shared.changed.notify_one();
            }
        }
        Subscription {
            channel,
            notify,
            shared: self.shared.clone(),
        }
    }

    /// Stops the task and closes the dedicated connection.
    ///
    /// If this future is dropped first (the shutdown deadline), the task is
    /// aborted rather than left running. Dropping its session then makes
    /// sqlx send an `UNLISTEN *` in the background, which a stalled socket
    /// can hold up until the operating system gives the connection up.
    pub(crate) async fn stop(&self) {
        self.stop.cancel();
        let task = lock(&self.task).take();
        let _abort = task.as_ref().map(|task| AbortOnDrop(task.abort_handle()));
        if let Some(task) = task
            && let Err(err) = task.await
            && !err.is_cancelled()
        {
            tracing::error!(
                error = &err as &dyn std::error::Error,
                "listener task failed"
            );
        }
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
    }
}

/// Aborts a task when dropped (a no-op once it finished).
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn run(pool: PgPool, shared: Arc<Shared>, stop: CancellationToken) {
    let mut listener: Option<PgListener> = None;
    let mut listening: HashSet<String> = HashSet::new();
    // A new session whose `LISTEN`s are not yet all in place.
    let mut fresh = false;
    let mut backoff = MIN_BACKOFF;
    while !stop.is_cancelled() {
        let Some(session) = listener.as_mut() else {
            // Connect only once something subscribes: a publish-only
            // connection holds no `LISTEN` session.
            if lock(&shared.subscribers).is_empty() {
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = shared.changed.notified() => {}
                }
                continue;
            }
            let connected = tokio::select! {
                () = stop.cancelled() => break,
                connected = PgListener::connect_with(&pool) => connected,
            };
            match connected {
                Ok(mut session) => {
                    session.ignore_pool_close_event(true);
                    // Reconnects are ours (a fresh session below), never
                    // sqlx's in-call reconnect: cancelling `try_recv` mid
                    // reconnect could lose the session's notification buffer.
                    session.eager_reconnect(false);
                    listener = Some(session);
                    listening.clear();
                    fresh = true;
                }
                Err(err) => {
                    tracing::warn!(
                        error = &err as &dyn std::error::Error,
                        retry_in = ?backoff,
                        "notification listener cannot connect"
                    );
                    backoff = pause(&stop, backoff).await;
                }
            }
            continue;
        };

        let reconciled = tokio::select! {
            () = stop.cancelled() => break,
            result = reconcile(session, &shared, &mut listening) => result,
        };
        match reconciled {
            // Anything sent while we were not listening is lost: wake
            // everyone, but only now that the `LISTEN`s are in place (a
            // consumer woken earlier could re-check before them and miss a
            // message published in between).
            Ok(_) if fresh => {
                fresh = false;
                shared.wake_all();
            }
            Ok(added) => {
                for channel in &added {
                    shared.wake(channel);
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = &err as &dyn std::error::Error,
                    "notification listener lost its session"
                );
                listener = None;
                backoff = pause(&stop, backoff).await;
                continue;
            }
        }

        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            () = shared.changed.notified() => {}
            // With an established connection `try_recv` only reads, and
            // reads consume whole messages (sqlx-postgres 0.9
            // src/connection/stream.rs `recv_unchecked`: "this should be
            // cancel-safe"), so it may be cancelled when the subscription set
            // changes. Re-check on every sqlx upgrade.
            received = session.try_recv() => match received {
                Ok(Some(notification)) => {
                    backoff = MIN_BACKOFF;
                    shared.wake(notification.channel());
                }
                // The connection was lost: rebuild the session (which wakes
                // everyone, since notifications in the gap are gone).
                Ok(None) => {
                    tracing::warn!("notification listener lost its connection");
                    listener = None;
                    // A server that accepts and then drops connections must
                    // not make this a tight reconnect loop.
                    backoff = pause(&stop, backoff).await;
                }
                Err(err) => {
                    tracing::warn!(
                        error = &err as &dyn std::error::Error,
                        "notification listener failed"
                    );
                    listener = None;
                    backoff = pause(&stop, backoff).await;
                }
            },
        }
    }
    // Close the dedicated connection even when the task ends because every
    // handle was dropped (not only on `Listener::stop`).
    drop(listener);
    pool.close().await;
}

/// Brings the session's `LISTEN`s in line with the subscribed channels and
/// returns the channels newly listened to.
async fn reconcile(
    session: &mut PgListener,
    shared: &Shared,
    listening: &mut HashSet<String>,
) -> Result<Vec<String>, sqlx::Error> {
    let wanted = shared.channels();
    let added: Vec<String> = wanted.difference(listening).cloned().collect();
    for channel in &added {
        session.listen(channel).await?;
        listening.insert(channel.clone());
    }
    for channel in listening.difference(&wanted).cloned().collect::<Vec<_>>() {
        session.unlisten(&channel).await?;
        listening.remove(&channel);
    }
    Ok(added)
}

/// Sleeps for `backoff` (unless stopped) and returns the next backoff.
async fn pause(stop: &CancellationToken, backoff: Duration) -> Duration {
    tokio::select! {
        () = stop.cancelled() => {}
        () = tokio::time::sleep(backoff) => {}
    }
    backoff.saturating_mul(2).min(MAX_BACKOFF)
}
