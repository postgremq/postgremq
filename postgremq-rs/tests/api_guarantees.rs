//! Rust-specific API guarantees: Send futures, cancel safety, Drop semantics,
//! concurrent settlement, stream fusing, Debug redaction, option validation
//! and the error contract.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::error::Error as _;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{TestDb, unique, within};
use futures_core::Stream;
use futures_core::stream::FusedStream as _;
use postgremq::{
    Connection, ConnectionOptions, ConsumeOptions, Consumer, Delivery, ErrorKind, PublishOptions,
    QueueOptions, RetryConfig,
};
use sqlx::PgConnection;

fn assert_send<T: Send>(_: T) {}

/// Never called: it only has to compile. Every public future must be `Send`
/// so applications can `tokio::spawn` it.
#[allow(dead_code, reason = "compile-time check only")]
fn public_futures_are_send(
    conn: &Connection,
    consumer: &mut Consumer,
    delivery: &Delivery,
    tx: &mut PgConnection,
) {
    assert_send(conn.publish("t", &1, PublishOptions::default()));
    assert_send(conn.publish_tx(tx, "t", &1, PublishOptions::default()));
    assert_send(conn.consume("q", ConsumeOptions::default()));
    assert_send(conn.consume_with_handler(
        "q",
        ConsumeOptions::default(),
        Some(NonZeroUsize::MIN),
        |_d: Delivery| async { Ok(()) },
    ));
    assert_send(conn.create_queue("q", "t", QueueOptions::default()));
    assert_send(conn.close());
    assert_send(Connection::connect("", ConnectionOptions::default()));
    assert_send(Connection::from_pool(
        conn.pool().clone(),
        ConnectionOptions::default(),
    ));
    assert_send(consumer.next());
    assert_send(consumer.stop());
    assert_send(delivery.ack());
    assert_send(delivery.ack_tx(tx));
    assert_send(delivery.nack(None));
    assert_send(delivery.release());
    assert_send(delivery.extend(std::num::NonZeroU32::new(1).unwrap()));
}

#[test]
fn public_futures_compile_as_send() {
    let _checked = public_futures_are_send;
}

async fn setup(db: &TestDb) -> (Connection, String, String) {
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    (conn, topic, queue)
}

async fn rows(db: &TestDb, queue: &str) -> Vec<(String, i32)> {
    sqlx::query_as(
        "SELECT status::text, delivery_attempts FROM postgremq.queue_messages \
         WHERE queue_name = $1 ORDER BY message_id",
    )
    .bind(queue)
    .fetch_all(&db.pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn next_is_cancel_safe_in_select() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();

    // Abandon several `next()` calls while nothing is there.
    for _ in 0..3 {
        tokio::select! {
            item = consumer.next() => panic!("unexpected {item:?}"),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    assert_eq!(
        (delivery.message_id(), delivery.delivery_attempts()),
        (id, 1)
    );
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_consumer_releases_its_buffered_rows() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    for n in 0..5 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().batch_size(NonZeroU32::new(5).unwrap()),
        )
        .await
        .unwrap();
    within(5, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .ack()
        .await
        .unwrap();
    drop(consumer);

    // Settled once every buffered row is back, unclaimed: a prefetch still in
    // flight at the drop may re-claim rows between releases (its rows are
    // released too), so "nothing processing" alone can be momentary.
    within(5, async {
        while !rows(&db, &queue)
            .await
            .iter()
            .skip(1)
            .all(|row| row == &("pending".to_owned(), 0))
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let rows = rows(&db, &queue).await;
    assert_eq!(rows[0], ("completed".to_owned(), 1));
    for row in &rows[1..] {
        assert_eq!(row, &("pending".to_owned(), 0));
    }
    within(5, conn.close()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_settlements_have_exactly_one_winner() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = Arc::new(within(5, consumer.next()).await.unwrap().unwrap());

    // The winner's SQL is held up by a table lock, so the loser runs while
    // the winner is still in flight: it must lose on the client, without
    // SQL of its own (a server-side loss would carry a database source).
    let mut locker = db.pool.acquire().await.unwrap().detach();
    let mut lock = sqlx::Connection::begin(&mut locker).await.unwrap();
    sqlx::query("LOCK TABLE postgremq.queue_messages IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let winner = tokio::spawn({
        let delivery = Arc::clone(&delivery);
        async move { delivery.ack().await }
    });
    within(5, async {
        while !delivery.is_settled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let lost = within(1, delivery.nack(None)).await.unwrap_err();
    assert_eq!(lost.kind(), ErrorKind::LeaseLost);
    assert!(
        lost.source().is_none(),
        "lost without running SQL: {lost:?}"
    );
    assert!(!winner.is_finished(), "the winner is still in flight");

    lock.rollback().await.unwrap();
    within(5, winner).await.unwrap().unwrap();
    assert_eq!(rows(&db, &queue).await[0].0, "completed");
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_settle_future_still_ends_tracking() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();

    // The ack blocks on a table lock, so the timeout certainly drops it
    // before the SQL answers: the first settlement still owns the outcome and
    // tracking ends, so stop() does not wait for it.
    let mut locker = db.pool.acquire().await.unwrap().detach();
    let mut lock = sqlx::Connection::begin(&mut locker).await.unwrap();
    sqlx::query("LOCK TABLE postgremq.queue_messages IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), delivery.ack())
            .await
            .is_err(),
        "the ack must still be blocked when dropped"
    );
    lock.rollback().await.unwrap();
    assert!(delivery.is_settled());
    assert_eq!(
        delivery.ack().await.unwrap_err().kind(),
        ErrorKind::LeaseLost
    );
    within(5, consumer.stop()).await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_stream_is_fused_after_queue_gone() {
    let db = TestDb::new().await;
    let (conn, _topic, queue) = setup(&db).await;
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    conn.delete_queue(&queue).await.unwrap();

    let first = within(
        5,
        std::future::poll_fn(|cx| Pin::new(&mut consumer).poll_next(cx)),
    )
    .await;
    assert_eq!(first.unwrap().unwrap_err().kind(), ErrorKind::QueueGone);
    assert!(!consumer.is_terminated(), "the error is not the end yet");
    for _ in 0..3 {
        assert!(within(5, consumer.next()).await.is_none());
        let polled = within(
            5,
            std::future::poll_fn(|cx| Pin::new(&mut consumer).poll_next(cx)),
        )
        .await;
        assert!(polled.is_none());
        assert!(consumer.is_terminated());
    }
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn debug_output_hides_payloads() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    conn.publish(&topic, &"s3cret-payload", PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    let shown = format!("{delivery:?} {conn:?} {consumer:?}");
    assert!(!shown.contains("s3cret"), "{shown}");
    assert!(!shown.contains("password"), "{shown}");
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_options_are_rejected_without_panicking() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    let consume = |options: ConsumeOptions| {
        let conn = conn.clone();
        let queue = queue.clone();
        async move { conn.consume(&queue, options).await.map(drop) }
    };
    for options in [
        ConsumeOptions::default().extension_threshold(f64::NAN),
        ConsumeOptions::default().extension_threshold(0.0),
        ConsumeOptions::default().extension_threshold(1.0),
        ConsumeOptions::default().check_timeout(Duration::ZERO),
        ConsumeOptions::default().check_timeout(Duration::from_millis(9)),
        ConsumeOptions::default().check_timeout(Duration::MAX),
        ConsumeOptions::default().vt_secs(NonZeroU32::MAX),
        ConsumeOptions::default().batch_size(NonZeroU32::MAX),
    ] {
        let err = consume(options.clone()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Validation, "{options:?}");
    }
    for options in [
        QueueOptions::default().keep_alive_interval(Duration::ZERO),
        QueueOptions::default().keep_alive_interval(Duration::from_millis(999)),
        QueueOptions::default().keep_alive_interval(Duration::MAX),
        QueueOptions::default().max_delivery_attempts(u32::MAX),
    ] {
        let err = conn
            .create_queue(&unique("q"), &topic, options.clone())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Validation, "{options:?}");
    }
    for retry in [
        RetryConfig::default().multiplier(f64::NAN),
        RetryConfig::default().initial_backoff(Duration::ZERO),
    ] {
        let err = Connection::from_pool(db.pool.clone(), ConnectionOptions::default().retry(retry))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Validation);
    }
    for hours in [u32::MAX, 1_000_000] {
        assert_eq!(
            conn.cleanup_completed_messages(hours, NonZeroU32::MIN)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Validation
        );
        assert_eq!(
            conn.cleanup_unreferenced_messages(hours, NonZeroU32::MIN)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Validation
        );
    }
    let far = std::time::SystemTime::now() + Duration::from_secs(200 * 365 * 86_400);
    assert_eq!(
        conn.publish(&topic, &1, PublishOptions::default().deliver_after(far))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Validation
    );

    // A delay the server cannot represent is rejected before the settlement
    // is claimed: the delivery can still be settled.
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    assert_eq!(
        delivery.nack(Some(Duration::MAX)).await.unwrap_err().kind(),
        ErrorKind::Validation
    );
    assert!(!delivery.is_settled());
    delivery.ack().await.unwrap();
    within(5, consumer.stop()).await;

    // An unrepresentably long shutdown timeout means "no deadline".
    let patient = db
        .connect(ConnectionOptions::default().shutdown_timeout(Duration::MAX))
        .await;
    within(5, patient.close()).await;
    assert_eq!(
        patient.create_topic(&topic).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_handler_consumer_cancels_and_nacks_running_handlers() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let started = Arc::new(tokio::sync::Notify::new());
    let handler_consumer = conn
        .consume_with_handler(
            &queue,
            ConsumeOptions::default(),
            Some(NonZeroUsize::MIN),
            {
                let (saw_cancel, started) = (Arc::clone(&saw_cancel), Arc::clone(&started));
                move |delivery: Delivery| {
                    let (saw_cancel, started) = (Arc::clone(&saw_cancel), Arc::clone(&started));
                    async move {
                        started.notify_one();
                        delivery.stopped().cancelled().await;
                        saw_cancel.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();
    within(5, started.notified()).await;
    drop(handler_consumer);

    within(5, async {
        while rows(&db, &queue).await[0].0 != "pending" {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(saw_cancel.load(Ordering::SeqCst));
    // A cancelled return is nacked: the attempt counts.
    assert_eq!(rows(&db, &queue).await[0], ("pending".to_owned(), 1));
    within(5, conn.close()).await;
}

async fn listen_sessions(db: &TestDb) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
           AND (query ILIKE 'LISTEN %' OR query ILIKE 'UNLISTEN %')",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_every_handle_without_close_stops_the_listener() {
    let db = TestDb::new().await;
    let conn = db.connect_owned(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    within(5, async {
        while listen_sessions(&db).await == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    drop(consumer);
    drop(conn);
    within(10, async {
        while listen_sessions(&db).await > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_lost_errors_keep_their_source_and_sqlstate() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    sqlx::query(
        "UPDATE postgremq.queue_messages SET consumer_token = 'other' WHERE queue_name = $1",
    )
    .bind(&queue)
    .execute(&db.pool)
    .await
    .unwrap();

    let server = delivery.ack().await.unwrap_err();
    assert_eq!(server.kind(), ErrorKind::LeaseLost);
    assert!(server.source().is_some());
    assert_eq!(server.sqlstate().as_deref(), Some("PMQ01"));
    assert!(server.to_string().starts_with("lease lost"));

    let client = delivery.ack().await.unwrap_err();
    assert_eq!(client.kind(), ErrorKind::LeaseLost);
    assert!(client.source().is_none());
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_future_polled_after_close_is_rejected() {
    let db = TestDb::new().await;
    let (conn, topic, _queue) = setup(&db).await;
    let early = conn.publish(&topic, &1, PublishOptions::default());
    let mut tx = db.pool.begin().await.unwrap();
    let early_tx = conn.publish_tx(&mut tx, &topic, &2, PublishOptions::default());
    conn.close().await;
    assert_eq!(early.await.unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(early_tx.await.unwrap_err().kind(), ErrorKind::Closed);
    tx.rollback().await.unwrap();
    // Admin calls after close are rejected without touching the database.
    assert_eq!(
        conn.list_queues().await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    assert_eq!(
        conn.delete_queue("anything").await.unwrap_err().kind(),
        ErrorKind::Closed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn close_waits_for_handlers_that_settled_but_are_still_running() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let consumer = conn
        .consume_with_handler(&queue, ConsumeOptions::default(), None, {
            let started = Arc::clone(&started);
            let finished = Arc::clone(&finished);
            move |delivery: Delivery| {
                let started = Arc::clone(&started);
                let finished = Arc::clone(&finished);
                async move {
                    let stopped = delivery.stopped();
                    delivery.ack().await?;
                    started.notify_one();
                    // Settled, but still working until told to stop.
                    stopped.cancelled().await;
                    finished.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
    within(5, started.notified()).await;
    within(5, conn.close()).await;
    assert!(
        finished.load(Ordering::SeqCst),
        "close returned before the handler did"
    );
    drop(consumer);
}
