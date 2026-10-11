//! Shutdown, queue-gone teardown, exclusive-queue keep-alive and handlers.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{TestDb, unique, within};
use postgremq::{
    ConnectionOptions, ConsumeOptions, Delivery, ErrorKind, PublishOptions, QueueOptions,
};

async fn attempts(db: &TestDb, queue: &str) -> Vec<(String, i32)> {
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
async fn close_drains_in_flight_work_and_releases_buffered_rows() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
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
    // One handed out; four claimed into the buffer but never delivered.
    let in_flight = within(5, consumer.next()).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let closing = tokio::spawn({
        let conn = conn.clone();
        async move { conn.close().await }
    });
    within(5, in_flight.stopped().cancelled()).await;
    assert!(!closing.is_finished(), "close must wait for in-flight work");
    // Settlement still works while draining; new work is rejected.
    assert_eq!(
        conn.publish(&topic, &9, PublishOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Closed
    );
    in_flight.ack().await.unwrap();
    within(10, closing).await.unwrap();

    let rows = attempts(&db, &queue).await;
    assert_eq!(rows[0], ("completed".to_owned(), 1));
    for row in &rows[1..] {
        assert_eq!(
            row,
            &("pending".to_owned(), 0),
            "buffered rows are released unattempted"
        );
    }
    assert!(within(5, consumer.next()).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn close_abandons_unsettled_work_at_the_shutdown_timeout() {
    let db = TestDb::new().await;
    let conn = db
        .connect(ConnectionOptions::default().shutdown_timeout(Duration::from_millis(300)))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let held = within(5, consumer.next()).await.unwrap().unwrap();

    within(5, conn.close()).await;
    assert_eq!(held.ack().await.unwrap_err().kind(), ErrorKind::Closed);
    // The abandoned attempt is not given back; its lease simply expires.
    assert_eq!(
        attempts(&db, &queue).await,
        vec![("processing".to_owned(), 1)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_the_queue_ends_the_stream_with_queue_gone() {
    let db = TestDb::new().await;
    let fatal = Arc::new(Mutex::new(Vec::<String>::new()));
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal({
            let fatal = Arc::clone(&fatal);
            move |queue, err| {
                assert_eq!(err.kind(), ErrorKind::QueueGone);
                fatal.lock().unwrap().push(queue.to_owned());
            }
        }))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let options = ConsumeOptions::default().check_timeout(Duration::from_millis(200));
    let mut first = conn.consume(&queue, options.clone()).await.unwrap();
    let mut second = conn.consume(&queue, options).await.unwrap();

    // Deleted out of band (another process).
    let other = db.connect_owned(ConnectionOptions::default()).await;
    other.delete_queue(&queue).await.unwrap();

    for consumer in [&mut first, &mut second] {
        let err = within(5, consumer.next()).await.unwrap().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::QueueGone);
        assert!(within(5, consumer.next()).await.is_none());
    }
    // The hook runs on a blocking thread, unordered with the streams ending.
    let signalled = within(5, async {
        loop {
            let seen = fatal.lock().unwrap().clone();
            if !seen.is_empty() {
                break seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // Give a (wrong) second signal a chance to arrive.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(signalled, vec![queue.clone()]);
    assert_eq!(
        *fatal.lock().unwrap(),
        vec![queue.clone()],
        "signalled once per queue"
    );
    other.close().await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_through_the_same_connection_still_stops_every_consumer() {
    let db = TestDb::new().await;
    let fatal = Arc::new(AtomicUsize::new(0));
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal({
            let fatal = Arc::clone(&fatal);
            move |_, _| {
                fatal.fetch_add(1, Ordering::SeqCst);
            }
        }))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    for n in 0..2 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let options = ConsumeOptions::default()
        .batch_size(NonZeroU32::MIN)
        .check_timeout(Duration::from_millis(200));
    // B holds one delivery and has the other buffered (both rows are
    // processing): it does not fetch again, so only a sibling can tell it
    // the queue is gone.
    let mut full = conn.consume(&queue, options.clone()).await.unwrap();
    let held = within(5, full.next()).await.unwrap().unwrap();
    within(5, async {
        while attempts(&db, &queue)
            .await
            .iter()
            .any(|(status, _)| status != "processing")
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let mut polling = conn.consume(&queue, options).await.unwrap();

    conn.delete_queue(&queue).await.unwrap();
    // The polling consumer finds the queue gone at its next fetch...
    let err = within(5, async {
        loop {
            match polling.next().await {
                Some(Ok(stale)) => drop(stale.release().await),
                Some(Err(err)) => break err,
                None => panic!("ended without QueueGone"),
            }
        }
    })
    .await;
    assert_eq!(err.kind(), ErrorKind::QueueGone);
    // ... and the connection tears down every consumer of the queue before
    // the hook runs.
    within(5, async {
        while fatal.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    // So the sibling's buffered delivery is discarded: its next item is
    // QueueGone, though it never fetched.
    let sibling = within(5, full.next()).await.unwrap();
    assert_eq!(
        sibling
            .map(|delivery| delivery.message_id())
            .unwrap_err()
            .kind(),
        ErrorKind::QueueGone
    );
    drop(held.release().await);
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exclusive_queues_are_kept_alive_and_their_loss_is_fatal() {
    let db = TestDb::new().await;
    let fatal = Arc::new(AtomicUsize::new(0));
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal({
            let fatal = Arc::clone(&fatal);
            move |_, _| {
                fatal.fetch_add(1, Ordering::SeqCst);
            }
        }))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(
        &queue,
        &topic,
        QueueOptions::default()
            .exclusive(true)
            .keep_alive_interval(Duration::from_secs(2)),
    )
    .await
    .unwrap();

    // Well past its interval the queue is alive and still receives messages.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        conn.maintenance_fast()
            .await
            .unwrap()
            .inactive_queues_dropped
            == 0
    );
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_millis(200)),
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

    // Gone out of band: the next keep-alive omits it, which is fatal.
    sqlx::query("DELETE FROM postgremq.queues WHERE name = $1")
        .bind(&queue)
        .execute(&db.pool)
        .await
        .unwrap();
    let err = within(5, consumer.next()).await.unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::QueueGone);
    within(5, async {
        while fatal.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert_eq!(fatal.load(Ordering::SeqCst), 1);
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_alive_alone_detects_a_producer_only_exclusive_queue_is_gone() {
    let db = TestDb::new().await;
    let fatal: Arc<Mutex<Vec<String>>> = Arc::default();
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal({
            let fatal = Arc::clone(&fatal);
            move |queue, _| fatal.lock().unwrap().push(queue.to_owned())
        }))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(
        &queue,
        &topic,
        QueueOptions::default()
            .exclusive(true)
            .keep_alive_interval(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    // No consumer: only the keep-alive can notice.
    sqlx::query("DELETE FROM postgremq.queues WHERE name = $1")
        .bind(&queue)
        .execute(&db.pool)
        .await
        .unwrap();
    within(5, async {
        while fatal.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        *fatal.lock().unwrap(),
        vec![queue],
        "signalled exactly once"
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_auto_ack_and_auto_nack_on_error_and_panic() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    // "ok" returns Ok without settling; "err" returns Err; "panic" panics;
    // "explicit" settles itself with a release.
    for kind in ["ok", "err", "panic", "explicit"] {
        conn.publish(&topic, &kind, PublishOptions::default())
            .await
            .unwrap();
    }
    let calls = Arc::new(Mutex::new(Vec::<(String, u32)>::new()));
    let handler_consumer = conn
        .consume_with_handler(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_millis(200)),
            NonZeroUsize::new(4),
            {
                let calls = Arc::clone(&calls);
                move |delivery: Delivery| {
                    let calls = Arc::clone(&calls);
                    async move {
                        let kind: String = delivery.payload_as().unwrap();
                        let first_call = {
                            let mut calls = calls.lock().unwrap();
                            let first = !calls.iter().any(|(k, _)| *k == kind);
                            calls.push((kind.clone(), delivery.delivery_attempts()));
                            first
                        };
                        match (kind.as_str(), first_call) {
                            ("err", true) => Err("boom".into()),
                            ("panic", true) => panic!("handler panic"),
                            ("explicit", true) => {
                                delivery.release().await?;
                                Ok(())
                            }
                            _ => Ok(()),
                        }
                    }
                }
            },
        )
        .await
        .unwrap();

    within(15, async {
        loop {
            let done = attempts(&db, &queue)
                .await
                .iter()
                .all(|(status, _)| status == "completed");
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    handler_consumer.stop().await;

    let calls = calls.lock().unwrap().clone();
    let attempts_of = |kind: &str| -> Vec<u32> {
        calls
            .iter()
            .filter(|(k, _)| k == kind)
            .map(|(_, a)| *a)
            .collect()
    };
    assert_eq!(attempts_of("ok"), vec![1]);
    assert_eq!(attempts_of("err"), vec![1, 2], "Err auto-nacks");
    assert_eq!(attempts_of("panic"), vec![1, 2], "a panic auto-nacks");
    assert_eq!(
        attempts_of("explicit"),
        vec![1, 1],
        "release does not count an attempt"
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_consumer_reports_queue_gone() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let handler_consumer = conn
        .consume_with_handler(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_millis(200)),
            NonZeroUsize::new(1),
            |_delivery: Delivery| async { Ok(()) },
        )
        .await
        .unwrap();
    conn.delete_queue(&queue).await.unwrap();
    let reason = within(5, handler_consumer.closed()).await;
    assert_eq!(
        reason.map(postgremq::Error::kind),
        Some(ErrorKind::QueueGone)
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_while_building_the_handler_future_auto_nacks() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_consumer = conn
        .consume_with_handler(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_millis(200)),
            NonZeroUsize::new(1),
            {
                let calls = Arc::clone(&calls);
                move |delivery: Delivery| {
                    // Panics in the synchronous part of the closure, before
                    // any future exists.
                    assert!(
                        calls.fetch_add(1, Ordering::SeqCst) > 0,
                        "first call panics"
                    );
                    async move {
                        assert_eq!(delivery.delivery_attempts(), 2, "nacked, then redelivered");
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();
    within(10, async {
        while attempts(&db, &queue).await != vec![("completed".to_owned(), 2)] {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    handler_consumer.stop().await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_close_call_does_not_cancel_the_shutdown() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let held = within(5, consumer.next()).await.unwrap().unwrap();

    // The first close waits for `held`; abandon that call.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), conn.close())
            .await
            .is_err()
    );
    held.ack().await.unwrap();
    // The shutdown kept running: a second call just awaits it.
    within(5, conn.close()).await;
    assert_eq!(
        conn.create_topic(&topic).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
}
