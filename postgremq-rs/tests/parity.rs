//! Behaviours the Go client's suite guards, ported: admin round-trips,
//! fan-out keying, queue-fatal fan-out, handler concurrency, NOTIFY wakes,
//! typed errors, restart, slow readers, prefetch bounds.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{TestDb, unique, within};
use postgremq::{
    Connection, ConnectionOptions, ConsumeOptions, Consumer, Delivery, ErrorKind, PublishOptions,
    QueueOptions,
};

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

async fn setup(db: &TestDb, options: QueueOptions) -> (Connection, String, String) {
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, options).await.unwrap();
    (conn, topic, queue)
}

async fn statuses(db: &TestDb, queue: &str) -> Vec<(String, i32)> {
    sqlx::query_as(
        "SELECT status::text, delivery_attempts FROM postgremq.queue_messages \
         WHERE queue_name = $1 ORDER BY message_id",
    )
    .bind(queue)
    .fetch_all(&db.pool)
    .await
    .unwrap()
}

async fn next(consumer: &mut Consumer) -> Delivery {
    within(10, consumer.next()).await.unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_operations_round_trip() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default().max_delivery_attempts(1)).await;

    let info = conn.list_queues().await.unwrap();
    let mine = info.iter().find(|q| q.name == queue).unwrap();
    assert_eq!(
        (
            mine.topic.as_str(),
            mine.max_delivery_attempts,
            mine.exclusive
        ),
        (topic.as_str(), 1, false)
    );

    for n in 0..3 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    assert_eq!(
        conn.queue_statistics(Some(&queue)).await.unwrap().pending,
        3
    );
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    next(&mut consumer).await.ack().await.unwrap();
    next(&mut consumer).await.nack(None).await.unwrap(); // final attempt → DLQ
    let third = next(&mut consumer).await;
    third.ack().await.unwrap();

    let stats = conn.queue_statistics(Some(&queue)).await.unwrap();
    assert_eq!(
        (
            stats.completed,
            stats.pending,
            stats.processing,
            stats.total
        ),
        (2, 0, 0, 2)
    );
    let dlq = conn.list_dlq().await.unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].retry_count, 1);
    assert!(conn.maintenance_fast().await.is_ok());

    // A topic with messages cannot be deleted; nor a queue with DLQ entries.
    assert_eq!(
        conn.delete_topic(&topic).await.unwrap_err().kind(),
        ErrorKind::Validation
    );
    assert_eq!(
        conn.delete_queue(&queue).await.unwrap_err().kind(),
        ErrorKind::Validation
    );

    conn.purge_dlq().await.unwrap();
    assert!(conn.list_dlq().await.unwrap().is_empty());
    assert_eq!(
        conn.cleanup_completed_messages(0, NonZeroU32::new(100).unwrap())
            .await
            .unwrap(),
        2
    );
    consumer.stop().await;
    conn.delete_queue(&queue).await.unwrap();
    // cleanup_completed_messages already collected the unreferenced payloads
    // (including the purged DLQ message's).
    assert_eq!(
        conn.cleanup_unreferenced_messages(0, NonZeroU32::new(100).unwrap())
            .await
            .unwrap(),
        0
    );
    conn.delete_topic(&topic).await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_out_deliveries_are_keyed_per_queue() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let (q1, q2) = (unique("q"), unique("q"));
    conn.create_topic(&topic).await.unwrap();
    for q in [&q1, &q2] {
        conn.create_queue(q, &topic, QueueOptions::default())
            .await
            .unwrap();
    }
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let options = ConsumeOptions::default().vt_secs(nz(2));
    let mut c1 = conn.consume(&q1, options.clone()).await.unwrap();
    let mut c2 = conn.consume(&q2, options).await.unwrap();
    let (d1, d2) = (next(&mut c1).await, next(&mut c2).await);
    assert_eq!((d1.message_id(), d2.message_id()), (id, id));

    // Revoking the same message ID on q1 must not touch q2's lease.
    sqlx::query("UPDATE postgremq.queue_messages SET consumer_token = 'x' WHERE queue_name = $1")
        .bind(&q1)
        .execute(&db.pool)
        .await
        .unwrap();
    within(5, d1.stopped().cancelled()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!d2.stopped().is_cancelled());
    d2.ack().await.unwrap();
    assert_eq!(d1.ack().await.unwrap_err().kind(), ErrorKind::LeaseLost);
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gone_queue_fails_every_consumer_on_it_once() {
    let db = TestDb::new().await;
    let hooks = Arc::new(AtomicUsize::new(0));
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal({
            let hooks = Arc::clone(&hooks);
            move |_, _| {
                hooks.fetch_add(1, Ordering::SeqCst);
            }
        }))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    // Each pull consumer leases one message ahead (prefetch), so publish
    // enough for the handler consumer too.
    for n in 0..8 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let options = ConsumeOptions::default()
        .batch_size(nz(1))
        .check_timeout(Duration::from_millis(200));
    let mut c1 = conn.consume(&queue, options.clone()).await.unwrap();
    let mut c2 = conn.consume(&queue, options.clone()).await.unwrap();
    let held1 = next(&mut c1).await;
    let held2 = next(&mut c2).await;
    let handled = Arc::new(tokio::sync::Notify::new());
    let handler = conn
        .consume_with_handler(&queue, options, Some(NonZeroUsize::MIN), {
            let handled = Arc::clone(&handled);
            move |delivery: Delivery| {
                let handled = Arc::clone(&handled);
                async move {
                    handled.notify_one();
                    delivery.stopped().cancelled().await;
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
    within(5, handled.notified()).await;

    let other = db.connect(ConnectionOptions::default()).await;
    other.delete_queue(&queue).await.unwrap();

    for consumer in [&mut c1, &mut c2] {
        // A delivery read before the consumer learned of the deletion may
        // still arrive (its settlement then fails); once the queue is known
        // to be gone, buffered deliveries are discarded and the stream ends
        // with QueueGone.
        let err = within(5, async {
            loop {
                match consumer.next().await.unwrap() {
                    Ok(stale) => assert!(stale.ack().await.is_err()),
                    Err(err) => break err,
                }
            }
        })
        .await;
        assert_eq!(err.kind(), ErrorKind::QueueGone);
        assert!(within(5, consumer.next()).await.is_none());
    }
    let reason = within(5, handler.closed()).await;
    assert_eq!(
        reason.map(postgremq::Error::kind),
        Some(ErrorKind::QueueGone)
    );
    // Held deliveries were told to stop and can no longer settle.
    within(5, held1.stopped().cancelled()).await;
    assert!(held2.stopped().is_cancelled());
    assert_eq!(held1.ack().await.unwrap_err().kind(), ErrorKind::LeaseLost);
    within(5, async {
        while hooks.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    // Held deliveries must be settled (or dropped) before an unbounded close.
    assert!(held2.ack().await.is_err());
    within(10, conn.close()).await;
    within(10, other.close()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handlers_respect_max_in_flight() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    for n in 0..12 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler = conn
        .consume_with_handler(&queue, ConsumeOptions::default(), NonZeroUsize::new(2), {
            let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
            move |_delivery: Delivery| {
                let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
                async move {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
    within(15, async {
        while statuses(&db, &queue)
            .await
            .iter()
            .any(|(s, _)| s != "completed")
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    handler.stop().await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_handlers_under_load_neither_hangs_nor_strands_rows() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    for n in 0..20 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let handler = conn
        .consume_with_handler(
            &queue,
            ConsumeOptions::default(),
            NonZeroUsize::new(4),
            |_delivery: Delivery| async {
                // Ignores cancellation on purpose.
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(())
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    within(10, handler.stop()).await;
    for (status, _) in statuses(&db, &queue).await {
        assert!(status == "completed" || status == "pending", "{status}");
    }
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_and_release_wake_other_consumers() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let slow_poll = ConsumeOptions::default()
        .batch_size(nz(1))
        .check_timeout(Duration::from_secs(30));
    let mut holder = conn.consume(&queue, slow_poll.clone()).await.unwrap();
    let held = next(&mut holder).await;
    let other = db.connect(ConnectionOptions::default()).await;
    let mut waiter = other.consume(&queue, slow_poll).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await; // idle after its first fetch

    let nacked = Instant::now();
    held.nack(None).await.unwrap();
    let again = tokio::select! {
        d = waiter.next() => d,
        d = holder.next() => d,
    }
    .unwrap()
    .unwrap();
    assert!(nacked.elapsed() < Duration::from_secs(2));

    let released = Instant::now();
    again.release().await.unwrap();
    let last = within(5, async {
        tokio::select! {
            d = waiter.next() => d,
            d = holder.next() => d,
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert!(released.elapsed() < Duration::from_secs(2));
    last.ack().await.unwrap();
    conn.close().await;
    other.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shared_listen_channel_survives_one_subscriber_leaving() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let slow_poll = ConsumeOptions::default().check_timeout(Duration::from_secs(30));
    let first = conn.consume(&queue, slow_poll.clone()).await.unwrap();
    let mut second = conn.consume(&queue, slow_poll).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.stop().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let published = Instant::now();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    next(&mut second).await.ack().await.unwrap();
    assert!(
        published.elapsed() < Duration::from_secs(2),
        "{:?}",
        published.elapsed()
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn each_settlement_reports_its_typed_error() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &0, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();

    // Release after ack.
    let first = next(&mut consumer).await;
    first.ack().await.unwrap();
    assert_eq!(
        first.release().await.unwrap_err().kind(),
        ErrorKind::LeaseLost
    );
    consumer.stop().await;

    // Extend and nack after the lease expired and the message was redelivered.
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default()
                .vt_secs(nz(1))
                .auto_extend(false)
                .check_timeout(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    let stale = next(&mut consumer).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(
        stale
            .extend(std::num::NonZeroU32::new(5).unwrap())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::LeaseLost
    );
    let redelivered = next(&mut consumer).await;
    assert_eq!(
        (redelivered.message_id(), redelivered.delivery_attempts()),
        (id, 2)
    );
    redelivered.ack().await.unwrap();
    assert_eq!(
        stale.nack(None).await.unwrap_err().kind(),
        ErrorKind::LeaseLost
    );

    // Re-declaring a queue: same options are idempotent, others are rejected.
    let generation = conn
        .create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    assert_eq!(
        conn.create_queue(&queue, &topic, QueueOptions::default())
            .await
            .unwrap(),
        generation
    );
    assert_eq!(
        conn.create_queue(
            &queue,
            &topic,
            QueueOptions::default().max_delivery_attempts(9)
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::Validation
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queue_can_be_consumed_again_after_stop() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let first = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    first.stop().await;
    let mut again = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    next(&mut again).await.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_reader_gets_every_message_exactly_once() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    for n in 0..40 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default().batch_size(nz(5)))
        .await
        .unwrap();
    let mut seen = HashSet::new();
    for _ in 0..40 {
        let delivery = next(&mut consumer).await;
        assert!(seen.insert(delivery.message_id()), "duplicate {delivery:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
        delivery.ack().await.unwrap();
    }
    assert!(
        statuses(&db, &queue)
            .await
            .iter()
            .all(|(s, a)| s == "completed" && *a == 1)
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn at_most_about_two_batches_are_leased_ahead_of_the_reader() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    for n in 0..30 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default().batch_size(nz(5)))
        .await
        .unwrap();
    let held = next(&mut consumer).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let processing = statuses(&db, &queue)
        .await
        .iter()
        .filter(|(s, _)| s == "processing")
        .count();
    assert!((5..=10).contains(&processing), "{processing}");
    held.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_stream_ends_without_an_error() {
    let db = TestDb::new().await;
    let (conn, _topic, queue) = setup(&db, QueueOptions::default()).await;
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    consumer.stop().await;
    for _ in 0..3 {
        assert!(within(5, consumer.next()).await.is_none());
    }
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_queue_fatal_hook_does_not_break_the_connection() {
    let db = TestDb::new().await;
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal(|_, _| panic!("hook panic")))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    conn.delete_queue(&queue).await.unwrap();
    let err = within(5, consumer.next()).await.unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::QueueGone);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The connection keeps working.
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let mut again = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    next(&mut again).await.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_drops_its_delivery_and_returns_ok_still_acks() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let handler = conn
        .consume_with_handler(
            &queue,
            ConsumeOptions::default(),
            Some(NonZeroUsize::MIN),
            |delivery: Delivery| async move {
                drop(delivery);
                Ok(())
            },
        )
        .await
        .unwrap();
    within(5, async {
        while statuses(&db, &queue).await != vec![("completed".to_owned(), 1)] {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    handler.stop().await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_select_loop_over_next_loses_nothing() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    for n in 0..50 {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let seen = Mutex::new(HashSet::new());
    within(20, async {
        while seen.lock().unwrap().len() < 50 {
            tokio::select! {
                item = consumer.next() => {
                    let delivery = item.unwrap().unwrap();
                    assert!(seen.lock().unwrap().insert(delivery.message_id()));
                    delivery.ack().await.unwrap();
                }
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    })
    .await;
    assert!(
        statuses(&db, &queue)
            .await
            .iter()
            .all(|(s, a)| s == "completed" && *a == 1)
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_an_exclusive_queue_does_not_signal_it_as_fatal() {
    let db = TestDb::new().await;
    let hooks = Arc::new(AtomicUsize::new(0));
    let conn = db
        .connect(ConnectionOptions::default().on_queue_fatal({
            let hooks = Arc::clone(&hooks);
            move |_, _| {
                hooks.fetch_add(1, Ordering::SeqCst);
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
            .keep_alive_interval(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    conn.delete_queue(&queue).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        hooks.load(Ordering::SeqCst),
        0,
        "an intentional delete is not a failure"
    );
    conn.close().await;
}
