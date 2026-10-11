//! Publish, consume and settlement against a real database.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use common::{TestDb, unique, within};
use postgremq::{
    Connection, ConnectionOptions, ConsumeOptions, Consumer, Delivery, ErrorKind, PublishOptions,
    QueueOptions,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Order {
    id: u32,
}

fn secs(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

async fn setup(db: &TestDb, queue_options: QueueOptions) -> (Connection, String, String) {
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, queue_options)
        .await
        .unwrap();
    (conn, topic, queue)
}

async fn next(consumer: &mut Consumer) -> Delivery {
    within(10, consumer.next())
        .await
        .expect("stream ended")
        .unwrap()
}

/// Asserts nothing arrives for `millis`.
async fn nothing_for(consumer: &mut Consumer, millis: u64) {
    if let Ok(item) = tokio::time::timeout(Duration::from_millis(millis), consumer.next()).await {
        panic!("unexpected delivery: {item:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_consume_and_ack() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;

    let id = conn
        .publish(&topic, &Order { id: 7 }, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = next(&mut consumer).await;

    assert_eq!(delivery.message_id(), id);
    assert_eq!(delivery.queue(), queue);
    assert_eq!(delivery.payload_as::<Order>().unwrap(), Order { id: 7 });
    assert_eq!(delivery.delivery_attempts(), 1);
    assert_eq!(delivery.group_key(), None);
    assert!(delivery.vt() > std::time::SystemTime::now());
    delivery.ack().await.unwrap();

    let stats = conn.queue_statistics(Some(&queue)).await.unwrap();
    assert_eq!(
        (stats.completed, stats.pending, stats.processing),
        (1, 0, 0)
    );

    // The first settlement owns the outcome.
    let err = delivery.ack().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::LeaseLost);
    assert_eq!(
        delivery.nack(None).await.unwrap_err().kind(),
        ErrorKind::LeaseLost
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_tx_rolled_back_publishes_nothing() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;

    let mut tx = db.pool.begin().await.unwrap();
    conn.publish_tx(&mut tx, &topic, &"rolled back", PublishOptions::default())
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let mut tx = db.pool.begin().await.unwrap();
    let committed = conn
        .publish_tx(&mut tx, &topic, &"committed", PublishOptions::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = next(&mut consumer).await;
    assert_eq!(delivery.message_id(), committed);
    assert_eq!(delivery.payload(), &serde_json::json!("committed"));
    delivery.ack().await.unwrap();
    nothing_for(&mut consumer, 500).await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_tx_rolled_back_redelivers_after_lease_expiry() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    // Auto-extension stays on: ack_tx must end renewal (once its statement
    // completes), or the rolled-back delivery would be kept alive forever.
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default()
                .vt_secs(secs(1))
                .check_timeout(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    let delivery = next(&mut consumer).await;

    let mut tx = db.pool.begin().await.unwrap();
    delivery.ack_tx(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();

    let redelivered = next(&mut consumer).await;
    assert_eq!(redelivered.message_id(), id);
    assert_eq!(redelivered.delivery_attempts(), 2);
    redelivered.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_with_delay_redelivers_after_the_delay() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();

    let delivery = next(&mut consumer).await;
    let nacked_at = Instant::now();
    delivery
        .nack(Some(Duration::from_millis(1500)))
        .await
        .unwrap();
    nothing_for(&mut consumer, 1000).await;

    let redelivered = next(&mut consumer).await;
    assert!(nacked_at.elapsed() >= Duration::from_millis(1400));
    assert_eq!(redelivered.message_id(), id);
    assert_eq!(redelivered.delivery_attempts(), 2);
    redelivered.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_expired_lease_is_redelivered_and_fences_the_old_delivery() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default()
                .vt_secs(secs(1))
                .auto_extend(false)
                .check_timeout(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    let stale = next(&mut consumer).await;
    let redelivered = next(&mut consumer).await;
    assert_eq!(redelivered.message_id(), id);
    assert_eq!(redelivered.delivery_attempts(), 2);

    assert_eq!(stale.ack().await.unwrap_err().kind(), ErrorKind::LeaseLost);
    redelivered.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_renewal_keeps_a_handler_three_times_the_lease_alive() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    // A 2 s lease renews every second, so one slow round trip on a busy
    // runner does not lose it.
    let options = ConsumeOptions::default()
        .vt_secs(secs(2))
        .check_timeout(Duration::from_millis(200));
    let mut consumer = conn.consume(&queue, options.clone()).await.unwrap();
    let delivery = next(&mut consumer).await;
    let mut rival = conn.consume(&queue, options).await.unwrap();

    let stopped = delivery.stopped();
    // Three leases long, while a rival consumer polls the same queue.
    nothing_for(&mut rival, 6300).await;
    assert!(!stopped.is_cancelled());
    assert!(delivery.vt() > std::time::SystemTime::now());
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_extensions_alongside_automatic_renewal_keep_the_lease() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let options = ConsumeOptions::default()
        .vt_secs(secs(2))
        .check_timeout(Duration::from_millis(200));
    let mut consumer = conn.consume(&queue, options.clone()).await.unwrap();
    let delivery = std::sync::Arc::new(next(&mut consumer).await);
    let mut rival = conn.consume(&queue, options).await.unwrap();

    // Manual extensions (none shorter than the consumer's lease) race the
    // 1 s renewals for three leases: whichever lands last, the lease never
    // lapses. (Extending below `vt_secs` alongside renewal is the caller's
    // responsibility; see `Delivery::extend`.)
    let extensions = {
        let delivery = std::sync::Arc::clone(&delivery);
        tokio::spawn(async move {
            for round in 0..30_u32 {
                let secs = secs(if round % 2 == 0 { 2 } else { 5 });
                let deadline = delivery.extend(secs).await.unwrap();
                assert!(deadline > std::time::SystemTime::now());
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    nothing_for(&mut rival, 6300).await;
    extensions.await.unwrap();
    assert!(!delivery.stopped().is_cancelled());
    assert!(delivery.vt() > std::time::SystemTime::now());
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_lease_cancels_the_delivery_token() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default().vt_secs(secs(2)))
        .await
        .unwrap();
    let delivery = next(&mut consumer).await;

    // Another owner takes the lease out of band.
    sqlx::query(
        "UPDATE postgremq.queue_messages SET consumer_token = 'revoked' \
         WHERE queue_name = $1 AND message_id = $2",
    )
    .bind(&queue)
    .bind(delivery.message_id().get())
    .execute(&db.pool)
    .await
    .unwrap();

    within(5, delivery.stopped().cancelled()).await;
    assert_eq!(
        delivery.ack().await.unwrap_err().kind(),
        ErrorKind::LeaseLost
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn release_does_not_count_an_attempt() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    next(&mut consumer).await.release().await.unwrap();
    let again = next(&mut consumer).await;
    assert_eq!(again.delivery_attempts(), 1);
    again.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn extend_moves_the_lease() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default().auto_extend(false))
        .await
        .unwrap();
    let delivery = next(&mut consumer).await;
    let before = delivery.vt();
    let after = delivery
        .extend(std::num::NonZeroU32::new(120).unwrap())
        .await
        .unwrap();
    assert!(after > before);
    assert_eq!(delivery.vt(), after);
    delivery.ack().await.unwrap();
    assert_eq!(
        delivery
            .extend(std::num::NonZeroU32::new(10).unwrap())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::LeaseLost
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn final_attempt_nack_retires_to_the_dead_letter_queue() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default().max_delivery_attempts(1)).await;
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    next(&mut consumer).await.nack(None).await.unwrap();

    let dlq = conn.list_dlq().await.unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(
        (dlq[0].queue.as_str(), dlq[0].message_id),
        (queue.as_str(), id)
    );

    conn.requeue_dlq(&queue).await.unwrap();
    let requeued = next(&mut consumer).await;
    assert_eq!(requeued.message_id(), id);
    requeued.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_errors_map_to_typed_variants() {
    let db = TestDb::new().await;
    let (conn, topic, _queue) = setup(&db, QueueOptions::default()).await;

    let err = conn
        .publish(&unique("missing"), &1, PublishOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::QueueNotFound);
    assert_eq!(err.sqlstate().as_deref(), Some("PMQ02"));

    let err = conn
        .publish(&topic, &1, PublishOptions::default().group_key(""))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Validation);

    let err = conn
        .consume(&unique("missing"), ConsumeOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::QueueNotFound);

    let err = conn
        .consume(&topic, ConsumeOptions::default().extension_threshold(1.5))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Validation);
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_publish_is_invisible_until_deliver_after() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let at = std::time::SystemTime::now() + Duration::from_millis(1200);
    conn.publish(&topic, &1, PublishOptions::default().deliver_after(at))
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    // Invisible until shortly before the deadline (however long setup took)...
    if let Ok(left) = at.duration_since(std::time::SystemTime::now())
        && let Some(quiet) = left.checked_sub(Duration::from_millis(150))
    {
        nothing_for(&mut consumer, u64::try_from(quiet.as_millis()).unwrap()).await;
    }
    // ... and not delivered before it (the server shares this host's clock,
    // give or take a little).
    let delivery = next(&mut consumer).await;
    assert!(
        std::time::SystemTime::now() + Duration::from_millis(50) >= at,
        "delivered before deliver_after"
    );
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_an_unsettled_delivery_abandons_it_without_blocking_stop() {
    let db = TestDb::new().await;
    let (conn, topic, queue) = setup(&db, QueueOptions::default()).await;
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default()
                .vt_secs(secs(1))
                .check_timeout(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    drop(next(&mut consumer).await);

    // Renewal ended with the drop: the lease expires and the message returns.
    let again = next(&mut consumer).await;
    assert_eq!((again.message_id(), again.delivery_attempts()), (id, 2));
    drop(again);
    within(5, consumer.stop()).await;
    conn.close().await;
}
