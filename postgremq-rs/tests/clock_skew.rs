//! The lease end a delivery shows is on the application's clock, whatever the
//! database clock reads.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use common::{TestDb, unique, within};
use postgremq::{
    Connection, ConnectionOptions, ConsumeOptions, Consumer, Delivery, PublishOptions, QueueOptions,
};

/// Seconds from now until `vt` on this process's clock.
fn secs_until(vt: SystemTime) -> f64 {
    vt.duration_since(SystemTime::now())
        .map_or(0.0, |left| left.as_secs_f64())
}

/// One delivery claimed with `vt_secs` from a database whose clock is 10 s
/// ahead of this process's.
async fn claim_ahead(db: &TestDb, vt_secs: u32) -> (Connection, Consumer, Delivery) {
    let pool = db.skewed_pool(10_000).await;
    let conn = Connection::from_pool(pool, ConnectionOptions::default())
        .await
        .unwrap();
    let (topic, queue) = (unique("t"), unique("q"));
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    conn.publish(&topic, &serde_json::json!({}), PublishOptions::default())
        .await
        .unwrap();
    let options = ConsumeOptions::default().vt_secs(NonZeroU32::new(vt_secs).unwrap());
    let mut consumer = conn.consume(&queue, options).await.unwrap();
    let delivery = within(10, consumer.next()).await.unwrap().unwrap();
    (conn, consumer, delivery)
}

#[tokio::test(flavor = "multi_thread")]
async fn database_ahead_does_not_lengthen_the_lease_end_shown() {
    let db = TestDb::new().await;
    let (conn, consumer, delivery) = claim_ahead(&db, 30).await;
    assert!(
        (secs_until(delivery.vt()) - 30.0).abs() < 2.0,
        "after claim"
    );
    let vt = delivery.extend(NonZeroU32::new(20).unwrap()).await.unwrap();
    assert!((secs_until(vt) - 20.0).abs() < 2.0, "after extend");
    assert_eq!(delivery.vt(), vt);
    delivery.ack().await.unwrap();
    consumer.stop().await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn database_ahead_does_not_lengthen_the_renewed_lease_end_shown() {
    let db = TestDb::new().await;
    let (conn, consumer, delivery) = claim_ahead(&db, 2).await;
    let first = delivery.vt();
    within(3, async {
        while delivery.vt() <= first {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        (secs_until(delivery.vt()) - 2.0).abs() < 1.0,
        "after renewal"
    );
    delivery.ack().await.unwrap();
    consumer.stop().await;
    conn.close().await;
}
