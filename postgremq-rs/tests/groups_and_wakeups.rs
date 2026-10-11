//! Message-group ordering and LISTEN/poll wake-ups.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{TestDb, unique, within};
use postgremq::{ConnectionOptions, ConsumeOptions, PublishOptions, QueueOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_order_holds_under_four_consumers() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();

    const GROUPS: usize = 8;
    const PER_GROUP: usize = 25;
    const UNGROUPED: usize = 20;
    let total = GROUPS * PER_GROUP + UNGROUPED;

    // group -> sequences in delivery order; recorded before acking, and a
    // successor is not claimable until its head's ack commits.
    let received: Arc<Mutex<HashMap<Option<String>, Vec<i64>>>> = Arc::default();
    let seen: Arc<Mutex<HashSet<i64>>> = Arc::default();
    let duplicates = Arc::new(Mutex::new(0_usize));
    let done = postgremq::CancellationToken::new();

    let mut workers = Vec::new();
    for worker in 0..4 {
        // Two connections, two consumers each.
        let consumer_conn = if worker % 2 == 0 {
            conn.clone()
        } else {
            db.connect_owned(ConnectionOptions::default()).await
        };
        let mut consumer = consumer_conn
            .consume(
                &queue,
                ConsumeOptions::default()
                    .batch_size(NonZeroU32::new(3).unwrap())
                    .check_timeout(Duration::from_millis(250)),
            )
            .await
            .unwrap();
        let (received, seen, duplicates, done) = (
            Arc::clone(&received),
            Arc::clone(&seen),
            Arc::clone(&duplicates),
            done.clone(),
        );
        workers.push(tokio::spawn(async move {
            loop {
                let delivery = tokio::select! {
                    () = done.cancelled() => break,
                    item = consumer.next() => match item {
                        Some(item) => item.unwrap(),
                        None => break,
                    },
                };
                received
                    .lock()
                    .unwrap()
                    .entry(delivery.group_key().map(str::to_owned))
                    .or_default()
                    .push(delivery.group_seq().unwrap_or(0));
                if !seen.lock().unwrap().insert(delivery.message_id().get()) {
                    *duplicates.lock().unwrap() += 1;
                }
                tokio::time::sleep(Duration::from_millis(u64::from(fastrand_ms()))).await;
                delivery.ack().await.unwrap();
            }
            consumer.stop().await;
            consumer_conn
        }));
    }

    let mut published_grouped = 0;
    let mut published_ungrouped = 0;
    for i in 0..total {
        let options = if published_ungrouped < UNGROUPED && i % 11 == 0 {
            published_ungrouped += 1;
            PublishOptions::default()
        } else {
            let group = published_grouped % GROUPS;
            published_grouped += 1;
            PublishOptions::default().group_key(format!("session-{group}"))
        };
        conn.publish(&topic, &i, options).await.unwrap();
    }
    assert_eq!(published_grouped, GROUPS * PER_GROUP);

    within(60, async {
        while seen.lock().unwrap().len() < total {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    done.cancel();
    for worker in workers {
        within(10, worker).await.unwrap().close().await;
    }

    assert_eq!(*duplicates.lock().unwrap(), 0);
    let received = received.lock().unwrap();
    for group in 0..GROUPS {
        let seqs = &received[&Some(format!("session-{group}"))];
        let expected: Vec<i64> = (1..=i64::try_from(PER_GROUP).unwrap()).collect();
        assert_eq!(seqs, &expected, "group session-{group} out of order");
    }
    assert_eq!(received[&None].len(), UNGROUPED);
}

/// A cheap pseudo-random 0..3 ms delay without an RNG dependency.
fn fastrand_ms() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() % 3)
}

#[tokio::test(flavor = "multi_thread")]
async fn acking_a_group_head_wakes_its_successor() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    for n in 0..2 {
        conn.publish(&topic, &n, PublishOptions::default().group_key("s"))
            .await
            .unwrap();
    }
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    let head = within(5, consumer.next()).await.unwrap().unwrap();
    assert_eq!(head.group_seq(), Some(1));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), consumer.next())
            .await
            .is_err(),
        "the successor must wait for its head"
    );
    let acked = Instant::now();
    head.ack().await.unwrap();
    let successor = within(5, consumer.next()).await.unwrap().unwrap();
    assert_eq!(successor.group_seq(), Some(2));
    assert!(acked.elapsed() < Duration::from_secs(2));
    successor.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn listen_wakes_a_consumer_within_100ms() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    // Let the first (empty) fetch finish and the LISTEN be established.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Every delivery must come from NOTIFY, not the 30 s poll. After one
    // warm-up delivery, the median of 15 must be under 100 ms (a median, so a
    // brief stall of a busy machine does not decide the verdict).
    let mut latencies = Vec::new();
    for n in 0..16 {
        let published = Instant::now();
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
        let delivery = within(5, consumer.next()).await.unwrap().unwrap();
        if n > 0 {
            latencies.push(published.elapsed());
        }
        delivery.ack().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    latencies.sort();
    assert!(latencies[7] < Duration::from_millis(100), "{latencies:?}");
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn polling_delivers_with_listen_disabled() {
    let db = TestDb::new().await;
    let app = unique("pmq_poll");
    let conn = db
        .connect_tagged(&app, ConnectionOptions::default().notifications(false))
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
            ConsumeOptions::default().check_timeout(Duration::from_millis(400)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let published = Instant::now();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    // Without NOTIFY the delivery waits for the next poll.
    assert!(published.elapsed() < Duration::from_secs(2));
    // ... and it was polling: no `LISTEN` session ever existed.
    assert!(db.listen_pids(&app).await.is_empty());
    delivery.ack().await.unwrap();
    conn.close().await;
}
