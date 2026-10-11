//! Failure handling: killed backends, blocked claims, ambiguous publishes,
//! large identifiers and payloads.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::time::{Duration, Instant};

use common::{TestDb, unique, within};
use postgremq::{
    ConnectionOptions, ConsumeOptions, ErrorKind, MessageId, PublishOptions, QueueOptions,
};
use sqlx::Connection as _;

#[tokio::test(flavor = "multi_thread")]
async fn the_client_recovers_after_its_backends_are_killed() {
    let db = TestDb::new().await;
    let app = unique("pmq_client");
    let conn = db.connect_tagged(&app, ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    // Polling is far slower than the recovery this test requires.
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    conn.publish(&topic, &0, PublishOptions::default())
        .await
        .unwrap();
    within(5, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .ack()
        .await
        .unwrap();
    within(5, async {
        while db.listen_pids(&app).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    // Every pooled and LISTEN session of the client dies (failover, a
    // pooler restart, an idle cut-off).
    assert!(db.kill_tagged(&app).await > 0);
    let other = db.connect(ConnectionOptions::default()).await;
    for n in 1..=5 {
        other
            .publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }

    // Claims fail once and recover on the next tick; settlements and admin
    // reads retry a dropped connection.
    let started = Instant::now();
    for _ in 1..=5 {
        let delivery = within(15, consumer.next()).await.unwrap().unwrap();
        delivery.ack().await.unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "recovered in {:?}",
        started.elapsed()
    );
    assert!(!conn.list_queues().await.unwrap().is_empty());
    conn.close().await;
    other.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blocked_claim_is_bounded_by_the_server_and_recovers() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();

    let mut locker = db.pool.acquire().await.unwrap().detach();
    let mut lock = locker.begin().await.unwrap();
    sqlx::query("LOCK TABLE postgremq.queue_messages IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();

    // vt 2 s → the claim's statement_timeout is 1 s: it fails, rolls back and
    // is retried, instead of hanging or leasing rows to nobody.
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default()
                .vt_secs(std::num::NonZeroU32::new(2).unwrap())
                .check_timeout(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    // While the lock is held, claims keep starting afresh: each is cancelled
    // by the server-side timeout (1 s) rather than left waiting on the lock.
    let mut starts = std::collections::HashSet::new();
    let sampling = Instant::now();
    while sampling.elapsed() < Duration::from_millis(3500) {
        let waiting: Vec<(f64, String)> = sqlx::query_as(
            "SELECT extract(epoch FROM clock_timestamp() - query_start)::float8, \
                    query_start::text \
             FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() \
               AND wait_event_type = 'Lock' AND query ILIKE '%consume_message%'",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap();
        for (age, started) in waiting {
            assert!(age < 1.5, "a claim has waited {age}s");
            starts.insert(started);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(starts.len() >= 2, "claims observed: {starts:?}");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), consumer.next())
            .await
            .is_err()
    );
    lock.commit().await.unwrap();

    let delivery = within(10, consumer.next()).await.unwrap().unwrap();
    assert_eq!(
        (delivery.message_id(), delivery.delivery_attempts()),
        (id, 1)
    );
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_start_blocked_on_queue_metadata_gives_up() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let mut locker = db.pool.acquire().await.unwrap().detach();
    let mut lock = locker.begin().await.unwrap();
    sqlx::query("LOCK TABLE postgremq.queues IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let started = Instant::now();
    let err = within(5, conn.consume(&queue, ConsumeOptions::default()))
        .await
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(err.kind(), ErrorKind::Sqlx, "{err:?}");
    lock.rollback().await.unwrap();
    // The pool recovered: the abandoned lookup's connection was not kept.
    let consumer = within(5, conn.consume(&queue, ConsumeOptions::default()))
        .await
        .unwrap();
    drop(consumer);
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_is_not_held_up_by_a_blocked_claim() {
    let db = TestDb::new().await;
    let conn = db
        .connect(ConnectionOptions::default().shutdown_timeout(Duration::from_millis(500)))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let mut locker = db.pool.acquire().await.unwrap().detach();
    let mut lock = locker.begin().await.unwrap();
    sqlx::query("LOCK TABLE postgremq.queue_messages IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    // A 60 s lease means a 30 s server-side bound on the blocked claim.
    let consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().vt_secs(std::num::NonZeroU32::new(60).unwrap()),
        )
        .await
        .unwrap();
    // Close only once the claim is actually waiting on the lock.
    within(5, async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND pid <> pg_backend_pid() \
                   AND wait_event_type = 'Lock' AND query ILIKE '%consume_message%'",
            )
            .fetch_one(&db.pool)
            .await
            .unwrap();
            if waiting > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    let started = Instant::now();
    within(5, conn.close()).await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    drop(consumer);
    lock.rollback().await.unwrap();
}

/// Installs a trigger that counts publish attempts for topic `boom` (in a
/// sequence, which a rollback does not undo) and fails them with `sqlstate`.
async fn failing_publishes(db: &TestDb, sqlstate: &str) {
    sqlx::raw_sql(
        "CREATE SEQUENCE publish_attempts;
         CREATE FUNCTION fail_publish() RETURNS trigger AS $$
         BEGIN
           IF NEW.topic_name = 'boom' THEN
             PERFORM nextval('publish_attempts');
             RAISE EXCEPTION 'injected' USING ERRCODE = current_setting('test.sqlstate');
           END IF;
           RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER fail_publish BEFORE INSERT ON postgremq.messages
           FOR EACH ROW EXECUTE FUNCTION fail_publish();",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("SELECT set_config('test.sqlstate', $1, false)")
        .bind(sqlstate)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE {} SET test.sqlstate = '{sqlstate}'",
        db.name
    )))
    .execute(&db.pool)
    .await
    .unwrap();
}

async fn publish_attempts(db: &TestDb) -> i64 {
    sqlx::query_scalar("SELECT last_value FROM publish_attempts")
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_failing_without_an_abort_is_not_retried() {
    let db = TestDb::new().await;
    failing_publishes(&db, "XX000").await;
    // Fresh connections pick up the database-level setting.
    let conn = db.connect_owned(ConnectionOptions::default()).await;
    conn.create_topic("boom").await.unwrap();
    let err = conn
        .publish("boom", &1, PublishOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.sqlstate().as_deref(), Some("XX000"));
    assert_eq!(publish_attempts(&db).await, 1, "never retried");
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_aborted_by_a_serialization_failure_is_retried() {
    let db = TestDb::new().await;
    failing_publishes(&db, "40001").await;
    let conn = db.connect_owned(ConnectionOptions::default()).await;
    conn.create_topic("boom").await.unwrap();
    let err = conn
        .publish("boom", &1, PublishOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.sqlstate().as_deref(), Some("40001"));
    assert_eq!(
        publish_attempts(&db).await,
        3,
        "the default policy's 3 attempts"
    );
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_listener_reconnects_after_its_session_is_killed() {
    let db = TestDb::new().await;
    let app = unique("pmq_listen");
    let conn = db.connect_tagged(&app, ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    // Only a wake-up can deliver within the bound below.
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().check_timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    let other = db.connect(ConnectionOptions::default()).await;
    for round in 1..=3 {
        let session = within(10, async {
            loop {
                if let [pid] = db.listen_pids(&app).await[..] {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(session)
            .execute(&db.pool)
            .await
            .unwrap();
        // Ready once a new session has re-`LISTEN`ed.
        within(10, async {
            loop {
                let pids = db.listen_pids(&app).await;
                if !pids.is_empty() && !pids.contains(&session) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        // Let the reconnect's wake-up fetch (empty) finish first.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let published = Instant::now();
        other
            .publish(&topic, &round, PublishOptions::default())
            .await
            .unwrap();
        let delivery = within(5, consumer.next()).await.unwrap().unwrap();
        assert!(
            published.elapsed() < Duration::from_secs(2),
            "round {round}: woken after {:?}",
            published.elapsed()
        );
        assert_eq!(delivery.payload_as::<i32>().unwrap(), round);
        delivery.ack().await.unwrap();
    }
    conn.close().await;
    other.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn message_ids_beyond_i32_round_trip() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    sqlx::query("SELECT setval('postgremq.messages_id_seq', 3000000000)")
        .execute(&db.pool)
        .await
        .unwrap();
    let id = conn
        .publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    assert!(id > MessageId::new(i64::from(i32::MAX)));
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    assert_eq!(delivery.message_id(), id);
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_payload_round_trips() {
    let db = TestDb::new().await;
    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    let big = "x".repeat(2 * 1024 * 1024);
    conn.publish(&topic, &big, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = within(10, consumer.next()).await.unwrap().unwrap();
    assert_eq!(delivery.payload_as::<String>().unwrap(), big);
    delivery.ack().await.unwrap();
    assert_eq!(
        conn.publish(&unique("missing"), &1, PublishOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::QueueNotFound
    );
    conn.close().await;
}
