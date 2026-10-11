//! Schema migrations: golang-migrate-compatible version table, up only.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use common::{TestDb, unique};
use postgremq::{ConnectionOptions, ConsumeOptions, ErrorKind, migrate, migration_status};
use sqlx::PgPool;
use std::time::Duration;

async fn advisory_locks_held(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn status_of_an_empty_database() {
    let db = TestDb::empty().await;
    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, 0);
    assert!(!status.dirty);
    assert!(status.latest_version >= 1);
    assert!(status.needs_migration);
    let absent: bool = sqlx::query_scalar("SELECT to_regnamespace('postgremq') IS NULL")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(absent, "reading the status creates nothing");
}

#[tokio::test]
async fn installs_a_usable_schema_and_records_the_version_like_golang_migrate() {
    let db = TestDb::empty().await;
    migrate(&db.pool).await.unwrap();

    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, status.latest_version);
    assert!(!status.dirty && !status.needs_migration);

    let columns: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT column_name::text, data_type::text, is_nullable::text FROM information_schema.columns \
         WHERE table_schema = 'postgremq' AND table_name = 'postgremq_migrations' \
         ORDER BY ordinal_position",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let columns: Vec<(&str, &str, &str)> = columns
        .iter()
        .map(|(a, b, c)| (a.as_str(), b.as_str(), c.as_str()))
        .collect();
    assert_eq!(
        columns,
        [("version", "bigint", "NO"), ("dirty", "boolean", "NO")]
    );
    let rows: Vec<(i64, bool)> =
        sqlx::query_as("SELECT version, dirty FROM postgremq.postgremq_migrations")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        [(i64::try_from(status.latest_version).unwrap(), false)]
    );
    assert_eq!(advisory_locks_held(&db.pool).await, 0);

    let conn = db.connect(ConnectionOptions::default()).await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, Default::default())
        .await
        .unwrap();
    let id = conn
        .publish(&topic, &serde_json::json!({"ok": true}), Default::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(&queue, ConsumeOptions::default())
        .await
        .unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.message_id(), id);
    delivery.ack().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn a_latest_sql_install_is_recognised_as_current() {
    let db = TestDb::new().await; // installs latest.sql
    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, status.latest_version);
    assert!(!status.dirty && !status.needs_migration);
    migrate(&db.pool).await.unwrap();
    assert_eq!(migration_status(&db.pool).await.unwrap(), status);
}

#[tokio::test]
async fn is_idempotent() {
    let db = TestDb::empty().await;
    migrate(&db.pool).await.unwrap();
    migrate(&db.pool).await.unwrap();
    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, status.latest_version);
    assert!(!status.needs_migration);
}

#[tokio::test]
async fn concurrent_callers_in_separate_pools_are_serialised() {
    let db = TestDb::empty().await;
    let pools = [
        db.other_pool().await,
        db.other_pool().await,
        db.other_pool().await,
    ];
    let tasks: Vec<_> = pools
        .iter()
        .cloned()
        .map(|pool| tokio::spawn(async move { migrate(&pool).await }))
        .collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, status.latest_version);
    assert!(!status.dirty);
    for pool in pools {
        pool.close().await;
    }
}

#[tokio::test]
async fn leaves_a_database_migrated_by_a_newer_client_unchanged() {
    let db = TestDb::empty().await;
    migrate(&db.pool).await.unwrap();
    sqlx::query("UPDATE postgremq.postgremq_migrations SET version = 999")
        .execute(&db.pool)
        .await
        .unwrap();

    migrate(&db.pool).await.unwrap();

    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, 999);
    assert!(!status.dirty && !status.needs_migration);
}

#[tokio::test]
async fn refuses_a_dirty_database_and_releases_the_lock() {
    let db = TestDb::empty().await;
    migrate(&db.pool).await.unwrap();
    sqlx::query("UPDATE postgremq.postgremq_migrations SET dirty = true")
        .execute(&db.pool)
        .await
        .unwrap();

    let err = migrate(&db.pool).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::DirtySchema);
    let latest = migration_status(&db.pool).await.unwrap().latest_version;
    assert!(
        matches!(err, postgremq::Error::DirtySchema { version, .. } if version == latest),
        "{err:?}"
    );
    assert_eq!(advisory_locks_held(&db.pool).await, 0);
    assert!(migration_status(&db.pool).await.unwrap().dirty);
}

#[tokio::test]
async fn a_dropped_migrate_future_releases_the_lock() {
    let db = TestDb::empty().await;
    migrate(&db.pool).await.unwrap();
    // Block the next migrate() after it takes the advisory lock.
    let mut blocker = db.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE postgremq.postgremq_migrations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let timed_out = tokio::time::timeout(Duration::from_millis(300), migrate(&db.pool)).await;
    assert!(timed_out.is_err(), "migrate() should still be blocked");
    assert_eq!(advisory_locks_held(&db.pool).await, 1);
    blocker.commit().await.unwrap();

    // The dropped future's session ends, so the server releases its lock.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while advisory_locks_held(&db.pool).await > 0 {
        assert!(tokio::time::Instant::now() < deadline, "the lock leaked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::timeout(Duration::from_secs(10), migrate(&db.pool))
        .await
        .expect("migrate() after a dropped call")
        .unwrap();
}

#[tokio::test]
async fn a_migrate_dropped_while_applying_still_finishes_cleanly() {
    let db = TestDb::empty().await;
    sqlx::query("CREATE SCHEMA postgremq")
        .execute(&db.pool)
        .await
        .unwrap();
    // An uncommitted table of the same name blocks the migration body.
    let mut blocker = db.pool.begin().await.unwrap();
    sqlx::query("CREATE TABLE postgremq.topics (blocker int)")
        .execute(&mut *blocker)
        .await
        .unwrap();

    let timed_out = tokio::time::timeout(Duration::from_millis(500), migrate(&db.pool)).await;
    assert!(timed_out.is_err(), "migrate() should be blocked mid-apply");
    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!((status.current_version, status.dirty), (1, true));
    blocker.rollback().await.unwrap();

    // The apply phase keeps running without its caller, through every
    // migration.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = migration_status(&db.pool).await.unwrap();
        if (status.current_version, status.dirty) == (status.latest_version, false) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "migrations left unfinished: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    migrate(&db.pool).await.unwrap();
}

#[tokio::test]
async fn an_empty_version_table_means_nothing_applied() {
    let db = TestDb::empty().await;
    sqlx::raw_sql(
        "CREATE SCHEMA postgremq; \
         CREATE TABLE postgremq.postgremq_migrations (version bigint not null primary key, dirty boolean not null)",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(migration_status(&db.pool).await.unwrap().current_version, 0);

    migrate(&db.pool).await.unwrap();

    let status = migration_status(&db.pool).await.unwrap();
    assert_eq!(status.current_version, status.latest_version);
    assert!(!status.dirty);
}
