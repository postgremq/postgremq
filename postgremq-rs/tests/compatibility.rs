//! Schema compatibility: the `postgremq.info()` check at connect.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use common::TestDb;
use std::future::Future as _;

use postgremq::{Connection, ConnectionOptions, Error, ErrorKind, SUPPORTED_MQ_VERSIONS};

async fn connect(pool: &sqlx::PgPool) -> postgremq::Result<Connection> {
    Connection::from_pool(pool.clone(), ConnectionOptions::default()).await
}

async fn execute(pool: &sqlx::PgPool, sql: &'static str) {
    sqlx::raw_sql(sql).execute(pool).await.unwrap();
}

async fn fake_info(pool: &sqlx::PgPool, body: &str) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb LANGUAGE sql STABLE AS $$ {body} $$"
    )))
    .execute(pool)
    .await
    .unwrap();
}

#[test]
fn declares_mq_line_0_1() {
    assert_eq!(SUPPORTED_MQ_VERSIONS, &["0.1"]);
}

#[tokio::test]
async fn the_installed_schema_is_on_a_supported_line() {
    let db = TestDb::new().await;
    let version: i64 = sqlx::query_scalar("SELECT (postgremq.info()->>'schema_version')::bigint")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    let line = if version < 100_000_000 {
        format!("0.{}", version / 10_000)
    } else {
        (version / 100_000_000).to_string()
    };
    assert!(SUPPORTED_MQ_VERSIONS.contains(&line.as_str()));
    connect(&db.pool).await.unwrap().close().await;
}

#[tokio::test]
async fn other_versions_of_the_same_line_are_accepted() {
    // protocol_major is only for older clients: it is ignored.
    for body in [
        "SELECT jsonb_build_object('schema_version', 10000, 'protocol_major', 99)",
        "SELECT jsonb_build_object('schema_version', 19999)",
        "SELECT jsonb_build_object('schema_version', 10001)",
    ] {
        let db = TestDb::new().await;
        fake_info(&db.pool, body).await;
        connect(&db.pool).await.unwrap().close().await;
    }
}

#[tokio::test]
async fn an_unsupported_major_is_rejected_with_the_versions_involved() {
    let db = TestDb::new().await;
    fake_info(
        &db.pool,
        "SELECT jsonb_build_object('schema_version', 100020003, 'protocol_major', 1)",
    )
    .await;

    let err = connect(&db.pool).await.unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Incompatible);
    match &err {
        Error::Incompatible {
            schema_version,
            source,
            ..
        } => {
            assert_eq!(*schema_version, Some(100_020_003));
            assert!(source.is_none());
        }
        other => panic!("{other:?}"),
    }
    let message = err.to_string();
    assert!(
        message.contains("mq 1.2.3 (schema version 100020003)")
            && message.contains("supports mq [0.1]"),
        "{message}"
    );
    assert!(std::error::Error::source(&err).is_none());
}

/// Before 1.0 a breaking schema change bumps the minor, so another 0.y is
/// another contract.
#[tokio::test]
async fn another_minor_before_1_is_rejected() {
    for version in [20_000_u64, 1, 99_999_999] {
        let db = TestDb::new().await;
        fake_info(
            &db.pool,
            &format!("SELECT jsonb_build_object('schema_version', {version}, 'protocol_major', 1)"),
        )
        .await;

        let err = connect(&db.pool).await.unwrap_err();

        match &err {
            Error::Incompatible { schema_version, .. } => {
                assert_eq!(*schema_version, Some(version))
            }
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test]
async fn missing_or_malformed_schema_versions_are_incompatible() {
    for body in [
        "SELECT jsonb_build_object('schema_version', NULL, 'protocol_major', 1)",
        "SELECT jsonb_build_object('schema_version', 'one')",
        "SELECT jsonb_build_object('schema_version', -1)",
        "SELECT jsonb_build_object('schema_version', 0)",
        "SELECT jsonb_build_object('schema_version', 1e30)",
        "SELECT jsonb_build_object('protocol_major', 1)",
        "SELECT NULL::jsonb",
    ] {
        let db = TestDb::new().await;
        fake_info(&db.pool, body).await;

        let err = connect(&db.pool).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Incompatible, "{body}: {err:?}");
        assert!(
            err.to_string().contains("no schema version"),
            "{body}: {err}"
        );
    }
}

#[test]
fn from_pool_outside_a_tokio_runtime_is_a_validation_error() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = runtime.block_on(async {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://127.0.0.1:1/none")
            .unwrap()
    });
    // Polled on a thread without a runtime: the first poll must return the
    // error instead of reaching sqlx, which panics there.
    let outcome = std::thread::spawn(move || {
        let mut future = std::pin::pin!(Connection::from_pool(pool, ConnectionOptions::default()));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(result) => result.map(drop).map_err(|err| err.kind()),
            std::task::Poll::Pending => panic!("from_pool did not fail fast"),
        }
    })
    .join()
    .unwrap();
    assert_eq!(outcome, Err(ErrorKind::Validation));
}

#[tokio::test]
async fn connect_rejects_an_unsupported_major() {
    let db = TestDb::new().await;
    fake_info(
        &db.pool,
        "SELECT jsonb_build_object('schema_version', 100000000)",
    )
    .await;

    let err = Connection::connect(&db.url(), ConnectionOptions::default())
        .await
        .unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Incompatible);
}

#[tokio::test]
async fn missing_discovery_means_the_installation_needs_an_upgrade() {
    let db = TestDb::new().await;
    execute(&db.pool, "DROP FUNCTION postgremq.info()").await;

    let err = connect(&db.pool).await.unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Incompatible);
    assert_eq!(
        err.sqlstate().as_deref(),
        Some("42883"),
        "the database error is kept"
    );
    assert!(std::error::Error::source(&err).is_some());
    assert!(err.to_string().contains("upgrade"), "{err}");
}

#[tokio::test]
async fn a_database_without_postgremq_needs_an_installation() {
    let db = TestDb::empty().await;

    let err = connect(&db.pool).await.unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Incompatible);
    assert_eq!(err.sqlstate().as_deref(), Some("3F000"));
}

#[tokio::test]
async fn a_permission_error_keeps_its_cause() {
    let db = TestDb::new().await;
    let role = format!("pmq_noaccess_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'x'"
    )))
    .execute(&db.pool)
    .await
    .unwrap();

    let result = Connection::connect(&db.url_as(&role, "x"), ConnectionOptions::default()).await;

    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(&db.pool)
        .await
        .unwrap();
    let err = result.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Sqlx);
    assert_eq!(err.sqlstate().as_deref(), Some("42501"));
}

#[tokio::test]
async fn a_function_missing_from_the_installation_fails_through_normal_error_handling() {
    let db = TestDb::new().await;
    let conn = connect(&db.pool).await.unwrap();
    execute(&db.pool, "DROP FUNCTION postgremq.pmq_maintenance_fast()").await;

    let err = conn.maintenance_fast().await.unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Sqlx);
    assert_eq!(err.sqlstate().as_deref(), Some("42883"));
    conn.close().await;
}
