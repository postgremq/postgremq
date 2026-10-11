//! Protocol compatibility: the `postgremq.info()` check at connect.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use common::TestDb;
use std::future::Future as _;

use postgremq::{Connection, ConnectionOptions, Error, ErrorKind, SUPPORTED_PROTOCOL_MAJORS};

async fn connect(pool: &sqlx::PgPool) -> postgremq::Result<Connection> {
    Connection::from_pool(pool.clone(), ConnectionOptions::default()).await
}

async fn execute(pool: &sqlx::PgPool, sql: &'static str) {
    sqlx::raw_sql(sql).execute(pool).await.unwrap();
}

#[test]
fn declares_protocol_major_1() {
    assert_eq!(SUPPORTED_PROTOCOL_MAJORS, &[1]);
}

#[tokio::test]
async fn the_installed_schema_speaks_a_supported_major() {
    let db = TestDb::new().await;
    let major: i32 = sqlx::query_scalar("SELECT (postgremq.info()->>'protocol_major')::int")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(SUPPORTED_PROTOCOL_MAJORS.contains(&u32::try_from(major).unwrap()));
    connect(&db.pool).await.unwrap().close().await;
}

#[tokio::test]
async fn an_unsupported_major_is_rejected_with_the_versions_involved() {
    let db = TestDb::new().await;
    execute(
        &db.pool,
        "CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb LANGUAGE sql STABLE \
         AS $$ SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 99) $$",
    )
    .await;

    let err = connect(&db.pool).await.unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Incompatible);
    match &err {
        Error::Incompatible {
            schema_version,
            protocol_major,
            source,
            ..
        } => {
            assert_eq!(*schema_version, Some(42));
            assert_eq!(*protocol_major, Some(99));
            assert!(source.is_none());
        }
        other => panic!("{other:?}"),
    }
    let message = err.to_string();
    assert!(
        message.contains("version 42") && message.contains("99") && message.contains("[1]"),
        "{message}"
    );
    assert!(std::error::Error::source(&err).is_none());
}

#[tokio::test]
async fn malformed_discovery_is_incompatible() {
    for body in [
        "SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 'one')",
        "SELECT jsonb_build_object('schema_version', 42, 'protocol_major', -1)",
        "SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 1e30)",
        "SELECT jsonb_build_object('schema_version', 42)",
        "SELECT NULL::jsonb",
    ] {
        let db = TestDb::new().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb LANGUAGE sql STABLE AS $$ {body} $$"
        )))
        .execute(&db.pool)
        .await
        .unwrap();

        let err = connect(&db.pool).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Incompatible, "{body}: {err:?}");
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
    execute(
        &db.pool,
        "CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb LANGUAGE sql STABLE \
         AS $$ SELECT jsonb_build_object('schema_version', 42, 'protocol_major', 99) $$",
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
