//! Integration-test harness: a fresh database per test, cloned from a template
//! with `mq/sql/latest.sql` installed.
//!
//! The server is `POSTGREMQ_TEST_DATABASE_URL` when set (a PostgreSQL 15+
//! server whose user can create databases); otherwise the harness starts (or
//! reuses) a `postgres:15` container named `postgremq-rs-test` itself (Docker
//! required). It is kept for later runs; remove it with
//! `docker rm -f postgremq-rs-test`.
//! The template is named after a hash of the schema, so a schema change gets
//! a fresh template.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test harness shared by several test crates"
)]

use std::sync::OnceLock;
use std::time::Duration;

use postgremq::{Connection, ConnectionOptions};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions as _, Connection as _, PgConnection, PgPool};
use tokio::sync::OnceCell;

const SCHEMA: &str = include_str!("../../../mq/sql/latest.sql");

static TEMPLATE: OnceCell<String> = OnceCell::const_new();

static SERVER: OnceLock<PgConnectOptions> = OnceLock::new();

fn server_options() -> PgConnectOptions {
    SERVER.get_or_init(start_server).clone()
}

fn start_server() -> PgConnectOptions {
    if let Ok(url) = std::env::var("POSTGREMQ_TEST_DATABASE_URL") {
        return url.parse().expect("invalid POSTGREMQ_TEST_DATABASE_URL");
    }
    // Each #[tokio::test] has its own short-lived runtime, so the container
    // handle lives on a dedicated thread and runtime for the whole process.
    // A named, reusable container is shared by every run instead of one
    // leaking per run.
    let (ready, address) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("postgres-testcontainer".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                use testcontainers::runners::AsyncRunner as _;
                use testcontainers::{ImageExt as _, ReuseDirective};
                use testcontainers_modules::postgres::Postgres;
                let container = Postgres::default()
                    .with_tag("15")
                    .with_container_name("postgremq-rs-test")
                    .with_reuse(ReuseDirective::Always)
                    .start()
                    .await
                    .expect("start postgres:15 (Docker is required when POSTGREMQ_TEST_DATABASE_URL is unset)");
                let host = container.get_host().await.unwrap().to_string();
                let port = container.get_host_port_ipv4(5432).await.unwrap();
                ready.send((host, port)).unwrap();
                std::future::pending::<()>().await;
                drop(container);
            });
        })
        .unwrap();
    let (host, port) = address
        .recv()
        .expect("postgres test container did not start");
    PgConnectOptions::new()
        .host(&host)
        .port(port)
        .username("postgres")
        .password("postgres")
        .database("postgres")
}

/// FNV-1a: a stable schema fingerprint for the template name.
fn fingerprint(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Runs one dynamic DDL statement (names are generated, never user input).
async fn ddl(conn: &mut PgConnection, sql: String) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(conn)
        .await
        .map(drop)
}

async fn admin() -> PgConnection {
    server_options()
        .connect()
        .await
        .expect("connect to the test server")
}

async fn template() -> &'static str {
    TEMPLATE
        .get_or_init(|| async {
            let name = format!("pmq_rs_tpl_{:016x}", fingerprint(SCHEMA));
            let mut admin = admin().await;
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                    .bind(&name)
                    .fetch_one(&mut admin)
                    .await
                    .unwrap();
            if !exists {
                let building = format!("{name}_build_{}", std::process::id());
                ddl(&mut admin, format!("DROP DATABASE IF EXISTS {building}"))
                    .await
                    .unwrap();
                ddl(&mut admin, format!("CREATE DATABASE {building}"))
                    .await
                    .unwrap();
                let mut db = server_options()
                    .database(&building)
                    .connect()
                    .await
                    .unwrap();
                sqlx::raw_sql(SCHEMA).execute(&mut db).await.unwrap();
                db.close().await.unwrap();
                // Another process may have won the race; either way the
                // template exists afterwards.
                let renamed = ddl(
                    &mut admin,
                    format!("ALTER DATABASE {building} RENAME TO {name}"),
                )
                .await;
                if renamed.is_err() {
                    ddl(&mut admin, format!("DROP DATABASE IF EXISTS {building}"))
                        .await
                        .unwrap();
                }
            }
            admin.close().await.unwrap();
            name
        })
        .await
}

/// A database dropped when this value is dropped.
pub(crate) struct TestDb {
    pub(crate) name: String,
    pub(crate) pool: PgPool,
}

impl TestDb {
    pub(crate) async fn new() -> Self {
        let template = template().await;
        let name = format!("pmq_rs_{}", uuid::Uuid::new_v4().simple());
        let mut admin = admin().await;
        let create = format!("CREATE DATABASE {name} TEMPLATE {template}");
        let mut attempt = 0;
        // Concurrent clones of one template can briefly collide.
        while let Err(err) = ddl(&mut admin, create.clone()).await {
            attempt += 1;
            assert!(attempt < 20, "cannot create test database: {err}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        admin.close().await.unwrap();
        let pool = PgPoolOptions::new()
            .test_before_acquire(false)
            .max_connections(20)
            .connect_with(server_options().database(&name))
            .await
            .unwrap();
        Self { name, pool }
    }

    /// A fresh database WITHOUT the PostgreMQ schema (for migration tests).
    pub(crate) async fn empty() -> Self {
        let name = format!("pmq_rs_empty_{}", uuid::Uuid::new_v4().simple());
        let mut admin = admin().await;
        ddl(&mut admin, format!("CREATE DATABASE {name}"))
            .await
            .unwrap();
        admin.close().await.unwrap();
        let pool = PgPoolOptions::new()
            .test_before_acquire(false)
            .max_connections(10)
            .connect_with(server_options().database(&name))
            .await
            .unwrap();
        Self { name, pool }
    }

    /// A separate pool on this database (as another process would have).
    pub(crate) async fn other_pool(&self) -> PgPool {
        PgPoolOptions::new()
            .max_connections(2)
            .connect_with(server_options().database(&self.name))
            .await
            .unwrap()
    }

    /// A connection over this database's pool.
    pub(crate) async fn connect(&self, options: ConnectionOptions) -> Connection {
        Connection::from_pool(self.pool.clone(), options)
            .await
            .unwrap()
    }

    /// This database's URL.
    pub(crate) fn url(&self) -> String {
        server_options()
            .database(&self.name)
            .to_url_lossy()
            .to_string()
    }

    /// This database's URL, connecting as `user`.
    pub(crate) fn url_as(&self, user: &str, password: &str) -> String {
        server_options()
            .database(&self.name)
            .username(user)
            .password(password)
            .to_url_lossy()
            .to_string()
    }

    /// A connection with its own pool (as a separate process would have).
    pub(crate) async fn connect_owned(&self, options: ConnectionOptions) -> Connection {
        let url = server_options()
            .database(&self.name)
            .to_url_lossy()
            .to_string();
        Connection::connect(&url, options).await.unwrap()
    }

    /// Like [`connect_owned`](Self::connect_owned), with every session of the
    /// connection (pool and LISTEN) tagged with `application_name = app`.
    pub(crate) async fn connect_tagged(&self, app: &str, options: ConnectionOptions) -> Connection {
        let pool = PgPoolOptions::new()
            .test_before_acquire(false)
            .connect_with(server_options().database(&self.name).application_name(app))
            .await
            .unwrap();
        Connection::from_pool(pool, options).await.unwrap()
    }

    /// The `LISTEN` session backends of this database tagged `app`.
    pub(crate) async fn listen_pids(&self, app: &str) -> Vec<i32> {
        sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = current_database() AND application_name = $1 \
               AND (query ILIKE 'LISTEN %' OR query ILIKE 'UNLISTEN %')",
        )
        .bind(app)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    /// Terminates every backend of this database tagged `app`; returns how
    /// many were terminated.
    pub(crate) async fn kill_tagged(&self, app: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = current_database() AND application_name = $1) killed",
        )
        .bind(app)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        // Drop cannot await; drop the database from a helper thread.
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let mut admin = admin().await;
                let _ignored = ddl(
                    &mut admin,
                    format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
                )
                .await;
            });
        });
        let _ignored = dropped.join();
    }
}

/// A unique, NOTIFY-safe name.
pub(crate) fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Fails the test if `future` does not finish within `secs`.
pub(crate) async fn within<T>(secs: u64, future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(secs), future)
        .await
        .expect("timed out")
}
