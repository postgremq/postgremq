//! Smoke test of the packaged crate, built outside the repository
//! (scripts/release/smoke-rust.sh): migrate a fresh database, connect
//! (protocol check), publish, consume and ack one message.

use postgremq::{Connection, ConnectionOptions, ConsumeOptions, PublishOptions, QueueOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("SMOKE_DATABASE_URL")?;
    let pool = postgremq::sqlx::PgPool::connect(&url).await?;
    postgremq::migrate(&pool).await?;
    let status = postgremq::migration_status(&pool).await?;
    assert!(!status.needs_migration && !status.dirty, "{status:?}");

    let conn = Connection::from_pool(pool, ConnectionOptions::default()).await?;
    conn.create_topic("smoke").await?;
    conn.create_queue("smoke-q", "smoke", QueueOptions::default()).await?;
    let id = conn
        .publish("smoke", &serde_json::json!({"ok": true}), PublishOptions::default())
        .await?;
    let mut consumer = conn.consume("smoke-q", ConsumeOptions::default()).await?;
    let delivery = consumer.next().await.ok_or("no message")??;
    assert_eq!(delivery.message_id(), id);
    delivery.ack().await?;
    conn.close().await;
    println!(
        "rust smoke ok: protocol majors {:?}, schema version {}",
        postgremq::SUPPORTED_PROTOCOL_MAJORS,
        status.current_version
    );
    Ok(())
}
