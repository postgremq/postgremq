//! Client metrics exported over OTLP/HTTP: run against
//! `observability/compose.yaml`; see `docs/observability.md`.
//!
//! ```sh
//! cargo run --example metrics --features otel
//! ```
//!
//! `DATABASE_URL` and `OTEL_EXPORTER_OTLP_ENDPOINT` override the defaults
//! (the compose demo database and `http://localhost:4318`).

#![allow(clippy::print_stdout, reason = "an example reports its outcome")]

use std::num::NonZeroUsize;
use std::time::Duration;

use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use postgremq::{
    Connection, ConnectionOptions, ConsumeOptions, Delivery, PublishOptions, QueueOptions,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // The application owns the SDK pipeline: the client only records.
    let exporter = MetricExporter::builder().with_http().build()?;
    let provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(5))
                .build(),
        )
        .with_resource(
            Resource::builder()
                .with_service_name("postgremq-rs-example")
                .build(),
        )
        .build();
    let result = run(&provider).await;
    // Flush after the queue connection drained, so final settlements count.
    let flushed = {
        let provider = provider.clone();
        tokio::task::spawn_blocking(move || {
            let flushed = provider.force_flush();
            let _ = provider.shutdown();
            flushed
        })
        .await?
    };
    result?;
    flushed?;
    println!("Rust client metrics exported");
    Ok(())
}

async fn run(provider: &SdkMeterProvider) -> Result<(), BoxError> {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgremq@localhost:55432/postgremq?sslmode=disable".to_owned()
    });
    let conn = Connection::connect(
        &url,
        ConnectionOptions::default()
            .meter_provider(provider)
            .shutdown_timeout(Duration::from_secs(5)),
    )
    .await?;
    conn.create_topic("metrics_rs").await?;
    conn.create_queue("metrics_rs", "metrics_rs", QueueOptions::default())
        .await?;

    let (done, handled) = tokio::sync::oneshot::channel();
    let done = std::sync::Mutex::new(Some(done));
    let consumer = conn
        .consume_with_handler(
            "metrics_rs",
            ConsumeOptions::default(),
            NonZeroUsize::new(1),
            move |delivery: Delivery| {
                let done = done.lock().ok().and_then(|mut done| done.take());
                async move {
                    // Business work goes here. The explicit ack's latency is
                    // measured as its own operation.
                    let acked = delivery.ack().await;
                    if let Some(done) = done {
                        let _ = done.send(acked.is_ok());
                    }
                    Ok(acked?)
                }
            },
        )
        .await?;
    conn.publish(
        "metrics_rs",
        &serde_json::json!({ "example": "metrics" }),
        PublishOptions::default(),
    )
    .await?;
    let acked = tokio::time::timeout(Duration::from_secs(30), handled).await??;
    consumer.stop().await;
    conn.close().await;
    if !acked {
        return Err("the message was not acked".into());
    }
    Ok(())
}
