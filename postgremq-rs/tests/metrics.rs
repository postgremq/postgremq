//! Client metrics against the shared contract (`observability/client-contract.json`,
//! `docs/observability.md`), as the Go and TypeScript clients test it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests"
)]

mod common;

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::time::Duration;

use common::{TestDb, unique, within};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use postgremq::{
    Connection, ConnectionOptions, ConsumeOptions, Consumer, Delivery, ErrorKind, PublishOptions,
    QueueOptions, RetryConfig,
};
use serde::Deserialize;

/// An application-owned SDK pipeline reading into memory.
struct Telemetry {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
}

impl Telemetry {
    fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        Self { exporter, provider }
    }

    /// The latest cumulative snapshot.
    fn collect(&self) -> Snapshot {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        let exported = self.exporter.get_finished_metrics().unwrap();
        // Nothing recorded yet exports nothing.
        exported.last().map(Snapshot::from).unwrap_or_default()
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        let _ = self.provider.shutdown();
    }
}

type Attributes = BTreeMap<String, String>;

fn attributes<'a>(pairs: impl Iterator<Item = &'a KeyValue>) -> Attributes {
    pairs
        .map(|pair| (pair.key.as_str().to_owned(), pair.value.to_string()))
        .collect()
}

#[derive(Debug)]
struct HistogramPoint {
    attributes: Attributes,
    count: u64,
    sum: f64,
    bounds: Vec<f64>,
}

#[derive(Debug, Default)]
struct Snapshot {
    /// Scope name → version.
    scopes: Vec<(String, Option<String>)>,
    units: HashMap<String, String>,
    histograms: HashMap<String, Vec<HistogramPoint>>,
    sums: HashMap<String, Vec<(Attributes, i128)>>,
}

impl From<&ResourceMetrics> for Snapshot {
    fn from(resource: &ResourceMetrics) -> Self {
        let mut snapshot = Self::default();
        for scope in resource.scope_metrics() {
            snapshot.scopes.push((
                scope.scope().name().to_owned(),
                scope.scope().version().map(str::to_owned),
            ));
            if scope.scope().name() != "postgremq" {
                continue;
            }
            for metric in scope.metrics() {
                let name = metric.name().to_owned();
                snapshot
                    .units
                    .insert(name.clone(), metric.unit().to_owned());
                match metric.data() {
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        snapshot.histograms.insert(
                            name,
                            histogram
                                .data_points()
                                .map(|point| HistogramPoint {
                                    attributes: attributes(point.attributes()),
                                    count: point.count(),
                                    sum: point.sum(),
                                    bounds: point.bounds().collect(),
                                })
                                .collect(),
                        );
                    }
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        snapshot.sums.insert(
                            name,
                            sum.data_points()
                                .map(|point| {
                                    (attributes(point.attributes()), i128::from(point.value()))
                                })
                                .collect(),
                        );
                    }
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                        snapshot.sums.insert(
                            name,
                            sum.data_points()
                                .map(|point| {
                                    (attributes(point.attributes()), i128::from(point.value()))
                                })
                                .collect(),
                        );
                    }
                    other => panic!("unexpected aggregation for {name}: {other:?}"),
                }
            }
        }
        snapshot
    }
}

impl Snapshot {
    fn operations(&self) -> &[HistogramPoint] {
        self.histograms
            .get("messaging.client.operation.duration")
            .map_or(&[], Vec::as_slice)
    }

    fn operation_count(&self, name: &str) -> u64 {
        self.operations()
            .iter()
            .filter(|point| point.attributes["messaging.operation.name"] == name)
            .map(|point| point.count)
            .sum()
    }

    fn sum(&self, name: &str) -> &[(Attributes, i128)] {
        self.sums.get(name).map_or(&[], Vec::as_slice)
    }
}

#[derive(Deserialize)]
struct Contract {
    scope: String,
    version: String,
    histogram_boundaries: Vec<f64>,
    metrics: HashMap<String, String>,
    scenario_operations: Vec<ExpectedOperation>,
    received: Received,
    sent: Vec<ExpectedOperation>,
}

#[derive(Deserialize)]
struct ExpectedOperation {
    name: String,
    destination: String,
    transaction: bool,
    error: String,
    count: u64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct Received {
    first: i128,
    redelivered: i128,
}

fn contract() -> Contract {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../observability/client-contract.json"
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Every exported metric uses the contract's unit, and every histogram its
/// buckets.
fn assert_contract_shape(snapshot: &Snapshot) {
    let contract = contract();
    for (name, unit) in &snapshot.units {
        assert_eq!(Some(unit), contract.metrics.get(name), "{name}");
    }
    for (name, points) in &snapshot.histograms {
        for point in points {
            assert_eq!(point.bounds, contract.histogram_boundaries, "{name}");
        }
    }
}

fn expected_attributes(expected: &ExpectedOperation) -> Attributes {
    let mut attributes: Attributes = [
        ("messaging.system", "postgremq".to_owned()),
        ("messaging.operation.name", expected.name.clone()),
        ("messaging.operation.type", expected.kind.clone()),
        ("messaging.destination.name", expected.destination.clone()),
        ("postgremq.transaction", expected.transaction.to_string()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();
    if !expected.error.is_empty() {
        attributes.insert("error.type".to_owned(), expected.error.clone());
    }
    attributes
}

/// A consumer that claims exactly once when it starts: no notifications, no
/// renewal, and no poll for a day (Go and TypeScript test with a direct,
/// single-shot consume call; Rust consumes only through consumers).
fn single_claim() -> ConsumeOptions {
    ConsumeOptions::default()
        .auto_extend(false)
        .check_timeout(Duration::from_secs(24 * 3600))
}

async fn take(conn: &Connection, queue: &str) -> (Consumer, Delivery) {
    let mut consumer = conn.consume(queue, single_claim()).await.unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    (consumer, delivery)
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_contract_with_transactions_and_redelivery() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let conn = db
        .connect(
            ConnectionOptions::default()
                .notifications(false)
                .retry(RetryConfig::default().max_attempts(NonZeroU32::MIN))
                .meter_provider(&telemetry.provider),
        )
        .await;
    conn.create_topic("metrics").await.unwrap();
    conn.create_queue("q", "metrics", QueueOptions::default())
        .await
        .unwrap();
    conn.publish("metrics", &serde_json::json!({}), PublishOptions::default())
        .await
        .unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    conn.publish_tx(
        &mut tx,
        "metrics",
        &serde_json::json!({}),
        PublishOptions::default(),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        conn.publish("missing", &serde_json::json!({}), PublishOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::QueueNotFound
    );

    let (consumer, delivery) = take(&conn, "q").await;
    delivery.extend(NonZeroU32::new(30).unwrap()).await.unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    delivery.ack_tx(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();
    // A local duplicate settlement runs no operation and records none.
    assert_eq!(
        delivery.ack().await.unwrap_err().kind(),
        ErrorKind::LeaseLost
    );
    consumer.stop().await;
    sqlx::query("UPDATE postgremq.queue_messages SET vt = clock_timestamp() - interval '1 second'")
        .execute(&db.pool)
        .await
        .unwrap();
    let (consumer, delivery) = take(&conn, "q").await;
    delivery.nack(None).await.unwrap();
    consumer.stop().await;
    let (consumer, delivery) = take(&conn, "q").await;
    delivery.release().await.unwrap();
    consumer.stop().await;

    // No live messages left, but an empty receive is still an operation.
    sqlx::query("DELETE FROM postgremq.queue_messages")
        .execute(&db.pool)
        .await
        .unwrap();
    let empty = conn.consume("q", single_claim()).await.unwrap();
    within(5, async {
        while telemetry.collect().operation_count("consume") < 4 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    empty.stop().await;

    // A delivery whose server-side lease disappears fails to settle.
    sqlx::query("SELECT postgremq.publish_message('metrics', '{}')")
        .execute(&db.pool)
        .await
        .unwrap();
    let (consumer, lost) = take(&conn, "q").await;
    sqlx::query("UPDATE postgremq.queue_messages SET consumer_token = gen_random_uuid()::text")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(lost.ack().await.unwrap_err().kind(), ErrorKind::LeaseLost);
    consumer.stop().await;
    conn.close().await;

    let contract = contract();
    let snapshot = telemetry.collect();
    assert_eq!(
        snapshot.scopes,
        vec![(contract.scope.clone(), Some(contract.version.clone()))]
    );
    for (name, unit) in &snapshot.units {
        assert_eq!(Some(unit), contract.metrics.get(name), "{name}");
    }
    let operations = snapshot.operations();
    assert_eq!(
        operations.len(),
        contract.scenario_operations.len(),
        "{operations:#?}"
    );
    for expected in &contract.scenario_operations {
        let attributes = expected_attributes(expected);
        let point = operations
            .iter()
            .find(|point| point.attributes == attributes)
            .unwrap_or_else(|| panic!("missing operation {attributes:?} in {operations:#?}"));
        assert_eq!(point.count, expected.count, "{attributes:?}");
        assert!(point.sum >= 0.0);
        assert_eq!(point.bounds, contract.histogram_boundaries);
    }
    let sent = snapshot.sum("messaging.client.sent.messages");
    assert_eq!(sent.len(), contract.sent.len(), "{sent:?}");
    for expected in &contract.sent {
        let attributes = expected_attributes(expected);
        let (_, value) = sent
            .iter()
            .find(|(point, _)| *point == attributes)
            .unwrap_or_else(|| panic!("missing sent counter {attributes:?} in {sent:?}"));
        assert_eq!(*value, i128::from(expected.count));
    }
    let consumed = snapshot.sum("messaging.client.consumed.messages");
    let total = |redelivered: &str| -> i128 {
        consumed
            .iter()
            .filter(|(attributes, _)| attributes["postgremq.redelivered"] == redelivered)
            .map(|(_, value)| value)
            .sum()
    };
    assert_eq!(total("false"), contract.received.first);
    assert_eq!(total("true"), contract.received.redelivered);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_publish_is_one_operation_and_two_send_attempts() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let conn = db
        .connect(ConnectionOptions::default().meter_provider(&telemetry.provider))
        .await;
    conn.create_topic("metrics").await.unwrap();
    let fault = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../observability/retry-once.sql"
    ))
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(fault))
        .execute(&db.pool)
        .await
        .unwrap();
    conn.publish("metrics", &serde_json::json!({}), PublishOptions::default())
        .await
        .unwrap();
    let attempts: i64 = sqlx::query_scalar("SELECT last_value FROM public.metrics_retry_probe")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(attempts, 2);
    conn.close().await;

    let snapshot = telemetry.collect();
    let mut by_error: BTreeMap<String, i128> = BTreeMap::new();
    for (attributes, value) in snapshot.sum("messaging.client.sent.messages") {
        *by_error
            .entry(attributes.get("error.type").cloned().unwrap_or_default())
            .or_default() += value;
    }
    assert_eq!(
        by_error,
        BTreeMap::from([(String::new(), 1), ("other".to_owned(), 1)])
    );
    let operations = snapshot.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].count, 1);
    assert!(!operations[0].attributes.contains_key("error.type"));
    assert!(operations[0].sum >= 0.1, "includes the retry backoff");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_provider_is_optional_and_application_owned() {
    for enabled in [false, true] {
        let db = TestDb::new().await;
        let telemetry = Telemetry::new();
        // A global provider alone never enables the client's metrics.
        opentelemetry::global::set_meter_provider(telemetry.provider.clone());
        let mut options = ConnectionOptions::default();
        if enabled {
            options = options.meter_provider(&telemetry.provider);
        }
        let conn = db.connect(options).await;
        conn.create_topic("optional").await.unwrap();
        conn.create_queue("q", "optional", QueueOptions::default())
            .await
            .unwrap();
        conn.publish(
            "optional",
            &serde_json::json!({}),
            PublishOptions::default(),
        )
        .await
        .unwrap();
        let (consumer, delivery) = take(&conn, "q").await;
        delivery.ack().await.unwrap();
        consumer.stop().await;
        conn.close().await;
        // Rejected before any query: not an attempted send.
        assert_eq!(
            conn.publish(
                "optional",
                &serde_json::json!({}),
                PublishOptions::default()
            )
            .await
            .unwrap_err()
            .kind(),
            ErrorKind::Closed
        );
        let probe = telemetry
            .provider
            .meter("application")
            .u64_counter("application.probe")
            .build();
        probe.add(1, &[]);

        let snapshot = telemetry.collect();
        let scopes: Vec<&str> = snapshot
            .scopes
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert!(scopes.contains(&"application"), "{scopes:?}");
        assert_eq!(scopes.contains(&"postgremq"), enabled, "{scopes:?}");
        if enabled {
            let sent: i128 = snapshot
                .sum("messaging.client.sent.messages")
                .iter()
                .map(|(_, value)| value)
                .sum();
            assert_eq!(sent, 1);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_callbacks_are_measured_and_balanced() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let conn = db
        .connect(ConnectionOptions::default().meter_provider(&telemetry.provider))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    // One attempt: a failed callback is retired, not redelivered.
    conn.create_queue(
        &queue,
        &topic,
        QueueOptions::default().max_delivery_attempts(1),
    )
    .await
    .unwrap();
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let consumer = conn
        .consume_with_handler(&queue, ConsumeOptions::default(), None, {
            let entered = std::sync::Arc::clone(&entered);
            move |delivery: Delivery| {
                let entered = std::sync::Arc::clone(&entered);
                async move {
                    match delivery.payload_as::<String>()?.as_str() {
                        "error" => Err("private handler detail".into()),
                        "panic" => panic!("handler panicked"),
                        "wait" => {
                            entered.notify_one();
                            delivery.stopped().cancelled().await;
                            Ok(())
                        }
                        _ => Ok(()),
                    }
                }
            }
        })
        .await
        .unwrap();
    for outcome in ["ok", "error", "panic"] {
        conn.publish(&topic, &outcome, PublishOptions::default())
            .await
            .unwrap();
    }
    within(10, async {
        loop {
            let done: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM postgremq.queue_messages WHERE queue_name = $1 \
                   AND status = 'completed'",
            )
            .bind(&queue)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            let dead: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name = $1",
            )
            .bind(&queue)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            if done == 1 && dead == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    conn.publish(&topic, &"wait", PublishOptions::default())
        .await
        .unwrap();
    within(5, entered.notified()).await;
    let active: i128 = telemetry
        .collect()
        .sum("postgremq.client.handlers.active")
        .iter()
        .map(|(_, value)| value)
        .sum();
    assert_eq!(active, 1, "the waiting callback is active");
    within(10, consumer.stop()).await;
    conn.close().await;

    let snapshot = telemetry.collect();
    assert_contract_shape(&snapshot);
    let active: i128 = snapshot
        .sum("postgremq.client.handlers.active")
        .iter()
        .map(|(_, value)| value)
        .sum();
    assert_eq!(active, 0, "balanced after every callback returned");
    let mut by_error: BTreeMap<String, u64> = BTreeMap::new();
    for point in &snapshot.histograms["messaging.process.duration"] {
        assert_eq!(point.attributes["messaging.operation.type"], "process");
        assert_eq!(point.attributes["messaging.destination.name"], queue);
        *by_error
            .entry(
                point
                    .attributes
                    .get("error.type")
                    .cloned()
                    .unwrap_or_default(),
            )
            .or_default() += point.count;
    }
    assert_eq!(
        by_error,
        BTreeMap::from([
            (String::new(), 1),
            ("cancelled".to_owned(), 1),
            ("handler_error".to_owned(), 2),
        ])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_renewal_and_background_batches_are_recorded() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let conn = db
        .connect(ConnectionOptions::default().meter_provider(&telemetry.provider))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    let exclusive = unique("x");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    conn.create_queue(
        &exclusive,
        &topic,
        QueueOptions::default()
            .exclusive(true)
            .keep_alive_interval(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    conn.publish(&topic, &1, PublishOptions::default())
        .await
        .unwrap();
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().vt_secs(NonZeroU32::new(2).unwrap()),
        )
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    // The lease is taken away: the next renewal finds it gone.
    sqlx::query(
        "UPDATE postgremq.queue_messages SET consumer_token = gen_random_uuid()::text \
         WHERE queue_name = $1",
    )
    .bind(&queue)
    .execute(&db.pool)
    .await
    .unwrap();
    within(5, delivery.stopped().cancelled()).await;
    drop(delivery);
    consumer.stop().await;
    // The keep-alive runs on its own schedule: wait for a completed one.
    within(10, async {
        while !telemetry.collect().operations().iter().any(|point| {
            point.attributes["messaging.operation.name"] == "keep_alive"
                && !point.attributes.contains_key("error.type")
        }) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    conn.close().await;

    let snapshot = telemetry.collect();
    assert_contract_shape(&snapshot);
    let lost = snapshot.sum("postgremq.client.renewal.lost");
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(
        lost[0].0,
        Attributes::from([
            ("messaging.destination.name".to_owned(), queue.clone()),
            ("messaging.system".to_owned(), "postgremq".to_owned()),
        ])
    );
    assert_eq!(lost[0].1, 1);
    for (operation, kind) in [
        ("extend_batch", "extend_batch"),
        ("keep_alive", "keep_alive"),
    ] {
        let point = snapshot
            .operations()
            .iter()
            .find(|point| {
                point.attributes["messaging.operation.name"] == operation
                    && !point.attributes.contains_key("error.type")
            })
            .unwrap_or_else(|| panic!("no successful {operation} sample"));
        assert!(point.count >= 1);
        // Batches span queues: no destination.
        assert_eq!(
            point.attributes.keys().cloned().collect::<Vec<_>>(),
            [
                "messaging.operation.name",
                "messaging.operation.type",
                "messaging.system",
                "postgremq.transaction"
            ]
        );
        assert_eq!(point.attributes["messaging.operation.type"], kind);
    }
}

/// A connection with metrics, a topic and a queue holding `messages`.
async fn seeded(
    telemetry: &Telemetry,
    db: &TestDb,
    messages: usize,
) -> (Connection, String, String) {
    let conn = db
        .connect(ConnectionOptions::default().meter_provider(&telemetry.provider))
        .await;
    let topic = unique("t");
    let queue = unique("q");
    conn.create_topic(&topic).await.unwrap();
    conn.create_queue(&queue, &topic, QueueOptions::default())
        .await
        .unwrap();
    for n in 0..messages {
        conn.publish(&topic, &n, PublishOptions::default())
            .await
            .unwrap();
    }
    (conn, topic, queue)
}

/// Holds `table` locked (ACCESS EXCLUSIVE) until the returned transaction
/// ends, so statements on it block.
async fn lock_table(db: &TestDb, table: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut lock = db.pool.begin().await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE postgremq.{table} IN ACCESS EXCLUSIVE MODE"
    )))
    .execute(&mut *lock)
    .await
    .unwrap();
    lock
}

/// Waits until a statement containing `needle` is blocked on a lock.
async fn blocked_on_lock(db: &TestDb, needle: &str) {
    within(10, async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND pid <> pg_backend_pid() \
                   AND wait_event_type = 'Lock' AND query ILIKE '%' || $1 || '%'",
            )
            .bind(needle)
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
}

#[tokio::test(flavor = "multi_thread")]
async fn operations_dropped_mid_flight_are_recorded_as_cancelled() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let (conn, topic, queue) = seeded(&telemetry, &db, 1).await;
    let mut consumer = conn.consume(&queue, single_claim()).await.unwrap();
    let delivery = std::sync::Arc::new(within(5, consumer.next()).await.unwrap().unwrap());

    // A publish dropped while its statement is blocked: the attempt counts
    // (it may still commit) and the operation is cancelled.
    let lock = lock_table(&db, "messages").await;
    let publish = tokio::spawn({
        let conn = conn.clone();
        let topic = topic.clone();
        async move { conn.publish(&topic, &1, PublishOptions::default()).await }
    });
    blocked_on_lock(&db, "publish_message").await;
    publish.abort();
    assert!(publish.await.unwrap_err().is_cancelled());
    lock.rollback().await.unwrap();

    // Likewise a settlement.
    let lock = lock_table(&db, "queue_messages").await;
    let ack = tokio::spawn({
        let delivery = std::sync::Arc::clone(&delivery);
        async move { delivery.ack().await }
    });
    blocked_on_lock(&db, "ack_message").await;
    ack.abort();
    assert!(ack.await.unwrap_err().is_cancelled());
    lock.rollback().await.unwrap();
    drop(delivery);
    consumer.stop().await;
    conn.close().await;

    let snapshot = telemetry.collect();
    assert_contract_shape(&snapshot);
    let cancelled = |name: &str| -> u64 {
        snapshot
            .operations()
            .iter()
            .filter(|point| {
                point.attributes["messaging.operation.name"] == name
                    && point.attributes.get("error.type").map(String::as_str) == Some("cancelled")
            })
            .map(|point| point.count)
            .sum()
    };
    assert_eq!(cancelled("publish"), 1);
    assert_eq!(cancelled("ack"), 1);
    let sent: i128 = snapshot
        .sum("messaging.client.sent.messages")
        .iter()
        .filter(|(attributes, _)| {
            attributes.get("error.type").map(String::as_str) == Some("cancelled")
        })
        .map(|(_, value)| value)
        .sum();
    assert_eq!(sent, 1, "the dropped attempt counts");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_batch_cut_off_by_its_deadline_is_recorded() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let (conn, _topic, queue) = seeded(&telemetry, &db, 1).await;
    let mut consumer = conn
        .consume(
            &queue,
            ConsumeOptions::default().vt_secs(NonZeroU32::new(8).unwrap()),
        )
        .await
        .unwrap();
    let delivery = within(5, consumer.next()).await.unwrap().unwrap();
    // The renewal (due after 4 s) blocks on the table and hits its 1 s bound;
    // the 8 s lease leaves ample time to ack afterwards.
    let lock = lock_table(&db, "queue_messages").await;
    within(10, async {
        while !telemetry.collect().operations().iter().any(|point| {
            point.attributes["messaging.operation.name"] == "extend_batch"
                && point.attributes.get("error.type").map(String::as_str)
                    == Some("deadline_exceeded")
        }) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    lock.rollback().await.unwrap();
    delivery.ack().await.unwrap();
    consumer.stop().await;
    conn.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn processing_excludes_the_automatic_settlement() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let (conn, _topic, queue) = seeded(&telemetry, &db, 1).await;
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let consumer = conn
        .consume_with_handler(&queue, ConsumeOptions::default(), None, {
            let (entered, release) = (
                std::sync::Arc::clone(&entered),
                std::sync::Arc::clone(&release),
            );
            move |_delivery: Delivery| {
                let (entered, release) = (
                    std::sync::Arc::clone(&entered),
                    std::sync::Arc::clone(&release),
                );
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
    within(5, entered.notified()).await;
    // The callback returns; its automatic ack then blocks on the row.
    let mut lock = db.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM postgremq.queue_messages WHERE queue_name = $1 FOR UPDATE")
        .bind(&queue)
        .execute(&mut *lock)
        .await
        .unwrap();
    release.notify_one();
    blocked_on_lock(&db, "ack_message").await;

    // While the settlement is still blocked, processing has already ended:
    // recorded, and no longer active; the ack is not recorded yet.
    let during = telemetry.collect();
    let processed: u64 = during.histograms["messaging.process.duration"]
        .iter()
        .map(|point| point.count)
        .sum();
    assert_eq!(processed, 1);
    let active: i128 = during
        .sum("postgremq.client.handlers.active")
        .iter()
        .map(|(_, value)| value)
        .sum();
    assert_eq!(active, 0);
    assert_eq!(during.operation_count("ack"), 0);

    lock.rollback().await.unwrap();
    within(10, consumer.stop()).await;
    conn.close().await;
    let after = telemetry.collect();
    assert_contract_shape(&after);
    assert_eq!(
        after.operation_count("ack"),
        1,
        "the ack is its own operation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_administratively_cancelled_statement_is_not_a_deadline() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let (conn, topic, _queue) = seeded(&telemetry, &db, 0).await;
    let lock = lock_table(&db, "messages").await;
    let publish = tokio::spawn({
        let conn = conn.clone();
        async move { conn.publish(&topic, &1, PublishOptions::default()).await }
    });
    blocked_on_lock(&db, "publish_message").await;
    // An operator cancels the statement (SQLSTATE 57014): no deadline expired.
    sqlx::query(
        "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
           AND wait_event_type = 'Lock' AND query ILIKE '%publish_message%'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let err = within(5, publish).await.unwrap().unwrap_err();
    assert_eq!(err.sqlstate().as_deref(), Some("57014"));
    lock.rollback().await.unwrap();
    conn.close().await;

    let snapshot = telemetry.collect();
    let publish = snapshot
        .operations()
        .iter()
        .find(|point| point.attributes["messaging.operation.name"] == "publish")
        .expect("the publish operation");
    assert_eq!(
        publish.attributes.get("error.type").map(String::as_str),
        Some("other")
    );
    let sent = snapshot.sum("messaging.client.sent.messages");
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].0.get("error.type").map(String::as_str),
        Some("other")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_administratively_cancelled_claim_is_not_a_deadline() {
    let db = TestDb::new().await;
    let telemetry = Telemetry::new();
    let (conn, _topic, queue) = seeded(&telemetry, &db, 0).await;
    let lock = lock_table(&db, "queue_messages").await;
    // A 60 s lease bounds the claim at 30 s server-side: the cancel is first.
    let consumer = conn
        .consume(&queue, single_claim().vt_secs(NonZeroU32::new(60).unwrap()))
        .await
        .unwrap();
    blocked_on_lock(&db, "consume_message").await;
    sqlx::query(
        "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
           AND wait_event_type = 'Lock' AND query ILIKE '%consume_message%'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    within(5, async {
        while telemetry.collect().operation_count("consume") == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    lock.rollback().await.unwrap();
    consumer.stop().await;
    conn.close().await;

    let snapshot = telemetry.collect();
    let consume = snapshot
        .operations()
        .iter()
        .find(|point| {
            point.attributes["messaging.operation.name"] == "consume"
                && point.attributes.contains_key("error.type")
        })
        .expect("the cancelled claim");
    assert_eq!(consume.attributes["error.type"], "other");
}
