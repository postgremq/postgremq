# Observability

PostgreMQ exposes queue state through `postgremq.queue_metrics()` and client
activity through optional OpenTelemetry metrics. SQL is the authority for queue
state across all languages and application replicas. Clients record observations
that require application context. No client creates an SDK, exporter,
collection timer or global provider; your application owns those resources.

## Run the complete example

From the repository root, with Docker, Go 1.26+, Node.js 22+ and Rust 1.94+
installed:

```sh
docker compose -f observability/compose.yaml up -d
```

This starts an isolated PostgreSQL database on localhost:55432 and Collector
Contrib **0.147.0**, with OTLP/HTTP on localhost:4318 and a Prometheus endpoint on
localhost:8889. The SQL schema and demo queues are installed automatically.
Demo credentials are only for this local database.

Run the clients in separate terminals, or sequentially:

```sh
cd postgremq-go/examples/metrics
go run .
```

```sh
cd postgremq-ts
npm ci
npm exec -- ts-node examples/metrics.ts
```

```sh
cd postgremq-rs
cargo run --example metrics --features otel
```

The Go example is its own module (its OTLP exporter dependencies need Go 1.26);
the Go client itself does not. Each example publishes and handles a message,
drains the connection, and flushes its application-owned meter provider, using
the public client APIs. Set `DATABASE_URL` and `OTEL_EXPORTER_OTLP_ENDPOINT` to
override their defaults (the demo database and `http://localhost:4318`).

```sh
curl http://localhost:8889/metrics
```

Allow up to 15 seconds for queue metrics to appear. Names are translated to
Prometheus syntax: for example `messaging.client.operation.duration` becomes
`messaging_client_operation_duration_seconds`, with `_bucket`, `_count`, and
`_sum` series, counters gain `_total`, and `postgremq.queue.active` (unit `1`)
becomes `postgremq_queue_active_ratio`. Prometheus can scrape this endpoint; no
Prometheus server is required to run the example. Stop and remove the demo
database with:

```sh
docker compose -f observability/compose.yaml down
```

## Database metrics

```sql
SELECT * FROM postgremq.queue_metrics();
```

The function returns one row per queue, including empty and expired exclusive
queues. Every row uses the same statement timestamp and database snapshot. It
runs with caller privileges, is read-only, and never reads message payloads.
`get_queue_statistics()` returns raw status counts instead.

| SQL column | OTel metric | Meaning |
|---|---|---|
| `active` | `postgremq.queue.active` | 1 for non-exclusive queues and unexpired exclusive queues; otherwise 0 |
| `ready` | `postgremq.queue.messages.ready` | Pending or expired processing deliveries eligible for another attempt; 0 for expired exclusive queues |
| `delayed` | `postgremq.queue.messages.delayed` | Pending deliveries whose visibility time is in the future |
| `processing` | `postgremq.queue.messages.processing` | Processing deliveries whose visibility timeout has not expired, including the final allowed attempt |
| `exhausted` | `postgremq.queue.messages.exhausted` | Pending/processing deliveries whose visibility timeout has expired and retry limit has been reached, awaiting DLQ maintenance |
| `dead_letter` | `postgremq.queue.messages.dead_letter` | Deliveries currently in the DLQ |
| `oldest_ready_age_seconds` | `postgremq.queue.oldest_ready_age` | Seconds since the earliest currently eligible visibility time (`vt`); 0 when nothing is ready or the exclusive queue has expired |

All are gauges, with `queue_name` and `topic_name` attributes. Count units are
`{message}`, age is `s`, and active is `1`. `ready` describes eligibility; a row
may still be locked by an in-progress transaction, or be a message-group
successor waiting behind its group's head (see
[Message groups](../mq/README.md#message-groups)). `delayed`, `processing`,
`exhausted` and DLQ counts describe retained state even in an expired queue;
`active` identifies that queue's unavailable state. They are not counts of active
application handlers.

Ready age measures waiting in the **current** delivery cycle. It excludes the
scheduled delay and resets when release/nack changes `vt`; it is not age since
original publication. There is no sequence-derived "total published" counter.
Rolled-back publications are not counted. Completed retention is excluded so
routine collection does not require counting retained completed deliveries.

The query aggregates unsettled delivery rows and DLQ rows once across all queues.
It can use the partial indexes on pending and processing deliveries; its cost
still grows with backlog and DLQ size. Measure scrape duration with your
backlog, retain a query deadline, and adjust collection frequency as needed.
It adds no counters, triggers, payload processing, or writes to the queue hot path.

## Configure the Collector

[`observability/collector.yaml`](../observability/collector.yaml) is a complete,
tested configuration using the [SQL Query receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.147.0/receiver/sqlqueryreceiver). Supply:

- `POSTGREMQ_METRICS_DSN`: PostgreSQL DSN for a dedicated monitoring login. The
  example sets `connect_timeout=5` and `statement_timeout=5000` (milliseconds).
- `POSTGREMQ_DATABASE_ID`: A stable identity for this database deployment, unique
  among databases sharing the same telemetry destination. It becomes the SQL
  metrics' `service.instance.id`; the SQL service name is `postgremq`.

Provision a monitoring role through your normal credential management, then grant:

```sql
GRANT USAGE ON SCHEMA postgremq TO postgremq_metrics;
GRANT SELECT ON postgremq.queues, postgremq.queue_messages,
    postgremq.dead_letter_queue TO postgremq_metrics;
GRANT EXECUTE ON FUNCTION postgremq.queue_metrics() TO postgremq_metrics;
```

`queue_metrics()` is security-invoker, not security-definer. These table grants
are required even when execution is granted. The login needs no access to
`postgremq.messages` or application tables. Use your deployment's database TLS
settings. The demo publishes Collector ports only on loopback; configure network
access and OTLP authentication/TLS for a shared deployment.

Run **one active SQL scraper per database**, independently of application replica
count. Multiple collectors scraping the same database can duplicate these gauges.
Application replicas should each send their own client metrics, with their own
`service.name` and `service.instance.id`. Never overwrite application resource
identity with the SQL scraper's identity. The supplied configuration uses separate
SQL/client pipelines to preserve this distinction.

The SQL Query receiver's metrics support is alpha. Keep the Collector image pinned
and run the integration test before upgrades. The configuration exports Prometheus
metrics for a self-contained example. For an OTLP backend, replace the exporter
and reference it in both pipelines:

```yaml
exporters:
  otlphttp/backend:
    endpoint: ${env:OTEL_EXPORTER_OTLP_ENDPOINT}
```

The provided Prometheus exporter promotes resource attributes to labels. Keep
application resource attributes bounded, as well as queue/topic names.

## Client instrumentation contract, version 1

Pass a meter provider when constructing the connection:

```go
conn, err := postgremq.Dial(ctx, poolConfig,
    postgremq.WithMeterProvider(meterProvider))
```

```typescript
const connection = await connect({ connectionString, meterProvider });
```

```rust
// Cargo.toml: postgremq = { version = "...", features = ["otel"] }
let conn = Connection::connect(url,
    ConnectionOptions::default().meter_provider(&meter_provider)).await?;
```

Metrics are disabled when the provider is omitted. In Rust, instrumentation is
compiled only with the `otel` cargo feature (an `opentelemetry` API dependency,
no SDK), which also provides `ConnectionOptions::meter_provider`; without it every
recorder is a zero-sized no-op. To use a global provider, pass it explicitly.
Close or drain the queue connection before flushing and shutting down the
provider. No client shuts down or flushes a supplied provider. Each obtains a
library-scoped meter from it and creates instruments; this is different from
constructing an SDK meter provider. The clients' runtime dependencies are the
OpenTelemetry API only; SDK and exporter packages are used by tests and examples.
This follows the [OTel library guidance](https://opentelemetry.io/docs/specs/otel/library-guidelines/#requirements).

To share an existing global provider explicitly:

```go
postgremq.WithMeterProvider(otel.GetMeterProvider())
```

```typescript
// import { metrics } from '@opentelemetry/api';
const connection = await connect({ connectionString, meterProvider: metrics.getMeterProvider() });
```

```rust
ConnectionOptions::default().meter_provider(&opentelemetry::global::meter_provider())
```

Omitting the option stays a no-op even if another library has configured a global
provider. Complete SDK and OTLP exporter setup is in the
[Go example](../postgremq-go/examples/metrics/main.go),
[TypeScript example](../postgremq-ts/examples/metrics.ts) and
[Rust example](../postgremq-rs/examples/metrics.rs).

All clients use instrumentation scope `postgremq`, version `1`. The
`messaging.*` names follow the OTel messaging conventions (1.44.0, in
Development status); the shared PostgreMQ contract is versioned separately and is
not changed when dependencies update.
[`observability/client-contract.json`](../observability/client-contract.json) is
the machine-readable form of the names, units, scope, buckets and the shared test
scenario.

| Metric | Instrument / unit | Recording boundary |
|---|---|---|
| `messaging.client.operation.duration` | Histogram / `s` | One logical operation, including pool acquisition and internal retries |
| `messaging.process.duration` | Histogram / `s` | One handler callback, from invocation until return, throw, error or panic |
| `messaging.client.sent.messages` | Counter / `{message}` | One message per attempted publish SQL query, successful or failed |
| `messaging.client.consumed.messages` | Counter / `{message}` | Each delivery successfully decoded from a consume result |
| `postgremq.client.handlers.active` | UpDownCounter / `{handler}` | +1 at callback entry, -1 at callback exit |
| `postgremq.client.renewal.lost` | Counter / `{message}` | The connection's automatic renewal retires a still-registered delivery whose lease it found lost (omitted from a renewal result, or out of confirmed lease) |

Histogram boundaries in seconds are `0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25,
0.5, 0.75, 1, 2.5, 5, 7.5, 10`. Applications can override aggregation through SDK
views. Histogram counts provide operation and handler counts without duplicate
counters.

### Operations

Operation names are `publish`, `consume`, `ack`, `nack`, `release`, `extend`
(manual extension), `extend_batch` (one automatic renewal call) and `keep_alive`
(one exclusive-queue keep-alive call). The TypeScript client's automatic
keep-alive records no `keep_alive` sample; Go and Rust record one per call. Admin
operations and notification reconnects are not instrumented in contract v1. Duration histograms record one
sample per logical operation, not per SQL attempt. Empty consume calls are
included. A duplicate local settlement rejected before the connection operation is
invoked produces no operation sample. Batch operations produce one sample per
batch, including batches in which no row was extended. Row-level renewal loss is
reported by the separate renewal-loss counter, since the SQL batch can succeed
overall.

### Attributes

Operation duration, processing duration, active handlers, sent and consumed
counts share:

- `messaging.system = postgremq`.
- `messaging.operation.name`: the operation listed above, or `process` for
  handler callbacks.
- `messaging.operation.type`: `send` for publish, `receive` for consume, `settle`
  for ack/nack/release; otherwise the same value as the operation name (`process`,
  `extend`, `extend_batch`, `keep_alive`).
- `messaging.destination.name`: the topic for publish, the queue for single-queue
  operations, processing and consumed counts. Omitted for `extend_batch` and
  `keep_alive`, which span queues.

Operation duration and the sent counter also have `postgremq.transaction`: `true`
for publish and ack inside a caller-owned transaction, `false` otherwise. A
successful transactional call means **the SQL operation returned successfully**,
not that the transaction committed. A failed call may also have an ambiguous
server outcome. These metrics are not committed-publication counters; use the SQL
gauges for database state.

Failed operation durations and failed send attempts carry `error.type`, one of
`lease_lost`, `queue_not_found`, `validation`, `connection_closed`, `cancelled`,
`deadline_exceeded` or `other`. Raw error text, SQL, message IDs, tokens and
payloads are never metric attributes. `queue_not_found` includes a queue that
became fatal. The clients differ in what they can observe as cancellation or a
deadline:

| | `cancelled` | `deadline_exceeded` |
|---|---|---|
| Go | the operation's `context` was cancelled | the operation's `context` deadline passed, including the client's internal bounds on fetch, renewal and keep-alive calls |
| TypeScript | an `AbortError` | not emitted: internal query deadlines are reported as `other` |
| Rust | the operation's future was dropped before it completed, or a renewal or keep-alive call was abandoned at the shutdown deadline | a socket I/O timeout, a pool acquire timeout, a claim that outlived its safety bound, or a renewal or keep-alive call cut off by its own time bound |

A server-side statement cancellation (SQLSTATE `57014`) is `other` in every client.

### Sent messages

Sent counts follow the attempted-send definition in the
[OTel conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#producer-metrics).
A send attempt begins when the client invokes its driver's publish query API. Each
completed attempt adds one message, with `error.type` if that attempt failed.
Internal retries count separately: an aborted first query followed by a successful
retry produces two sent messages (one failed, one successful) and one successful
operation-duration observation.

A connection rejection, or a payload serialization failure in TypeScript or Rust,
happens before the query is invoked and adds no sent message. In Go the pool's
query call also acquires the connection, so a failed acquisition counts as a failed
attempt; TypeScript and Rust acquire the connection first and count only the query.
In Rust, an attempt dropped mid-query counts as `cancelled`, since the statement
may still have been applied. A driver error can occur before the request reaches
PostgreSQL: this is a client attempt counter, not a wire-delivery or
durable-commit count. Transactional attempts count even if the caller later rolls
back.

### Consumed messages

Consumed counts have `postgremq.redelivered` (boolean), true when the delivery
attempt is greater than 1. They include prefetched deliveries that may later be
released, and can count the same logical publication more than once. They are
recorded once per delivery, not again on processing. A Go partial batch counts the
rows decoded before the error, with that operation's `error.type` attached; Rust
skips (and does not count) a claimed row it cannot decode. A response lost entirely
cannot be counted by the client.

### Handler processing

Handler metrics are recorded only for handler consumers (Go `ConsumeHandler`,
TypeScript `consumeHandler`, Rust `consume_with_handler`). Processing duration
excludes the automatic settlement performed **after** the callback returns.
Explicit settlement awaited **inside** the callback is part of callback time and
also has its own operation duration. Returning normally has no error attribute. A
panic (Go, Rust), a throw (TypeScript) or a returned `Err` (Rust) records
`handler_error`; returning normally after the delivery's stop signal was cancelled
records `cancelled` (in Rust, also an aborted handler task). Explicit nack is
measured as a nack operation, not inferred as a callback failure. These
observations describe callback execution, not business success or transaction
commit.

Manual iterator, channel or stream consumption has no automatic handler duration
or active count: the library cannot identify your processing boundary. Instrument
that application code directly. Other client metrics still work for manual
consumers.

### Renewal loss

Renewal loss has only `messaging.system` and `messaging.destination.name`. It
counts retirement by the automatic renewal schedule, not every lease-lost API
error, and is not emitted for an entry already deregistered when a batch finishes.
Go and TypeScript deregister a delivery when its settlement call finishes, so a
renewal that finds the row already settled while that call is still in progress
counts it. Rust does not count a delivery whose settlement has started. Ordinary
shutdown cancellation is not counted.

No distributed tracing or trace-header propagation is added by this metrics API.

## Tests

From the repository root:

```sh
pytest mq/tests/tests.py observability/test_collector.py -v
```

The Collector test starts isolated Docker containers with the pinned image, runs
the shipped configuration using a restricted database login, verifies SQL metric
values and their change after a scrape, runs the three client examples, and
verifies their exported OTLP metrics. It uses dynamic host ports and removes all
containers and networks on exit. It needs `mq/tests/requirements.txt`, Docker,
Go 1.26+ (it runs `go run .` in `postgremq-go/examples/metrics`), `npm ci` in
`postgremq-ts`, and a Rust toolchain (it builds the Rust example on first run).
The dedicated observability CI job runs this test.

The normal client suites include in-memory SDK tests: `go test ./...` in
`postgremq-go`, `npm test` in `postgremq-ts`, and `cargo test --features otel` in
`postgremq-rs` (which starts a PostgreSQL container unless
`POSTGREMQ_TEST_DATABASE_URL` names a server). All read
[`client-contract.json`](../observability/client-contract.json) to check names,
units, scope, buckets, operation types and an identical sequence of transactional
publish/ack, rollback, redelivery, empty receive and failed settlement. Additional
tests verify active handlers, panic/throw/cancellation, automatic renewal loss,
one logical operation sample across a retried SQL publication, failed and
successful sent counts, and no-op instrumentation without a provider.
Provider-ownership tests verify that closing a queue connection leaves the
application's provider usable.

Useful initial alerts are sustained ready age, exhausted deliveries awaiting
maintenance, nonzero DLQ, increasing renewal loss, and failed operations. Set
thresholds from your processing latency and retry policy.
