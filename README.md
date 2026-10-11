# PostgreMQ

A message queue built entirely on PostgreSQL: plain SQL tables and functions,
with client libraries for Go, TypeScript and Rust.

[![SQL Tests](https://github.com/postgremq/postgremq/actions/workflows/sql-tests.yml/badge.svg)](https://github.com/postgremq/postgremq/actions/workflows/sql-tests.yml)
[![Go Tests](https://github.com/postgremq/postgremq/actions/workflows/go-tests.yml/badge.svg)](https://github.com/postgremq/postgremq/actions/workflows/go-tests.yml)
[![TypeScript Tests](https://github.com/postgremq/postgremq/actions/workflows/typescript-tests.yml/badge.svg)](https://github.com/postgremq/postgremq/actions/workflows/typescript-tests.yml)
[![Rust Tests](https://github.com/postgremq/postgremq/actions/workflows/rust-tests.yml/badge.svg)](https://github.com/postgremq/postgremq/actions/workflows/rust-tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

PostgreMQ runs in the PostgreSQL database you already have. It is a SQL
script, with no extension and no extra server. Messages are rows, so
publishing and acknowledging can take part in your application's own
transactions.

## Features

- **Topics with fan-out**: publish once; every queue subscribed to the topic
  gets its own copy.
- **Transactional**: publish and acknowledge inside the caller's transaction
  (transactional outbox and inbox).
- **Visibility-timeout leases** with per-delivery ownership tokens; clients
  renew leases automatically while work runs.
- **Retries and a dead letter queue**: a per-queue delivery attempt limit,
  delayed nack, DLQ requeue and purge.
- **Delayed delivery**: publish now, deliver later.
- **Ordered delivery with message groups**: messages sharing a group key are
  delivered in publish order, one at a time, within each queue.
- **Exclusive queues**: temporary queues that live while their owner keeps
  them alive.
- **Low-latency wake-ups** through LISTEN/NOTIFY, with polling as a fallback.
- **Many consumers per queue**: rows are claimed with `SKIP LOCKED`.
- **Observability**: per-queue metrics in SQL and OpenTelemetry client
  metrics, described by one contract shared by the clients.

## Components

| Component | Path | Install |
|-----------|------|---------|
| SQL schema and functions | [`mq/`](./mq/README.md) | run `mq/sql/latest.sql`, or the migrations |
| Go client | [`postgremq-go/`](./postgremq-go/README.md) | `go get postgremq.dev/postgremq-go` |
| TypeScript client | [`postgremq-ts/`](./postgremq-ts/README.md) | `npm install postgremq` |
| Rust client | [`postgremq-rs/`](./postgremq-rs/README.md) | `cargo add postgremq` |
| CLI (migrations) | [`postgremq-go/cmd/postgremq/`](./postgremq-go/cmd/postgremq/README.md) | `go install postgremq.dev/postgremq-go/cmd/postgremq@latest` |

Requirements: PostgreSQL 15+. Go 1.25+, Node.js 22+ or Rust 1.94+ for the
clients.

Each component has its own version and changelog, and the clients check the
database's protocol version when they connect; see [RELEASE.md](./RELEASE.md).

## Quick start

Install the schema into your database. It creates everything in the
`postgremq` schema:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f mq/sql/latest.sql   # fresh installs only
# or, with versioned migrations:
postgremq migrate --dsn "$DATABASE_URL"
```

Each client can also apply the migrations itself at start-up (`Migrate` in Go,
`migrate` in TypeScript and Rust). Either way, later releases upgrade the
schema with the migrations: `latest.sql` records the version it installs.

Then schedule the maintenance functions (see
[Maintenance and retention](./mq/README.md#maintenance-and-retention)).

### Go

```go
conn, err := postgremq.Dial(ctx, cfg) // cfg from pgxpool.ParseConfig
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

_ = conn.CreateTopic(ctx, "orders")
_ = conn.CreateQueue(ctx, "order-processing", "orders", false,
	postgremq.WithMaxDeliveryAttempts(5))

_, _ = conn.Publish(ctx, "orders", json.RawMessage(`{"order_id": 12345}`))

consumer, err := conn.Consume("order-processing", postgremq.WithVT(30))
if err != nil {
	log.Fatal(err)
}
for msg := range consumer.Messages() {
	log.Printf("message %d: %s", msg.ID, msg.Payload)
	_ = msg.Ack(ctx)
}
```

### TypeScript

```typescript
import { connect } from 'postgremq';

const client = await connect({ connectionString: process.env.DATABASE_URL });

await client.createTopic('orders');
await client.createQueue('order-processing', 'orders', false, { maxDeliveryAttempts: 5 });

await client.publish('orders', { orderId: 12345 });

const consumer = client.consume('order-processing', { visibilityTimeoutSec: 30 });
for await (const message of consumer.messages()) {
  console.log(message.payload);
  await message.ack();
}
```

### Rust

```rust
use postgremq::{Connection, ConnectionOptions, ConsumeOptions, PublishOptions, QueueOptions};

let conn = Connection::connect(&database_url, ConnectionOptions::default()).await?;
conn.create_topic("orders").await?;
conn.create_queue("order-processing", "orders", QueueOptions::default().max_delivery_attempts(5))
    .await?;

conn.publish("orders", &serde_json::json!({ "order_id": 12345 }), PublishOptions::default())
    .await?;

let mut consumer = conn.consume("order-processing", ConsumeOptions::default()).await?;
while let Some(delivery) = consumer.next().await {
    let delivery = delivery?;
    println!("{}", delivery.payload());
    delivery.ack().await?;
}
```

Each client's README covers connection options, handler consumers,
transactions, message groups, exclusive queues, shutdown and errors.

## How it works

1. **Publish**: `publish_message` stores the message. A trigger copies it into
   every queue on the topic and notifies `pmq:t:<topic>`.
2. **Consume**: `consume_message` claims visible rows with `SKIP LOCKED`, sets
   their visibility timeout and gives each delivery a fresh ownership token.
3. **Process**: while a handler runs, the client renews the lease in the
   background, batching every in-flight delivery of the connection into one
   call.
4. **Settle**: an **ack** completes the delivery. A **nack** makes it visible
   again, optionally after a delay; its final allowed attempt moves it to the
   dead letter queue. A **release** returns it without counting the attempt.
   A delivery whose lease expires is redelivered.

[docs/architecture.md](./docs/architecture.md) describes the data model,
leases, queue generations, message groups, notifications and the client
design. [docs/delivery-lifecycle.md](./docs/delivery-lifecycle.md) describes
how the clients handle settlement, cancellation, renewal and shutdown, and
where they differ.

## Client feature matrix

| Feature | Go | TypeScript | Rust |
|---------|:--:|:----------:|:----:|
| Publish / ack in the caller's transaction | ✅ | ✅ | ✅ |
| Stream consumer | channel | async iterator | `Stream` |
| Handler consumer with bounded concurrency | ✅ | ✅ | ✅ |
| Automatic lease renewal (batched per connection) | ✅ | ✅ | ✅ |
| Exclusive queues with keep-alive | ✅ | ✅ | ✅ |
| Message groups | ✅ | ✅ | ✅ |
| Delayed delivery and delayed nack | ✅ | ✅ | ✅ |
| Queue-loss notification | ✅ | ✅ | ✅ |
| Graceful shutdown with deadline | ✅ | ✅ | ✅ |
| Retry of transient errors | ✅ | ✅ | ✅ |
| OpenTelemetry metrics | ✅ | ✅ | ✅ (`otel` feature) |
| Schema migrations | ✅ | ✅ | ✅ |

## When to use it

PostgreMQ fits when you already run PostgreSQL and want:

- queueing without operating another system;
- messages that commit or roll back with your data;
- moderate throughput, where durability and simplicity matter more than raw
  speed.

Consider a dedicated broker if you need very high throughput (hundreds of
thousands of messages per second), cross-region replication of queues, or
complex routing.

## Documentation

- [Architecture](./docs/architecture.md)
- [SQL reference and delivery contract](./mq/README.md)
- [Delivery lifecycle (client contract)](./docs/delivery-lifecycle.md)
- [Observability](./docs/observability.md)
- Clients: [Go](./postgremq-go/README.md) · [TypeScript](./postgremq-ts/README.md) · [Rust](./postgremq-rs/README.md) · [CLI](./postgremq-go/cmd/postgremq/README.md)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) for how
to build and test each component, and [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).
Report security issues privately as described in [SECURITY.md](./SECURITY.md).

## License

[MIT](./LICENSE)

## Acknowledgments

PostgreMQ was inspired by [PGMQ](https://github.com/pgmq/pgmq), which showed
that PostgreSQL makes a capable message queue. PostgreMQ puts its own emphasis
on a few areas:

- strictly ordered message groups enforced by the database: a group is
  delivered in publish commit order, one message at a time, whichever way
  consumers read;
- exclusive queues that live only while their owner keeps them alive;
- per-delivery ownership tokens and queue generations, so a stale consumer
  cannot affect a redelivered message or a re-created queue;
- a built-in dead letter queue with per-queue delivery attempt limits;
- client libraries for Go, TypeScript and Rust that renew leases
  automatically, wake up through LISTEN/NOTIFY and shut down gracefully.
