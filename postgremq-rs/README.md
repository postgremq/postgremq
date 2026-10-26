# postgremq

Rust client for [PostgreMQ](https://github.com/postgremq/postgremq), a message
queue that lives in PostgreSQL. Topics fan out to queues; each delivery is
leased with a visibility timeout and fenced by a per-claim token; deliveries
that exhaust their attempts move to a dead letter queue; `LISTEN`/`NOTIFY`
wakes consumers, with polling as the fallback. Publishing and acknowledging
can run inside the application's own transaction.

The crate is built on `sqlx` 0.9 (Postgres, rustls) and Tokio. It follows the
shared [delivery lifecycle contract](https://github.com/postgremq/postgremq/blob/main/docs/delivery-lifecycle.md)
of the PostgreMQ clients.

## Installation

```sh
cargo add postgremq
# or, with OpenTelemetry client metrics:
cargo add postgremq --features otel
```

The minimum supported Rust version is 1.94 (edition 2024). TLS is rustls with
the platform's native roots (`tls-rustls-ring-native-roots`); this is not
configurable. The crate re-exports the `sqlx`, `serde_json` and (with `otel`)
`opentelemetry` versions its API uses, plus `CancellationToken` from
`tokio-util`.

The database needs the PostgreMQ schema; PostgreSQL 15 or later is required.
Connecting does not create it. Install or upgrade it with `migrate`, which
applies the migrations embedded in the crate:

```rust,no_run
# async fn example(pool: sqlx::PgPool) -> postgremq::Result<()> {
postgremq::migrate(&pool).await?; // no-op when the schema is current
let status = postgremq::migration_status(&pool).await?;
assert!(!status.needs_migration);
println!("mq {}", postgremq::format_schema_version(status.current_version)); // such as mq 0.1.0
# Ok(())
# }
```

Versions are schema versions: the mq version stored as one number (10000 is
mq 0.1.0), which `format_schema_version` turns back into the mq version.
`migrate` only goes up, to the latest version in this crate. It uses the same
version table and advisory lock as the Go client and the
[migration CLI](https://github.com/postgremq/postgremq/blob/main/postgremq-go/cmd/postgremq/README.md),
so concurrent callers are safe and any of them can upgrade a database another
one installed. A database already at a newer version (migrated by a newer
release) is left unchanged. A failed migration is `Error::Migration` (its
version is left dirty, and it was rolled back when the database reported the
error; it has the `version`, and the `position` (line and column) in the
migration's file when PostgreSQL reports one it can map; the database error
is its source, and the message adds its detail, hint, failing query inside a
function and context). A dirty database (a migration failed
partway) is `Error::DirtySchema`, and a recorded version below the latest
that is none of the migrations, from which no upgrade is known, is
`Error::UnknownSchemaVersion`. `migrate` creates the `postgremq` schema when it is
missing, which needs the `CREATE` privilege on the database. You can also
install the schema from
[`mq/sql/latest.sql`](https://github.com/postgremq/postgremq/blob/main/mq/sql/latest.sql)
or with the CLI. `latest.sql` is for fresh databases only (it refuses to run
on an existing installation) and records the version it installs, so `migrate`
can upgrade it later.

## Quick start

```rust,no_run
use postgremq::{Connection, ConnectionOptions, ConsumeOptions, PublishOptions, QueueOptions};

# async fn example() -> postgremq::Result<()> {
let conn = Connection::connect("postgres://localhost/app", ConnectionOptions::default()).await?;
conn.create_topic("orders").await?;
conn.create_queue("billing", "orders", QueueOptions::default().max_delivery_attempts(5))
    .await?;

conn.publish("orders", &postgremq::serde_json::json!({ "order_id": 7 }), PublishOptions::default())
    .await?;

let mut consumer = conn.consume("billing", ConsumeOptions::default()).await?;
while let Some(delivery) = consumer.next().await {
    let delivery = delivery?; // only Err(QueueGone) ends the stream with an error
    // ... process delivery.payload() ...
    delivery.ack().await?;
}
conn.close().await;
# Ok(())
# }
```

## Connecting

`Connection::connect(url, options)` creates a pool that the connection owns
and closes in `close`. `Connection::from_pool(pool, options).await` uses an
existing `PgPool`, which it never closes; it must be called inside a Tokio
runtime.

Both read `postgremq.info()` and check that the installation's schema version
is on an mq compatibility line this client implements
(`postgremq::SUPPORTED_MQ_VERSIONS`, currently `["0.3"]`). A line is an mq
major, or before 1.0 a major and minor: a breaking schema change bumps the
major, or before 1.0 the minor, so a client built for mq 0.3 accepts 0.3.x and
rejects 0.2.x and 0.4.0. Otherwise they return `Error::Incompatible` with the schema
version (the message also shows its mq version and the supported lines). A
database without `info()` (not installed, or older than discovery), or whose
`info()` reports no schema version, gets it too, meaning it needs an
installation or upgrade; when `info()` is missing, its source is the database
error. Connection and permission errors are returned as they are. Within a
supported line, a function the installation lacks fails with the database's
error (SQLSTATE `42883`, `Error::Sqlx`) like any other.
`Connection` is cheap to clone: clones share the pool, one `LISTEN` session and
the two background schedulers (lease renewal and queue keep-alive).
`Connection::pool()` returns the pool, e.g. to begin a transaction.

`connect` builds its pool with `test_before_acquire(false)` (sqlx's default
ping before every checkout doubles the round trips of each claim and
settlement) and a 3-minute `idle_timeout`, so idle sockets are recycled before
typical load-balancer cut-offs; operations that are safe to repeat retry a
dropped connection. Consider the same settings for a pool passed to
`from_pool`.

`ConnectionOptions` (start from `default()` and chain the setters):

| Option | Default | Meaning |
| --- | --- | --- |
| `shutdown_timeout` | none (wait indefinitely) | How long `close` waits for in-flight deliveries to settle |
| `retry` | `RetryConfig::default()` | Retry policy for transient database errors (see [Retries](#retries)) |
| `renewal_batch_size` | 100 | Most deliveries renewed by one `set_vt_batch_multi` call |
| `notifications` | `true` | Use `LISTEN`/`NOTIFY`; when `false`, consumers poll every `check_timeout` (e.g. behind a transaction-pooling proxy, which cannot carry a `LISTEN` session) |
| `on_queue_fatal` | none (logged) | Hook called once per queue incarnation when a queue is gone (see [Queue gone](#queue-gone)) |
| `meter_provider` | none | OpenTelemetry meter provider, `otel` feature only (see [Metrics](#metrics)) |

## Topics and queues

```rust,no_run
# async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
use std::time::Duration;
use postgremq::QueueOptions;

conn.create_topic("orders").await?; // idempotent
let generation = conn
    .create_queue("billing", "orders", QueueOptions::default().max_delivery_attempts(5))
    .await?;

// A temporary queue that exists while this connection keeps it alive:
conn.create_queue(
    "orders-audit-tmp",
    "orders",
    QueueOptions::default()
        .exclusive(true)
        .keep_alive_interval(Duration::from_secs(30)),
)
.await?;
# let _ = generation;
# Ok(())
# }
```

`create_queue` returns the queue's `Generation`, the UUID of this incarnation
of the queue (a queue deleted and recreated under the same name gets a new
one). Re-declaring a queue with identical options returns its generation;
different options are an `Error::Validation`.

`QueueOptions`:

| Option | Default | Meaning |
| --- | --- | --- |
| `max_delivery_attempts` | 0 | Attempts before a delivery moves to the dead letter queue; 0 retries forever |
| `exclusive` | `false` | The queue expires unless kept alive |
| `keep_alive_interval` | 300 s | An exclusive queue's lease, in `[1s, 30 days]` |

An exclusive queue declared through a connection is kept alive by that
connection while it is open: one keep-alive task per connection renews all of
its exclusive queues with one `extend_queue_keep_alive_multi` call per tick,
each at half its remaining lease. Transient errors are retried until the last
confirmed keep-alive deadline; a queue the server no longer reports, or one
whose deadline passes, is gone (see [Queue gone](#queue-gone)).

`delete_queue` deletes a queue and its deliveries (refused with
`Error::Validation` while it has dead-letter entries; a queue that does not
exist is not an error); `delete_topic` deletes a topic that has no messages. Topic and queue names are limited to 57 ASCII
bytes (PostgreSQL channel names, see the
[SQL README](https://github.com/postgremq/postgremq/blob/main/mq/README.md)).

## Publishing

```rust,no_run
# async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
use std::time::{Duration, SystemTime};
use postgremq::PublishOptions;

let order = postgremq::serde_json::json!({ "id": 7, "total": 42 });
let id = conn.publish("orders", &order, PublishOptions::default()).await?;

// Invisible for a minute:
let later = SystemTime::now() + Duration::from_secs(60);
conn.publish("orders", &order, PublishOptions::default().deliver_after(later))
    .await?;
# let _ = id;
# Ok(())
# }
```

`publish` takes any `T: Serialize` payload (stored as JSONB) and returns the
`MessageId`. It is retried only on SQLSTATE `40001`/`40P01`, the only proof
that nothing was published; any other failure, including a disconnect, has an
unknown outcome and is returned as is. `PublishOptions::deliver_after` delays
delivery until the database clock reaches it (at most about 100 years ahead);
`group_key` is described under
[Ordered delivery](#ordered-delivery-message-groups).

### Inside the caller's transaction

`publish_tx` and `Delivery::ack_tx` take the caller's connection (`&mut tx`
for a `sqlx::Transaction`). The client never begins, commits or rolls back:
the message exists, and the ack takes effect, only if the caller's
transaction commits. These calls are never retried, and a database error
leaves the caller's transaction aborted.

```rust,no_run
# async fn example(conn: postgremq::Connection, delivery: postgremq::Delivery) -> postgremq::Result<()> {
let mut tx = conn.pool().begin().await?;
postgremq::sqlx::query("UPDATE app.orders SET billed = true WHERE id = 7")
    .execute(&mut *tx)
    .await?;
conn.publish_tx(&mut tx, "audit", &"order billed", Default::default()).await?;
delivery.ack_tx(&mut tx).await?;
tx.commit().await?;
# Ok(())
# }
```

`ack_tx` ends lease renewal once its statement completes; if the transaction
then rolls back, the delivery is redelivered after its lease expires.

## Ordered delivery (message groups)

Publish with `PublishOptions::default().group_key(key)` (1 to 255
characters). Within each queue, a group's messages are delivered one at a
time, in publish commit order; `Delivery::group_key` and `Delivery::group_seq`
(dense, 1-based) identify them. A leased or delayed head blocks its group; a
head that exhausts its attempts moves to the dead letter queue, which unblocks
the group. Publishers of one group are serialized by the database, so a group
should be something like a session or an order, not a hot shared key. A
buffered group head blocks its group for every consumer until processed, so
keep `batch_size` small when groups move slowly. See the
[SQL README](https://github.com/postgremq/postgremq/blob/main/mq/README.md#message-groups)
for the full contract.

```rust,no_run
# async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
use postgremq::PublishOptions;

let event = postgremq::serde_json::json!({ "customer": 17, "event": "paid" });
conn.publish("orders", &event, PublishOptions::default().group_key("customer-17"))
    .await?;
# Ok(())
# }
```

## Consuming

`ConsumeOptions` (shared by both consumer styles):

| Option | Default | Meaning |
| --- | --- | --- |
| `batch_size` | 10 | Deliveries claimed per fetch; the next batch is prefetched, so up to about twice this many can be leased but not yet handed out |
| `vt_secs` | 30 | Lease (visibility timeout) per claim, in seconds |
| `check_timeout` | 10 s | Poll interval when no notification arrives, in `[10ms, 24h]` |
| `auto_extend` | `true` | Renew in-flight leases automatically |
| `extension_threshold` | 0.5 | Fraction of the remaining lease after which a renewal is sent, in `(0, 1)` |
| `generation` | current | Bind to this queue incarnation |

A consumer binds to `ConsumeOptions::generation`, else to the generation this
connection last declared with `create_queue`, else to the queue's current one.
Starting one on a queue (or generation) that does not exist fails with
`Error::QueueNotFound`; so does starting one without `generation` after the
queue was recreated elsewhere, until this connection declares it again.

After a full batch the consumer fetches again at once. After an empty claim it
waits until the next row becomes visible (as reported by the database), at
most `check_timeout`; after a partial claim it waits for a notification or
`check_timeout`.

### Stream consumer

`Connection::consume(queue, options)` returns a `Consumer`. Call
`consumer.next().await` (cancel-safe), or use it as a
`futures_core::Stream<Item = Result<Delivery>>` (it is also a `FusedStream`).
The stream ends with `None` when the consumer stops; if it stopped because the
queue is gone, the last item is `Err(Error::QueueGone)`.

`Consumer::stop()` stops fetching, releases buffered never-delivered messages,
cancels the `stopped()` token of each delivery still in flight, and waits until
those settle (or the connection's shutdown deadline abandons them); do not
await it while holding unsettled deliveries in the same task. Dropping a
`Consumer` stops it without waiting.

### Handler consumer

```rust,no_run
# async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
use std::num::NonZeroUsize;
use postgremq::{ConsumeOptions, Delivery};

let consumer = conn
    .consume_with_handler(
        "billing",
        ConsumeOptions::default(),
        NonZeroUsize::new(8), // at most 8 handlers at once
        |delivery: Delivery| async move {
            let order: postgremq::serde_json::Value = delivery.payload_as()?;
            // ... `Err` nacks; `Ok` without settling acks ...
            let _ = order;
            Ok(())
        },
    )
    .await?;
// ... later:
consumer.stop().await;
# Ok(())
# }
```

`consume_with_handler(queue, options, max_in_flight, handler)` runs `handler`
for each delivery, at most `max_in_flight` at once. `None` is unbounded: every
claimed delivery gets a handler task immediately, so prefer `Some(n)` in
production. The handler returns `Result<(), HandlerError>` (a boxed error). It
may settle the delivery itself; if it does not, the delivery is acked when the
handler returned `Ok` and its `stopped()` token is still live, and nacked when
it returned `Err`, panicked (with `panic = "unwind"`), or returned after the
token was cancelled.

`HandlerConsumer::stop()` stops fetching, cancels running handlers' `stopped()`
tokens and waits for every handler to finish and its delivery to settle.
`HandlerConsumer::closed()` waits until the consumer stops and returns
`Some(&Error::QueueGone { .. })` if it stopped because the queue is gone. Dropping a
`HandlerConsumer` stops it. A handler must not await `close` or `stop` (see
[Shutdown](#shutdown)); spawn the call on another task instead.

## Settling deliveries

A `Delivery` exposes `queue()`, `message_id()`, `payload()`,
`payload_as::<T>()`, `group_key()`, `group_seq()`, `delivery_attempts()`
(including this claim), `published_at()`, `vt()` (the lease deadline last
confirmed by the server, on this process's clock), `is_settled()` and `stopped()`.

| Method | Effect |
| --- | --- |
| `ack()` | Marks the message completed |
| `ack_tx(&mut conn)` | Marks it completed inside the caller's transaction |
| `nack(delay)` | Returns it for another attempt, after `delay` if given (measured by the database clock); on the queue's final attempt it moves to the dead letter queue |
| `release()` | Returns it without counting this claim as an attempt (for work that never started) |
| `extend(secs)` | Sets the lease to `secs` from now and returns the new deadline |

A delivery is settled at most once: the first of `ack`, `ack_tx`, `nack` or
`release` owns the outcome, and any later settlement (or `extend`) returns
`Error::LeaseLost` without touching the database. Settlement ends the
delivery's tracking and renewal even if its SQL fails; the lease then expires
and the message is redelivered. A settlement the server rejects because the
delivery is no longer held (its token was superseded or the row is no longer
processing) also returns `Error::LeaseLost`. Settlement is fenced by the token
only: a delivery whose lease expired but was not claimed again can still be
settled. `extend` additionally requires an unexpired lease. Settlements hit the database immediately; they
are never batched.

Dropping an unsettled delivery abandons it: renewal stops and the message is
redelivered after its lease expires. The settlement futures are not
cancel-safe: dropping one mid-flight (for example in a `tokio::select!`) leaves
the outcome unknown and makes later settlements return `LeaseLost`. `extend`
may be dropped freely.

`stopped()` returns a `CancellationToken` that is cancelled when the lease is
lost or the consumer is shutting down. Watch it and stop work whose result
could no longer be acknowledged.

## Lease renewal

With `auto_extend` (the default), every in-flight delivery's lease is renewed
in the background. Renewal is connection-level: one task per `Connection`
renews the due deliveries of all its consumers with a single
`set_vt_batch_multi` call per tick (at most `renewal_batch_size` per call). A
renewal is sent once `extension_threshold` of the remaining lease has passed
and resets the lease to `vt_secs`. Each call is bounded by the earliest
confirmed deadline, but by no less than one second and no more than five, with
the connection's retry policy applied inside that bound. A `busy` result or a
transport error is retried until the last confirmed deadline; a delivery the
server no longer reports, or whose confirmed lease runs out, has lost its lease:
renewal stops and its `stopped()` token is cancelled. A delivery whose settlement has started is not reported lost, since
that settlement removes the row on purpose; it leaves the schedule when the
settlement completes, successfully or not.

Manual `extend` is independent of automatic renewal. Renewal keeps its own
schedule and resets the lease to `vt_secs`, so an extension longer than
`vt_secs` lasts only until the next renewal, and a shorter one is not seen by
renewal, which may then run too late. An application that uses `extend` with
`auto_extend` is responsible for keeping the two consistent; for long work
under a single explicit lease, consume with `auto_extend(false)` and call
`extend` yourself.

## Queue gone

A consumer whose queue is gone is stopped permanently. Loss is tracked per
queue incarnation (name and generation). The queue is gone when a fetch
reports its incarnation missing (SQLSTATE `PMQ02`: it was deleted, or deleted
and recreated), or, for an exclusive queue this connection keeps alive, when
the server no longer reports it or keep-alive errors outlast its last
confirmed deadline. Then:

- every consumer of this connection bound to that incarnation stops:
  deliveries still buffered are discarded (their leases expire), in-flight
  deliveries' `stopped()` tokens are cancelled and the normal drain runs;
- a `Consumer` stream yields `Err(Error::QueueGone { queue, .. })` as its last
  item at once (unless the stream has already ended), and
  `HandlerConsumer::closed()` returns it once the drain has finished;
- this connection stops keeping that incarnation alive;
- `ConnectionOptions::on_queue_fatal` is called with the queue name and an
  `Error::QueueGone`, on Tokio's blocking pool, unordered with the consumers'
  streams ending. It is called at most once per incarnation for the life of
  the connection; `close` does not wait for a hook that is still running.
  Without a hook the event is logged. This hook is the only signal for an
  exclusive queue that has no consumers.

When this connection has declared the queue with `create_queue`, the hook is
not called for a loss of any other incarnation of it (one that was replaced):
consumers bound to that incarnation still stop, and a keep-alive report for it
is ignored.

`delete_queue` itself does not stop consumers. It stops this connection's
keep-alive of the deleted incarnation and forgets the generation this
connection declared for it. Consumers of the queue, on this connection or
another, find it gone at their next fetch (`PMQ02`) and end with `QueueGone`,
which their connection reports to its hook as above. A keep-alive loss of an
exclusive queue that this connection deleted successfully (reported during
the delete or up to 60 seconds after it) is attributed to the deletion and is
not passed to the hook; if the delete fails, a loss held back during it is
reported normally.

Otherwise the generation this connection declared stays cached after a loss,
so a later `consume` of the name without `ConsumeOptions::generation` keeps
binding to it, and fails with `Error::QueueNotFound` if the queue was
recreated, until the queue is declared again. Consuming a queue that does not
exist fails with `Error::QueueNotFound` and is not a loss: the hook is not
called.

```rust,no_run
# async fn example() -> postgremq::Result<()> {
use postgremq::{Connection, ConnectionOptions};

let options = ConnectionOptions::default().on_queue_fatal(|queue, err| {
    tracing::error!(%queue, error = %err, "queue is gone");
});
let conn = Connection::connect("postgres://localhost/app", options).await?;
# let _ = conn;
# Ok(())
# }
```

## Shutdown

`Connection::close()` shuts down gracefully; concurrent and repeated calls
await the same shutdown, and cancelling a `close` call does not cancel it.
`close` waits for in-flight deliveries to settle and handlers to return, so
awaiting it from inside a handler, or from a consume loop while holding an
unsettled delivery, waits on itself until `shutdown_timeout` passes (forever
without one). `Consumer::stop` awaited while holding one of its unsettled
deliveries waits until a running `close` reaches its deadline (forever
otherwise), and `HandlerConsumer::stop` awaited inside its own handler waits
forever, even with a timeout and while `close` runs. From such places, start
the call on another task: `tokio::spawn(async move { conn.close().await })`.

1. New publishes, consumers and declarations (`create_topic`,
   `create_queue`) are rejected with `Error::Closed`. Settlement, `extend`
   and the maintenance, inspection, DLQ and delete calls keep working while
   consumers drain; once `shutdown_timeout` has passed or `close` has
   finished, they return `Error::Closed`.
2. The `LISTEN` session ends; consumers stop fetching, release buffered
   never-delivered messages and cancel in-flight deliveries' `stopped()`
   tokens.
3. Settlement, lease renewal and queue keep-alive continue until every
   in-flight delivery settles or `shutdown_timeout` passes; abandoned leases
   then expire normally, and an operation cut off at the deadline returns
   `Error::Closed` with an unknown outcome.
4. The renewal and keep-alive tasks stop last, and an owned pool is closed.

```rust,no_run
# async fn example() -> postgremq::Result<()> {
use std::time::Duration;
use postgremq::{Connection, ConnectionOptions};

let conn = Connection::connect(
    "postgres://localhost/app",
    ConnectionOptions::default().shutdown_timeout(Duration::from_secs(30)),
)
.await?;
// ... on SIGTERM: in-flight work gets up to 30 s to settle.
conn.close().await;
# Ok(())
# }
```

Without `close`, the background tasks stop (without draining) once the last
`Connection`, consumer and delivery are dropped.

## Retries

`RetryConfig` (default: 3 attempts, 100 ms initial backoff, 2 s maximum,
multiplier 2; `RetryConfig::disabled()` makes a single attempt) applies to
transient database errors:

- Operations that are safe to repeat (settlement, extension, renewal,
  keep-alive, declarations, reads) retry SQLSTATEs `40001`, `40P01`, `55P03`,
  class `08` and `57P01`/`57P02`/`57P03`, and a dropped connection. Renewal
  and keep-alive retry inside their per-call bound.
- Destructive or counting admin calls (`delete_queue`, `delete_topic`,
  `purge_dlq`, `requeue_dlq`, both cleanups and `maintenance_fast`) retry only
  `40001`, `40P01` and `55P03`, which prove a rollback.
- `publish` retries only `40001`/`40P01`; a claim is never retried.
- `publish_tx` and `ack_tx` are never retried.

## Errors

All fallible calls return `postgremq::Result<T>` with `postgremq::Error`
(`#[non_exhaustive]`; match struct variants with `..`). `Error::kind()` returns
an `ErrorKind` for matching without destructuring, and `Error::sqlstate()` the
SQLSTATE when the error came from the database.

| Variant | Cause |
| --- | --- |
| `LeaseLost` | `PMQ01`: the lease is no longer held, or the delivery was already settled (no source) |
| `QueueNotFound` | `PMQ02`: a queue or topic does not exist, or an exclusive queue expired |
| `Validation` | `PMQ03`, or options the client rejects; nothing changed |
| `Busy` | `55P03`: a row lock was contended after retries; lease ownership is unaffected, retry within the lease |
| `QueueGone` | The consumer's queue is gone; ends the consumer |
| `Payload` | A payload could not be serialized or deserialized as JSON |
| `Incompatible` | The database's PostgreMQ schema is on an mq line this client does not support, or is not installed |
| `Migration` | A migration failed in `migrate`; its version is left dirty. `position` (a `SqlPosition`) locates the error in the migration's file when PostgreSQL reports a position it can map (not in an EUC_JIS_2004 database); the database error is the source |
| `DirtySchema` | `migrate` found a migration that failed partway; the schema needs manual repair |
| `UnknownSchemaVersion` | `migrate` found a recorded version below its latest migration that is none of its migrations; nothing is applied |
| `Closed` | The connection is closing or closed, or the shutdown deadline abandoned the call |
| `Sqlx` | Any other database or driver error |

## Maintenance and inspection

The schema does not schedule its own maintenance. Run these periodically from
one place, as described in the
[SQL README](https://github.com/postgremq/postgremq/blob/main/mq/README.md#maintenance-and-retention):

```rust,no_run
# async fn example(conn: postgremq::Connection) -> postgremq::Result<()> {
use std::num::NonZeroU32;

const BATCH: NonZeroU32 = NonZeroU32::new(1000).unwrap();

// Every 1–10 seconds (at least once a minute): retire abandoned final
// attempts, drop expired exclusive queues.
let counters = conn.maintenance_fast().await?;
// For example every 10 seconds: delete completed deliveries older than 24 h.
let deleted = conn.cleanup_completed_messages(24, BATCH).await?;
# let _ = (counters, deleted);
# Ok(())
# }
```

| Method | SQL function |
| --- | --- |
| `maintenance_fast()` → `MaintenanceCounters` | `pmq_maintenance_fast` |
| `cleanup_completed_messages(hours, batch)` | `cleanup_completed_messages` |
| `cleanup_unreferenced_messages(hours, batch)` | `cleanup_unreferenced_messages` |
| `list_queues()` → `Vec<QueueInfo>` | `list_queues` |
| `queue_statistics(queue: Option<&str>)` → `QueueStatistics` | `get_queue_statistics` |
| `list_dlq()` → `Vec<DlqMessage>` | `list_dlq_messages` |
| `requeue_dlq(queue)` | `requeue_dlq_messages` |
| `purge_dlq()` | `purge_dlq` |
| `delete_queue(queue)`, `delete_topic(topic)` | `delete_queue`, `delete_topic` |

## Metrics

With the `otel` feature, `ConnectionOptions::meter_provider(&provider)` records
the PostgreMQ client metrics (operation and handler durations, sent and
consumed messages, active handlers, lost renewals) under the instrumentation
scope `postgremq`, contract version 1, defined in
[`docs/observability.md`](https://github.com/postgremq/postgremq/blob/main/docs/observability.md).
Nothing is recorded without `meter_provider`, even when a global provider is
set; pass `opentelemetry::global::meter_provider()` to use that one.

The crate depends only on the `opentelemetry` API; the SDK, readers, exporters
and their flush and shutdown belong to the application. Close the connection
first, then flush. `postgremq::opentelemetry` re-exports the API version the
option takes; the `otel` feature follows `opentelemetry`'s minor releases.

```rust,no_run
# #[cfg(feature = "otel")]
# async fn example(provider: opentelemetry_sdk::metrics::SdkMeterProvider)
#     -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use postgremq::{Connection, ConnectionOptions};

let conn = Connection::connect(
    "postgres://localhost/app",
    ConnectionOptions::default().meter_provider(&provider),
)
.await?;
// ... publish and consume ...
conn.close().await;
// The SDK's flush blocks: run it off the async workers.
tokio::task::spawn_blocking(move || provider.force_flush()).await??;
# Ok(())
# }
```

[`examples/metrics.rs`](https://github.com/postgremq/postgremq/blob/main/postgremq-rs/examples/metrics.rs)
exports over OTLP/HTTP to the Collector of
[`observability/compose.yaml`](https://github.com/postgremq/postgremq/blob/main/observability/compose.yaml):

```sh
cargo run --example metrics --features otel
```

## Logging

The crate logs through `tracing` (target `postgremq`) and never installs a
subscriber. Timestamps are `std::time::SystemTime`. Lease and keep-alive
scheduling, and `Delivery::vt()`, use the remaining time reported by the
database server, so client clock skew does not shorten leases.

## Running the tests

The integration tests live in the
[repository](https://github.com/postgremq/postgremq/tree/main/postgremq-rs/tests)
and are not part of the published crate; they load `mq/sql/latest.sql` from
the checkout. From `postgremq-rs/`:

```sh
cargo test                  # starts or reuses a postgres:15 container (Docker required)
cargo test --all-features   # also runs the metrics contract tests
```

Without further setup, the tests start (or reuse) a `postgres:15` container
named `postgremq-rs-test` through testcontainers and keep it for later runs
(remove it with `docker rm -f postgremq-rs-test`). To use an existing
PostgreSQL 15+ server instead, whose user can create databases and roles:

```sh
export POSTGREMQ_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test
```

Each test runs in a fresh database cloned from a template with the schema
installed.

## License

MIT
