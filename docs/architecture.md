# Architecture

PostgreMQ is a message queue implemented as SQL tables and PL/pgSQL functions
in a `postgremq` schema, plus three client libraries (Go, TypeScript, Rust) and
a CLI. The database holds all state and enforces every delivery guarantee; the
clients add consumption loops, automatic lease renewal, keep-alive for
temporary queues, wake-ups through LISTEN/NOTIFY and graceful shutdown.

This document describes how the pieces fit. The details are in:

- [`mq/README.md`](../mq/README.md): the SQL API and the delivery contract;
- [`delivery-lifecycle.md`](delivery-lifecycle.md): how the clients handle
  settlement, cancellation, renewal and shutdown, and where they differ;
- [`observability.md`](observability.md): queue metrics and the client metrics
  contract.

## Repository layout

| Path | Contents |
|------|----------|
| `mq/sql/latest.sql` | The complete schema and functions (install script) |
| `mq/migrations/` | The same schema as golang-migrate migrations |
| `mq/` (Go module) | Embeds the SQL and migrations for Go programs |
| `postgremq-go/` | Go client (pgx v5) |
| `postgremq-ts/` | TypeScript client for Node.js (node-postgres) |
| `postgremq-rs/` | Rust client, crate `postgremq` (sqlx + Tokio) |
| `postgremq-go/cmd/postgremq/` | CLI that applies and reports schema migrations, part of the Go client module |
| `observability/` | Client metrics contract, Collector configuration and its end-to-end test |

## The SQL layer

### Data model

```
topics ──< queues ──< queue_messages >── messages >── topics
                │                            │
                └──< dead_letter_queue >─────┘
topics ──< message_groups
```

- **topics**: named publication targets.
- **queues**: each subscribes to one topic. A queue has a `generation` UUID,
  a `max_delivery_attempts` limit (0 means unlimited) and, for exclusive
  queues, a `keep_alive_until` deadline.
- **messages**: one row per publication: a JSONB payload, `deliver_after`
  and an optional group key and sequence.
- **queue_messages**: one row per (queue, message), the per-queue delivery
  state: `status` (`pending`, `processing`, `completed`), `vt`,
  `delivery_attempts` and the current `consumer_token`.
- **dead_letter_queue**: (queue, message) pairs retired after their final
  attempt. Its foreign keys use `ON DELETE RESTRICT`, so a queue or message
  with DLQ entries is not removed until the entries are purged or requeued.
- **message_groups**: the per-(topic, group key) sequence allocator.

`delete_topic` refuses while the topic still has messages; deleting a queue
cascades to its delivery rows.

### Fan-out

`publish_message` inserts into `messages`. An `AFTER INSERT` trigger,
`distribute_message`, copies the message into `queue_messages` for every live
queue on the topic, then sends `NOTIFY` on `pmq:t:<topic>`. Publishing is one
statement in the caller's transaction, so a publish commits or rolls back
with the application's own writes (transactional outbox).

### Leases and delivery identity

`consume_message(queue, vt, limit, generation)` claims up to `limit` visible
rows with `FOR UPDATE SKIP LOCKED`. Each claimed row moves to `processing`,
its `vt` is set to now + `vt` seconds, `delivery_attempts` is incremented and
it gets a fresh `consumer_token` (`gen_random_uuid()`).

A delivery is identified by `(queue, message_id, consumer_token)`. Every
settlement and renewal names the token, so a consumer whose lease expired and
whose message was claimed again cannot ack, nack, release or extend the new
delivery. Settlement is fenced by the token alone: a delivery whose lease
expired but was not claimed again can still be settled. Extending requires an
unexpired lease. Settlement outcomes:

- `ack_message` marks the row `completed`.
- `nack_message` returns it to `pending` (optionally delayed). If it was the
  final allowed attempt, it is retired to the dead letter queue.
- `release_message` returns it to `pending` and refunds the attempt.
- The lease expires: the row becomes visible again. `pmq_maintenance_fast`
  retires expired rows that used their final attempt.

Message IDs are shared by all queues on a topic, so clients always key
in-flight state by `(queue, message_id)`, never by message ID alone.

### Queue generations and exclusive queues

`create_queue` returns the queue's generation. Consumers and keep-alive pass
it back, so a queue that is deleted and re-created under the same name is a
different resource: consumers and keep-alives bound to the old generation
cannot touch it.

An **exclusive** queue is temporary. It lives while its owner keeps calling
`extend_queue_keep_alive_multi`. At `keep_alive_until` it is dead at once:
publications stop being distributed to it and it cannot be consumed. It is
then deleted by `pmq_maintenance_fast`. A **non-exclusive** queue has no
deadline.

`consume_message` raises `PMQ02` when the queue does not exist, so a consumer
of a deleted queue fails instead of polling an empty result forever.

### Message groups

A message published with a group key gets the next dense `group_seq` for its
(topic, group). The sequence is allocated under the `message_groups` row
lock, which is held until commit, so group order is commit order.
`consume_message` claims a grouped row only when no lower-sequence row of the
same (queue, group) is pending or processing. Each queue therefore delivers a
group strictly in order, one delivery at a time, with head-of-line blocking.
Settling or removing a grouped row notifies `pmq:q:<queue>` to wake consumers
waiting on the successor. See the [groups contract](../mq/README.md) for what
is and is not promised.

### Notifications

| Channel | Sent when |
|---------|-----------|
| `pmq:t:<topic>` | A message is distributed to the topic's queues |
| `pmq:q:<queue>` | A row becomes claimable again on that queue: nack, release, DLQ requeue, or a grouped head settled or removed |

Notifications carry no payload and are only hints. Clients also poll, so a
missed notification delays delivery but never loses a message.
`get_next_visible_time(queue)` tells a client when the next row becomes
visible (a delayed message or an expiring lease), so it can sleep until
then instead of polling at a fixed rate.

### Batched background calls

Two functions exist so that clients renew in bulk:

- `set_vt_batch_multi(queues, ids, tokens, vts)` renews many leases across
  many queues in one call. Each row comes back `extended` (with the confirmed
  `vt`) or `busy` (the row was locked; `NOWAIT` keeps one contended row from
  delaying the rest). A row that is omitted has lost its lease.
- `extend_queue_keep_alive_multi(queues, intervals, generations)` does the
  same for exclusive queues: `extended`, `busy`, or omitted when the queue is
  gone, expired or of a different generation.

### Maintenance

The database does no background work of its own. An installation schedules:

- `pmq_maintenance_fast()`, frequently: retires expired final-attempt rows
  to the DLQ and drops expired exclusive queues;
- `cleanup_completed_messages(hours, batch)`: deletes completed delivery rows
  older than the retention period;
- `cleanup_unreferenced_messages(hours, batch)`: deletes payloads that no
  queue or DLQ row references, and prunes empty group allocators.

`queue_metrics()` reports per-queue depth and age for monitoring.

## Client architecture

The three clients share one design. Names differ by language; the main
behavioural differences are noted below and under
[Client differences](#client-differences).

### Connection

A `Connection` wraps a connection pool (pgx `pgxpool`, node-postgres `Pool`,
sqlx `PgPool`) and owns:

- the consumers created through it;
- one **notification listener**: a single LISTEN session on the `pmq:t:` and
  `pmq:q:` channels its consumers need. Go acquires its connection from the
  pool on the first consume and holds it until close; TypeScript holds a pooled
  client only while subscriptions exist; Rust uses a separate single-connection
  pool, built from the main pool's connect options, outside the main pool.
  Subscriptions are reference-counted per channel. After a disconnect the
  listener reconnects with exponential backoff capped at 30 s and restores
  its subscriptions; the Rust listener also wakes every subscriber once a new
  session's LISTENs are in place. Polling remains the fallback;
- one **lease-renewal scheduler** for the in-flight deliveries of all its
  consumers;
- one **keep-alive scheduler** for all exclusive queues it created;
- the retry policy for transient database errors.

Publishing, settlement (ack, nack, release) and manual extension go straight
to the database. They are never batched, so each call's result is that
operation's own outcome. Publish and ack also have variants that run on a
caller-owned transaction (Go `PublishWithTx` / `AckWithTx`, TypeScript
`publishWithTransaction` / `ackWithTransaction`, Rust `publish_tx` / `ack_tx`).
The caller owns `BEGIN`/`COMMIT`; the client never opens, commits or rolls back
a caller's transaction.

### Consumer

A consumer runs one loop per queue:

1. Claim up to `batchSize` rows with `consume_message`, passing the queue's
   generation: the one cached when this connection declared the queue, or
   else the one a database lookup finds. A queue that does not exist fails
   `Consume` with a queue-not-found error in Go and Rust; in TypeScript the
   first fetch reports it as a queue loss.
2. Register every claimed delivery with the connection's renewal scheduler,
   then buffer the deliveries for the application.
3. After a claim that returned fewer rows than requested, wait for the first
   of: a notification on the topic or queue channel, the poll interval, or
   the time `get_next_visible_time` reports (looked up after an empty claim,
   and in TypeScript after a partial one too).

Applications take deliveries from an iterator or stream (Go channel,
TypeScript async iterator, Rust `Stream`), or use a handler consumer that
runs a callback per delivery with bounded concurrency. A handler that
returns without settling auto-acks. A handler that fails, panics or returns
after its cancellation signal auto-nacks.

Each delivery carries a cancellation signal: Go `context.Context`, TypeScript
`AbortSignal`, Rust `CancellationToken`. It fires when the lease is lost, the
consumer stops or the queue is gone. Cancellation is advisory: it cannot undo
side effects the handler already caused.

### Lease renewal

Each connection has one renewal scheduler for every consumer's in-flight
deliveries. A delivery is due when a configured fraction of its lease has
elapsed. On each tick the scheduler sends all due deliveries, across all
queues, in one `set_vt_batch_multi` call:

- `extended`: the new confirmed deadline is recorded and the next renewal is
  scheduled;
- `busy` or a transport error: retried, but only until the last confirmed
  deadline;
- omitted, or the confirmed deadline passed without a successful renewal:
  the lease is lost. The scheduler cancels the delivery's signal and stops
  tracking it.

Go and TypeScript compare the `vt` the server returns with the local
clock, so their scheduling assumes the client and database clocks agree.
Rust records the local send instant plus the remaining time the server
reports, which does not depend on the clocks agreeing. A delivery leaves the
renewal schedule when its settlement call completes, successfully or not; in
Rust a loss seen while its settlement is in progress is not reported. Manual
extension of a delivery is independent of this schedule: automatic renewal
continues from its own last confirmation, so an application that uses both
is responsible for keeping them consistent.

### Keep-alive

The keep-alive scheduler renews every exclusive queue the connection created
with one `extend_queue_keep_alive_multi` call per tick:

- `extended`: the new deadline is recorded and the next keep-alive is
  scheduled at half the remaining time;
- `busy` or a transport error: retried, but only until the last known
  deadline;
- omitted from a successful result (the queue is gone, expired, no longer
  exclusive or of another generation), or the deadline passed without a
  successful renewal (the queue has expired on the server): the queue is
  dropped from the schedule and treated as lost (below).

### Queue loss

When a queue a consumer depends on is gone, the consumer is finished, as with
a consumer cancellation in RabbitMQ. Two triggers feed one connection
routine:

- `consume_message` raises `PMQ02` (the queue is missing, expired or of
  another generation);
- the keep-alive scheduler gives up on an exclusive queue (above).

The routine deregisters the queue's keep-alive and tears down the
connection's consumers bound to the lost generation through the normal
drain: handlers are cancelled and in-flight deliveries leave the renewal
schedule. Each consumer's stream then ends with a queue-gone error. The
connection-level queue-fatal callback is called when the teardown starts,
without waiting for the drain. That callback is the only signal for an
exclusive queue that has a publisher but no consumer. Go and TypeScript
signal a queue name once until it is declared again on the connection; Rust
signals each incarnation (name and generation) once.

Deleting a queue through a connection drops that connection's keep-alive
registration and cached generation for it, but does not close consumers or
call the callback. Consumers of the deleted queue find out at their next
claim through `PMQ02`, which runs the routine above.

### Shutdown

`Connection.Close` / `close()`:

1. Rejects new publishes, declarations and consumers. Settlement, manual
   extension and admin calls keep working until close finishes.
2. Ends the notification listener's session, stops fetching, releases
   buffered deliveries that were never handed to the application, and
   cancels running handlers.
3. Keeps settlement, lease renewal and keep-alive running while in-flight
   work drains. The two schedulers outlive the consumers for this reason.
4. When drained or at the shutdown deadline, stops the schedulers, then
   closes the pool if the connection owns it.

Work abandoned at the deadline is not settled. Its lease expires and the
message is redelivered.

Concurrent close calls wait for the same shutdown. Because close waits for
in-flight deliveries, calling it (or a consumer's stop) from inside a
handler, or while holding an unsettled delivery, waits on itself, forever in
Go and Rust when no shutdown timeout is set. Call it from another task or set
a timeout.

## Client differences

| | Go | TypeScript | Rust |
|---|---|---|---|
| Driver | pgx v5 | node-postgres | sqlx 0.9 |
| Background schedules | One goroutine per scheduler owns its schedule; register and deregister are commands on a channel; the SQL call runs on a separate goroutine | Timers on the event loop over a `Map` schedule | `std::sync::Mutex`-guarded schedule driven by one Tokio task per scheduler; the SQL call runs on a separate task |
| Renewal order | Min-heap keyed by (queue, message ID, token) | `Map` keyed by (queue, message ID, token) | Map of live deliveries plus a min-heap of pending renewals |
| Lease and keep-alive deadlines | Server timestamps compared with the local clock | Server timestamps compared with the local clock | Local send instant plus the remaining time the server reports |
| Same-name declare and delete | Not serialized | Not serialized | Serialized by a per-name lock on the connection |
| Delivery stream | `Consumer.Messages()` channel | `for await` over `consumer.messages()` | `Stream` / `Consumer::next()` |
| Cancellation | `context.Context` | `AbortSignal` | `CancellationToken` |
| Queue-loss signal | `Consumer.NotifyClose`, `WithQueueFatalHandler`, `ErrQueueGone` | `onClose`, `onQueueFatal` / `'queueFatal'` event, `QueueFatalError` | stream ends with `Error::QueueGone`, `ConnectionOptions::on_queue_fatal` |
| Metrics | OpenTelemetry API, opt-in | OpenTelemetry API, opt-in | OpenTelemetry API behind the `otel` cargo feature |

[Delivery lifecycle](delivery-lifecycle.md) lists the remaining behavioural
differences. Each client's README documents its options, defaults and errors.
