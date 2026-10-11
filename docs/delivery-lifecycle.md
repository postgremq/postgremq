# Delivery lifecycle

The Go (`postgremq-go`), TypeScript (`postgremq-ts`) and Rust (`postgremq`)
clients share one model of delivery ownership, settlement, cancellation,
shutdown and background renewal. The SQL ownership token, the queue generation
and the client resource lifetime define its boundaries. Where the clients
differ, this document says so. The SQL side of the contract is in
[PostgreMQ SQL](../mq/README.md#delivery-contract).

## Ownership

A delivery is `(queue name, message ID, consumer token)`. The same message ID is
distributed to every queue on a topic, and every claim mints a new token, so the
queue and token are part of the identity everywhere: in settlement SQL, in renewal
results and in each client's tracking. A superseded handler may still be running,
but its settlement cannot affect the new delivery's tracking or renewal. A renewal
result applies only to the registration that sent it.

Settlement SQL (`ack_message`, `nack_message`, `release_message`) is fenced by the
token: it succeeds while the row is still `processing` under that token, and raises
`PMQ01` (lease lost) once the row was settled, removed or claimed again. Renewal
additionally requires the lease (`vt`) not to have expired.

The first ack, nack, release or transactional ack on a delivery owns settlement and
completion. Later or concurrent terminal calls issue no SQL:

| Client | Terminal calls | Later terminal call | Manual extension after settlement |
|---|---|---|---|
| Go | `Ack`, `AckWithTx`, `Nack`, `Release` | returns `ErrLeaseLost` | `SetVT` still sends `set_vt`; after a committed settlement it fails with `ErrLeaseLost` |
| TypeScript | `ack`, `ackWithTransaction`, `nack`, `release` | rejects with a plain `Error` ("Message *id* has already been processed") | `setVt` rejects the same way, without SQL |
| Rust | `ack`, `ack_tx`, `nack`, `release` | returns `Error::LeaseLost` without a source | `extend` returns the same, without SQL |

Completion removes runtime tracking and renewal even when the SQL fails: the
operation has ended and its lease is left to expire. A retried settlement whose
first attempt did commit fails with lease lost. Handle a failed settlement
explicitly instead of assuming a successful business outcome.

In Rust, dropping every handle of an unsettled `Delivery` abandons it: renewal ends
and the lease expires. The settlement futures are not cancel-safe: the first one
claims the settlement before its SQL runs, so dropping it leaves the outcome unknown.

A queue has a UUID generation, and every claim passes the generation the consumer
is bound to. A replacement queue with the same name is a new resource: old
consumers and keep-alive registrations cannot operate on it. An expired exclusive
queue is dead from its expiry instant, before maintenance reaps it, and cannot be
revived. Delete and recreate it deliberately; publications made while it was
expired are not caught up.

| | Go | TypeScript | Rust |
|---|---|---|---|
| Generation source | cached from this connection's `CreateQueue`, else a database lookup (1 s, not retried) when `Consume` is called | cached from this connection's `createQueue`, else a database lookup at the consumer's first fetch | `ConsumeOptions::generation`, else cached from this connection's `create_queue`, else a lookup (retried, 1 s overall) in `consume` |
| Topic source | `WithTopic` or a declaration on this connection; otherwise `Consume` returns `ErrQueueNotFound` | `topic` option or a declaration on this connection; otherwise a plain `Error` (from `consumeHandler()`, or from `consume()`'s `messages()`) | the queue row |
| Queue does not exist | `Consume` returns `ErrQueueNotFound` | the first fetch reports the loss through [queue-fatal teardown](#queue-fatal-teardown) | `consume` returns `Error::QueueNotFound` |

## Handlers and cancellation

Each delivery carries a stop signal, cancelled when the consumer stops, the
connection closes, the queue becomes fatal, or renewal finds the lease lost.

| Client | Handler API | Stop signal |
|---|---|---|
| Go | `ConsumeHandler(queue, func(ctx, msg))` | `msg.StoppedCtx` (also passed as `ctx`) |
| TypeScript | `consumeHandler(queue, async (msg) => …)` | `msg.signal` (`AbortSignal`) |
| Rust | `consume_with_handler(queue, options, max_in_flight, \|delivery\| async { … })` | `delivery.stopped()` (`CancellationToken`) |

A handler that settles the delivery itself keeps that outcome. Otherwise, when the
handler returns:

- it is acked only if it completed normally and its stop signal is still live;
- it is nacked if it returned after the stop signal was cancelled, or failed: a
  panic (Go), a thrown exception or rejected promise (TypeScript), or an `Err`
  result or panic (Rust; panics are caught only with `panic = "unwind"`).

Close and stop cancel the stop signal of every in-flight delivery, so a handler
that returns during shutdown without settling is nacked and the attempt counts.
An explicit ack still reports completed work during cancellation. Only buffered,
never-delivered messages are released automatically as unattempted work.

Cancellation is advisory. It cannot undo external side effects or stop a handler
that ignores it. Use idempotency keys or application-level fencing for such
effects. With caller-owned SQL transactions, commit and rollback remain the
caller's responsibility: a transactional ack ends automatic renewal once its
statement completes, and a rollback leaves the original delivery to be redelivered
after its lease expires.

## Connection shutdown

1. Enter draining and reject new publishes (including transactional ones), topic
   and queue declarations, and new consumers with the client's closed error.
   Settlement, manual extension and admin calls keep working during the drain
   and fail with the closed error once close has finished.
2. End the LISTEN session, stop fetching, release buffered never-delivered
   deliveries, and cancel the stop signal of every in-flight delivery.
3. Continue settlement, queue keep-alive and lease renewal while in-flight
   deliveries drain.
4. When every delivery has settled, or the shutdown deadline passes, stop
   keep-alive and renewal, and close the pool if the connection created it.

Concurrent and repeated close calls await the same shutdown.

Close waits for in-flight deliveries, so calling it from inside a message
handler, or from a consume loop while holding an unsettled delivery, waits on
itself: forever in Go and Rust without a shutdown timeout, and until the
deadline otherwise (TypeScript always has one). Call it from another task
instead (Go `go conn.Close()`, Rust `tokio::spawn(async move { conn.close().await })`,
TypeScript `void client.close()` rather than awaiting it in the handler), or set
a shutdown timeout. A consumer's own stop waits on itself the same way; in Go and
Rust it has no timeout of its own (only a running close's deadline ends it), so
call it from another task as well.

| | Go | TypeScript | Rust |
|---|---|---|---|
| Shutdown option | `WithShutdownTimeout` | `shutdownTimeoutMs` | `ConnectionOptions::shutdown_timeout` |
| Default | `0`: wait without limit | 30 000 ms (must be positive) | `None`: wait without limit |
| Standalone consumer stop | `Consumer.Stop` / `HandlerConsumer.Stop` wait until every in-flight delivery settles; only `Connection.Close`'s deadline ends that wait | `stop()` is bounded by `shutdownTimeoutMs` (or the remaining close budget) | `stop().await` waits until in-flight deliveries settle or the connection's shutdown deadline abandons them; dropping a `Consumer` or `HandlerConsumer` stops it without waiting |

Set a timeout for a bounded deployment shutdown. In Rust, `close` runs on its own
task, so cancelling the `close` future does not cancel the shutdown; past the
deadline each remaining cleanup step gets about 50 ms.

At a forced deadline, active work is abandoned without reducing delivery attempts;
its database lease expires normally. Late fetch responses are only released or
left to expire: they cannot register for renewal or enter a stopped consumer's
buffer. A timed-out or cancelled fetch can have committed, so not every claimed row
can be released immediately; those rows return when their lease expires.

Clients bound their internal queries. TypeScript destroys a timed-out pooled socket
instead of only rejecting a promise. Go passes cancellation and deadlines through
pgx, bounds each claim and each release of a buffered delivery to one second, and
joins in-flight background flushes before `Close` returns. Rust abandons in-flight
pool operations at the deadline with `Error::Closed` (outcome unknown; `ack_tx` and
`publish_tx` run on the caller's transaction and are not abandoned), closes
abandoned connections instead of returning them to the pool, and bounds each claim
with a server-side `statement_timeout`. An application-supplied pool must honor
cancellation, and handlers that ignore cancellation can outlive a forced close. In
Go, close the `Connection` before an application-supplied pool: its LISTEN session
holds one of the pool's connections until `Close`, so a pool with `MaxConns` 1 is
starved by it.

TypeScript aborts database calls still in flight when `shutdownTimeoutMs` passes.
It ends the pool only if it created it.

## Lease renewal

With automatic renewal enabled (the default: Go without `WithNoAutoExtension`,
TypeScript `autoExtension.enabled`, Rust `ConsumeOptions::auto_extend`), each
consumer registers its claimed deliveries with one connection-level renewal
schedule when it fetches them and deregisters them when they settle.

- A delivery is renewed once `extensionThreshold` / `extension_threshold` (default
  0.5) of its remaining lease has elapsed. Renewal resets the lease to the
  consumer's visibility timeout.
- Each tick sends every due delivery, across all queues and consumers of the
  connection, in one `set_vt_batch_multi` call of at most 100 rows (Go
  `WithExtenderBatchSize`, TypeScript `extenderBatchSize`, Rust
  `renewal_batch_size`). At most one call is in flight.
- Each call is bounded by one second or the earliest lease deadline in the batch,
  whichever is sooner. Go and Rust apply the connection's retry policy inside that
  bound; TypeScript sends the call once.
- The SQL locks rows with `NOWAIT` and re-reads the wall clock after locking, so a
  contended row cannot delay unrelated rows and an old transaction timestamp cannot
  revive an expired lease.
- Due entries are not re-checked against their deadline before sending. A batch
  that contains an already expired entry has a time bound in the past and fails at
  once; its expired entries are lost and the rest are retried after about one
  second.

How the client knows a lease's deadline:

| Client | Lease deadline |
|---|---|
| Go | the `vt` returned by the claim or renewal, compared with the local clock |
| TypeScript | the `vt` returned by the claim or renewal, compared with the local clock |
| Rust | the local instant the request was sent (after acquiring a pool connection) plus the remaining time the server reports (`vt - clock_timestamp()`), so it does not depend on the two clocks agreeing |

In Go and TypeScript, keep the client and database clocks synchronized: a client
clock running behind the database's lets renewal start too late.

| Result for a delivery | Action |
|---|---|
| `extended` | Record the new deadline and schedule the next renewal |
| `busy` (row lock contended) | Retry after 100 ms, until the deadline |
| Omitted (wrong token, no longer processing, or expired) | Lease lost |
| Transport or database error | Retry after about one second, until the deadline |
| Deadline passed without a renewal | Lease lost |

A lost lease is removed from the schedule, cancels the delivery's stop signal
(`StoppedCtx`, `signal`, `stopped()`) and increments
`postgremq.client.renewal.lost`. A delivery leaves the schedule when its settlement
call completes, successfully or not. In Rust a loss found while settlement is in
progress is dropped silently, since the settlement removes the row on purpose. Go
and TypeScript report such a loss like any other until the settlement call has
completed.

Manual extension (Go `Message.SetVT`, TypeScript `message.setVt`, Rust
`Delivery::extend`) is independent of automatic renewal in all clients. Renewal
keeps its own schedule and resets the lease to the consumer's visibility timeout at
its next run, so a longer manual extension lasts only until then and a shorter one
is not seen by it. If you use both, keeping them aligned is the application's
responsibility. In TypeScript, a `setVt` that the database rejects with lease
lost also aborts `message.signal`.

### Background work per client

- **Go:** one generic actor (`actor.go`) per schedule: a goroutine owns the
  schedule without a mutex and applies registrations and deregistrations from a
  single ordered command channel. The batched SQL call runs on a separate
  goroutine so registration stays responsive during I/O. The renewal schedule is a
  min-heap indexed by `(queue, message ID, token)` (`extender.go`); keep-alive is a
  map of exclusive queues (`keepalive.go`).
- **TypeScript:** timers on the event loop over a `Map` per schedule, with one
  flush in flight at a time (`connection.ts`).
- **Rust:** each schedule sits behind a `std::sync::Mutex`, so registration and
  deregistration are synchronous (callable from `Drop`) and never wait on I/O. One
  Tokio task per schedule drives it, runs the batched call on a separate task and
  folds the outcome back in; the lock is never held across an `.await`
  (`scheduler.rs`, with `renewal.rs` and `keepalive.rs`). Renewal uses a live map
  plus a heap whose stale entries are skipped by epoch.

## Exclusive-queue keep-alive

An exclusive queue declared through a connection is registered with that
connection's keep-alive schedule, bound to the queue's generation. The default
interval is 300 seconds (Go `WithKeepAliveInterval`, TypeScript
`keepAliveInterval` in seconds, Rust `QueueOptions::keep_alive_interval`, between
one second and 30 days in Rust).

- The queue is renewed when half of its remaining keep-alive has elapsed. Each
  tick renews every due queue of the connection in one
  `extend_queue_keep_alive_multi` call, bounded by one second or the earliest
  deadline. Go and Rust apply the connection's retry policy inside that bound;
  TypeScript sends the call once.
- `busy` and transport errors are retried after 100 ms until the last known
  keep-alive deadline.
- The first deadline after a declaration is the requested interval, counted in Go
  and TypeScript from when the declaration returned and in Rust from when it was
  sent. Later deadlines come from the server's `keep_alive_until`: compared with
  the local clock in Go and TypeScript, converted to remaining time as for leases
  in Rust.
- A queue omitted from a successful result (deleted, expired, non-exclusive or
  replaced by a new generation), or one whose deadline passes without a renewal,
  has failed permanently and becomes fatal.
- A successful delete of the queue through the same connection deregisters it
  (Go and TypeScript by name, Rust that generation only); see
  [deletion](#deletion).

Keep-alive continues while consumers drain and stops last, so an exclusive queue
stays alive until its consumers finish.

## Queue-fatal teardown

A consumer whose queue is gone is dead, as with a RabbitMQ consumer cancel. Two
triggers converge on one connection routine:

- `consume_message` raises `PMQ02` because the queue row is absent, expired, or
  has a different generation (an existing empty queue returns zero rows). In
  TypeScript the generation lookup at a consumer's first fetch reports a missing
  queue the same way.
- The keep-alive schedule reports a permanent failure (above).

The routine deregisters the queue's keep-alive (Rust: that generation's only),
stops the connection's consumers bound to the lost generation through the normal
drain (handlers are cancelled, in-flight deliveries leave the renewal schedule),
and calls the connection-wide handler. The
handler is called when the teardown starts, without waiting for the drain, and
close does not wait for it. Without a handler the event is logged. That handler is
the only signal for an exclusive queue that has a producer but no consumer. The
loss does not clear the connection's cached generation or topic for the queue.

| Client | Per consumer | Connection-wide | Error |
|---|---|---|---|
| Go | `Consumer.NotifyClose(chan error)` / `HandlerConsumer.NotifyClose`, after the drain; `Messages()` closes | `WithQueueFatalHandler`, called on its own goroutine | `*QueueFatalError`, matches `errors.Is(err, ErrQueueGone)` |
| TypeScript | `Consumer.onClose` / `HandlerConsumer.onClose`, after the drain; the iterator ends | `onQueueFatal` option and the `'queueFatal'` event | `QueueFatalError` |
| Rust | the `Consumer` stream yields `Err(Error::QueueGone)` at once, then ends; `HandlerConsumer::closed()` returns it after the drain | `ConnectionOptions::on_queue_fatal` (runs on Tokio's blocking pool) | `Error::QueueGone` (`ErrorKind::QueueGone`) |

Go sends the reason to a `NotifyClose` channel without blocking and then closes
it, so pass a buffered channel.

Duplicate reports are suppressed differently:

| | Go and TypeScript | Rust |
|---|---|---|
| Recorded per | queue name | queue incarnation (name and generation) |
| Record cleared by | a successful declaration of that name on the connection | a declaration on the connection that returns that generation |
| Report for a generation other than the cached one | ignored: no teardown of other consumers, no handler call (a Go consumer that hit `PMQ02` still closes itself with the error) | a keep-alive report is ignored; a consumer's report tears down that incarnation's consumers without calling the handler |

### Deletion

Deleting a queue (Go `DeleteQueue`, TypeScript `deleteQueue`, Rust `delete_queue`)
updates only the connection's own bookkeeping. On success it deregisters the
queue's keep-alive and drops the cached generation (and in Go and TypeScript the
cached topic). It does not close consumers or call the queue-fatal handler.
Consumers of the deleted queue, on this or any other connection, find out at their
next claim through `PMQ02`, and their connections then run the queue-fatal teardown
above, handler included. In Rust, a keep-alive failure of an exclusive queue this
connection deleted within the last 60 seconds is treated as intentional and not
reported.

Rust serializes declarations and deletions of the same queue name on a connection,
from before their SQL until their bookkeeping is done. Go and TypeScript do not.

## Notifications and polling

Consumers subscribe to `pmq:t:<topic>` (every publication) and `pmq:q:<queue>`
(events that make rows claimable again; see
[Notifications](../mq/README.md#notifications)). Each connection holds at most one
LISTEN session shared by its consumers, with reference-counted channel
subscriptions. A notification wakes the consumers subscribed to its channel, which
then fetch.

Notifications are hints. Polling is the correctness fallback. After a full batch
a consumer fetches again as soon as it has room for more deliveries. After a
shorter claim it waits for the first of a notification, its poll interval, or the
next visible time where the client looks it up (below).

| | Go | TypeScript | Rust |
|---|---|---|---|
| LISTEN connection | acquired from the pool when the first consumer starts; closed, not returned to the pool, when a session ends | a pooled client held while subscriptions exist; destroyed, not returned to the pool, when the session ends | a separate single-connection pool built from the main pool's connect options, outside the main pool; connects once something subscribes |
| Reconnects | until `Close`, even with no subscribers | while subscriptions exist | while subscriptions exist |
| Reconnect backoff (no jitter, capped at 30 s) | 1 s, ×1.5 per attempt, never reset | 500 ms, ×2, reset once a session's subscriptions are in place | 250 ms, ×2, reset when a notification arrives |
| After a (re)connect | subscriptions are restored; no subscriber is woken | subscriptions are restored; no subscriber is woken | every subscriber is woken once its LISTENs are in place, including on the first session |
| Subscription changes | applied after a NOTIFY on a per-instance control channel | reconciled on every change and after each reconnect | reconciled on every change and after each reconnect |
| `get_next_visible_time` lookup | after an empty claim; uses the retry policy | after an empty or partial claim; uses the retry policy | after an empty claim; bounded by 2 s, not retried |
| Wait when the next row is due but locked | 100 ms | 100 ms | 100 ms |
| Poll interval (default) | `WithCheckTimeout` (10 s) | `pollingIntervalMs` (1 s) | `check_timeout` (10 s) |
| Claim bound | 1 s | half the visibility timeout, at least 1 s | `statement_timeout` of half the visibility timeout, between 1 s and 30 s |

A failed claim or lookup is followed by a new attempt after one second. In Go and
TypeScript, notifications missed while the LISTEN session was down are caught up
by polling.

Rust's `ConnectionOptions::notifications(false)` disables LISTEN and relies on
polling alone, for example behind a transaction-pooling proxy that cannot carry a
LISTEN session.

## Publication and retries

Automatic publication retry is limited to aborted transactions (`40001`, `40P01`)
in all clients. A disconnect after commit has an unknown outcome and is returned to
the caller. Publishing inside a caller-owned transaction (Go `PublishWithTx`,
TypeScript `publishWithTransaction`, Rust `publish_tx`) is never retried, nor is a
transactional ack. An application retry may publish twice; no broker can make
arbitrary external effects exactly once.

Other operations use the connection's retry policy, which retries `40001`, `40P01`,
`55P03`, class `08` and `57P01`–`57P03`. Go retries only errors the server
reported with one of these codes; TypeScript also retries Node network errors
(`ECONNRESET`, `ENOTFOUND` and similar) and Rust also retries driver I/O errors. A
claim (`consume_message`) is never retried.

| | Go | TypeScript | Rust |
|---|---|---|---|
| Option | `WithRetryConfig`, `WithoutRetries` | `retry` | `ConnectionOptions::retry` (`RetryConfig::disabled()`) |
| Attempts | 3 | 5 | 3 |
| Backoff | 100 ms, ×2, up to 2 s | 100 ms, ×2, up to 5 s | 100 ms, ×2, up to 2 s |
| Settlement and manual extension | retry policy | retry policy | retry policy |
| Renewal and keep-alive calls | retry policy | not retried | retry policy |
| Destructive admin calls | retry policy | retry policy | only `40001`, `40P01`, `55P03` |

Rust's destructive admin calls are `delete_queue`, `delete_topic`, `purge_dlq`,
`requeue_dlq`, `cleanup_completed_messages`, `cleanup_unreferenced_messages` and
`maintenance_fast`.

## Maintenance

Installations must schedule `postgremq.pmq_maintenance_fast()` and bounded
`postgremq.cleanup_completed_messages(retention_hours, batch_size)` calls. Payload
collection preserves payloads still referenced by any queue or the DLQ. See
[Maintenance and retention](../mq/README.md#maintenance-and-retention).

## Tests

- SQL tests cover strict expiry, queue generations, nonblocking heartbeat
  contention, wall-clock expiry, and bounded retention with lagging queues and the
  DLQ.
- Go, TypeScript and Rust tests cover stale deliveries and results, shutdown
  settlement, cancelled handlers, late fetches, queue-fatal teardown and
  notification recovery.
- `POSTGREMQ_SOAK_SECONDS=300 go test -race -run TestProductionTopologySoak -count=1 -timeout=7m -v`
  in `postgremq-go` runs 20 topics, 100 queues, 400 handler consumers (2–6 per
  queue), 10 publications per second and 1/16/256 KiB payloads, with maintenance
  and zero-hour cleanup retention running. It checks that every delivery is
  handled exactly once in a healthy run and that payload storage drains to zero.

The soak test acknowledges immediately and uses zero-hour retention to verify
delivery and reclamation. It does not model handler cost or long-term storage
growth; size deployments for actual payloads, fan-out, handler duration and
retention.
