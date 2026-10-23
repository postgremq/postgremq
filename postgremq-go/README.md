# PostgreMQ Go Client

Go client for PostgreMQ, a message queue that runs inside PostgreSQL. Messages are published to topics, copied to every queue subscribed to the topic, and claimed by consumers under a visibility timeout (a lease). Because the queue lives in your database, you can publish and acknowledge in the same transaction as your application writes.

Requirements: Go 1.25+, PostgreSQL 15+.

## Installation

```bash
go get postgremq.dev/postgremq-go
```

The package name is `postgremq`:

```go
import "postgremq.dev/postgremq-go"
```

Install the SQL schema in the same database as your application. All queue objects live in the fixed `postgremq` schema, and the client schema-qualifies every call, so your `search_path` is never changed. Install it with one of:

- the CLI: `postgremq migrate --dsn "$DATABASE_URL"` (see [cmd/postgremq](cmd/postgremq/README.md); `go install postgremq.dev/postgremq-go/cmd/postgremq@latest`);
- `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f mq/sql/latest.sql`;
- the Go API: `postgremq.Migrate(pool)` and `postgremq.GetMigrationStatus(pool)` (see [examples/migration](examples/migration/main.go)). `Migrate` returns typed errors (see [Errors](#errors)): a failed migration is a `*MigrationError` with its version and, when PostgreSQL reports a position it can map, the line and column in its file (its message adds the server's detail, hint, failing query inside a function and context; the `*pgconn.PgError` stays in the chain), a dirty database a `*DirtySchemaError`, and a recorded version none of the migrations knows an `*UnknownSchemaVersionError`. Versions are schema versions: the mq version stored as one number (10000 is mq 0.1.0); `postgremq.FormatSchemaVersion` turns one back into the mq version.

`latest.sql` is for fresh databases only (it refuses to run on an existing installation) and records the migration version it installs, so `Migrate` can upgrade it later.

See [the SQL reference](../mq/README.md) for the schema, privileges and required maintenance.

## Quick start

```go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"postgremq.dev/postgremq-go"
)

func main() {
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/app")
	if err != nil {
		log.Fatal(err)
	}
	conn, err := postgremq.Dial(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Both calls are idempotent.
	if err := conn.CreateTopic(ctx, "orders"); err != nil {
		log.Fatal(err)
	}
	if err := conn.CreateQueue(ctx, "order-processing", "orders", false,
		postgremq.WithMaxDeliveryAttempts(5)); err != nil {
		log.Fatal(err)
	}

	if _, err := conn.Publish(ctx, "orders",
		json.RawMessage(`{"order_id": 12345}`)); err != nil {
		log.Fatal(err)
	}

	consumer, err := conn.Consume("order-processing",
		postgremq.WithVT(30), postgremq.WithBatchSize(10))
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Stop()

	for msg := range consumer.Messages() {
		log.Printf("message %d: %s", msg.ID, msg.Payload)
		if err := msg.Ack(ctx); err != nil {
			log.Printf("ack failed: %v", err)
		}
	}
}
```

Runnable programs are in [examples/](examples/README.md).

## Connecting

- `Dial(ctx, *pgxpool.Config, opts...)` creates and owns a `pgxpool.Pool`; `Close` closes it. `ctx` only bounds pool creation and the schema check.
- `DialFromPool(ctx, pool, opts...)` uses an existing pool (a `Pool` implementation, normally `*pgxpool.Pool`). `Close` leaves the pool open; close the `Connection` before the pool, since its `LISTEN` session holds a pooled connection until `Connection.Close`. A pool with `MaxConns` 1 is starved by that session.

From its first `Consume` or `ConsumeHandler` until `Close`, each `Connection` holds one pooled connection for `LISTEN` (shared by all its consumers), so size the pool for that plus publish, consume, settle and heartbeat traffic. `LISTEN` needs session affinity and does not work through a transaction-mode pooler. If the session fails, the client closes that connection and reconnects after a delay that starts at 1 second and grows ×1.5 per attempt up to 30 seconds, jittered down by up to half, and starts again from 1 second once a session has been listening for 5 seconds. Notifications sent while it is disconnected are lost, so once the new session is listening every consumer is woken to fetch; consumers also poll every `WithCheckTimeout`. Connection methods are safe for concurrent use.

On connect, `Dial` and `DialFromPool` read `postgremq.info()` and check that the installation's schema version is on an mq compatibility line this client implements (`postgremq.SupportedMQVersions()`, currently `["0.2"]`). A line is an mq major, or before 1.0 a major and minor: a breaking schema change bumps the major, or before 1.0 the minor, so a client built for mq 0.2 accepts 0.2.x and rejects 0.1.x and 0.3.0. Otherwise they return a `*postgremq.CompatibilityError` (`errors.Is(err, postgremq.ErrIncompatibleSchema)`) with the schema version and the supported lines; a database without `info()` (not installed, or older than discovery), or whose `info()` reports no schema version, gets the same error saying it needs an installation or upgrade. Connection and permission errors are returned as they are. Within a supported line, a function the installation lacks fails with the database's error (SQLSTATE `42883`) like any other.

| Connection option | Default | Effect |
|---|---|---|
| `WithShutdownTimeout(d)` | `0` (no limit) | Bounds how long `Close` waits for consumers to drain. |
| `WithRetryConfig(RetryConfig{...})` | 3 attempts, 100 ms initial backoff, 2 s max, ×2 | Retry policy for transient errors. All fields must be positive unless `Disabled` is set. |
| `WithoutRetries()` | | Disables retries. |
| `WithExtenderBatchSize(n)` | `100` | Maximum messages per auto-extension call. |
| `WithQueueFatalHandler(fn)` | error logged through the configured logger (silent with the default no-op logger) | Called when a queue this connection uses is gone. |
| `WithMeterProvider(p)` | metrics off | Enables OpenTelemetry client metrics. |
| `WithLogger(l)` / `WithLevelLogger(l)` | `NoopLogger` | Printf-style or leveled (`Debugf`/`Infof`/`Warnf`/`Errorf`) logger. |

## Topics and queues

```go
err := conn.CreateTopic(ctx, "orders")
err = conn.CreateQueue(ctx, "billing", "orders", false,
	postgremq.WithMaxDeliveryAttempts(5))
```

- Topic and queue names must match `^[A-Za-z0-9_:.\-]+$` and be at most 57 bytes (PostgreSQL limits `NOTIFY` channel names).
- `CreateTopic` is idempotent. `CreateQueue` is idempotent when the parameters match. Re-creating a queue with a different topic, max attempts, exclusive flag or keep-alive interval fails with `ErrValidation`.
- A queue receives only messages published after it was created.
- `WithMaxDeliveryAttempts(n)`: after `n` deliveries a message moves to the dead letter queue. The default `0` means unlimited attempts and no DLQ.
- `WithKeepAliveInterval(d)`: lease length for exclusive queues (default 5 minutes, at least 1 ms, sent in whole milliseconds). See [Exclusive queues](#exclusive-queues-and-keep-alive).

`Consume` needs to know the queue's topic. It knows the topic for queues created through `CreateQueue` on the same `Connection`. For any other queue, pass `WithTopic(topic)` or call `CreateQueue` (idempotent) first; otherwise `Consume` returns `ErrQueueNotFound`.

A consumer binds to one queue generation (incarnation): the generation cached by the last `CreateQueue` of that name on this `Connection`, otherwise the one the database reports when `Consume` runs (`ErrQueueNotFound` if the queue does not exist). If the queue is later deleted and re-created, the old consumer does not consume from the new queue; it is torn down (see [Queue-fatal teardown](#queue-fatal-teardown)). The cached generation stays until `CreateQueue` or `DeleteQueue` of that name on this `Connection` replaces or clears it, so after a queue is re-created elsewhere, call `CreateQueue` before consuming it again.

## Publishing

```go
id, err := conn.Publish(ctx, "orders", json.RawMessage(`{"order_id": 1}`))

// Not visible to consumers until the database's clock reaches the given time.
id, err = conn.Publish(ctx, "orders", payload,
	postgremq.WithDeliverAfter(time.Now().Add(5*time.Minute)))
```

The payload must be valid JSON; it is stored as `JSONB`. Message IDs are `int64`. Publishing to a topic that does not exist returns `ErrQueueNotFound`. A topic with no queues accepts the message, but nothing delivers it.

`Publish` retries automatically only on serialization failure (`40001`) and deadlock (`40P01`), which guarantee that the transaction rolled back. Other errors, such as a dropped connection, leave the outcome unknown and go back to the caller. If you retry them yourself, the message may be published twice.

## Consuming

### Channel consumer

`Consume(queue, opts...)` returns a `*Consumer` whose `Messages()` channel yields `*Message` values:

```go
consumer, err := conn.Consume("billing", postgremq.WithVT(60))
if err != nil {
	return err
}
defer consumer.Stop()

for msg := range consumer.Messages() {
	if err := process(msg.StoppedCtx, msg.Payload); err != nil {
		_ = msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(10*time.Second)))
		continue
	}
	_ = msg.Ack(ctx)
}
```

The consumer fetches in batches, starting immediately. After that, a fetch is triggered by:

- a publish `NOTIFY` on the topic, or a `NOTIFY` on the queue (sent by nack, release, DLQ requeue, and settlements that unblock a message group);
- a full batch, which triggers an immediate refetch;
- an empty batch, after which the consumer asks the database how long until the next row becomes visible (a delayed message or an expiring lease) and fetches then, or after 100 ms if that time has already passed;
- a failed fetch, which is retried after about one second;
- `WithCheckTimeout` passing without any of the above. It bounds every wait; after a partial batch the consumer waits for a notification or this timeout.

A notification that arrives while a fetch is running triggers another fetch right after it. Each fetch is bounded by one second. The consumer fetches only once the previous batch has been handed to the channel. The channel buffers one batch, and one more batch can wait inside the consumer, so up to two batches can be claimed ahead of your code. Those messages are leased and auto-extended while they wait.

| Consume option | Default | Effect |
|---|---|---|
| `WithVT(seconds)` | 30 | Visibility timeout per claim. `0` means the default. |
| `WithBatchSize(n)` | 10 | Messages per fetch, which is also the channel buffer size. |
| `WithCheckTimeout(d)` | 10 s | Longest wait between fetches when no notification arrives. |
| `WithExtensionThreshold(f)` | 0.5 | Fraction of the remaining lease that elapses before auto-extension. Must be in (0, 1). |
| `WithNoAutoExtension()` | | Disables auto-extension. Requires an explicit `WithVT`. |
| `WithTopic(topic)` | | Topic of a queue this `Connection` did not create. |

`Message` fields: `ID`, `Payload` (`json.RawMessage`), `PublishedAt`, `DeliveryAttempt` (1 on the first delivery), `VT` (lease deadline; use `GetVT()` while auto-extension may update it), `GroupKey`, `GroupSeq`, and `StoppedCtx`. `ConsumerToken()` returns the delivery's ownership token.

`StoppedCtx` is cancelled when the consumer stops (including a queue-fatal teardown), when the connection closes, or when auto-extension finds that the lease was lost. Pass it to your processing code.

### Handler consumer

`ConsumeHandler` runs a function per message on its own goroutine:

```go
hc, err := conn.ConsumeHandler("billing",
	func(ctx context.Context, msg *postgremq.Message) {
		// ctx is the stop signal; settle with a context that shutdown does not cancel.
		settleCtx := context.WithoutCancel(ctx)
		if err := process(ctx, msg.Payload); err != nil {
			_ = msg.Nack(settleCtx, postgremq.WithDelayUntil(time.Now().Add(5*time.Second)))
			return
		}
		_ = msg.Ack(settleCtx)
	},
	postgremq.WithVT(60),
	postgremq.WithMaxInFlight(10),
)
if err != nil {
	return err
}
defer hc.Stop()
```

- `ctx` is the message's `StoppedCtx`: it is cancelled when the lease is lost or the consumer stops. Use it to abandon work, not to settle — a settlement started with a cancelled context fails, and because it already claimed settlement, the client does not auto-settle the message either.
- If the handler returns without settling the message, the client settles it. It acks when `ctx` is still live and nacks when `ctx` was cancelled. A panic is recovered and the message is nacked. An explicit settlement always takes precedence.
- `WithMaxInFlight(n)` limits how many handlers run at once. The default `0` means unlimited: every delivered message starts a goroutine immediately.
- All consume options also apply to `ConsumeHandler`.
- `HandlerConsumer.Stop()` stops fetching, cancels running handlers' contexts, and waits for them to return.

### Settling messages

| Method | Effect |
|---|---|
| `Ack(ctx)` | Marks the message completed. |
| `Nack(ctx, opts...)` | Returns the message for redelivery: immediately, or at `WithDelayUntil(t)`, where `t` is read on the local clock (the client sends the time remaining until `t`). On the final allowed attempt, it moves the message to the DLQ. |
| `Release(ctx)` | Returns the message immediately and does not count the attempt. Use it for work you never started. |
| `AckWithTx(ctx, tx)` | `Ack` inside your transaction. See [Transactions](#transactions). |
| `SetVT(ctx, seconds)` | Sets the lease deadline to now + `seconds`, updates `msg.VT` and returns the new deadline. It does not settle the message. It always runs SQL, also after a settle call; the server returns `ErrLeaseLost` for a delivery that is no longer processing under this token or whose lease has passed. |

Only the first `Ack`, `Nack`, `Release` or `AckWithTx` on a `Message` runs SQL. Later calls return `ErrLeaseLost`, as does settling a message whose lease has passed to another consumer. The first call ends the client's tracking of the message even if the SQL fails. A message is redelivered when its lease expires without settlement. Delivery is at least once, so make your side effects idempotent.

Every delivered message must be settled. `Connection.Close` waits for settlement unless `WithShutdownTimeout` expires; `Consumer.Stop` and `HandlerConsumer.Stop` have no timeout of their own.

## Transactions

`PublishWithTx` and `AckWithTx` take any `Tx` (normally a `pgx.Tx`) and run inside your transaction, so queue changes commit or roll back together with your application writes:

```go
tx, err := pool.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx) // no-op after Commit

if _, err := tx.Exec(ctx, "UPDATE app.orders SET status = 'paid' WHERE id = $1", orderID); err != nil {
	return err
}
if _, err := conn.PublishWithTx(ctx, tx, "order-paid", payload); err != nil {
	return err
}
if err := msg.AckWithTx(ctx, tx); err != nil { // msg from a consumer
	return err
}
return tx.Commit(ctx)
```

- The caller owns the transaction. The client never begins, commits or rolls it back, and it never retries `WithTx` calls.
- A published message is distributed to queues, and becomes visible, only when the transaction commits.
- `AckWithTx` settles the `Message` on the client side when it runs: auto-extension stops, and further settle calls return `ErrLeaseLost`. If the transaction rolls back, the delivery stays claimed until its lease expires and is then redelivered.
- `Nack`, `Release` and `SetVT` have no transactional variants.

## Message groups

`WithGroupKey` gives ordered delivery per key:

```go
_, err := conn.Publish(ctx, "session-events", payload, postgremq.WithGroupKey(sessionID))
```

- Within each queue, a group's messages are delivered one at a time, in publish commit order. A message is not claimable while an earlier message of its group is pending or being processed. This is head-of-line blocking, as in SQS FIFO. Messages without a key are unordered and never blocked.
- `Message.GroupKey` and `Message.GroupSeq` give a delivered message's group and its dense, 1-based position in the group. `ListMessages` and `GetMessage` also return them.
- A batch contains at most one message per group.
- A head nacked with a delay, or published with `WithDeliverAfter`, blocks its group until it becomes visible. A head that exhausts its attempts moves to the DLQ, which unblocks the group. With `WithMaxDeliveryAttempts(0)`, a message that always fails blocks its group forever.
- The database serialises publishers of the same group until their transaction commits. Use a session, order or account as the key, never a hot key shared by unrelated work. A transaction that publishes to several groups should take them in a consistent order to avoid deadlocks.
- The key must be 1 to 255 characters. An empty key returns `ErrValidation`.

Order is guaranteed within one queue only. The full contract is in [mq/README.md](../mq/README.md#message-groups).

## Visibility timeout and auto-extension

A claimed message is invisible to other consumers until its visibility timeout (`WithVT`) expires. By default the client keeps extending the lease of every claimed message until the message is settled.

- Auto-extension runs once per `Connection`, not per consumer. One background goroutine collects the due extensions of every consumer, across all queues, and sends them in one `set_vt_batch_multi` call per tick, at most `WithExtenderBatchSize` messages per call; the rest follow on the next tick.
- A message is registered for extension when it is fetched, before it is delivered, and leaves the schedule when its first settle call returns, successfully or not.
- A message is extended when `WithExtensionThreshold` (default 0.5) of its remaining lease has elapsed. Each extension sets the deadline to now + the consumer's `WithVT` and updates `msg.VT` (read it with `GetVT()`).
- Each call is bounded by the earliest confirmed deadline in the batch, but by no less than one second and no more than five, and the connection's retry policy applies inside that bound.
- If the row is locked by another statement, the message is retried after 100 ms; after a failed call, its messages are retried after one second. Both retries happen only until the message's last confirmed deadline.
- A lease's deadline is the local time read before the claim or renewal call plus the time left that the database reports, so scheduling does not depend on the client and database clocks agreeing. `msg.VT` is this deadline, on the local clock.
- If the lease is lost, because the server omits the message from the result (it is no longer processing under this token, or its lease has passed) or the last confirmed deadline passes before a renewal succeeds, the client cancels the message's `StoppedCtx`, stops extending it, and counts it in the `postgremq.client.renewal.lost` metric. Another consumer may now receive the message. Your code should stop work and still settle the message; the settle call will usually return `ErrLeaseLost`.
- A message stays scheduled until its settle call returns, so a renewal that runs after the settlement has committed but before the call returns omits the message and reports it lost in the same way.

With `WithNoAutoExtension()`, call `msg.SetVT(ctx, seconds)` yourself before the lease expires.

`SetVT` and auto-extension are independent. The auto-extender does not see manual `SetVT` calls: its next renewal sets the deadline to now + the consumer's `WithVT`, which can shorten a longer deadline you set by hand. If you use both, keeping them consistent is your responsibility.

## Exclusive queues and keep-alive

An exclusive queue (`exclusive = true`) has a lease. It stays alive only while keep-alives renew its deadline, which suits per-instance queues such as reply or broadcast queues. "Exclusive" does not limit which connections can consume from the queue.

```go
err := conn.CreateQueue(ctx, "events-"+instanceID, "events", true,
	postgremq.WithKeepAliveInterval(30*time.Second))
```

- `CreateQueue` registers the queue with the connection's keep-alive goroutine. This goroutine renews every exclusive queue the connection created in one `extend_queue_keep_alive_multi` call per tick. The first renewal is due half an interval after `CreateQueue`; each later one when half of the remaining time to the confirmed deadline has passed. Renewal continues whether or not the queue has consumers, until `DeleteQueue` on this connection, a queue-fatal teardown, or `Close`. Consuming does not renew the queue.
- Each call is bounded by the earliest confirmed deadline in the batch, but by no less than one second and no more than five, with the retry policy inside that bound. After a failed call, or when the queue row is locked, the queue is retried every 100 ms until its last confirmed deadline.
- If renewals stop, for example because the process exits, the queue expires at its deadline. An expired queue receives no new messages and serves no consumers. `MaintenanceFast` (or `DeleteInactiveQueues`) then deletes it, unless it still has DLQ entries.
- A re-create with the same parameters refreshes a live queue's deadline. An expired queue cannot be revived: `CreateQueue` returns `ErrQueueNotFound` until the queue is deleted and created again.
- When a renewal omits the queue (deleted, expired, or replaced by a new generation), or failures last past the last confirmed deadline, the queue is treated as gone. See below.

## Queue-fatal teardown

A queue is gone for this connection when either of these happens:

- a consumer's fetch fails with `PMQ02` (`ErrQueueNotFound`): the queue was deleted (through this or any other connection), replaced by a new queue with the same name, or, for an exclusive queue, has expired;
- for an exclusive queue created through this connection, a keep-alive renewal omits it, or failures last past its last confirmed deadline.

A consumer whose fetch fails with `PMQ02` records the reason and starts its own drain. The connection then, on a separate goroutine:

1. stops the queue's keep-alive;
2. tears down every consumer of that queue generation on this connection: fetching stops, `Messages()` closes, buffered messages are released, and the `StoppedCtx` of delivered messages is cancelled;
3. calls `WithQueueFatalHandler` with the queue name and the reason, or logs the reason at error level if no handler is set.

The connection handles each queue name once. A later `CreateQueue` of that name on this connection re-arms it. A loss reported for a generation other than the one cached by this connection's last `CreateQueue` is ignored at the connection level; a consumer whose own fetch saw it still closes with the reason. The connection does not clear its cached topic or generation for the queue.

The reason is a `*QueueFatalError`, which matches `errors.Is(err, postgremq.ErrQueueGone)`.

`DeleteQueue` does not stop consumers itself. On success it stops this connection's keep-alive for the queue and forgets its cached topic and generation. Consumers of the deleted queue, on this or any other connection, fail with `PMQ02` on their next fetch, which starts the teardown above. A connection with no consumer of the deleted queue reports the deletion only if it renews the queue's keep-alive.

```go
conn, err := postgremq.Dial(ctx, cfg,
	postgremq.WithQueueFatalHandler(func(queue string, err error) {
		log.Printf("queue %s is gone: %v", queue, err)
	}))

// Per consumer. Use a buffered channel: the send is non-blocking.
closed := consumer.NotifyClose(make(chan error, 1))
go func() {
	if err, ok := <-closed; ok && errors.Is(err, postgremq.ErrQueueGone) {
		// re-create the queue and start a new consumer, or exit
	}
}()
```

`NotifyClose` fires when the consumer has finished draining. If the consumer was torn down because its queue is gone, the channel receives the reason and is then closed; a consumer stopped by `Stop` or `Close` without a loss closes it without a value. A channel registered after the consumer closed gets the outcome immediately. `HandlerConsumer.NotifyClose` works the same way. A handler consumer has no message loop to end, so this is how it learns that its queue is gone. The connection handler runs on its own goroutine and may block. It is the only signal for an exclusive queue that has a producer but no consumer.

## Shutdown

`Connection.Close()`:

1. Stops accepting new work: `Publish`, `PublishWithTx`, `CreateTopic`, `CreateQueue`, `Consume` and `ConsumeHandler` return `ErrConnectionClosed`. All other methods (settlement, `SetVT`, `SetVTBatchMulti`, administration, inspection, maintenance and DLQ calls) keep working until `Close` finishes, then return `ErrConnectionClosed`.
2. Stops the `LISTEN` session and stops every consumer. Consumers stop fetching, release buffered messages that were never delivered (the attempt is not counted), and cancel the `StoppedCtx` of delivered messages.
3. Waits for every delivered message to be settled. Auto-extension and exclusive-queue keep-alive keep running during this drain.
4. Stops the background goroutines and closes the pool if the `Connection` owns it.

With `WithShutdownTimeout(d)`, step 3 ends after `d`. Unsettled messages are abandoned: the connection's database calls still in progress are cancelled, auto-extension stops, and their leases expire normally, so the attempt counts. The default `0` waits indefinitely. Set a timeout for bounded deployments. `Close` is idempotent and always returns `nil`; concurrent calls wait for the same shutdown.

Because `Close` waits for delivered messages, calling it from inside a message handler, or from a consume loop while holding an unsettled message, waits on itself — forever without `WithShutdownTimeout`. Call it from another goroutine there (`go conn.Close()`), or set a shutdown timeout. `HandlerConsumer.Stop` called inside one of its handlers, or `Consumer.Stop` called while holding an unsettled message, waits on itself, since neither has a timeout of its own; call them from another goroutine.

`Consumer.Stop()` runs the same drain for one consumer. It has no timeout and blocks until every delivered message is settled, so do not call it from the goroutine that holds an unsettled message. `Stop` is idempotent.

## Retries

Non-transactional operations retry transient errors with exponential backoff (`WithRetryConfig`). The retryable SQLSTATEs are:

- `40001` (serialization failure) and `40P01` (deadlock);
- `55P03` (lock not available);
- class `08` (connection errors);
- `57P01`, `57P02` and `57P03` (server shutdown or restart).

`IsRetryableError(err)` exposes the same check. Declarations, settlement, `SetVT`, `SetVTBatchMulti`, keep-alive, and every administration, inspection and maintenance method use this policy. The exceptions:

- `Publish` retries only `40001` and `40P01` (see [Publishing](#publishing)).
- `PublishWithTx` and `AckWithTx` never retry.
- A consumer's fetch is never retried in place, so a retry cannot claim a second batch; the consumer tries again on the next fetch, about one second later.

A retry can follow an attempt that committed before its error reached the client. For a settle call, the retry then returns `ErrLeaseLost`, because the delivery is already settled.

## Errors

Use `errors.Is` with these sentinels. Methods that map SQLSTATEs to sentinels keep the original `*pgconn.PgError` in the chain, so `errors.As` still works.

| Error | When |
|---|---|
| `ErrConnectionClosed` | `Publish`, `PublishWithTx`, `CreateTopic`, `CreateQueue`, `Consume` and `ConsumeHandler` called after `Close` began. Every other method once `Close` has finished. |
| `ErrLeaseLost` (`PMQ01`) | Ack, nack, release or `SetVT` on a delivery this consumer no longer owns: the lease expired and another consumer claimed the message, the token does not match, or the message is not being processed. Also returned, without any SQL, by a second settle call on the same `Message`. |
| `ErrQueueNotFound` (`PMQ02`) | Publishing to a missing topic, creating a queue on a missing topic, re-creating an expired exclusive queue, or `Consume`/`ConsumeHandler` on a queue whose topic is unknown to this connection or that does not exist. |
| `ErrValidation` (`PMQ03`) | A rejected request: an invalid or too long name, an empty or too long group key, a negative max delivery attempts, a queue re-created with different parameters, deleting a topic that still has messages, and similar. |
| `ErrQueueGone` | A consumer's queue is gone. Delivered as `*QueueFatalError{Queue, Err}` through `NotifyClose` and `WithQueueFatalHandler`. |
| `ErrMigrationFailed` | A migration failed in `Migrate` and its version is left dirty. When the database reported the error (`Err` is a `*pgconn.PgError`) the migration was rolled back; after a connection failure its outcome is unknown. Returned as `*MigrationError{Version, Line, Column, Err}`: `Line` and `Column` locate the error in the migration's file (0 when PostgreSQL reports no position, or one that cannot be mapped to the file: with a multibyte database or client encoding other than UTF8, such as EUC_JP), `Err` is the database error. |
| `ErrDirtySchema` | `Migrate` found the recorded schema version marked dirty: a migration failed partway and needs manual repair ([Dirty state](cmd/postgremq/README.md#dirty-state)). Returned as `*DirtySchemaError{Version}`. |
| `ErrUnknownSchemaVersion` | `Migrate` found a recorded version below its latest migration that is none of its migrations, so it knows no upgrade from it; nothing is applied. Returned as `*UnknownSchemaVersionError{Version}`. |

The sentinels are mapped by `CreateTopic`, `CreateQueue`, `Publish`, `PublishWithTx`, `Ack`, `Nack`, `Release`, `AckWithTx`, `SetVT`, `SetVTBatchMulti` and `DeleteTopic`, and by consumers' fetches. The other administration, inspection and maintenance methods return the database error as is: use `errors.As` with `*pgconn.PgError` and check `Code` (for example `PMQ03` from `DeleteQueue` or `CleanUpTopic` while DLQ entries exist).

Invalid client options, such as a bad retry config, `WithExtensionThreshold` outside (0, 1), or `WithNoAutoExtension` without `WithVT`, return plain errors from `Dial`, `DialFromPool`, `Consume` or `ConsumeHandler`. `CreateQueue` returns a plain error for a keep-alive interval below 1 ms.

## Administration and maintenance

| Method | Purpose |
|---|---|
| `ListTopics`, `ListQueues` | Topics, and queues with their configuration (`QueueInfo`). |
| `ListMessages(ctx, queue)` | Message metadata in a queue, without payloads (`QueueMessage`). |
| `GetMessage(ctx, id)` | One message with its payload. Returns `nil, nil` if it does not exist. |
| `GetQueueStatistics(ctx, &queue)` | Pending, processing, completed and total counts. Pass `nil` for all queues. |
| `ListDLQMessages`, `RequeueDLQMessages(ctx, queue)`, `PurgeDLQ` | Inspect the dead letter queue, return its messages to their queue with attempts reset, or delete them. |
| `DeleteTopic` | Deletes an empty topic and its queues. Fails while the topic has messages. |
| `DeleteQueue` | Deletes a queue and its deliveries. Fails while the queue has DLQ entries; deleting a missing queue succeeds. On success it stops this connection's keep-alive for the queue and forgets its cached topic and generation. It does not stop consumers; see [Queue-fatal teardown](#queue-fatal-teardown). |
| `DeleteQueueMessage`, `CleanUpQueue`, `CleanUpTopic`, `PurgeAllMessages` | Destructive purges. They can remove active work. |
| `MaintenanceFast` | Moves final attempts whose consumer crashed to the DLQ, and deletes expired exclusive queues that have no DLQ entries. Returns `MaintenanceCounters`. |
| `CleanupCompletedMessages(ctx, &hours)` | Deletes completed deliveries older than `hours` (`nil` = 24), up to 1000 per call, plus old payloads that no queue or DLQ entry references. Returns the number of deliveries deleted. |
| `DeleteInactiveQueues` | Deletes expired exclusive queues that have no DLQ entries. |
| `SetVTBatchMulti` | Low-level batched lease extension across queues, as used by auto-extension. Returns one `MultiLock` per extended or busy delivery; a requested delivery missing from the result has lost its lease. |

Nothing runs maintenance automatically. Schedule `MaintenanceFast` and `CleanupCompletedMessages` yourself, for example from a ticker or a cron job:

```go
ticker := time.NewTicker(5 * time.Second)
defer ticker.Stop()
for range ticker.C {
	if _, err := conn.MaintenanceFast(ctx); err != nil {
		log.Printf("maintenance: %v", err)
	}
	// Each call deletes at most 1000 deliveries; repeat until caught up.
	for {
		n, err := conn.CleanupCompletedMessages(ctx, nil)
		if err != nil {
			log.Printf("cleanup: %v", err)
			break
		}
		if n == 0 {
			break
		}
	}
}
```

Choose intervals with the guidance in [mq/README.md](../mq/README.md#maintenance-and-retention).

## Observability

`WithMeterProvider(provider)` enables OpenTelemetry metrics. They are off without it, even when a global provider is set; pass `otel.GetMeterProvider()` to use the global one. The application owns the provider, its exporters and its shutdown. The client records:

- `messaging.client.operation.duration`, `messaging.process.duration`;
- `messaging.client.sent.messages`, `messaging.client.consumed.messages`;
- `postgremq.client.handlers.active`, `postgremq.client.renewal.lost`.

`messaging.process.duration` and `postgremq.client.handlers.active` cover `ConsumeHandler` handlers only.

Queue state (ready, delayed, processing, dead-lettered, oldest ready age) comes from SQL: `SELECT * FROM postgremq.queue_metrics()`.

The [observability guide](../docs/observability.md) has the metric definitions, a tested Collector configuration, and a runnable example ([examples/metrics](examples/metrics/main.go)).

## Delivery guarantees

Delivery is at least once. Use idempotency keys for side effects, and treat a publish whose outcome is unknown as possibly committed. The shared [delivery lifecycle](../docs/delivery-lifecycle.md) explains ownership, cancellation and shutdown. The [architecture overview](../docs/architecture.md) explains how the pieces fit together.

## Development

Tests run against `POSTGREMQ_TEST_DATABASE_URL` when it is set: a `postgres://` URL of a PostgreSQL 15+ server whose user can create databases and roles, with `max_connections` of 200 or more. The tests connect to the URL's database (normally `/postgres`) to create and drop their own; a Unix socket goes in `?host=/path`. Otherwise each test package starts a `postgres:15` container with [testcontainers-go](https://github.com/testcontainers/testcontainers-go) (Docker must be running) and removes it when the package's tests finish. Each test gets its own database.

testcontainers-go is not a dependency of this module: the container is started by a small launcher in [internal/testserver/launcher](internal/testserver/launcher), a separate module that the tests build and run.

```bash
go test ./...                      # all tests
go test -short ./...               # skip long-running tests
go test -race -coverprofile=coverage.out -covermode=atomic ./...
POSTGREMQ_SOAK_SECONDS=60 go test -race -run TestProductionTopologySoak -timeout=3m -v
```

`make help` lists the Makefile targets. `make check` runs gofmt, go vet and staticcheck (if installed). `make lint` runs golangci-lint, as CI does.
