# PostgreMQ TypeScript Client

TypeScript/Node.js client for [PostgreMQ](https://github.com/postgremq/postgremq), a message queue that runs inside PostgreSQL. Topics fan out to queues, consumers claim messages with a visibility-timeout lease, and publishing and acknowledging can join your application's own database transaction.

- Promise-based publishing, including delayed delivery and publishing inside a caller-owned transaction
- Two consumer styles: an async iterator (`consume`) and a handler with bounded concurrency (`consumeHandler`)
- Automatic lease renewal, batched across all of a connection's consumers
- Message groups for ordered, one-at-a-time delivery per key
- Exclusive (expiring) queues with automatic keep-alive
- Queue-fatal notification when a consumed queue disappears
- Dead letter queue after a configurable number of delivery attempts
- LISTEN/NOTIFY wake-ups with polling as a fallback
- Optional OpenTelemetry metrics

## Requirements

- Node.js 22 or later
- PostgreSQL 15 or later, with the PostgreMQ SQL schema installed

## Installation

```bash
npm install postgremq
```

Connecting does not create the schema. Install or upgrade it once per database with `migrate`, which applies the migrations embedded in the package:

```typescript
import { Pool } from 'pg';
import { migrate, getMigrationStatus, formatSchemaVersion } from 'postgremq';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool); // no-op when the schema is current
const status = await getMigrationStatus(pool); // { currentVersion, dirty, latestVersion, needsMigration }
console.log(formatSchemaVersion(status.currentVersion)); // the mq version, such as 0.1.0
```

Versions are schema versions: the mq version stored as one number (10000 is mq 0.1.0), which `formatSchemaVersion` turns back into the mq version. `migrate` only goes up, to the latest version in this package. It uses the same version table and advisory lock as the Go client and the [PostgreMQ CLI](https://github.com/postgremq/postgremq/blob/main/postgremq-go/cmd/postgremq/README.md), so concurrent callers are safe and any of them can upgrade a database another one installed. A database already at a newer version (migrated by a newer release) is left unchanged. A dirty database (a migration failed partway) throws `DirtySchemaError`. `migrate` creates the `postgremq` schema when it is missing, which needs the `CREATE` privilege on the database.

You can also install the schema with the CLI (`postgremq migrate --dsn "$DATABASE_URL"`) or directly from [`mq/sql/latest.sql`](https://github.com/postgremq/postgremq/blob/main/mq/sql/latest.sql). `latest.sql` is for fresh databases only (it refuses to run on an existing installation) and records the version it installs, so `migrate` can upgrade it later.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f mq/sql/latest.sql
```

All queue objects live in the `postgremq` schema, and client queries are schema-qualified, so your application's `search_path` is not changed. Install the schema in the same database as your application data to use transactional publish and ack. See the [SQL reference](https://github.com/postgremq/postgremq/blob/main/mq/README.md) for the full contract.

## Quick start

```typescript
import { connect } from 'postgremq';

async function main() {
  const client = await connect({
    connectionString: 'postgresql://user:password@localhost:5432/mydb',
  });

  try {
    await client.createTopic('orders');
    await client.createQueue('order-processing', 'orders', false, {
      maxDeliveryAttempts: 5,
    });

    const messageId = await client.publish('orders', { orderId: '12345', total: 99.99 });
    console.log(`Published message ${messageId}`);

    const consumer = client.consume('order-processing', {
      batchSize: 5,
      visibilityTimeoutSec: 60,
    });

    for await (const message of consumer.messages()) {
      try {
        console.log('Processing', message.payload);
        await message.ack();
      } catch (error) {
        // Retry after 10 seconds. On the 5th failed attempt the message
        // moves to the dead letter queue instead.
        await message.nack({ delaySeconds: 10 });
      }
    }
  } finally {
    await client.close();
  }
}

main().catch(console.error);
```

## Connecting

`connect(options)` creates a connection, checks the installation's schema version, and returns it.

The check reads `postgremq.info()`: the installation's schema version must be on an mq compatibility line this client implements (`SUPPORTED_MQ_VERSIONS`, currently `['0.2']`). A line is an mq major, or before 1.0 a major and minor: a breaking schema change bumps the major, or before 1.0 the minor, so a client built for mq 0.2 accepts 0.2.x and rejects 0.1.x and 0.3.0. Otherwise `connect` throws a `CompatibilityError` with `schemaVersion` and `supportedVersions`; a database without `info()` (not installed, or older than discovery), or whose `info()` reports no schema version, throws it too, meaning it needs an installation or upgrade; when `info()` is missing, `cause` is the database error. Connection and permission errors are thrown as they are. Within a supported line, a function the installation lacks fails with the database's error (`code` `42883`) like any other.

```typescript
import { connect } from 'postgremq';

const client = await connect({
  connectionString: 'postgresql://user:password@localhost:5432/mydb',
  // or: config: { host: 'localhost', port: 5432, user: 'user', password: 'password', database: 'mydb', max: 20 },
  // or: pool: existingPgPool,
  shutdownTimeoutMs: 30000,
  retry: {
    maxAttempts: 5,
    initialBackoffMs: 100,
    backoffMultiplier: 2,
    maxBackoffMs: 5000,
  },
  onQueueFatal: (queue, error) => console.error(`queue ${queue} is gone`, error),
});
```

| Option | Default | Description |
|---|---|---|
| `connectionString` | — | PostgreSQL connection string. Merged into `config` when both are given. |
| `config` | `{}` | A `pg` `PoolConfig`. The connection creates its own pool from it and ends that pool on `close()`. |
| `pool` | — | An existing `pg` `Pool`. Takes precedence over `connectionString`/`config`. The connection does not end it on `close()`. |
| `shutdownTimeoutMs` | `30000` | Deadline for `close()` and for each consumer's `stop()`. Must be positive. |
| `retry` | see [Retries](#retries) | Retry policy for transient errors. All four fields are required. |
| `onQueueFatal` | — | `(queue, error) => void`, called when a queue becomes unusable. See [Queue-fatal teardown](#queue-fatal-teardown). |
| `extenderBatchSize` | `100` | Maximum number of messages renewed in one batched SQL call. |
| `meterProvider` | — | OpenTelemetry `MeterProvider`. See [Observability](#observability). |

While at least one consumer is running, the connection holds one pool client for its `LISTEN` session, so size the pool for that plus your publish, consume, settle and renewal traffic. `LISTEN` needs a session, so it does not work through a transaction-mode pooler such as PgBouncer in transaction mode.

## Publishing

```typescript
// Payload is any JSON-serializable value; it is stored as JSONB.
const id = await client.publish('orders', { orderId: '12345' });

// Delayed delivery (the time is compared with the database's clock)
await client.publish('orders', { orderId: '67890' }, {
  deliverAfter: new Date(Date.now() + 60_000),
});

// Ordered delivery within a group (see "Message groups")
await client.publish('session-events', { kind: 'started' }, { groupKey: 'session-42' });
```

`publish()` returns the message ID (a number). The topic must exist; a message published to a topic with no live queues is stored but delivered nowhere. Consumers receive the payload as parsed JSON, so values like `Date` arrive as strings.

### Publishing in a transaction

`publishWithTransaction(tx, topic, payload, options?)` runs the publish on a transaction you control. The message reaches its queues only if your transaction commits. `tx` is anything with `query(text, values)`, for example a `pg` `PoolClient` after `BEGIN`. You own `BEGIN`/`COMMIT`/`ROLLBACK`, and the client does not retry the call; retry the whole transaction yourself on a serialization failure or deadlock.

```typescript
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const tx = await pool.connect();
try {
  await tx.query('BEGIN');
  await tx.query('INSERT INTO app.orders (id) VALUES ($1)', ['12345']);
  await client.publishWithTransaction(tx, 'orders', { orderId: '12345' });
  await tx.query('COMMIT');
} catch (err) {
  await tx.query('ROLLBACK');
  throw err;
} finally {
  tx.release();
}
```

## Consuming

A consumer claims up to `batchSize` messages at a time with `consume_message`, which marks them as processing, increments their delivery attempt count, and gives each claim a new consumer token and a visibility timeout (`vt`). Until the message is settled or its lease expires, no other consumer can claim it. The consumer is woken by `NOTIFY` when messages are published to the queue's topic or returned to the queue, and polls as a fallback. After an empty or partial claim it asks the database how long until the next row becomes visible and waits that long, at most `pollingIntervalMs`; if a row is already due but could not be claimed, it waits 100 ms. A failed fetch is logged and retried after one second. If the `LISTEN` session drops, the connection reconnects with a backoff that starts at 500 ms and doubles up to 30 s, and resets once a new session is listening. Notifications sent while the session is down are not replayed; consumers pick those messages up on their next poll.

### Iterator consumer

```typescript
const consumer = client.consume('order-processing', {
  batchSize: 10,
  visibilityTimeoutSec: 60,
  autoExtension: { enabled: true, extensionThreshold: 0.5 },
});

for await (const message of consumer.messages()) {
  console.log(message.id, message.deliveryAttempts, message.payload);
  try {
    await handleOrder(message.payload);
    await message.ack();
  } catch (error) {
    await message.nack({ delaySeconds: 30 });
  }
}
```

`consume()` returns a `Consumer` that starts fetching when you call `messages()`. `messages()` throws an `Error` if the connection does not know the queue's topic (see the `topic` option below). The iterator ends when the consumer stops, either because you called `consumer.stop()` or because its queue became unusable. Leaving the `for await` loop early (`break`, `return`, `throw`) also stops the consumer.

### Handler consumer

`consumeHandler(queue, handler, options?)` dispatches each message to `handler` and starts immediately. `maxInFlight` caps how many handlers run at once (`0`, the default, means unlimited). The consumer still claims (and leases) up to `batchSize` messages per fetch; `maxInFlight` only gates handing them to handlers, so a claimed message may wait for a slot while its lease is renewed. Keep `batchSize` at or below `maxInFlight`.

```typescript
const handlerConsumer = client.consumeHandler(
  'order-processing',
  async (message) => {
    // message.signal aborts on shutdown or when the lease is lost.
    await handleOrder(message.payload, message.signal);
    // Returning without settling auto-acks; throwing auto-nacks.
  },
  { maxInFlight: 10, visibilityTimeoutSec: 60 },
);

// Later
await handlerConsumer.stop();
```

Settlement rules for a handler consumer:

| Handler outcome | Result |
|---|---|
| Returns without settling, signal not aborted | Auto-ack |
| Returns without settling after `message.signal` aborted | Auto-nack |
| Throws | Auto-nack (unless the handler already settled the message) |
| Calls `ack`/`nack`/`release`/`ackWithTransaction` itself | No automatic action |

Auto-settlement failures are logged, not thrown.

### Consumer options

| Option | Default | Description |
|---|---|---|
| `batchSize` | `10` | Maximum messages claimed per fetch, and the size of the local buffer. |
| `visibilityTimeoutSec` | `30` | Lease length in seconds, used when claiming and for each automatic renewal. `0` means the default. |
| `autoExtension.enabled` | `true` | Renew leases of claimed, unsettled messages automatically. |
| `autoExtension.extensionThreshold` | `0.5` | Fraction of the remaining lease that elapses before renewal. Must be between 0 and 1, exclusive. |
| `pollingIntervalMs` | `1000` | Longest wait before the next fetch after a claim that returned fewer than `batchSize` messages, when no notification arrives. |
| `topic` | — | The queue's topic. Required when the queue was not created with `createQueue()` on this connection; otherwise `consumeHandler()`, or `messages()` for an iterator consumer, throws an `Error` asking for it. |
| `maxInFlight` | `0` | `consumeHandler` only. Maximum concurrent handler calls; `0` is unlimited. |

`autoExtension.extensionSec` and `autoExtension.maxBatchSize` are accepted but ignored: renewals always extend by `visibilityTimeoutSec`, and the batch size is the connection's `extenderBatchSize`. Invalid consumer options throw a `ValidationError` from `consume()`/`consumeHandler()`; an invalid `maxInFlight` throws a plain `Error`.

### Messages

| Member | Description |
|---|---|
| `id` | Message ID (number). The same ID appears in every queue on the topic. |
| `queueName` | Queue the message was claimed from. |
| `payload` | Parsed JSON payload. |
| `deliveryAttempts` | Attempts so far, including this one. |
| `publishedAt` | Publish time. |
| `groupKey` / `groupSeq` | Group key and its 1-based position in the group, or `null` for an ungrouped message. |
| `consumerToken` | Token identifying this claim. |
| `vt` | Current lease deadline (`Date`). Updated by renewals and `setVt()`. |
| `signal` | `AbortSignal` that aborts when the lease is lost or the consumer stops. |
| `isSettled` | `true` once a settlement has been attempted. |
| `ack()` | Mark the message completed. |
| `ackWithTransaction(tx)` | Ack inside your transaction; the ack commits or rolls back with it. |
| `nack({ delaySeconds }?)` | Return the message for another attempt, optionally after a delay (added to the database's clock, so the clocks need not agree). On the final attempt of a queue with `maxDeliveryAttempts` set, the message moves to the dead letter queue instead. |
| `release()` | Return the message without counting the attempt (the attempt count is decremented). |
| `setVt(seconds)` | Set the lease to `seconds` from now; resolves with the new deadline. |

A message can be settled once. After the first `ack`, `nack`, `release` or `ackWithTransaction` call, whether it succeeded or failed, any later settlement or `setVt` call rejects with a plain `Error` ("Message *id* has already been processed") without touching the database. Settling a message whose claim is no longer current (another consumer has claimed it, or it is no longer processing) throws `LeaseLostError`. An ack after the lease deadline still succeeds if no other consumer has claimed the message yet; `setVt` after the deadline throws `LeaseLostError` and aborts `message.signal`.

`ackWithTransaction` stops automatic renewal for that message once its statement completes, before your transaction commits. If your transaction rolls back, the message is redelivered after its lease expires.

### Lease renewal

With auto-extension enabled, each message is registered with a single renewal scheduler owned by the connection as soon as it is claimed, including messages still waiting in the consumer's buffer. When a message's renewal is due (`extensionThreshold` of its remaining lease has elapsed), the scheduler renews it to `visibilityTimeoutSec` from now. The messages due at a tick, across all consumers and queues of the connection, are renewed in one `set_vt_batch_multi` call of at most `extenderBatchSize` messages. Each call is a single attempt bounded by the earliest lease deadline among them, but by no less than one second and no more than five; the connection's retry policy is not applied. A message leaves the schedule when its settlement call returns, successfully or not. A lease's deadline is the local time read before the claim or renewal call plus the time left that the database reports, so renewal does not depend on the client and database clocks agreeing; `message.vt` is this deadline, on the local clock.

If a message is missing from the result (its claim is no longer current or its lease has already expired), the scheduler drops it and aborts `message.signal`. A message whose row is locked by another transaction is retried after 100 ms. If the call fails, the due messages are retried about one second later until their last confirmed lease deadline passes on the local clock; after that they are treated as lost the same way. Because a message stays scheduled until its settlement call returns, a renewal that overlaps a successful settlement can also abort its signal. Check `message.signal.aborted` (or listen for `abort`) in long-running work, since settling a lost message throws `LeaseLostError`.

`setVt()` is independent of auto-extension. It does not change the scheduler's timing, and the scheduler's next renewal sets the lease to `visibilityTimeoutSec` from now, which can shorten a longer manual extension. If you use both, keeping them consistent is your responsibility; to manage leases entirely yourself, set `autoExtension: { enabled: false }`.

## Message groups

Pass `groupKey` (1 to 255 characters) to publish a message into a group:

```typescript
await client.publish('session-events', { kind: 'clicked' }, { groupKey: 'session-42' });
```

Within one queue, a group's messages are delivered one at a time in publish commit order: a message is not claimed while an earlier message of its group is pending or being processed. Consumed messages expose `message.groupKey` and `message.groupSeq`, and `listMessages()`/`getMessage()` return them too.

- Head-of-line blocking is intended. A head message that is leased, or nacked with a delay, blocks its group until it is settled or visible again. A head that reaches its final delivery attempt moves to the dead letter queue, which unblocks the group. With `maxDeliveryAttempts: 0` a head that always fails blocks its group indefinitely.
- One consume batch holds at most one message per group.
- Each queue on a topic orders its own copy independently. There is no ordering across queues.
- Publishers of the same group are serialized by the database until their transactions commit, so use groups such as a session, an order or an account, not a hot key shared by unrelated work. A transaction that publishes to several groups should take them in a consistent order to avoid deadlocks.
- An empty `groupKey` is rejected with `ValidationError`.

See [Message groups](https://github.com/postgremq/postgremq/blob/main/mq/README.md#message-groups) in the SQL reference for the full contract.

## Exclusive queues and keep-alive

An exclusive queue expires unless its keep-alive is renewed. Use it for temporary subscriptions, such as one queue per process instance.

```typescript
// Expires 60 seconds after its last keep-alive renewal.
await client.createQueue('instance-7-events', 'orders', true, {
  keepAliveInterval: 60, // seconds; default 300
});
```

- The connection that calls `createQueue(..., true, ...)` keeps the queue alive in the background, renewing it roughly every half interval. One scheduler per connection renews all of its exclusive queues in a single `extend_queue_keep_alive_multi` call per tick. Renewal continues while consumers drain during `close()`.
- Consuming does not renew the keep-alive. Another process that uses the queue should call `createQueue` with the same parameters so its connection renews it too. "Exclusive" means expiring, not restricted to one connection.
- Re-declaring a queue with different parameters throws `ValidationError`.
- Deleting the queue with `deleteQueue()` on this connection stops its keep-alive.
- Once expired, the queue receives no new publications and cannot be consumed. It cannot be revived: `createQueue` throws `QueueNotFoundError` until you `deleteQueue()` it and create it again. Expired queues are physically removed by `maintenanceFast()` or `deleteInactiveQueues()`.
- Each keep-alive call is a single attempt bounded by the earliest confirmed keep-alive deadline among its queues, but by no less than one second and no more than five; the connection's retry policy is not applied. A failed call, or a queue row locked by another transaction, is retried after 100 ms until the last confirmed keep-alive deadline passes on the local clock.
- If the queue is missing from the result (it is gone, has expired, or was replaced by a new queue of the same name), or retries run past that deadline, the connection stops renewing it and treats the queue as fatal (see below).

## Queue-fatal teardown

When a queue that a consumer depends on is gone, the consumer cannot recover. The connection detects this when a consumer's fetch fails with `PMQ02` (the queue was deleted, has expired, or was replaced by a new queue of the same name), or when an exclusive queue's keep-alive fails as described above. A consumer binds to one queue incarnation (name and generation) on its first fetch, taking the generation from the connection's cache (filled by `createQueue()` or an earlier lookup) or looking it up; if the lookup finds no queue, that is handled the same way. The connection then:

- stops keeping the queue alive;
- stops every consumer of that incarnation on this connection through the normal `stop()` path (aborting in-flight message signals, releasing buffered messages, ending iterators), and calls each consumer's `onClose` listeners with a `QueueFatalError` once it has drained. When the lost generation is not known (the first-fetch lookup found no queue), every consumer of the queue name is stopped;
- emits `'queueFatal'` and calls the `onQueueFatal` option synchronously with the same `QueueFatalError`, right after starting the teardown. Without `onQueueFatal`, the error is also logged with `console.error`.

The error's `cause` is the `QueueNotFoundError` from the fetch or lookup, or `undefined` when keep-alive failed. A queue name is reported at most once per connection, until `createQueue()` for that name succeeds on the connection again. A report for a generation other than the one the connection has cached for the name is ignored. The connection keeps the cached topic and generation after a report, so call `createQueue()` before consuming the name again; until then, a new consumer of the name binds to the lost generation, and its `PMQ02` is not reported, so it stays open without receiving messages.

`deleteQueue()` stops this connection's keep-alive for the queue and forgets its cached topic and generation. It does not stop consumers or signal by itself: consumers of the deleted queue, on this connection or others, are closed when their next fetch fails with `PMQ02`, and are reported as above.

```typescript
import { connect, QueueFatalError } from 'postgremq';

const client = await connect({
  connectionString: process.env.DATABASE_URL,
  onQueueFatal: (queue, error) => console.error(`queue ${queue} is gone`, error),
});

client.on('queueFatal', (queue, error) => {
  // Same signal as onQueueFatal. It is the only signal for an exclusive
  // queue that this connection publishes to but does not consume.
});

const consumer = client.consume('instance-7-events', { topic: 'orders' });
consumer.onClose((err) => {
  if (err instanceof QueueFatalError) {
    console.error(`consumer for ${err.queue} closed`, err.cause);
  }
  // No argument means a normal stop().
});
```

`HandlerConsumer` has the same `onClose(listener)` method; it is the main way for a handler consumer to learn that its queue is gone. The close reason is the `QueueFatalError` if the queue is reported lost before the consumer finishes closing, even if `stop()` had already begun; otherwise listeners are called with no argument. Every listener sees the same reason, and a listener registered after the consumer has closed is called on the next microtask. Consumers are bound to the queue incarnation they started on, so a queue recreated under the same name needs a new consumer.

`on` and `off` are methods of the connection object `connect()` returns; the exported `Connection` interface does not declare them.

## Shutdown

- `consumer.stop()` stops fetching, aborts the signal of every delivered message, releases buffered messages that were never delivered, and waits for delivered messages to be settled, for up to `shutdownTimeoutMs`. Settle the current message before breaking out of a `for await` loop; otherwise `stop()` waits for the timeout.
- `handlerConsumer.stop()` does the same and waits for running handlers to return and be auto-settled.
- Do not `await client.close()` (or `stop()`) inside a handler, or in a `for await` loop while holding an unsettled message: it waits for that message, so it waits on itself until `shutdownTimeoutMs` passes. Start it without awaiting (`void client.close()`) or from outside the handler.
- `client.close()` stops all consumers concurrently while lease renewal and keep-alive keep running; then it stops the background schedulers and the `LISTEN` session, and ends the pool if the connection created it. The whole sequence is bounded by `shutdownTimeoutMs`: database calls started during `close()` have their deadline capped at that point, and calls still in flight when it passes, or when `close()` reaches its final step, are aborted with a "Database operation deadline exceeded" error.
- Once `close()` begins, `publish`, `publishWithTransaction`, `consume`, `consumeHandler`, `createTopic` and `createQueue` throw `ConnectionClosedError`. Settlement, `setVt`, and the administration, inspection, dead-letter and maintenance methods keep working while consumers drain; once the drain has finished they fail with `ConnectionClosedError` (settlement and `setVt` wrap it, see [Errors](#errors)). `close()` is idempotent, and a closed connection cannot be reconnected; `connect()` on it throws `ConnectionClosedError`.

Work still running at the deadline is abandoned without reducing its delivery attempt count; its lease expires normally and the message is redelivered.

## Retries

Transient errors are retried with exponential backoff according to `retry`:

| Field | Default | Meaning |
|---|---|---|
| `maxAttempts` | `5` | Total attempts, including the first. |
| `initialBackoffMs` | `100` | Delay before the second attempt. |
| `backoffMultiplier` | `2` | Factor applied to the delay after each retry. |
| `maxBackoffMs` | `5000` | Upper bound on the delay. |

- Settlement (`ack`, `nack`, `release`), `setVt`, `createTopic`, `createQueue`, and all administration, inspection, dead-letter and maintenance methods (including the deleting and purging ones) retry on connection errors (`ECONNRESET`, `ETIMEDOUT`, `EPIPE`, `ECONNREFUSED`, `ENOTFOUND`, `EAI_AGAIN`, SQLSTATE class `08`), serialization failures and deadlocks (`40001`, `40P01`), lock contention (`55P03`) and server shutdown (`57P01`–`57P03`).
- `publish()` retries only `40001` and `40P01`, which guarantee the transaction aborted. Any other failure, such as a dropped connection, is returned to you because the publish may have committed.
- Fetching (`consume_message`) is never retried, because a lost response may already have claimed rows. The consumer tries again on its next fetch.
- Lease renewal and keep-alive do not use the retry policy; they make one attempt per tick and reschedule as described in [Lease renewal](#lease-renewal) and [Exclusive queues and keep-alive](#exclusive-queues-and-keep-alive).
- `publishWithTransaction` and `ackWithTransaction` are never retried; the transaction belongs to you.

Every pooled database call the client makes, other than the `LISTEN` session, has a deadline per attempt: 30 seconds for most operations, the larger of one second and half of `visibilityTimeoutSec` for a fetch, and between one and five seconds for renewal and keep-alive (the earliest confirmed deadline in the batch, within those bounds). On timeout the pool client is destroyed rather than returned to the pool.

## Errors

| Error | `code` | Thrown when |
|---|---|---|
| `LeaseLostError` | `PMQ01` | `ack`, `nack`, `release`, `ackWithTransaction` or `setVt` on a claim that is no longer current, or `setVt` after the lease expired. |
| `QueueNotFoundError` | `PMQ02` | A topic or queue does not exist or has expired. When a consumer's fetch hits it, it becomes the `cause` of a `QueueFatalError`. |
| `ValidationError` | `PMQ03` | Invalid input or a rejected precondition: invalid or over-long names (names must match `[A-Za-z0-9_:.-]+`, at most 57 bytes), an empty group key, re-declaring a queue with different parameters, deleting a topic that still has messages, or invalid consumer options. |
| `ConnectionClosedError` | — | `publish`, `publishWithTransaction`, `consume`, `consumeHandler`, `createTopic` or `createQueue` after `close()` began; any other operation after `close()` finished draining; or `connect()` after `close()`. |
| `QueueFatalError` | — | Not thrown. Passed to `onClose`, `'queueFatal'` and `onQueueFatal`; has `queue` and `cause`. |

The codes are exported as `ErrCodeLeaseLost`, `ErrCodeQueueNotFound` and `ErrCodeValidation`.

```typescript
import { LeaseLostError } from 'postgremq';

try {
  await message.ack();
} catch (err) {
  if (err instanceof LeaseLostError) {
    // Another consumer may process this message; keep side effects idempotent.
  } else {
    throw err;
  }
}
```

Publishing, fetching, settlement, `setVt`, `createTopic`, `createQueue` and `deleteTopic` map these SQLSTATEs to the typed errors above. The other administration, inspection, dead-letter and maintenance methods, including `deleteQueue`, reject with the underlying `pg` error, whose `code` holds the SQLSTATE (for example, `deleteQueue` rejects with `code === 'PMQ03'` when it refuses a queue with dead-lettered messages). Invalid connection or queue options passed to `connect()` or `createQueue()` throw a plain `Error`.

Message methods (`ack`, `nack`, `release`, `ackWithTransaction`, `setVt`) pass `LeaseLostError`, `QueueNotFoundError` and `ValidationError` through unchanged and wrap any other failure, including `ConnectionClosedError`, in a plain `Error` whose message names the operation (for example, "Failed to acknowledge message: Client is not connected").

## Administration and maintenance

```typescript
// Topics and queues
await client.createTopic('orders');
await client.createQueue('order-processing', 'orders', false, { maxDeliveryAttempts: 5 });
const topics = await client.listTopics();          // string[]
const queues = await client.listQueues();          // QueueInfo[]
await client.deleteQueue('order-processing');      // refused while it has DLQ entries
await client.deleteTopic('orders');                // refused while it has messages

// Inspection
const stats = await client.getQueueStatistics('order-processing'); // omit the name for all queues
console.log(stats.pendingCount, stats.processingCount, stats.completedCount, stats.totalCount);
const rows = await client.listMessages('order-processing');        // QueueMessage[]
const published = await client.getMessage(123);                    // PublishedMessage | null

// Dead letter queue
const dead = await client.listDLQMessages();        // DLQMessage[]
await client.requeueDLQMessages('order-processing'); // reset attempts and make them available again
await client.purgeDLQ();

// Destructive purges (these can remove active work)
await client.deleteQueueMessage('order-processing', 123);
await client.cleanUpQueue('order-processing');      // delete all of the queue's deliveries
await client.cleanUpTopic('orders');                // delete all of the topic's messages
await client.purgeAllMessages();
```

The database does not schedule its own maintenance. Run these from a scheduler in one place, for example a single worker or a cron job:

```typescript
// Every 1–10 seconds (at least once a minute): move final-attempt messages whose consumer crashed to the
// dead letter queue, and remove expired exclusive queues.
const { retiredToDlq, inactiveQueuesDropped } = await client.maintenanceFast();

// Every ~10 seconds: delete completed deliveries older than 24 hours (the
// default) and payloads no queue or DLQ entry references. Each call removes
// at most 1000 rows of each kind; returns the number of deliveries deleted.
const deleted = await client.cleanupCompletedMessages(24);
```

`deleteInactiveQueues()` removes expired exclusive queues on its own. See [Maintenance and retention](https://github.com/postgremq/postgremq/blob/main/mq/README.md#maintenance-and-retention) for sizing guidance.

## Observability

Pass an OpenTelemetry `MeterProvider` to record client metrics: operation durations, handler durations and active handler count (for `consumeHandler`), sent and consumed messages, and leases lost by the renewal scheduler.

```typescript
import { metrics } from '@opentelemetry/api';
import { connect } from 'postgremq';

const client = await connect({
  connectionString: process.env.DATABASE_URL,
  meterProvider: metrics.getMeterProvider(),
});
```

Metrics are disabled when `meterProvider` is omitted, even if a global provider is registered. Your application owns the SDK and exporters; close the connection before flushing and shutting down the provider. Queue-state metrics come from the SQL function `postgremq.queue_metrics()`. See the [observability guide](https://github.com/postgremq/postgremq/blob/main/docs/observability.md) for metric definitions and Collector setup, and the [TypeScript example](https://github.com/postgremq/postgremq/blob/main/postgremq-ts/examples/metrics.ts) for a complete SDK configuration.

## Delivery guarantees

Delivery is at least once. A message can be processed more than once (after a lease expires, after a crash, or when a publish outcome was ambiguous and the application retried), so make side effects idempotent, for example with application idempotency keys. See the [delivery lifecycle](https://github.com/postgremq/postgremq/blob/main/docs/delivery-lifecycle.md) for the ownership, cancellation and shutdown contract shared by all clients.

## License

MIT
