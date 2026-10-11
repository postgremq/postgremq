# PostgreMQ SQL

This directory holds the PostgreMQ database layer: the schema and the SQL functions
that implement publishing, fan-out, leasing, settlement, dead-lettering, message
groups and maintenance. The Go, TypeScript and Rust clients are thin wrappers over
these functions. This document is the reference and delivery contract for that
layer.

**Requirement:** PostgreSQL 15 or later.

- Payloads (`JSONB`) are stored once in `postgremq.messages`. Each queue's
  delivery state lives in `postgremq.queue_messages`.
- Topics and queues are identified by name. Message IDs are `BIGINT`.
- Every queue row has a UUID **generation**. Every lease has a UUID **delivery
  token**.

## Installation

The installation creates the `postgremq` schema and everything inside it. Install
into a database that does not already contain a PostgreMQ installation.
Application data may already exist in other schemas.

`sql/latest.sql` is for fresh installs only. It refuses to run, before changing
anything, when the migrations' version table `postgremq.postgremq_migrations`
already exists: upgrade an existing installation with the migrations instead.
It ends by creating that table and recording the migration version it is
equivalent to, so the migrators upgrade a database installed from it like one
they installed. Run it with `ON_ERROR_STOP` (as below), so psql stops at the
first error instead of running the rest of the file.

Each mq release (`mq/vN`, N the last migration) attaches the files to its
[GitHub release](https://github.com/postgremq/postgremq/releases):
`postgremq-vN.sql` (`latest.sql`) and `postgremq-migrations-vN.tar.gz`.

**psql**

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f mq/sql/latest.sql
```

**Migrations.** `mq/migrations/` uses the golang-migrate file layout. The
[PostgreMQ CLI](../postgremq-go/cmd/postgremq/README.md) (`postgremq migrate --dsn …`) and
every client apply the files: Go `postgremq.Migrate(pool)`, TypeScript
`migrate(pool)` and Rust `postgremq::migrate(&pool)`. They share one protocol,
golang-migrate's: the version table `postgremq.postgremq_migrations` (one row,
`version` and `dirty`), the same advisory lock, and a `dirty` flag set while a
migration runs. Any of them can therefore upgrade a database another one
installed. Migrations only go up, to the latest version the client embeds; a
database already at a newer version is left unchanged, and a dirty one is an
error. `GetMigrationStatus` / `getMigrationStatus` / `migration_status` and
`postgremq status` only read, and work before installation too. To use the
golang-migrate CLI directly, create the schema first and point it at the same
version table:

```sh
psql "$DATABASE_URL" -c 'CREATE SCHEMA IF NOT EXISTS postgremq'
migrate -path mq/migrations \
  -database 'postgres://user:pass@host:5432/db?x-migrations-table=%22postgremq%22.%22postgremq_migrations%22&x-migrations-table-quoted=1' \
  up
```

Down migrations are not supported. To remove an installation and all of its queue
data, run `DROP SCHEMA postgremq CASCADE`.

Every client embeds all of these migrations: at any commit the Go client's
copy (`postgremq-go/migrations/`) and the Rust and TypeScript pins point at
the last one (`make sync`, checked by CI).

## Schema, roles and transactions

Every table, sequence, index, trigger and function belongs to the `postgremq`
schema. All object references are schema-qualified, so neither installation nor
runtime depends on `search_path`. All functions except `info()` are `SECURITY INVOKER`: they run
with the privileges of the caller.

A runtime role other than the installer needs:

```sql
GRANT USAGE ON SCHEMA postgremq TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    postgremq.topics, postgremq.queues, postgremq.messages,
    postgremq.message_groups, postgremq.queue_messages,
    postgremq.dead_letter_queue
  TO app_role;
GRANT USAGE ON SEQUENCE postgremq.messages_id_seq TO app_role;
```

Functions are executable by `PUBLIC` by default. If you revoke that, also grant
`EXECUTE ON ALL FUNCTIONS IN SCHEMA postgremq`. For a read-only metrics role, see
[Metrics](#metrics).

Queue calls take part in the caller's transaction. Put application writes and
queue calls in the same transaction, and they commit or roll back together:

```sql
BEGIN;
UPDATE app.orders SET status = 'paid' WHERE id = 123;
SELECT postgremq.publish_message('order_paid', '{"order_id":123}'::jsonb);
COMMIT;
```

The clients expose transaction-scoped variants that run these calls on a
transaction you supply:

| Client | Publish | Ack |
|---|---|---|
| Go | `PublishWithTx` | `AckWithTx` |
| TypeScript | `publishWithTransaction` | `ackWithTransaction` |
| Rust | `publish_tx` | `ack_tx` |

Row locks taken by a queue call are held until the caller's transaction ends.
Keep these transactions short. In particular, a publish to a message group
holds the group's lock until commit (see [Message groups](#message-groups)), and
a publish makes keep-alive of the exclusive queues it delivers to report `busy`
until commit (see [Exclusive queues and keep-alive](#exclusive-queues-and-keep-alive)).

## Data model

| Table | Columns | Notes |
|---|---|---|
| `topics` | `name VARCHAR(255)` PK | Root of all data. |
| `queues` | `name VARCHAR(255)` PK, `generation UUID` (default `gen_random_uuid()`), `topic_name` → `topics`, `max_delivery_attempts INT` (default 0 = unlimited), `exclusive BOOLEAN` (default false), `keep_alive_interval INTERVAL` (default 5 minutes), `keep_alive_until TIMESTAMPTZ` | `keep_alive_until` is NULL for non-exclusive queues. |
| `messages` | `id BIGSERIAL` PK, `topic_name` → `topics`, `payload JSONB NOT NULL`, `published_at`, `deliver_after`, `group_key VARCHAR(255)`, `group_seq BIGINT` | `group_key` and `group_seq` are both NULL or both set. |
| `message_groups` | (`topic_name` → `topics`, `group_key`) PK, `last_seq BIGINT` | Per-group sequence allocator. |
| `queue_messages` | (`queue_name` → `queues`, `message_id` → `messages`) PK, `status`, `published_at`, `vt TIMESTAMPTZ NOT NULL`, `delivery_attempts INT` (≥ 0), `consumer_token VARCHAR(64)`, `processed_at`, `group_key`, `group_seq` | One row per (queue, message). Status is `pending`, `processing` or `completed` (CHECK). `published_at` is when the row was created: at distribution, or at DLQ requeue. |
| `dead_letter_queue` | (`queue_name` → `queues`, `message_id` → `messages`) PK, `retry_count INT`, `published_at` | `retry_count` is the delivery attempt count at retirement. `published_at` is when the entry entered the DLQ. |

**Deletion rules.** Deleting a topic cascades to its queues, messages and
`message_groups` rows. Deleting a queue or a message cascades to its
`queue_messages` rows. The DLQ's foreign keys are `ON DELETE RESTRICT`: a queue or
payload referenced by a DLQ entry cannot be deleted until that entry is requeued
or purged.

**Trigger.** `after_message_insert` (`AFTER INSERT ON messages FOR EACH ROW`)
runs `distribute_message()`. It inserts one `queue_messages` row per live queue
on the topic, with `vt = deliver_after` and the group columns copied. It then
emits `NOTIFY pmq:t:<topic>`.

**Indexes.**

| Index | Definition | Serves |
|---|---|---|
| `idx_queue_messages_consume` | `queue_messages(queue_name, vt, published_at) WHERE status IN ('pending','processing')` | Claim scan in `vt` order |
| `idx_queue_messages_next_visible` | `queue_messages(queue_name, vt) WHERE status IN ('pending','processing')` | `get_next_visible_time` |
| `idx_queue_messages_group_head` | `queue_messages(queue_name, group_key, group_seq) WHERE status IN ('pending','processing') AND group_key IS NOT NULL` | Group-head check |
| `idx_queue_messages_pending` / `_processing` | `queue_messages(queue_name) WHERE status = …` | `get_queue_statistics` |
| `idx_queue_messages_completed_processed_at` | `queue_messages(processed_at) WHERE status = 'completed'` | Completed-row retention |
| `idx_queue_messages_message_id` | `queue_messages(message_id)` | Payload reference checks |
| `idx_dlq_message_id` | `dead_letter_queue(message_id)` | Payload reference checks |
| `idx_messages_retention` | `messages(published_at, id)` | Payload retention |
| `idx_messages_topic_name` | `messages(topic_name)` | Topic cleanup and cascades |
| `idx_messages_group` | `messages(topic_name, group_key) WHERE group_key IS NOT NULL` | Group-row pruning |
| `idx_queues_topic_name` | `queues(topic_name)` | Fan-out |

## Delivery contract

All lease and expiry checks use `clock_timestamp()`: the wall clock when the
statement runs, not when the transaction started.

### Topics, queues and publishing

Topic and queue names must match `^[A-Za-z0-9_:.\-]+$` and be at most 57 bytes.
The limit exists because PostgreSQL truncates NOTIFY channel names at 63 bytes, and
the `pmq:t:`/`pmq:q:` prefixes take 6 of them. `create_topic` is idempotent.

`create_queue(name, topic, max_attempts, exclusive, keep_alive_interval)`
subscribes a queue to a topic and returns its generation.

- The topic must already exist (`PMQ02` otherwise).
- `max_attempts` must be ≥ 0, where 0 means unlimited. `keep_alive_interval` must
  be positive, for non-exclusive queues too.
- Redeclaring a queue with identical parameters is idempotent and returns the
  existing generation. Redeclaring it with different parameters raises `PMQ03`,
  and the error message includes the existing values.
- A redeclaration locks the queue row `FOR UPDATE` until its transaction ends.
- A redeclaration that meets an uncommitted deletion of the queue waits for it;
  if the deletion commits, the queue is created anew with a new generation. If
  the deletion commits after the redeclaration has found the existing row, the
  call raises `PMQ03` and reports the existing values as NULL.
- The topic check takes no lock. If the topic is deleted after the check but
  before the insert, the insert fails with `23503`. Once the queue row is
  inserted or locked, a concurrent topic deletion waits until the transaction
  ends.

`publish_message(topic, payload, deliver_after, group_key)` inserts the payload
and returns its ID. The trigger then distributes it, in the same transaction, to
every **live** queue on the topic. A live queue is a non-exclusive queue, or an
exclusive queue whose `keep_alive_until` is still in the future.

- A queue receives only messages published after it was created.
- A publication with no live queues stores a payload with no delivery rows. Payload
  retention later collects it.
- `deliver_after` (default: now; NULL also means now) becomes the first `vt` of
  every delivery row. No consumer can claim the message before then.

If a publish returns an error, that does not prove the publish failed: the commit
outcome can be ambiguous after a connection failure. The clients retry their own
non-transactional publish automatically only on `40001` and `40P01`, because those
SQLSTATEs guarantee the transaction aborted.

### Queue generations

Each queue row gets a new random `generation` when it is created. Deleting a queue
and creating it again under the same name produces a new generation. Pass the
generation to `consume_message` and `extend_queue_keep_alive_multi` to bind them to
one incarnation of the queue. With a generation that no longer matches, a
consumer gets `PMQ02`, and a keep-alive is omitted from the result. Pass NULL only
when you deliberately target whichever incarnation currently exists.

### Exclusive queues and keep-alive

An exclusive queue expires unless its lease is renewed. "Exclusive" describes
this lifetime rule. It does not restrict the queue to one connection or consumer.

- Creation sets `keep_alive_until = now + keep_alive_interval`.
- Two things renew the lease:
  - `extend_queue_keep_alive_multi`, which sets `now + interval_ms` for each queue
    in the request.
  - Redeclaring a live queue with `create_queue`, which sets
    `now + keep_alive_interval`.

  Consuming, publishing and settling never renew it.
- The queue expires the moment `keep_alive_until` is reached. From then on, the
  same boundary applies everywhere:
  - it receives no new publications;
  - `consume_message` raises `PMQ02`;
  - keep-alive omits it from the result;
  - `create_queue` raises `PMQ02`. An expired queue cannot be revived: delete it
    and create a new generation.
- Physical deletion happens later, through `pmq_maintenance_fast` (or
  `delete_inactive_queues`). Reaping skips queues that still have DLQ entries.
  Reaping never moves the expiry boundary.

`extend_queue_keep_alive_multi` locks each queue row `FOR UPDATE NOWAIT`. If any
other transaction holds a lock on the row, the queue is reported `busy` and the
lease is unchanged; the clients retry until the last confirmed deadline and treat
the queue as lost after it. Keep-alive reports `busy` until the other transaction
ends when that transaction has:

- published to the queue's topic (distribution inserts the queue's delivery
  row, and the foreign-key check holds a `KEY SHARE` lock on the queue row);
- nacked a message of the queue (`nack_message` locks the queue row
  `FOR SHARE`);
- requeued DLQ entries of the queue, or retired its rows to the DLQ in
  `pmq_maintenance_fast` (foreign-key `KEY SHARE` lock);
- redeclared the queue with `create_queue`;
- run another keep-alive of the same queue;
- deleted the queue (`delete_queue`, maintenance reaping, a topic deletion);
- locked the row explicitly (`SELECT … FOR SHARE`, `FOR UPDATE`, …).

Consuming, acking, releasing and extending leases take no lock on the queue row
and do not conflict. Conversely, while a keep-alive call's transaction is open,
publishes to the topic, nacks and redeclarations of the queue wait for it.

### Claiming (visibility timeout)

`consume_message(queue, vt_seconds, batch_size, generation)` claims up to
`batch_size` rows in one statement, using `FOR UPDATE SKIP LOCKED`.

- A row is claimable when:
  - its status is `pending`, or it is `processing` with an expired lease;
  - its `vt` ≤ now;
  - it is under the attempt limit (`max_delivery_attempts = 0` or
    `delivery_attempts < max_delivery_attempts`);
  - it is its group's head (see [Message groups](#message-groups)).
- Rows are taken in `vt` order. Fresh messages therefore come out in
  delivery-time order. A redelivered message is ordered by the `vt` that nack or
  release gave it, not by its original publish time. Ungrouped messages have no
  strict FIFO guarantee.
- Each claimed row is set to `status = 'processing'` and
  `vt = now + vt_seconds`. Its `delivery_attempts` increases by 1, and it gets a
  new **delivery token**, `consumer_token = gen_random_uuid()::text`.
- The result columns are `queue_name`, `message_id`, `payload`, `consumer_token`,
  `delivery_attempts`, `vt`, `published_at`, `group_key` and `group_seq`.
- If the queue is missing, expired or of a different generation, the call raises
  `PMQ02`. A queue that exists but is empty returns zero rows.
- A negative `vt_seconds` or a `batch_size` ≤ 0 raises `PMQ03`. NULL is not
  rejected: a NULL `batch_size` claims every claimable row, and a NULL
  `vt_seconds` fails with `23502` as soon as a row is claimed.

### Delivery tokens and settlement

Each claim issues a new token. When a lease expires and another consumer claims the
row, the old token stops working. Every settle or extend call carries the token
and raises `PMQ01` if the row is not `processing` under that token.

| Function | Effect |
|---|---|
| `ack_message(queue, id, token)` | Sets status to `completed`, sets `processed_at`, and clears the token. |
| `nack_message(queue, id, token, delay_until DEFAULT now)` | Not the final attempt: sets status back to `pending` with `vt = delay_until`. The attempt still counts. Final attempt (`max_delivery_attempts > 0` and attempts ≥ max): moves the row to the DLQ. Locks the queue row `FOR SHARE` until the transaction ends. An explicit NULL `delay_until` fails with `23502`. |
| `release_message(queue, id, token)` | Sets status back to `pending` with `vt = now` and decrements `delivery_attempts` (floored at 0). Use it for work that was claimed but never attempted. |
| `set_vt(queue, id, token, seconds)` | Sets `vt = now + seconds` and returns the new deadline. Requires an unexpired lease (`PMQ01` otherwise). Raises `55P03` (retryable) rather than waiting if the row is locked. |
| `set_vt_batch_multi(queues[], ids[], tokens[], seconds[])` | Batched, cross-queue form of `set_vt`; see [Heartbeat batches](#heartbeat-batches). |

Ack, nack and release do not check the lease deadline. They succeed after `vt` has
passed as long as no other consumer has claimed the row. Only extension requires a
lease that is still live.

### Delivery attempts and the DLQ

- `delivery_attempts` counts claims. Release takes one back. A DLQ requeue resets
  it to 0.
- With `max_delivery_attempts = N > 0`, a row is claimed at most N times. On the
  final attempt there are two outcomes:
  - **Nack:** the row is retired to the DLQ inline.
  - **Abandoned** (the consumer crashed, or the lease expired without settlement):
    the row stays `processing` and is no longer claimable.
    `pmq_maintenance_fast` retires it once its `vt` has passed. It never retires a
    lease that is still live.
- With `max_delivery_attempts = 0`, a message is retried indefinitely and never
  reaches the DLQ.
- Retirement deletes the `queue_messages` row and inserts a
  `dead_letter_queue(queue, message, retry_count)` entry. The DLQ entry keeps the
  payload alive.
- `requeue_dlq_messages(queue)` moves every DLQ entry of that queue back to
  `pending` with `vt = now`, 0 attempts and the original group key and sequence.
- `purge_dlq()` deletes every DLQ entry in every queue. It takes no queue
  argument.

### Guarantees

Delivery is at least once:

- A message can be delivered again after a lease expires, a nack, a release, or a
  DLQ requeue.
- A stale consumer cannot settle a newer lease: its token no longer matches.
- External side effects are not fenced. Make handlers idempotent, for example with
  application idempotency keys.

## Message groups

`publish_message(..., group_key)` puts a message in a group. `group_key` is text
of 1–255 characters. NULL means ungrouped, and ungrouped messages are unaffected
by any of the rules below.

**Promised:**

- **Group order is publish commit order.** For each (topic, group), a publish
  upserts the `message_groups` row and allocates the next dense `group_seq`. It
  holds that row lock until the publishing transaction ends. Publishers of one
  group are therefore serialized, and a lower sequence can never become visible
  after a higher one.
- **Within one queue, a group is claimed in order, one delivery at a time.** A
  grouped row is claimable only when no row of the same (queue, group) with a lower
  `group_seq` is `pending` or `processing`. Rows that are completed, dead-lettered
  or deleted no longer block.
- **A batch claim returns at most one row per group.**
- **Fan-out.** `group_seq` belongs to the topic message and is copied to every
  queue's row. Each queue orders its own copy independently.

**Head-of-line blocking is by design.** A group's head blocks every later row of
that group in that queue while the head is:

- leased;
- nacked with a delay;
- published with a future `deliver_after`;
- waiting for its lease to expire.

A redelivery after lease expiry is the same head again. Under
`max_delivery_attempts > 0`, a poison head reaches the DLQ on its final attempt,
which unblocks the group. Under `max_delivery_attempts = 0`, a head that always
fails blocks its group indefinitely. A consumer that crashes on a final attempt
blocks the group until the lease expires and `pmq_maintenance_fast` retires the
row.

**Not promised:**

- ordering across queues;
- ordering across a DLQ detour: a requeued row becomes its group's head again,
  although later rows may already have run;
- per-group priority;
- exactly-once external effects.

**Operational notes.**

- **Empty keys.** The empty string is rejected (`PMQ03`). Omit the key to publish
  ungrouped.
- **Choosing keys.** Publishers of one group are serialized at the database, so a
  group should be a natural unit such as a session, an order or an account. Never
  use a hot shared key.
- **Deadlocks.** A transaction that publishes to several groups can deadlock
  (`40P01`) with another transaction that takes the same groups in a different
  order. Take groups in a consistent order and retry the whole transaction on
  `40P01`.
- **Serialization failures.** Under `REPEATABLE READ` or `SERIALIZABLE`, a grouped
  publish fails with `40001` if another publisher of the same group committed after
  the transaction's snapshot was taken. Retry the whole transaction.
- **Wake-ups.** Settling or removing an unsettled grouped row emits
  `NOTIFY pmq:q:<queue>`, because the group's successor has just become claimable
  (see [Notifications](#notifications)).
- **Polling.** `get_next_visible_time` applies the same group-head rule, so a
  consumer waiting behind a leased head is scheduled for the head's lease end
  instead of spinning.
- **Claim cost.** The claim scan walks the queue's visible rows in `vt` order and
  checks each grouped row against `idx_queue_messages_group_head` until the batch
  is full. Every visible row queued behind a leased or delayed head is stepped
  over on every claim, and again by `get_next_visible_time` when a client
  schedules its next fetch.
  That costs roughly 5 µs per blocked row: about 50 ms per claim with 10,000 rows
  waiting behind in-flight heads. Do not let a group build a deep backlog behind a
  long-running or repeatedly failing head. Use `max_delivery_attempts > 0` and
  nack delays so a poison head reaches the DLQ.
- **Metrics.** `queue_metrics()` counts a blocked successor as `ready`, because it
  is visible and under its attempt limit.
- **Group rows.** A `message_groups` row lives while its group has any message
  left (anywhere, including the DLQ). The following functions prune it through
  `prune_message_groups` once the group has no messages:
  - `cleanup_unreferenced_messages`, for the groups whose payloads it deleted;
  - `clean_up_topic`;
  - `purge_all_messages`.

  The next publish to a pruned group starts again at `group_seq` 1. Pruning skips
  rows that an in-flight publisher holds. A row left behind that way is harmless:
  the next publish to that group reuses it.

## Versions and discovery

```sql
SELECT postgremq.info();
-- {"schema_version": 7, "protocol_major": 1}
```

`schema_version` is the number of the last migration applied, the exact
schema and function state of the database, read from the version table
`postgremq.postgremq_migrations` that fresh installs (`latest.sql`) and
upgrades (the migrations) both maintain; `null` without that table. A
migration never changes once merged, so equal numbers mean equal schemas. The
mq release `mq/vN` is schema version N. `info()` is
the one `SECURITY DEFINER` function, so runtime roles need no privilege on the
version table to call it. `protocol_major` is
the contract clients rely on: function signatures, result types, error codes,
ownership rules and delivery semantics. Additive changes keep the major;
incompatible changes get a new one. Each client checks it when connecting and
rejects a major it does not implement. `info()` and these two fields stay
stable across protocol majors; fields may be added.

## Function reference

All functions live in the `postgremq` schema. "now" means `clock_timestamp()`.

### Topics and queues

| Function | Returns | Notes |
|---|---|---|
| `create_topic(p_topic)` | `VARCHAR` (the name) | Idempotent. Raises `PMQ03` for an invalid or too-long name. |
| `create_queue(p_queue_name, p_topic_name, p_max_attempts INT DEFAULT 0, p_exclusive BOOL DEFAULT false, p_keep_alive_interval INTERVAL DEFAULT '5 minutes')` | `UUID` (generation) | See [Topics, queues and publishing](#topics-queues-and-publishing). |
| `delete_topic(p_topic)` | `VOID` | Raises `PMQ03` while the topic has any message. Deletes the topic's queues by cascade. |
| `delete_queue(p_queue)` | `VOID` | Raises `PMQ03` while the queue has DLQ entries. Deletes its delivery rows by cascade; payloads become unreferenced. Does nothing if the queue does not exist. |
| `list_topics()` | `topic` | Ordered by name. |
| `list_queues()` | `queue_name, topic_name, max_delivery_attempts, exclusive, keep_alive_until` | Ordered by name. Includes expired, not-yet-reaped queues. |

### Publishing and consuming

| Function | Returns |
|---|---|
| `publish_message(p_topic, p_payload JSONB, p_deliver_after TIMESTAMPTZ DEFAULT now, p_group_key VARCHAR DEFAULT NULL)` | `BIGINT` message ID |
| `consume_message(p_queue_name, p_vt INT, p_limit INT DEFAULT 1, p_generation UUID DEFAULT NULL)` | `queue_name, message_id, payload, consumer_token, delivery_attempts, vt, published_at, group_key, group_seq` |
| `ack_message(p_queue_name, p_message_id, p_consumer_token)` | `VOID` |
| `nack_message(p_queue_name, p_message_id, p_consumer_token, p_delay_until TIMESTAMPTZ DEFAULT now)` | `VOID` |
| `release_message(p_queue_name, p_message_id, p_consumer_token)` | `VOID` |
| `set_vt(p_queue_name, p_message_id, p_consumer_token, p_vt INT)` | `TIMESTAMPTZ` new deadline |
| `get_next_visible_time(p_queue_name)` | `TIMESTAMPTZ`, or NULL |

`get_next_visible_time` returns the smallest `vt` among the queue's unsettled rows
that are under the attempt limit and at the head of their group. The result can
be in the past, meaning work is visible now. It includes the lease end of rows
that are `processing`. It returns NULL for a queue that does not exist, and does
not check whether an exclusive queue has expired.

### Heartbeat batches

| Function | Returns |
|---|---|
| `set_vt_batch_multi(p_queue_names VARCHAR[], p_message_ids BIGINT[], p_consumer_tokens VARCHAR[], p_vts INT[])` | `queue_name, message_id, vt, consumer_token, outcome` |
| `extend_queue_keep_alive_multi(p_queue_names VARCHAR[], p_intervals_ms BIGINT[], p_generations UUID[] DEFAULT NULL)` | `queue_name, keep_alive_until, outcome` |

Both functions take element-wise parallel arrays and process the entries in key
order, each in its own subtransaction. The arrays must have equal lengths, every
`p_vts` element must be non-NULL and ≥ 0, and every `p_intervals_ms` element must
be non-NULL and > 0; any violation raises `PMQ03`. A NULL queue name, message ID or
token element is not rejected: it matches no row, so the entry is omitted.
`p_generations` itself, or any of its elements, may be NULL, meaning whichever
incarnation exists. Each entry produces one of three results:

- **`outcome = 'extended'`:** the entry carries the confirmed new deadline.
- **`outcome = 'busy'`:** the row was locked (`NOWAIT`), so the deadline is NULL
  and the lease is unchanged. Being busy does not mean ownership was lost. Retry
  before the last confirmed deadline.
- **Omitted:** the lease is lost.
  - For messages: the row is gone, not `processing`, under another token, or
    already expired.
  - For queues: the queue is gone, not exclusive, expired, or a different
    generation.

Correlate message results by queue, message ID **and** token. Lock acquisition
never waits, but DDL locks or network failures can still stall a statement, so
the clients also put a time limit on heartbeat I/O.

### Inspection

| Function | Returns |
|---|---|
| `get_queue_statistics(p_queue DEFAULT NULL)` | `pending_count, processing_count, completed_count, total_count` for one queue, or summed over all queues when `p_queue` is NULL. DLQ entries are not counted. |
| `list_messages(p_queue_name)` | `message_id, status, published_at, delivery_attempts, vt, processed_at, group_key, group_seq`, ordered by `published_at`. No payload and no token. |
| `get_message(p_message_id)` | `message_id, topic_name, payload, published_at, group_key, group_seq` |
| `list_dlq_messages()` | `queue_name, message_id, retry_count, published_at` (time entered DLQ), for all queues, ordered by that time |
| `queue_metrics()` | See [Metrics](#metrics). |

### DLQ and destructive operations

| Function | Returns | Notes |
|---|---|---|
| `requeue_dlq_messages(p_queue_name)` | `VOID` | Moves all of the queue's DLQ entries back to `pending`, attempts 0. One NOTIFY if any were moved. |
| `purge_dlq()` | `VOID` | Deletes **all** DLQ entries. |
| `delete_queue_message(p_queue_name, p_message_id)` | `VOID` | Deletes one delivery row in any state. The payload stays until retention collects it. Notifies if the row was grouped and not completed. |
| `clean_up_queue(p_queue)` | `VOID` | Deletes all of the queue's delivery rows, including in-flight ones. |
| `clean_up_topic(p_topic)` | `VOID` | Deletes all of the topic's payloads and their delivery rows, then prunes its group rows. Raises `PMQ03` while any of the topic's messages is in a DLQ. |
| `purge_all_messages()` | `VOID` | Deletes every DLQ entry, delivery row and payload, and prunes group rows. |

The delete, clean-up and purge functions are purges, not retention: they can
remove active work.

### Maintenance

| Function | Returns |
|---|---|
| `pmq_maintenance_fast()` | One row: `retired_to_dlq BIGINT, inactive_queues_dropped BIGINT` |
| `cleanup_completed_messages(p_older_than_hours INT DEFAULT 24, p_batch_size INT DEFAULT 1000)` | `INT`: delivery rows deleted |
| `cleanup_unreferenced_messages(p_older_than_hours INT DEFAULT 24, p_batch_size INT DEFAULT 1000)` | `INT`: payloads deleted |
| `prune_message_groups(p_topics VARCHAR[], p_keys VARCHAR[])` | `INT`: group rows deleted |
| `delete_inactive_queues()` | `VOID` |

## Notifications

All notifications have an empty payload. Each one is a hint to fetch; polling is
the fallback. PostgreSQL delivers them on commit, and collapses identical
notifications raised in one transaction into one.

| Channel | Emitted by |
|---|---|
| `pmq:t:<topic>` | Every `publish_message`, from the distribution trigger, whether or not any queue receives the message. |
| `pmq:q:<queue>` | `nack_message` (non-final attempt). |
| | `release_message`. |
| | `requeue_dlq_messages`, when at least one entry is requeued. |
| | For **grouped** rows only, because the group's successor has just become claimable: `ack_message`, a final-attempt `nack_message`, `pmq_maintenance_fast` (once per queue that had a grouped row retired), and `delete_queue_message` of a row that is not completed. |

These operations never notify:

- `consume_message`;
- `set_vt` and the heartbeat batches;
- ungrouped acks;
- ungrouped final-attempt nacks and maintenance retirements;
- queue or topic creation and deletion;
- the cleanup and purge functions.

LISTEN needs a session-mode connection: a transaction-mode pooler cannot carry
it. Each client connection holds one LISTEN session shared by its consumers. Go
takes it from the pool on the first consume and holds it until close, and
TypeScript holds a pooled client while subscriptions exist, so size those pools
for it. Rust opens it through a separate single-connection pool, outside the main
pool.

## Maintenance and retention

Installing the SQL does not schedule anything. Run these jobs from an external
scheduler:

| Job | Suggested interval | Purpose |
|---|---|---|
| `SELECT * FROM postgremq.pmq_maintenance_fast();` | every 1–10 s, at most every 60 s | Retires abandoned final attempts to the DLQ and reaps expired exclusive queues. |
| `SELECT postgremq.cleanup_completed_messages(24, 1000);` | every 10–60 s | Deletes completed delivery rows past retention, then collects unreferenced payloads. |

The `pmq_maintenance_fast` interval sets how long these wait:

- an abandoned final attempt waits after its lease expires before it reaches the
  DLQ;
- a group blocked behind that attempt waits for the same retirement;
- an expired exclusive queue keeps occupying storage until it is reaped.

Expiry itself is enforced at the expiry instant regardless of this interval.

With [pg_cron](https://github.com/citusdata/pg_cron) 1.5 or later, which supports
second-level intervals:

```sql
SELECT cron.schedule('postgremq-maintenance', '5 seconds',
  'SELECT postgremq.pmq_maintenance_fast()');
SELECT cron.schedule('postgremq-cleanup', '10 seconds',
  'SELECT postgremq.cleanup_completed_messages(24, 1000)');
```

pg_cron runs jobs in the database named by `cron.database_name`. If PostgreMQ is
installed in another database, use `cron.schedule_in_database`.

**Retention functions.**

- **`cleanup_completed_messages(hours, batch)`**
  - Deletes up to `batch` delivery rows that are `completed` and whose
    `processed_at` is older than `hours`.
  - Then calls `cleanup_unreferenced_messages(hours, batch)`.
  - Returns only the delivery-row count.
  - Raises `PMQ03` if `hours` < 0 or `batch` ≤ 0. NULL is not rejected: a NULL
    `hours` deletes nothing, and a NULL `batch` removes the batch limit.
- **`cleanup_unreferenced_messages(hours, batch)`**
  - Deletes up to `batch` payloads that are older than `hours` (by
    `published_at`) and are referenced by no queue and no DLQ entry.
  - Collects publications that had no subscribers, and payloads left behind by
    queue deletion or a DLQ purge.
  - Prunes the group rows of the groups whose payloads it deleted.
  - It runs inside `cleanup_completed_messages`. Call it on its own only if you
    want a different batch size.
  - Validates and treats NULL arguments the same way as
    `cleanup_completed_messages`.
- **`prune_message_groups(topics[], keys[])`**
  - Runs automatically through the functions above. You do not need to schedule
    it.
  - Under `REPEATABLE READ` or `SERIALIZABLE` it can raise `40001`; retry.
- **`delete_inactive_queues()`**
  - Performs the same reaping as `pmq_maintenance_fast`, without the DLQ
    retirement step.

Both cleanup functions work in bounded batches (given a non-NULL batch size) and
use `SKIP LOCKED`, so they never wait on live work.

**Sizing the batch.** At 10 publications per second with five queues per topic,
fan-out creates about 3,000 delivery rows per minute. A 1,000-row batch every 10
seconds can delete 6,000 delivery rows and 6,000 payloads per minute, as long as
each call finishes on schedule and the rows are eligible and unlocked. For higher
fan-out, frequent retries or a backlog, increase the batch size or the frequency.

Lagging queues and retained DLQ entries keep their payloads by design. Monitor the
maintenance return values, the oldest pending and DLQ ages, table sizes and
autovacuum.

## Metrics

`queue_metrics()` returns one row per queue, including empty and expired exclusive
queues. It is `STABLE` and read-only, never reads payloads, and evaluates every row
against a single `statement_timestamp()` cutoff.

| Column | Meaning |
|---|---|
| `queue_name`, `topic_name` | `text` |
| `active` | 1 for non-exclusive queues and exclusive queues that have not expired; 0 otherwise |
| `ready` | Unsettled rows with `vt` ≤ cutoff and under the attempt limit. 0 for an expired queue. Includes blocked group successors. |
| `delayed` | `pending` rows with `vt` > cutoff |
| `processing` | `processing` rows with `vt` > cutoff |
| `exhausted` | Unsettled rows with `vt` ≤ cutoff at or over the attempt limit, waiting for maintenance |
| `dead_letter` | DLQ entries |
| `oldest_ready_age_seconds` | Seconds since the earliest `vt` among ready rows; 0 when none (or the queue expired) |

A metrics scraper needs only:

```sql
GRANT USAGE ON SCHEMA postgremq TO postgremq_metrics;
GRANT SELECT ON postgremq.queues, postgremq.queue_messages,
    postgremq.dead_letter_queue TO postgremq_metrics;
GRANT EXECUTE ON FUNCTION postgremq.queue_metrics() TO postgremq_metrics;
```

Run one SQL scraper per database. For the Collector configuration, the OTel metric
mapping and the client-side metrics, see the
[observability guide](../docs/observability.md).

## Errors

PostgreMQ raises three custom SQLSTATEs:

| SQLSTATE | Meaning | Raised by |
|---|---|---|
| `PMQ01` | The delivery is not held under this token: unknown message, already settled, reclaimed by another consumer, or (for extension) the lease has expired | `ack_message`, `nack_message`, `release_message`, `set_vt` |
| `PMQ02` | Target does not exist or is gone | `publish_message` and `create_queue` (topic missing); `consume_message` (queue missing, expired, or generation mismatch); `create_queue` (redeclaring an expired exclusive queue) |
| `PMQ03` | Invalid argument or refused operation | NULL, invalid or too-long name (`create_topic`, `create_queue`); negative `max_attempts`; NULL or non-positive `keep_alive_interval`; redeclaration with different parameters (or of a queue deleted concurrently, see [Topics, queues and publishing](#topics-queues-and-publishing)); empty or over-255-character group key; negative `vt` (`consume_message`, `set_vt`); non-positive batch size (`consume_message`); mismatched heartbeat or prune array lengths; NULL or negative `p_vts` element; NULL or non-positive `p_intervals_ms` element; negative retention or non-positive batch size in cleanup; `delete_topic` with messages; `delete_queue` or `clean_up_topic` with DLQ entries |

NULL arguments are validated only where listed above. Elsewhere a NULL is passed
through to the SQL:

- **Lookups match nothing.** Settle and extend calls raise `PMQ01`;
  `publish_message`, `create_queue` (topic) and `consume_message` raise `PMQ02`;
  heartbeat entries are omitted; management and inspection functions do nothing
  or return no rows.
- **NOT NULL columns raise `23502`.** This applies to `publish_message`'s payload,
  `create_queue`'s `max_attempts` and `exclusive`, an explicit NULL
  `nack_message` delay, and a NULL `vt` in `set_vt` or `consume_message` (when a
  row is claimed).
- **Limits become unbounded.** A NULL `batch_size` in `consume_message`, or a NULL
  batch size in the cleanup functions, removes the limit.

NULL is meaningful for `publish_message`'s `deliver_after` (now) and `group_key`
(ungrouped), the `consume_message` and keep-alive generations (any incarnation),
and `get_queue_statistics(NULL)` (all queues).

Standard SQLSTATEs callers should expect:

| SQLSTATE | When | Handling |
|---|---|---|
| `55P03` (`lock_not_available`) | `set_vt` on a row that is locked | Retry |
| `40P01` (deadlock) | Transactions publishing to several groups in different orders | Retry the transaction |
| `40001` (serialization failure) | Grouped publish or group pruning under `REPEATABLE READ`/`SERIALIZABLE` | Retry the transaction |
| `23503` (foreign-key violation) | Direct deletes of a queue, message or topic still referenced by the DLQ | Requeue or purge the DLQ first |
| | `create_queue` whose topic is deleted between its topic check and its insert | Create the topic again before retrying |
| `23502` (not-null violation) | A NULL argument written to a NOT NULL column (see above) | Fix the caller |

## Validation

```sh
cd mq
python3 -m pytest tests/tests.py -q
```

The tests need Docker: they start a PostgreSQL container. They cover:

- fan-out and delayed delivery;
- token ownership and DLQ handling;
- queue expiry and recreation;
- row-lock contention and fresh lease clocks;
- bounded payload collection;
- message-group ordering under concurrent publishers and consumers, including a
  guard on the claim's query plan.
