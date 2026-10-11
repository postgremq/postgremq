-- PostgreMQ uses a fixed schema, independent of the caller's search_path.
CREATE SCHEMA IF NOT EXISTS postgremq;

/*
 * PostgreSQL Message Queue System Implementation
 *
 * REQUIREMENTS: PostgreSQL 15 or later
 *
 * Overview
 * --------
 * This file contains the SQL schema and runtime API for PostgreMQ – a
 * PostgreSQL‑backed message queue. It implements message publishing and
 * fan‑out to queues, consumption with "visibility timeout" (vt), positive
 * and negative acknowledgments, visibility extension, dead‑letter queue
 * (DLQ) movement and restoration, queue/topic management, and list/inspect
 * helpers.
 *
 * Core Concepts
 * -------------
 * - Visibility Timeout (vt): When a consumer fetches a message it becomes
 *   invisible to other consumers until `vt`. Extending vt keeps the message
 *   with the current consumer without taking a heavyweight lock.
 * - Queue Types: Queues can be:
 *   - Non‑exclusive (persistent): never expire.
 *   - Exclusive (temporary): expire unless a client keeps them alive by
 *     periodically extending `keep_alive_until`.
 * - Keep‑Alive: Clients of exclusive queues should periodically call
 *   `postgremq.extend_queue_keep_alive_multi()` (both clients implement automatic
 *   connection-level keep‑alive) otherwise the queue is eligible for deletion
 *   by `postgremq.delete_inactive_queues()`.
 * - Delivery Attempts: Each time a message is consumed its
 *   `delivery_attempts` is incremented. When a queue has
 *   `max_delivery_attempts > 0` and a message reaches the limit, the
 *   final-attempt nack retires the message inline; `postgremq.pmq_maintenance_fast()`
 *   covers leftover crashed-final-attempt rows.
 * - DLQ (Dead Letter Queue): Failed messages are copied to
 *   `dead_letter_queue`. Use `postgremq.list_dlq_messages()`, `postgremq.requeue_dlq_messages()`
 *   and `postgremq.purge_dlq()` to manage.
 *
 * Event Notifications
 * -------------------
 * Several operations emit LISTEN/NOTIFY events on per-topic and per-queue
 * channels as a wake-up signal — the payload is empty; clients use the
 * channel name alone to decide what to fetch next.
 *   - publishes:           channel `pmq:t:<topic>`
 *   - nack/release/requeue: channel `pmq:q:<queue>`
 * Clients LISTEN to the channels they care about; refcounted LISTEN/UNLISTEN
 * is the client's responsibility. Channel names are derived from topic and
 * queue names, which are validated at create time to be safe NOTIFY identifiers
 * (matching `^[A-Za-z0-9_:.\-]+$`).
 *
 * Table Relationships and Cascade Behavior:
 * - topics: The root table containing topic names
 * - queues: References postgremq.topics.name with ON DELETE CASCADE
 * - messages: References postgremq.topics.name with ON DELETE CASCADE
 * - queue_messages: References both queues.name and messages.id with ON DELETE CASCADE
 * - dead_letter_queue: References both queues.name and messages.id with ON DELETE CASCADE
 *
 * This cascade behavior ensures that:
 * 1. When a topic is deleted, all related queues, messages, and queue entries are automatically removed
 * 2. When a message is deleted (e.g., via clean_up_topic), all related queue entries are automatically removed
 * 3. When a queue is deleted, all its message entries are automatically removed
 *
 * Functions (high level):
 *   - publish_message: Insert into postgremq.messages and trigger distribution to queues.
 *   - consume_message: Retrieve and mark messages as processing; sets vt.
 *   - ack_message: Mark as completed; clears consumer token; sets processed_at.
 *   - nack_message: Return to pending with optional delay; clears token; NOTIFY.
 *   - release_message: Return to pending without incrementing attempts; NOTIFY.
 *   - set_vt / set_vt_batch_multi: Extend visibility time for one or many
 *     (cross-queue) messages.
 *   - requeue_dlq_messages / purge_dlq for DLQ management.
 *   - extend_queue_keep_alive_multi / delete_inactive_queues for exclusive queues.
 *   - pmq_maintenance_fast: bundled cron entry — retires crashed-final-attempt
 *     rows to DLQ, reaps expired exclusive queues; returns counters.
 *   - info: discovery of the installed schema version and the protocol major.
 *   - Management: create_topic, create_queue, delete_topic, delete_queue,
 *     list_topics, list_queues, get_queue_statistics, clean_up_* helpers,
 *     list_messages, get_message, get_next_visible_time, cleanup_completed_messages.
 */


-- Discovery: the installed schema version and the protocol major that
-- clients check when they connect. schema_version is the number of the last
-- migration applied, read from the version table the migrators and
-- latest.sql maintain (NULL without it); protocol_major changes only with a
-- migration that breaks the client contract. The signature and these two
-- fields stay stable across protocol majors; fields may be added.
-- SECURITY DEFINER, unlike every other function, so that runtime roles need
-- no privilege on the version table to connect.
CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_schema_version bigint;
BEGIN
    IF to_regclass('postgremq.postgremq_migrations') IS NOT NULL THEN
        SELECT version INTO v_schema_version FROM postgremq.postgremq_migrations LIMIT 1;
    END IF;
    RETURN jsonb_build_object('schema_version', v_schema_version, 'protocol_major', 1);
END;
$$;

-- Topics table.
CREATE TABLE postgremq.topics (
  name VARCHAR(255) PRIMARY KEY
);

-- Queues table.
CREATE TABLE postgremq.queues (
  generation UUID NOT NULL DEFAULT gen_random_uuid(),
  name VARCHAR(255) PRIMARY KEY,
  topic_name VARCHAR(255) NOT NULL REFERENCES postgremq.topics(name) ON DELETE CASCADE,
  max_delivery_attempts INT NOT NULL DEFAULT 0,
  exclusive BOOLEAN NOT NULL DEFAULT false,  -- temporary queue kept alive by its owner
  keep_alive_interval INTERVAL NOT NULL DEFAULT '5 minutes',
  keep_alive_until TIMESTAMPTZ
);

-- Messages table: payload stored as JSONB.
-- id is BIGSERIAL: int32 SERIAL would wrap in months at sustained high publish
-- rates and silently corrupt message identity.
CREATE TABLE postgremq.messages (
  id BIGSERIAL PRIMARY KEY,
  topic_name VARCHAR(255) NOT NULL REFERENCES postgremq.topics(name) ON DELETE CASCADE,
  payload JSONB NOT NULL,
  published_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  deliver_after TIMESTAMPTZ DEFAULT clock_timestamp(),  -- first visibility time
  -- Message group (ordered delivery). NULL = ungrouped. group_seq is the
  -- dense per-(topic, group_key) sequence allocated by publish_message under
  -- the message_groups row lock, so group order is publish commit order.
  group_key VARCHAR(255),
  group_seq BIGINT,
  CONSTRAINT messages_group_key_seq_paired
    CHECK ((group_key IS NULL) = (group_seq IS NULL))
);

-- Per-(topic, group_key) sequence allocator. publish_message upserts the row
-- and holds its lock until commit, which serialises publishers of one group
-- and makes group_seq follow commit order. Rows whose group has no message
-- left are pruned by cleanup_unreferenced_messages.
CREATE TABLE postgremq.message_groups (
  topic_name VARCHAR(255) NOT NULL REFERENCES postgremq.topics(name) ON DELETE CASCADE,
  group_key VARCHAR(255) NOT NULL,
  last_seq BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (topic_name, group_key)
);

-- Queue Messages table.
-- Composite primary key: (queue_name, message_id).
CREATE TABLE postgremq.queue_messages (
  queue_name VARCHAR(255) REFERENCES postgremq.queues(name) ON DELETE CASCADE,
  message_id BIGINT REFERENCES postgremq.messages(id) ON DELETE CASCADE,
  status VARCHAR(16) DEFAULT 'pending',  -- Allowed: 'pending', 'processing', 'completed'
  published_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  vt TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),  -- visibility timeout (lease end)
  delivery_attempts INT DEFAULT 0,
  consumer_token VARCHAR(64),
  processed_at TIMESTAMPTZ,
  -- Denormalised from messages by distribute_message so the consume
  -- predicate's group-head check never joins messages.
  group_key VARCHAR(255),
  group_seq BIGINT,
  PRIMARY KEY (queue_name, message_id),
  CONSTRAINT queue_messages_group_key_seq_paired
    CHECK ((group_key IS NULL) = (group_seq IS NULL)),
  CONSTRAINT queue_messages_delivery_attempts_nonneg
    CHECK (delivery_attempts >= 0),
  -- Guard against a future code path writing a typo'd status (e.g.
  -- 'Processing') that no query would ever match, stranding the row.
  CONSTRAINT queue_messages_status_valid
    CHECK (status IN ('pending', 'processing', 'completed'))
);

-- Dead Letter Queue table.
-- Composite primary key: (queue_name, message_id).
-- ON DELETE RESTRICT on both FKs: DLQ entries are forensic data the
-- operator may want to keep across queue/topic cleanups, so cleanups never
-- remove them implicitly. Operators make an explicit choice — postgremq.purge_dlq() or postgremq.requeue_dlq_messages() — before
-- removing the underlying messages or queue.
CREATE TABLE postgremq.dead_letter_queue (
  queue_name VARCHAR(255) REFERENCES postgremq.queues(name) ON DELETE RESTRICT,
  message_id BIGINT REFERENCES postgremq.messages(id) ON DELETE RESTRICT,
  retry_count INT,
  published_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  PRIMARY KEY (queue_name, message_id)
);

---------------------------
-- Performance Indexes
---------------------------

-- Index for consume_message query: filters by queue, status, and vt
-- Partial index excludes completed messages to keep it small
CREATE INDEX idx_queue_messages_consume
ON postgremq.queue_messages(queue_name, vt, published_at)
WHERE status IN ('pending', 'processing');

-- Index for get_next_visible_time query
CREATE INDEX idx_queue_messages_next_visible
ON postgremq.queue_messages(queue_name, vt)
WHERE status IN ('pending', 'processing');

-- Index for distribute_message: every publish runs
-- `WHERE topic_name = NEW.topic_name` over the queues table. Without this,
-- a publish-heavy workload seqscans the queues table on every message —
-- linear in the queue count, the publish hot path's first scaling cliff.
CREATE INDEX idx_queues_topic_name
ON postgremq.queues(topic_name);

-- Index for clean_up_topic and the messages → topics FK cascade.
-- DELETE FROM postgremq.messages WHERE topic_name = X without this index seqscans
-- the entire messages table; the same is true for any cascade triggered
-- by deleting a topic.
CREATE INDEX idx_messages_topic_name
ON postgremq.messages(topic_name);

-- Index for cleanup_completed_messages. The cleanup query filters by
-- status='completed' AND processed_at < cutoff. The two pre-existing
-- partial indexes cover only pending/processing rows, so the cleanup
-- otherwise falls back to a heap seqscan over every queue_message —
-- the kind of bulk DELETE that fails to keep up with a busy queue.
CREATE INDEX idx_queue_messages_completed_processed_at
ON postgremq.queue_messages(processed_at)
WHERE status = 'completed';

-- Per-status partial indexes on queue_name for get_queue_statistics. The
-- combined consume indexes group pending+processing together, so a
-- single-status COUNT still needs a heap recheck to tell them apart. These
-- narrow partial indexes let the hot pending/processing counts run as
-- index-only/partial scans instead of full-table scans over the retained
-- completed set (which can reach tens of millions of rows within the 24h
-- completed-message retention window).
CREATE INDEX idx_queue_messages_pending
ON postgremq.queue_messages(queue_name)
WHERE status = 'pending';

CREATE INDEX idx_queue_messages_processing
ON postgremq.queue_messages(queue_name)
WHERE status = 'processing';

-- Group-head lookup for consume_message / get_next_visible_time: "is there an
-- unsettled row of this (queue, group) with a lower sequence?" is a single
-- index probe. Partial on unsettled grouped rows, so ungrouped traffic and the
-- retained completed set never touch it.
CREATE INDEX idx_queue_messages_group_head
ON postgremq.queue_messages(queue_name, group_key, group_seq)
WHERE status IN ('pending', 'processing') AND group_key IS NOT NULL;

-- Group-row pruning: "does any message of (topic, group_key) remain?".
CREATE INDEX idx_messages_group
ON postgremq.messages(topic_name, group_key)
WHERE group_key IS NOT NULL;

/* Function: distribute_message
 *
 * Description:
 *   Trigger function that fans a newly published message (row in `messages`)
 *   out to all queues subscribed to the message's topic. For each matching
 *   queue, it inserts a row into `queue_messages` with initial vt set to the
 *   message's `deliver_after`. It NOTIFYs once on the per-topic channel
 *   `pmq:t:<topic>` with the message id as plain text.
 *
 * Trigger:
 *   - Installed as AFTER INSERT trigger `after_message_insert` on `messages`.
 *
 * Side Effects:
 *   - Inserts multiple rows into `queue_messages`.
 *   - Emits a single NOTIFY event used by clients for push-based wakeups.
 *
 * Returns:
 *   The unmodified NEW row for the `messages` table.
 */
CREATE OR REPLACE FUNCTION postgremq.distribute_message()
RETURNS trigger AS $$
BEGIN
   -- Distribute to an exclusive queue only while its keep-alive is still live
   -- (keep_alive_until > clock_timestamp()). There is no grace window: the reaper and the
   -- consume-side gating use the same strict clock_timestamp() cutoff, so an expired queue
   -- is treated as dead everywhere at once. Clients are responsible for sending
   -- keep-alive well before expiry (with their own safety margin) so a queue is
   -- never considered expired while still in use.
   -- group_key/group_seq are copied so each queue orders its own copy of a
   -- group independently, without joining messages on the consume path.
   INSERT INTO postgremq.queue_messages(queue_name, message_id, vt, group_key, group_seq)
   SELECT q.name, NEW.id, NEW.deliver_after, NEW.group_key, NEW.group_seq
   FROM postgremq.queues q
   WHERE q.topic_name = NEW.topic_name
     AND (NOT q.exclusive OR q.keep_alive_until > clock_timestamp());

   -- Wake-up signal only; payload is empty. Clients use the channel name
   -- alone to decide what to fetch next.
   PERFORM pg_notify('pmq:t:' || NEW.topic_name, '');

   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

/* Trigger: after_message_insert
 *
 * Description:
 *   Automatically distributes newly published messages to all queues subscribed to the message's topic.
 *   This trigger fires after each message insert and calls postgremq.distribute_message() to handle the fan-out.
 *
 * Timing: AFTER INSERT
 * Granularity: FOR EACH ROW
 * Target Table: messages
 *
 * Behavior:
 *   For each newly inserted message, this trigger:
 *   1. Finds all active queues subscribed to the message's topic
 *   2. Creates queue_message entries for each queue via postgremq.distribute_message()
 *   3. Emits a single NOTIFY event on `pmq:t:<topic>` to wake consumers
 *
 * Side Effects:
 *   - Multiple inserts into postgremq.queue_messages (one per subscribed queue)
 *   - NOTIFY on the per-topic channel `pmq:t:<topic>` with the message id as plain text
 *   - Exclusive queues with expired keep_alive_until are excluded from distribution
 */
CREATE TRIGGER after_message_insert
AFTER INSERT ON postgremq.messages
FOR EACH ROW
EXECUTE FUNCTION postgremq.distribute_message();

---------------------------
-- Runtime API Functions
---------------------------

/* Function: create_topic
 *
 * Description:
 *   Creates a topic if it does not exist (idempotent).
 *
 * Parameters:
 *   - p_topic (VARCHAR): Name of the topic to create.
 *
 * Returns:
 *   VARCHAR: The topic name (for convenience/chaining).
 */
CREATE OR REPLACE FUNCTION postgremq.create_topic(p_topic VARCHAR(255))
RETURNS VARCHAR(255) AS $$
BEGIN
  IF p_topic IS NULL OR p_topic !~ '^[A-Za-z0-9_:.\-]+$' THEN
    RAISE EXCEPTION 'Invalid topic name "%": must match ^[A-Za-z0-9_:.\-]+$', p_topic
      USING ERRCODE = 'PMQ03';
  END IF;
  -- NOTIFY channel names are silently truncated by Postgres at
  -- NAMEDATALEN-1 = 63 bytes. The publish trigger emits on
  -- 'pmq:t:'||topic_name (6 bytes of prefix), so a topic name longer
  -- than 57 bytes would be truncated and could collide with another
  -- topic that shares the same first 57 bytes — cross-delivering
  -- wake events. Reject up front so the failure mode is loud.
  IF octet_length(p_topic) > 57 THEN
    RAISE EXCEPTION 'Topic name "%" is too long: maximum 57 bytes (limit imposed by NOTIFY channel length: 63 bytes minus the "pmq:t:" prefix)', p_topic
      USING ERRCODE = 'PMQ03';
  END IF;
  INSERT INTO postgremq.topics(name) VALUES (p_topic)
  ON CONFLICT (name) DO NOTHING;
  RETURN p_topic;
END;
$$ LANGUAGE plpgsql;

/* Function: create_queue
 *
 * Description:
 *   Idempotently creates a queue that subscribes to a topic. Re-creating a
 *   queue with the same parameters is a no-op success. If the queue exists
 *   with different parameters (different topic, max_delivery_attempts,
 *   exclusive flag, or keep_alive_interval), raises PMQ03 with the existing
 *   values surfaced for diagnosis — modelled on RabbitMQ's queue.declare
 *   "passive match-or-error" semantics.
 *
 *   For exclusive queues, an idempotent re-create also refreshes
 *   keep_alive_until to clock_timestamp() + keep_alive_interval. This means a re-create
 *   on an expired exclusive queue effectively revives it (the caller is
 *   asserting ownership now), consistent with consume_message's implicit
 *   refresh on the same column.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue to create.
 *   - p_topic_name (VARCHAR): Name of the topic to subscribe to.
 *   - p_max_attempts (INTEGER): Maximum delivery attempts before moving to DLQ.
 *   - p_exclusive (BOOLEAN): If true, queue will be deleted when keep_alive expires.
 *   - p_keep_alive_interval (INTERVAL): Stored on the queue and used for both the initial
 *                                       keep_alive_until (clock_timestamp() + interval) and for the
 *                                       implicit refresh in consume_message. Defaults to
 *                                       '5 minutes'. Effectively only matters for exclusive
 *                                       queues; non-exclusive ones never expire.
 *
 * Returns: VOID.
 *
 * Raises:
 *   - PMQ02 if p_topic_name doesn't exist.
 *   - PMQ03 if the name fails validation, p_max_attempts is negative, or
 *     a queue with this name already exists with different parameters.
 */
CREATE OR REPLACE FUNCTION postgremq.create_queue(
    p_queue_name VARCHAR(255),
    p_topic_name VARCHAR(255),
    p_max_attempts INTEGER DEFAULT 0,  -- 0 = unlimited retries
    p_exclusive BOOLEAN DEFAULT false,
    p_keep_alive_interval INTERVAL DEFAULT '5 minutes'
) RETURNS UUID AS $$
DECLARE
    v_existing postgremq.queues%ROWTYPE;
    v_generation UUID;
BEGIN
    IF p_queue_name IS NULL OR p_queue_name !~ '^[A-Za-z0-9_:.\-]+$' THEN
        RAISE EXCEPTION 'Invalid queue name "%": must match ^[A-Za-z0-9_:.\-]+$', p_queue_name
          USING ERRCODE = 'PMQ03';
    END IF;
    -- Same length cap as create_topic: NOTIFY channels truncate at 63
    -- bytes; nack/release/requeue emit on 'pmq:q:'||queue_name, leaving
    -- 57 bytes for the queue name.
    IF octet_length(p_queue_name) > 57 THEN
        RAISE EXCEPTION 'Queue name "%" is too long: maximum 57 bytes (limit imposed by NOTIFY channel length: 63 bytes minus the "pmq:q:" prefix)', p_queue_name
          USING ERRCODE = 'PMQ03';
    END IF;
    -- Negative max_delivery_attempts silently breaks consume_message's filter
    -- (qm.delivery_attempts < tq.max_delivery_attempts is never true once the
    -- attempts counter passes the negative threshold) and never retires to DLQ
    -- (nack_message and pmq_maintenance_fast both gate on max_attempts > 0).
    -- Reject upfront so callers see PMQ03 instead of an invisibly-broken queue.
    IF p_keep_alive_interval IS NULL OR p_keep_alive_interval <= interval '0' THEN
        RAISE EXCEPTION 'keep alive interval must be positive' USING ERRCODE = 'PMQ03';
    END IF;
    IF p_max_attempts < 0 THEN
        RAISE EXCEPTION 'p_max_attempts must be >= 0 (got %)', p_max_attempts
          USING ERRCODE = 'PMQ03';
    END IF;
    -- Surface a missing topic as PMQ02 (parity with publish_message). Without
    -- this check the INSERT below would still fail, but with a raw 23503 FK
    -- violation that doesn't map to ErrQueueNotFound on the client side.
    IF NOT EXISTS (SELECT 1 FROM postgremq.topics WHERE name = p_topic_name) THEN
        RAISE EXCEPTION 'Topic "%" does not exist', p_topic_name
          USING ERRCODE = 'PMQ02';
    END IF;
    INSERT INTO postgremq.queues (
        name,
        topic_name,
        max_delivery_attempts,
        exclusive,
        keep_alive_interval,
        keep_alive_until
    ) VALUES (
        p_queue_name,
        p_topic_name,
        p_max_attempts,
        p_exclusive,
        p_keep_alive_interval,
        CASE
            WHEN p_exclusive THEN clock_timestamp() + p_keep_alive_interval
            ELSE NULL
        END
    )
    ON CONFLICT (name) DO NOTHING RETURNING generation INTO v_generation;

    IF FOUND THEN
        RETURN v_generation;  -- inserted on the fast path
    END IF;

    -- Conflict path: queue with this name already exists. Verify the caller's
    -- parameters match the existing row; otherwise raise so accidental config
    -- drift is caught loudly rather than silently ignored.
    SELECT * INTO v_existing FROM postgremq.queues WHERE name = p_queue_name FOR UPDATE;
    IF v_existing.exclusive AND v_existing.keep_alive_until <= clock_timestamp() THEN
        RAISE EXCEPTION 'Queue "%" expired; delete it before recreating it', p_queue_name USING ERRCODE = 'PMQ02';
    END IF;
    IF v_existing.topic_name IS DISTINCT FROM p_topic_name
       OR v_existing.max_delivery_attempts IS DISTINCT FROM p_max_attempts
       OR v_existing.exclusive IS DISTINCT FROM p_exclusive
       OR v_existing.keep_alive_interval IS DISTINCT FROM p_keep_alive_interval THEN
        RAISE EXCEPTION 'Queue "%" already exists with different parameters '
                        '(existing: topic=%, max_attempts=%, exclusive=%, keep_alive_interval=%)',
            p_queue_name,
            v_existing.topic_name,
            v_existing.max_delivery_attempts,
            v_existing.exclusive,
            v_existing.keep_alive_interval
          USING ERRCODE = 'PMQ03';
    END IF;

    -- Redeclaration is idempotent only while this lease is live.
    IF p_exclusive THEN
        UPDATE postgremq.queues SET keep_alive_until = clock_timestamp() + keep_alive_interval
        WHERE name = p_queue_name;
    END IF;
    RETURN v_existing.generation;
END;
$$ LANGUAGE plpgsql;

/* Function: publish_message
 *
 * Description:
 *   Publishes a message into `messages` for the specified topic. The
 *   distribution to queues is performed by the `after_message_insert` trigger
 *   via `postgremq.distribute_message()`. If `p_deliver_after` is specified, message will
 *   be invisible to consumers until that timestamp; otherwise it is visible
 *   immediately.
 *
 * Parameters:
 *   - p_topic (VARCHAR): Topic name (must exist).
 *   - p_payload (JSONB): Arbitrary JSON payload stored in `messages.payload`.
 *   - p_deliver_after (TIMESTAMPTZ, default clock_timestamp()): First visibility time.
 *   - p_group_key (VARCHAR, default NULL): Message group. NULL publishes an
 *     ungrouped message. With a key, the message gets the next dense
 *     group_seq of (p_topic, p_group_key); within each queue, deliveries of
 *     one group are claimed strictly in group_seq order.
 *
 * Returns:
 *   BIGINT: The generated message id.
 *
 * Side Effects:
 *   - Triggers `postgremq.distribute_message()` which inserts into `queue_messages` and
 *     emits NOTIFY on `pmq:t:<topic>`.
 *   - With a group key: upserts the (topic, group_key) row of `message_groups`
 *     and holds its row lock until the caller's transaction ends. Publishers
 *     of one group are therefore serialised, and group_seq order is commit
 *     order: a lower sequence can never become visible after a higher one.
 *     A transaction that publishes to several groups can deadlock (40P01)
 *     with one that takes them in a different order; clients retry 40P01.
 *
 * Raises:
 *   - PMQ02 if the topic does not exist.
 *   - PMQ03 if p_group_key is empty or longer than 255 characters.
 */
CREATE OR REPLACE FUNCTION postgremq.publish_message(
    p_topic VARCHAR(255),
    p_payload JSONB,
    p_deliver_after TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_group_key VARCHAR(255) DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_message_id BIGINT;
    v_group_seq BIGINT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM postgremq.topics WHERE name = p_topic) THEN
        RAISE EXCEPTION 'Topic "%" does not exist', p_topic
          USING ERRCODE = 'PMQ02';
    END IF;

    IF p_group_key IS NOT NULL THEN
        -- Function argument typmods are not enforced, so check the length here
        -- rather than surfacing a raw 22001 from the INSERT.
        IF p_group_key = '' OR char_length(p_group_key) > 255 THEN
            RAISE EXCEPTION 'Invalid group key: must be 1..255 characters'
              USING ERRCODE = 'PMQ03';
        END IF;
        -- The upsert takes the group row's lock and keeps it until commit:
        -- that is the serialisation of a group's publishers, and why the
        -- sequence is dense and in commit order.
        INSERT INTO postgremq.message_groups AS g (topic_name, group_key, last_seq)
        VALUES (p_topic, p_group_key, 1)
        ON CONFLICT (topic_name, group_key) DO UPDATE SET last_seq = g.last_seq + 1
        RETURNING g.last_seq INTO v_group_seq;
    END IF;

    -- COALESCE: an explicit NULL deliver_after (a positional caller passing
    -- only a group key) means "now", not a NULL vt.
    INSERT INTO postgremq.messages(topic_name, payload, deliver_after, group_key, group_seq)
    VALUES (p_topic, p_payload, COALESCE(p_deliver_after, clock_timestamp()), p_group_key, v_group_seq)
    RETURNING id INTO v_message_id;

    RETURN v_message_id;
END;
$$ LANGUAGE plpgsql;

/* Function: consume_message
 *
 * Description:
 *   Retrieves one or more messages from the specified queue. For each message, it increments the 
 *   delivery_attempts field and locks the message for the provided duration. Only messages that
 *   haven't exceeded their max_delivery_attempts limit are returned.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_vt (INTEGER): The duration for which the message is locked in seconds.
 *   - p_limit (INT DEFAULT 1): Maximum number of messages to retrieve.
 *
 * Message groups:
 *   A grouped row is claimable only while no row of the same (queue, group_key)
 *   with a lower group_seq is pending or processing, so a group is claimed in
 *   order, one delivery at a time, and a batch holds at most one row per group.
 *   Head-of-line blocking is intended: a leased or delayed head blocks its group.
 *
 * Returns:
 *   A table of records with fields: queue_name, message_id, payload, consumer_token,
 *   delivery_attempts, vt, published_at, group_key, group_seq.
 *
 * Raises:
 *   - PMQ03 if p_vt < 0 or p_limit <= 0.
 *   - PMQ02 if the queue does not exist (deleted out-of-band). An existing but
 *     empty queue returns zero rows with no error.
 */
CREATE OR REPLACE FUNCTION postgremq.consume_message(
    p_queue_name VARCHAR(255),
    p_vt INTEGER,
    p_limit INT DEFAULT 1,
    p_generation UUID DEFAULT NULL
) RETURNS TABLE(
    queue_name VARCHAR(255),
    message_id BIGINT,
    payload JSONB,
    consumer_token VARCHAR(64),
    delivery_attempts INT,
    vt TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    group_key VARCHAR(255),
    group_seq BIGINT
) AS $$
DECLARE
    v_max_attempts INT;
    v_generation   UUID;
    v_now          TIMESTAMPTZ;
BEGIN
    IF p_vt < 0 THEN
        RAISE EXCEPTION 'p_vt must be >= 0' USING ERRCODE = 'PMQ03';
    END IF;
    IF p_limit <= 0 THEN
        RAISE EXCEPTION 'p_limit must be > 0' USING ERRCODE = 'PMQ03';
    END IF;

    -- Consuming a queue that no longer exists is fatal for the caller's consumer:
    -- RabbitMQ surfaces queue deletion to active consumers as a cancel / channel
    -- exception. Raise PMQ02 so the client can tear the consumer down instead of
    -- silently polling an empty result forever. An existing-but-empty queue still
    -- returns zero rows with no error (the common idle case); only an ABSENT
    -- queue row raises here.
    -- The generation is captured with the limit and pinned in the claim below,
    -- so a queue deleted and recreated in between (possible when the caller
    -- passes no generation) cannot be served with the old incarnation's limit.
    SELECT q.max_delivery_attempts, q.generation INTO v_max_attempts, v_generation
    FROM postgremq.queues q
    WHERE q.name = p_queue_name AND (p_generation IS NULL OR q.generation = p_generation)
      AND (NOT q.exclusive OR q.keep_alive_until > clock_timestamp());
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Queue "%" does not exist', p_queue_name
          USING ERRCODE = 'PMQ02';
    END IF;
    v_now := clock_timestamp();

    -- Queue lifetime belongs to the keep-alive protocol, not message polling.
    -- Consumption never locks the shared queue row to refresh its lease.
    --
    -- Plan shape matters here. The queue filter is the parameter itself
    -- (qm.queue_name = p_queue_name), so it is an index condition and
    -- idx_queue_messages_consume yields rows in vt order: the LIMIT stops the
    -- walk early and there is no Sort. (Joining a CTE that calls the volatile
    -- clock_timestamp() instead materialises the CTE, turns the queue match
    -- into a join filter, and degrades every consume to a Seq Scan over all
    -- queues' rows plus a Sort of the whole visible backlog.) The queue's
    -- liveness/generation guard stays in this statement as an uncorrelated
    -- one-time filter, so a queue deleted or replaced after the check above
    -- still serves nothing.
    RETURN QUERY
    WITH next_msg AS (
        SELECT qm.queue_name,
               qm.message_id,
               qm.status,
               qm.delivery_attempts,
               qm.published_at
        FROM postgremq.queue_messages qm
        WHERE qm.queue_name = p_queue_name
            AND EXISTS (
                SELECT 1 FROM postgremq.queues q
                WHERE q.name = p_queue_name
                  AND q.generation = v_generation
                  -- Strict clock_timestamp() cutoff, symmetric with distribute_message and the
                  -- reaper: an expired exclusive queue serves nothing. No grace window.
                  AND (NOT q.exclusive OR q.keep_alive_until > clock_timestamp()))
            AND (v_max_attempts = 0 OR qm.delivery_attempts < v_max_attempts)
            AND (qm.status = 'pending' OR qm.status = 'processing' )
            -- A captured cutoff (not clock_timestamp() itself, which is
            -- volatile and cannot be an index condition) bounds the index
            -- range to rows that are already visible, so an under-filled
            -- batch never walks the queue's future-vt rows.
            AND qm.vt <= v_now
            -- Group head only. A head another consumer has locked (and is
            -- claiming right now) is still 'pending' in this snapshot, so its
            -- successor stays blocked: SKIP LOCKED never falls through to the
            -- next row of a group. Two rows of one group cannot both pass in
            -- one statement (the lower one is unsettled in the snapshot).
            AND (qm.group_key IS NULL OR NOT EXISTS (
                SELECT 1 FROM postgremq.queue_messages h
                WHERE h.queue_name = qm.queue_name
                  AND h.group_key = qm.group_key
                  AND h.group_key IS NOT NULL
                  AND h.group_seq < qm.group_seq
                  AND h.status IN ('pending', 'processing')))
        -- Order by vt, not published_at, to match idx_queue_messages_consume
        -- (queue_name, vt, published_at). The `qm.queue_name = p_queue_name` /
        -- `vt <= v_now` range scan already walks the index in vt order, so ordering by vt eliminates the
        -- Sort node that ORDER BY published_at forced over the whole visible set
        -- (an O(n log n) cliff on a deep backlog). At distribution time vt equals
        -- published_at, so fresh messages keep FIFO order. Tradeoff: REDELIVERED
        -- messages (nack/release resets vt but not published_at) are ordered by
        -- their reset vt rather than original publish time — acceptable for
        -- visibility-timeout semantics.
        ORDER BY qm.vt
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    )
    UPDATE postgremq.queue_messages
    SET status = 'processing',
        vt = clock_timestamp() + make_interval(secs => p_vt),
        delivery_attempts = qm.delivery_attempts + 1,
        -- Per-lease ownership token. gen_random_uuid() is collision-free
        -- without the old timestamp+random()+txid_current() construction.
        consumer_token = gen_random_uuid()::text
    FROM next_msg qm
    WHERE queue_messages.queue_name = qm.queue_name
        AND queue_messages.message_id = qm.message_id
    RETURNING queue_messages.queue_name,
              queue_messages.message_id,
              (SELECT m.payload FROM postgremq.messages m WHERE m.id = queue_messages.message_id) AS payload,
              queue_messages.consumer_token,
              queue_messages.delivery_attempts,
              queue_messages.vt,
              queue_messages.published_at,
              queue_messages.group_key,
              queue_messages.group_seq;
END;
$$ LANGUAGE plpgsql;

/* Function: ack_message
 *
 * Description:
 *   Acknowledges a message by marking its status as 'completed'. This function ensures that only the
 *   correct consumer (verified via consumer_token) can acknowledge the intended message.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (BIGINT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token generated via consume_message.
 *
 * Returns: VOID.
 *
 * Side Effects:
 *   - Acking a grouped row NOTIFYs `pmq:q:<queue>`: the group's successor was
 *     unclaimable until this head settled. Ungrouped acks stay silent.
 */
CREATE OR REPLACE FUNCTION postgremq.ack_message(p_queue_name VARCHAR(255), p_message_id BIGINT, p_consumer_token VARCHAR(64))
RETURNS VOID AS $$
DECLARE
  v_group_key VARCHAR(255);
BEGIN
  UPDATE postgremq.queue_messages
  SET status = 'completed',
      processed_at = clock_timestamp(),
      consumer_token = NULL
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id
    AND status = 'processing'
    AND consumer_token = p_consumer_token
  RETURNING group_key INTO v_group_key;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Ack failed: message not found, not in processing state, or token mismatch'
      USING ERRCODE = 'PMQ01';
  END IF;
  -- Unconditional for grouped rows (no "does a successor exist?" probe): a
  -- successor committed after this statement but before our commit would be
  -- missed by the probe while its own publish NOTIFY found the head still
  -- unsettled.
  IF v_group_key IS NOT NULL THEN
    PERFORM pg_notify('pmq:q:' || p_queue_name, '');
  END IF;
END;
$$ LANGUAGE plpgsql;

/* Function: nack_message
 *
 * Description:
 *   Performs a negative acknowledgment (nack) on a message. The function resets the message status
 *   to 'pending', clears the lock and consumer token, and sends a notification event. The topic
 *   is determined by looking up the corresponding queue.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (BIGINT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token to verify the consumer.
 *   - p_delay_until (TIMESTAMPTZ): The timestamp until which the message should be delayed for redelivery.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.nack_message(
    p_queue_name VARCHAR(255),
    p_message_id BIGINT,
    p_consumer_token VARCHAR(64),
    p_delay_until TIMESTAMPTZ DEFAULT clock_timestamp()
) RETURNS VOID AS $$
DECLARE
    v_attempts     INT;
    v_max_attempts INT;
    v_group_key    VARCHAR(255);
BEGIN
    -- Look up the queue's retry limit. We need this to decide between the
    -- "reset to pending" path and the "inline DLQ retirement" path.
    -- FOR SHARE pins the queue row for the rest of the function: without it
    -- a concurrent delete_queue cascade could drop the queue (and its
    -- queue_messages) between this read and the UPDATE/INSERT below,
    -- leaving a stale v_max_attempts and a NOTIFY on a dropped channel.
    SELECT max_delivery_attempts INTO v_max_attempts
    FROM postgremq.queues WHERE name = p_queue_name
    FOR SHARE;

    -- Reset to pending. RETURNING gives us the (post-consume-increment)
    -- delivery_attempts so we can decide whether this was the final attempt.
    UPDATE postgremq.queue_messages
    SET status = 'pending',
        vt = p_delay_until,
        consumer_token = NULL
    WHERE queue_name = p_queue_name
        AND message_id = p_message_id
        AND status = 'processing'
        AND consumer_token = p_consumer_token
    RETURNING delivery_attempts, group_key INTO v_attempts, v_group_key;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Nack failed: message not in processing state or token mismatch'
          USING ERRCODE = 'PMQ01';
    END IF;

    IF v_max_attempts > 0 AND v_attempts >= v_max_attempts THEN
        -- Final attempt: retire to DLQ inline.
        INSERT INTO postgremq.dead_letter_queue(queue_name, message_id, retry_count)
        VALUES (p_queue_name, p_message_id, v_attempts)
        ON CONFLICT (queue_name, message_id) DO NOTHING;

        DELETE FROM postgremq.queue_messages
        WHERE queue_name = p_queue_name
          AND message_id = p_message_id;
        -- An ungrouped retirement leaves nothing new to consume. A grouped one
        -- retires its group's head, which unblocks the successor: wake it.
        IF v_group_key IS NOT NULL THEN
            PERFORM pg_notify('pmq:q:' || p_queue_name, '');
        END IF;
    ELSE
        -- Wake up consumers of this queue so redelivery is prompt.
        -- Payload empty; channel name is the signal.
        PERFORM pg_notify('pmq:q:' || p_queue_name, '');
    END IF;

    RETURN;
END;
$$ LANGUAGE plpgsql;

/* Function: release_message
 *
 * Description:
 *   Releases the message back for delivery without increasing retry count. Should be used when client fetched the
 *   message but didn't make an attempt to process it, like for example buffered consumption.
 *   The function resets the message status
 *   to 'pending', clears the lock and consumer token, and sends a notification event.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (BIGINT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token to verify the consumer.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.release_message(
    p_queue_name VARCHAR(255),
    p_message_id BIGINT,
    p_consumer_token VARCHAR(64)
)
    RETURNS VOID AS $$
BEGIN
    UPDATE postgremq.queue_messages
    SET status = 'pending',
        vt = clock_timestamp(),
        consumer_token = NULL,
        -- GREATEST floors at 0: a stale consumer racing a reclaim path could
        -- otherwise underflow delivery_attempts on repeated releases. The
        -- CHECK constraint on the column is a hard backstop.
        delivery_attempts = GREATEST(delivery_attempts - 1, 0)
    WHERE queue_name = p_queue_name
      AND message_id = p_message_id
      AND status = 'processing'
      AND consumer_token = p_consumer_token;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Release message failed: message not in processing state or token mismatch'
          USING ERRCODE = 'PMQ01';
    END IF;

    -- Wake-up signal only; payload empty.
    PERFORM pg_notify('pmq:q:' || p_queue_name, '');

    RETURN;
END;
$$ LANGUAGE plpgsql;

/* Function: set_vt
 *
 * Description:
 *   Extends the lock time on a particular message, giving the consumer more time to process it.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (BIGINT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token to verify the consumer.
 *   - p_vt (INT): New lease duration in seconds from the current wall clock.
 *
 * Returns:
 *   TIMESTAMPTZ indicating new lock time.
 *
 * Note:
 *   The visibility timeout (p_vt) parameter is not bounded by this function.
 *   Callers should ensure reasonable values are used to prevent messages from
 *   being locked for excessive periods. Recommended maximum: 43200 seconds (12 hours).
 */
CREATE OR REPLACE FUNCTION postgremq.set_vt(
    p_queue_name VARCHAR(255),
    p_message_id BIGINT,
    p_consumer_token VARCHAR(64),
    p_vt INTEGER
) RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_vt TIMESTAMPTZ;
BEGIN
    IF p_vt < 0 THEN
        RAISE EXCEPTION 'p_vt must be >= 0' USING ERRCODE = 'PMQ03';
    END IF;

    SELECT qm.vt INTO v_vt FROM postgremq.queue_messages qm
    WHERE qm.queue_name = p_queue_name AND qm.message_id = p_message_id
      AND qm.consumer_token = p_consumer_token AND qm.status = 'processing'
    FOR UPDATE NOWAIT;
    IF NOT FOUND OR v_vt <= clock_timestamp() THEN
        RAISE EXCEPTION 'Extend lock failed: message not in processing state, expired, or token mismatch'
          USING ERRCODE = 'PMQ01';
    END IF;
    UPDATE postgremq.queue_messages qm SET vt = clock_timestamp() + make_interval(secs => p_vt)
    WHERE qm.queue_name = p_queue_name AND qm.message_id = p_message_id
    RETURNING qm.vt INTO v_vt;

    RETURN v_vt;
END;
$$ LANGUAGE plpgsql;

/* Function: pmq_maintenance_fast
 *
 * Description:
 *   Bundles the latency-sensitive maintenance routines into one cron entry:
 *
 *   1. Retire crashed-final-attempt rows to DLQ. nack_message already retires
 *      messages inline when their delivery_attempts hit max_delivery_attempts,
 *      so this only catches rows whose consumer crashed mid-handler before
 *      acking/nacking the final attempt — those would otherwise stay stuck
 *      in 'processing' (consume_message refuses to re-pick them because
 *      delivery_attempts >= max_delivery_attempts). The retire predicate is
 *      gated on status='processing' AND vt <= clock_timestamp() so we never yank a
 *      healthy in-flight row out from under a still-running consumer.
 *      Retiring a grouped row unblocks its group's successor, so every queue
 *      that had a grouped row retired gets one NOTIFY on `pmq:q:<queue>`.
 *   2. Reap exclusive queues whose keep_alive_until has expired.
 *
 *   Intended to run every 30-60 seconds (≤ ½ × the shortest
 *   keep_alive_interval in your queues so dead exclusive queues are reaped
 *   within ~1.5× their interval).
 *
 *   cleanup_completed_messages stays separate because it's latency-tolerant
 *   bulk DELETE governed by retention policy, not freshness.
 *
 * Returns:
 *   A single row with two counters for monitoring:
 *     retired_to_dlq          - rows moved into postgremq.dead_letter_queue
 *     inactive_queues_dropped - exclusive queues whose keep_alive_until expired
 */
CREATE OR REPLACE FUNCTION postgremq.pmq_maintenance_fast()
RETURNS TABLE (
    retired_to_dlq          BIGINT,
    inactive_queues_dropped BIGINT
) AS $$
DECLARE
    v_retired BIGINT;
    v_dropped BIGINT;
    v_grouped_queues VARCHAR[];
BEGIN
    WITH deleted_messages AS (
        DELETE FROM postgremq.queue_messages qm
        USING postgremq.queues q
        WHERE qm.queue_name = q.name
          AND q.max_delivery_attempts > 0
          AND qm.delivery_attempts >= q.max_delivery_attempts
          -- Only retire rows that are genuinely abandoned: a consumer
          -- holding a still-valid lease (status='processing' AND vt > clock_timestamp())
          -- might be mid-handler on its final attempt; yanking the row out
          -- from under it would let its side-effects commit while the
          -- message also lands in DLQ. Restrict to expired processing rows.
          AND qm.status = 'processing'
          AND qm.vt <= clock_timestamp()
        RETURNING qm.queue_name, qm.message_id, qm.delivery_attempts, qm.group_key
    ),
    inserted AS (
        -- ON CONFLICT for parity with nack_message's inline retirement.
        -- The predicate already makes a double-insert impossible (nack flips
        -- status to 'pending' before deleting; maintenance only matches
        -- 'processing' rows); the guard is defence in depth.
        INSERT INTO postgremq.dead_letter_queue(queue_name, message_id, retry_count)
        SELECT queue_name, message_id, delivery_attempts
        FROM deleted_messages
        ON CONFLICT (queue_name, message_id) DO NOTHING
        RETURNING 1
    )
    SELECT (SELECT count(*) FROM inserted),
           (SELECT array_agg(DISTINCT d.queue_name) FROM deleted_messages d
             WHERE d.group_key IS NOT NULL)
    INTO v_retired, v_grouped_queues;

    -- Once per queue that had a grouped head retired (its successor is now
    -- claimable). Ungrouped retirements leave nothing new to consume.
    PERFORM pg_notify('pmq:q:' || gq, '')
    FROM unnest(v_grouped_queues) AS gq;

    -- Skip queues that have DLQ entries. dead_letter_queue.queue_name
    -- has ON DELETE RESTRICT so deleting them would error and abort
    -- the maintenance call. Operators can postgremq.purge_dlq() or
    -- postgremq.requeue_dlq_messages() to release the queue, OR leave it as
    -- forensic data — the queue stays until the operator decides.
    WITH dropped AS (
        DELETE FROM postgremq.queues q
        WHERE q.exclusive = true
          -- Strict expiry: reap as soon as keep_alive_until has passed. No
          -- grace window — symmetric with distribute_message / consume_message,
          -- so an expired queue is dead everywhere at the same instant. Clients
          -- must send keep-alive before expiry (with their own margin).
          AND (q.keep_alive_until IS NULL OR q.keep_alive_until <= clock_timestamp())
          AND NOT EXISTS (
              SELECT 1 FROM postgremq.dead_letter_queue dlq WHERE dlq.queue_name = q.name
          )
        RETURNING name
    )
    SELECT count(*) INTO v_dropped FROM dropped;

    RETURN QUERY SELECT v_retired, v_dropped;
END;
$$ LANGUAGE plpgsql;

/* Queue heartbeat outcomes: extended with deadline, busy with NULL deadline,
 * or omitted when gone/expired/wrong generation. Busy never means lease lost.
 * Pass generations to bind renewals to specific queue incarnations.
 */
CREATE OR REPLACE FUNCTION postgremq.extend_queue_keep_alive_multi(
    p_queue_names  VARCHAR[],
    p_intervals_ms BIGINT[],
    p_generations UUID[] DEFAULT NULL
) RETURNS TABLE (queue_name VARCHAR, keep_alive_until TIMESTAMPTZ, outcome TEXT) AS $$
DECLARE r RECORD; deadline TIMESTAMPTZ;
BEGIN
    IF cardinality(p_queue_names) IS DISTINCT FROM cardinality(p_intervals_ms)
       OR (p_generations IS NOT NULL AND cardinality(p_generations) IS DISTINCT FROM cardinality(p_queue_names))
       OR EXISTS (SELECT 1 FROM unnest(p_intervals_ms) v WHERE v IS NULL OR v <= 0) THEN
        RAISE EXCEPTION 'invalid keep alive batch' USING ERRCODE = 'PMQ03';
    END IF;
    FOR r IN SELECT * FROM unnest(p_queue_names, p_intervals_ms, COALESCE(p_generations, array_fill(NULL::UUID, ARRAY[cardinality(p_queue_names)]))) AS t(name, ms, generation) ORDER BY name LOOP
        BEGIN
            SELECT q.keep_alive_until INTO deadline FROM postgremq.queues q
            WHERE q.name = r.name AND q.exclusive AND (r.generation IS NULL OR q.generation = r.generation) FOR UPDATE NOWAIT;
            IF FOUND AND deadline > clock_timestamp() THEN
                UPDATE postgremq.queues q SET keep_alive_until = clock_timestamp() + make_interval(secs => r.ms / 1000.0)
                WHERE q.name = r.name RETURNING q.keep_alive_until INTO deadline;
                RETURN QUERY SELECT r.name, deadline, 'extended'::TEXT;
            END IF;
        EXCEPTION WHEN lock_not_available THEN
            -- Contention says nothing about ownership. Retry before the known deadline.
            RETURN QUERY SELECT r.name, NULL::TIMESTAMPTZ, 'busy'::TEXT;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

---------------------------
-- Management Functions
---------------------------

/* ---------------------------------------------------------------------
 * Management Functions
 * ---------------------------------------------------------------------
 * The following functions provide administrative and utility operations
 * for the message queue system. They allow you to list topics and queues,
 * obtain queue statistics, manage the dead letter queue (DLQ), and perform
 * cleanup or deletion of topics and queues.
 */

/* Function: list_topics
 *
 * Description:
 *   Retrieves a list of all topics in the system.
 *
 * Returns:
 *   A TABLE with one column:
 *     - topic (VARCHAR): The name of the topic.
 */
CREATE OR REPLACE FUNCTION postgremq.list_topics()
RETURNS TABLE(topic VARCHAR(255)) AS $$
BEGIN
  RETURN QUERY
    SELECT topics.name AS topic
    FROM postgremq.topics
    ORDER BY topics.name;
END;
$$ LANGUAGE plpgsql;

/* Function: list_queues
 *
 * Description:
 *   Retrieves a list of all queues in the system with details including the
 *   associated topic, maximum delivery attempts (max_delivery_attempts), durability,
 *   and the keep-alive expiration time.
 *
 * Returns:
 *   A TABLE with the following columns:
 *     - queue_name (VARCHAR): The name of the queue.
 *     - topic_name (VARCHAR): The associated topic name.
 *     - max_delivery_attempts (INT): Maximum delivery attempts (-1 indicates unlimited).
 *     - exclusive (BOOLEAN): Whether the queue is exclusive (temporary).
 *     - keep_alive_until (TIMESTAMPTZ): Expiration timestamp for exclusive queues.
 */
CREATE OR REPLACE FUNCTION postgremq.list_queues()
RETURNS TABLE(
  queue_name VARCHAR(255),
  topic_name VARCHAR(255),
  max_delivery_attempts INT,
  exclusive BOOLEAN,
  keep_alive_until TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
    SELECT 
      queues.name AS queue_name,
      queues.topic_name,
      queues.max_delivery_attempts,
      queues.exclusive,
      queues.keep_alive_until
    FROM postgremq.queues
    ORDER BY queues.name;
END;
$$ LANGUAGE plpgsql;

/* Function: get_queue_statistics
 *
 * Description:
 *   Provides message statistics for a specified queue or for all queues if none is specified.
 *
 * Parameters:
 *   - p_queue (VARCHAR, optional): The name of the queue to gather statistics for.
 *
 * Returns:
 *   A TABLE with the following columns:
 *     - pending_count (BIGINT): Number of messages with status 'pending'.
 *     - processing_count (BIGINT): Number of messages with status 'processing'.
 *     - completed_count (BIGINT): Number of messages with status 'completed'.
 *     - total_count (BIGINT): Total number of messages in the queue.
 *
 * Performance note:
 *   pending_count and processing_count are served by the narrow partial
 *   indexes idx_queue_messages_pending / idx_queue_messages_processing as
 *   index-only/partial scans, so the common dashboard poll never touches the
 *   retained completed set. completed_count and total_count inherently require
 *   counting the completed rows (retained up to 24h) and are served by
 *   idx_queue_messages_completed_processed_at; total_count is derived as the
 *   sum of the three status counts rather than a separate full scan, so no
 *   call performs a heap seqscan. The return shape is unchanged (four BIGINT
 *   columns in the same order); Go/TS clients select all four by name.
 */
CREATE OR REPLACE FUNCTION postgremq.get_queue_statistics(p_queue VARCHAR(255) DEFAULT NULL)
RETURNS TABLE(
  pending_count BIGINT,
  processing_count BIGINT,
  completed_count BIGINT,
  total_count BIGINT
) AS $$
BEGIN
  RETURN QUERY
    SELECT
      v_pending,
      v_processing,
      v_completed,
      v_pending + v_processing + v_completed
    FROM (
      SELECT
        (SELECT count(*) FROM postgremq.queue_messages qm
           WHERE qm.status = 'pending'
             AND (p_queue IS NULL OR qm.queue_name = p_queue)) AS v_pending,
        (SELECT count(*) FROM postgremq.queue_messages qm
           WHERE qm.status = 'processing'
             AND (p_queue IS NULL OR qm.queue_name = p_queue)) AS v_processing,
        (SELECT count(*) FROM postgremq.queue_messages qm
           WHERE qm.status = 'completed'
             AND (p_queue IS NULL OR qm.queue_name = p_queue)) AS v_completed
    ) counts;
END;
$$ LANGUAGE plpgsql;

/* Function: list_dlq_messages
 *
 * Description:
 *   Retrieves messages that have been moved to the Dead Letter Queue (DLQ).
 *
 * Returns:
 *   A TABLE with the following columns:
 *     - queue_name (VARCHAR): Name of the queue from which the message was moved.
 *     - message_id (INT): Identifier of the message.
 *     - retry_count (INT): Number of delivery attempts made (as stored in DLQ).
 *     - published_at (TIMESTAMPTZ): Timestamp when the message was moved into the DLQ.
 */
CREATE OR REPLACE FUNCTION postgremq.list_dlq_messages()
RETURNS TABLE(
  queue_name VARCHAR(255),
  message_id BIGINT,
  retry_count INT,
  published_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
    SELECT dl.queue_name, dl.message_id, dl.retry_count, dl.published_at
    FROM postgremq.dead_letter_queue dl
    ORDER BY dl.published_at;
END;
$$ LANGUAGE plpgsql;

/* Function: requeue_dlq_messages
 *
 * Description:
 *   Moves messages from the dead letter queue back to their original queues.
 *   The delivery_attempts counter is reset to 0 for these messages.
 *
 *   A grouped message re-enters with its original group_key/group_seq, so it
 *   becomes the head of its group again (blocking later pending rows of the
 *   group). Rows of the group that already ran while it sat in the DLQ are not
 *   re-ordered: ordering across a DLQ detour is not promised.
 *
 *   Emits one NOTIFY per requeued message on `pmq:q:<queue>` so consumers
 *   that are LISTENing wake up immediately rather than waiting for their
 *   poll-fallback (1s in TS, 10s in Go).
 *
 *   ON CONFLICT DO UPDATE: if a `queue_messages` row already exists for
 *   the (queue_name, message_id) pair (operator intervention, manual
 *   seed, or future code path), reset it to a clean pending state rather
 *   than aborting the entire requeue. This keeps the function idempotent
 *   under partial-state recovery.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue to requeue messages for.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.requeue_dlq_messages(p_queue_name VARCHAR(255))
RETURNS VOID AS $$
DECLARE
    v_requeued INT;
BEGIN
    WITH moved_messages AS (
        DELETE FROM postgremq.dead_letter_queue dlq
        WHERE dlq.queue_name = p_queue_name
        RETURNING dlq.queue_name, dlq.message_id
    )
    INSERT INTO postgremq.queue_messages(queue_name, message_id, status, delivery_attempts, vt,
                                         group_key, group_seq)
    SELECT mm.queue_name, mm.message_id, 'pending', 0, clock_timestamp(), m.group_key, m.group_seq
    FROM moved_messages mm
    JOIN postgremq.messages m ON m.id = mm.message_id
    ON CONFLICT (queue_name, message_id) DO UPDATE
      SET status = 'pending',
          delivery_attempts = 0,
          consumer_token = NULL,
          vt = clock_timestamp(),
          processed_at = NULL,
          group_key = EXCLUDED.group_key,
          group_seq = EXCLUDED.group_seq;

    -- Reads ROW_COUNT of the immediately-preceding INSERT (which counts
    -- both inserted and ON-CONFLICT-updated rows). If a future change
    -- inserts another SQL between the INSERT and this line, move the
    -- diagnostics read to keep referring to the requeue count.
    GET DIAGNOSTICS v_requeued = ROW_COUNT;
    -- Emit a single wake-up so consumers re-fetch. NOTIFY payload is empty
    -- (matches the rest of the codebase — clients use the channel as the
    -- signal). Skipped when nothing was requeued to avoid spurious wakes.
    IF v_requeued > 0 THEN
        PERFORM pg_notify('pmq:q:' || p_queue_name, '');
    END IF;
END;
$$ LANGUAGE plpgsql;

/* Function: purge_dlq
 *
 * Description:
 *   Deletes all messages from the Dead Letter Queue (DLQ).
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.purge_dlq()
RETURNS VOID AS $$
BEGIN
  DELETE FROM postgremq.dead_letter_queue;
END;
$$ LANGUAGE plpgsql;

/* Function: purge_all_messages
 *
 * Description:
 *   Deletes all messages from the system. This includes messages in both the DLQ and
 *   the primary messages table.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.purge_all_messages()
RETURNS VOID AS $$
BEGIN
  DELETE FROM postgremq.dead_letter_queue;
  DELETE FROM postgremq.queue_messages;
  DELETE FROM postgremq.messages;
  PERFORM postgremq.prune_message_groups(array_agg(g.topic_name ORDER BY g.topic_name, g.group_key),
                                         array_agg(g.group_key ORDER BY g.topic_name, g.group_key))
  FROM postgremq.message_groups g;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_topic
 *
 * Description:
 *   Deletes a topic from the system. The topic cannot be deleted if any messages are associated
 *   with it. Use clean_up_topic to remove messages first if necessary.
 *
 * Parameters:
 *   - p_topic (VARCHAR): The name of the topic to be deleted.
 *
 * Returns: VOID.
 *
 * Raises:
 *   Exception if messages exist for the topic.
 */
CREATE OR REPLACE FUNCTION postgremq.delete_topic(p_topic VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM postgremq.messages WHERE topic_name = p_topic) THEN
    RAISE EXCEPTION 'Cannot delete topic "%" because messages exist. Clean up the topic first.', p_topic
      USING ERRCODE = 'PMQ03';
  END IF;
  DELETE FROM postgremq.topics WHERE name = p_topic;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_queue
 *
 * Description:
 *   Deletes a queue from the system.
 *
 * Parameters:
 *   - p_queue (VARCHAR): The name of the queue to be deleted.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.delete_queue(p_queue VARCHAR(255))
RETURNS VOID AS $$
DECLARE
  v_dlq_count INT;
BEGIN
  -- Refuse if the queue has DLQ entries. Same reasoning as clean_up_topic:
  -- forensic data the operator may want to keep. Force an explicit
  -- decision (postgremq.purge_dlq() or postgremq.requeue_dlq_messages()) before deletion.
  SELECT count(*) INTO v_dlq_count
  FROM postgremq.dead_letter_queue WHERE queue_name = p_queue;
  IF v_dlq_count > 0 THEN
    RAISE EXCEPTION 'Cannot delete queue "%": % messages are in the dead letter queue. Use postgremq.requeue_dlq_messages() or postgremq.purge_dlq() first.', p_queue, v_dlq_count
      USING ERRCODE = 'PMQ03';
  END IF;
  DELETE FROM postgremq.queues WHERE name = p_queue;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_queue_message
 *
 * Description:
 *   Deletes a specific message from an active queue. Deleting an unsettled
 *   grouped row may unblock its group's successor, so that case NOTIFYs
 *   `pmq:q:<queue>`.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): The name of the queue.
 *   - p_message_id (BIGINT): The identifier of the message to be deleted.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.delete_queue_message(p_queue_name VARCHAR(255), p_message_id BIGINT)
RETURNS VOID AS $$
DECLARE
  v_group_key VARCHAR(255);
  v_status    VARCHAR(16);
BEGIN
  DELETE FROM postgremq.queue_messages
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id
  RETURNING group_key, status INTO v_group_key, v_status;
  IF v_group_key IS NOT NULL AND v_status <> 'completed' THEN
    PERFORM pg_notify('pmq:q:' || p_queue_name, '');
  END IF;
END;
$$ LANGUAGE plpgsql;

/* Function: clean_up_queue
 *
 * Description:
 *   Removes all messages from a specified queue.
 *
 * Parameters:
 *   - p_queue (VARCHAR): The name of the queue to clean up.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.clean_up_queue(p_queue VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  DELETE FROM postgremq.queue_messages WHERE queue_name = p_queue;
END;
$$ LANGUAGE plpgsql;

/* Function: clean_up_topic
 *
 * Description:
 *   Deletes all messages associated with a specified topic.
 *
 * Parameters:
 *   - p_topic (VARCHAR): The topic whose messages are to be removed.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.clean_up_topic(p_topic VARCHAR(255))
RETURNS VOID AS $$
DECLARE
  v_dlq_count INT;
BEGIN
  -- Refuse if any messages of this topic are in a DLQ — those entries
  -- are forensic data the operator may want to keep. Force an explicit
  -- decision (postgremq.purge_dlq() or postgremq.requeue_dlq_messages()) before clean_up.
  SELECT count(*) INTO v_dlq_count
  FROM postgremq.dead_letter_queue dlq
  JOIN postgremq.messages m ON m.id = dlq.message_id
  WHERE m.topic_name = p_topic;
  IF v_dlq_count > 0 THEN
    RAISE EXCEPTION 'Cannot clean up topic "%": % messages are in the dead letter queue. Use postgremq.requeue_dlq_messages() or postgremq.purge_dlq() first.', p_topic, v_dlq_count
      USING ERRCODE = 'PMQ03';
  END IF;
  DELETE FROM postgremq.messages WHERE topic_name = p_topic;
  PERFORM postgremq.prune_message_groups(array_agg(g.topic_name ORDER BY g.group_key),
                                         array_agg(g.group_key ORDER BY g.group_key))
  FROM postgremq.message_groups g WHERE g.topic_name = p_topic;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_inactive_queues
 *
 * Description:
 *   Deletes exclusive queues that are inactive: their keep_alive_until is NULL
 *   or has already expired.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION postgremq.delete_inactive_queues()
RETURNS VOID AS $$
BEGIN
  -- See pmq_maintenance_fast: skip queues with DLQ entries; the FK
  -- is ON DELETE RESTRICT and the operator should explicitly handle
  -- DLQ before dropping the queue.
  DELETE FROM postgremq.queues q
  WHERE q.exclusive = true
    -- Strict expiry, no grace window — see pmq_maintenance_fast.
    AND (q.keep_alive_until IS NULL OR q.keep_alive_until <= clock_timestamp())
    AND NOT EXISTS (
        SELECT 1 FROM postgremq.dead_letter_queue dlq WHERE dlq.queue_name = q.name
    );
END;
$$ LANGUAGE plpgsql;

---------------------------
-- End of Implementation Script
-- ============================================================

/* Cross-queue heartbeats, correlated by (queue, message_id, consumer_token).
 * Extended rows carry a confirmed vt. Busy rows carry NULL vt; retry within
 * the known lease budget. Omitted rows have lost ownership or expired.
 * NOWAIT prevents one contended row from delaying unrelated renewals.
 */
CREATE OR REPLACE FUNCTION postgremq.set_vt_batch_multi(
    p_queue_names     VARCHAR[],
    p_message_ids     BIGINT[],
    p_consumer_tokens VARCHAR[],
    p_vts             INTEGER[]
) RETURNS TABLE (queue_name VARCHAR, message_id BIGINT, vt TIMESTAMPTZ, consumer_token VARCHAR, outcome TEXT) AS $$
DECLARE r RECORD; owned postgremq.queue_messages%ROWTYPE;
BEGIN
    IF cardinality(p_queue_names) IS DISTINCT FROM cardinality(p_message_ids)
       OR cardinality(p_message_ids) IS DISTINCT FROM cardinality(p_consumer_tokens)
       OR cardinality(p_consumer_tokens) IS DISTINCT FROM cardinality(p_vts)
       OR EXISTS (SELECT 1 FROM unnest(p_vts) v WHERE v IS NULL OR v < 0) THEN
        RAISE EXCEPTION 'invalid visibility timeout batch' USING ERRCODE = 'PMQ03';
    END IF;
    FOR r IN SELECT * FROM unnest(p_queue_names, p_message_ids, p_consumer_tokens, p_vts)
        AS t(qname, id, token, seconds) ORDER BY qname, id LOOP
        BEGIN
            SELECT qm.* INTO owned FROM postgremq.queue_messages qm
            WHERE qm.queue_name = r.qname AND qm.message_id = r.id FOR UPDATE NOWAIT;
            IF FOUND AND owned.status = 'processing' AND owned.consumer_token = r.token
               AND owned.vt > clock_timestamp() THEN
                UPDATE postgremq.queue_messages qm SET vt = clock_timestamp() + make_interval(secs => r.seconds)
                WHERE qm.queue_name = r.qname AND qm.message_id = r.id RETURNING qm.vt INTO owned.vt;
                RETURN QUERY SELECT r.qname, r.id, owned.vt, r.token, 'extended'::TEXT;
            END IF;
        EXCEPTION WHEN lock_not_available THEN
            RETURN QUERY SELECT r.qname, r.id, NULL::TIMESTAMPTZ, r.token, 'busy'::TEXT;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/* Function: list_messages
 *
 * Description:
 *   Lists all messages in a queue without consuming them.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *
 * Returns:
 *   A TABLE with message details (excluding payload).
 */
CREATE OR REPLACE FUNCTION postgremq.list_messages(p_queue_name VARCHAR(255))
RETURNS TABLE(
    message_id BIGINT,
    status VARCHAR(16),
    published_at TIMESTAMPTZ,
    delivery_attempts INT,
    vt TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    group_key VARCHAR(255),
    group_seq BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        qm.message_id,
        qm.status,
        qm.published_at,
        qm.delivery_attempts,
        qm.vt,
        qm.processed_at,
        qm.group_key,
        qm.group_seq
    FROM postgremq.queue_messages qm
    WHERE qm.queue_name = p_queue_name
    ORDER BY qm.published_at;
END;
$$ LANGUAGE plpgsql;

/* Function: get_message
 *
 * Description:
 *   Retrieves a single message by ID, including its payload.
 *
 * Parameters:
 *   - p_message_id (BIGINT): ID of the message.
 *
 * Returns:
 *   A TABLE with message details and payload.
 */
CREATE OR REPLACE FUNCTION postgremq.get_message(p_message_id BIGINT)
RETURNS TABLE(
    message_id BIGINT,
    topic_name VARCHAR(255),
    payload JSONB,
    published_at TIMESTAMPTZ,
    group_key VARCHAR(255),
    group_seq BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.id,
        m.topic_name,
        m.payload,
        m.published_at,
        m.group_key,
        m.group_seq
    FROM postgremq.messages m
    WHERE m.id = p_message_id;
END;
$$ LANGUAGE plpgsql;

/* Function: get_next_visible_time
 *
 * Description:
 *   Returns the timestamp when the next message will become visible for delivery
 *   in the specified queue. Only considers messages in 'pending' or 'processing' state
 *   that haven't exceeded their max delivery attempts.
 *   Optimized to use an index-only scan with LIMIT 1 instead of MIN() aggregation.
 *
 *   Applies consume_message's group-head predicate: a blocked successor's vt
 *   is usually already in the past, and clients schedule their next fetch at
 *   the returned time, so counting it would make them refetch in a tight loop
 *   while the head is leased. The head's own vt (lease end or nack delay) is
 *   the time the group can move.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *
 * Returns:
 *   TIMESTAMPTZ indicating when the next message will be visible, or NULL if no messages.
 */
CREATE OR REPLACE FUNCTION postgremq.get_next_visible_time(p_queue_name VARCHAR(255))
RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_next_vt TIMESTAMPTZ;
BEGIN
    SELECT qm.vt INTO v_next_vt
    FROM postgremq.queue_messages qm
    JOIN postgremq.queues q ON q.name = qm.queue_name
    WHERE qm.queue_name = p_queue_name
      AND (qm.status = 'pending' OR qm.status = 'processing')
      AND (q.max_delivery_attempts = 0 OR qm.delivery_attempts < q.max_delivery_attempts)
      AND (qm.group_key IS NULL OR NOT EXISTS (
          SELECT 1 FROM postgremq.queue_messages h
          WHERE h.queue_name = qm.queue_name
            AND h.group_key = qm.group_key
            AND h.group_key IS NOT NULL
            AND h.group_seq < qm.group_seq
            AND h.status IN ('pending', 'processing')))
    ORDER BY qm.vt ASC
    LIMIT 1;

    RETURN v_next_vt;
END;
$$ LANGUAGE plpgsql;

/* Function: cleanup_completed_messages
 *
 * Description:
 *   Removes completed messages older than the specified retention period.
 *   This function should be called periodically (e.g., via cron or scheduled task)
 *   to prevent unbounded growth of the queue_messages table.
 *
 * Parameters:
 *   - p_older_than_hours (INTEGER): Delete completed messages processed more than this many hours ago.
 *                                   Defaults to 24 hours.
 *
 * Returns:
 *   INTEGER - number of completed messages deleted.
 *
 * Example:
 *   -- Delete messages completed more than 24 hours ago
 *   SELECT postgremq.cleanup_completed_messages();
 *
 *   -- Delete messages completed more than 7 days ago
 *   SELECT postgremq.cleanup_completed_messages(168);
 */
-- Reference indexes make payload collection independent of queue count.
CREATE INDEX idx_queue_messages_message_id ON postgremq.queue_messages(message_id);
CREATE INDEX idx_dlq_message_id ON postgremq.dead_letter_queue(message_id);
CREATE INDEX idx_messages_retention ON postgremq.messages(published_at, id);

/* Function: prune_message_groups
 *
 * Description:
 *   Deletes the given message_groups rows whose group has no message left.
 *   The next publish to a pruned group restarts at group_seq 1, which is safe
 *   only if no message of the group exists anywhere — including one a
 *   publisher is about to commit. Two statements make that hold:
 *     1. Lock the candidate rows, SKIPPING rows a publisher holds (it is
 *        about to add a message, so the group is not empty).
 *     2. A separate DELETE re-checks emptiness. Under READ COMMITTED it runs
 *        with a fresh snapshot taken after step 1's locks, so it sees every
 *        message whose publisher held the lock before us; while we hold the
 *        locks no publisher can add one. (A single DELETE … WHERE NOT EXISTS
 *        is NOT enough: when it waits on a publisher's row lock, the
 *        EvalPlanQual recheck evaluates the NOT EXISTS against the statement's
 *        original snapshot, which cannot see the just-committed message.)
 *   Under REPEATABLE READ / SERIALIZABLE step 1 raises a serialization
 *   failure instead of locking a concurrently updated row, so the caller
 *   retries rather than corrupting a sequence.
 *
 * Parameters:
 *   - p_topics, p_keys (VARCHAR[]): Candidate (topic_name, group_key) pairs,
 *     element-wise.
 *
 * Returns:
 *   INT - number of group rows deleted.
 */
CREATE OR REPLACE FUNCTION postgremq.prune_message_groups(p_topics VARCHAR[], p_keys VARCHAR[])
RETURNS INT AS $$
DECLARE
    v_topics  VARCHAR[];
    v_keys    VARCHAR[];
    v_deleted INT;
BEGIN
    IF cardinality(p_topics) IS DISTINCT FROM cardinality(p_keys) THEN
        RAISE EXCEPTION 'invalid group batch' USING ERRCODE = 'PMQ03';
    END IF;
    IF coalesce(cardinality(p_topics), 0) = 0 THEN
        RETURN 0;
    END IF;

    SELECT array_agg(l.topic_name), array_agg(l.group_key) INTO v_topics, v_keys
    FROM (
        SELECT g.topic_name, g.group_key
        FROM postgremq.message_groups g
        JOIN unnest(p_topics, p_keys) AS c(topic_name, group_key)
          ON g.topic_name = c.topic_name AND g.group_key = c.group_key
        ORDER BY g.topic_name, g.group_key
        FOR UPDATE OF g SKIP LOCKED
    ) l;
    IF v_topics IS NULL THEN
        RETURN 0;
    END IF;

    DELETE FROM postgremq.message_groups g
    USING unnest(v_topics, v_keys) AS c(topic_name, group_key)
    WHERE g.topic_name = c.topic_name AND g.group_key = c.group_key
      AND NOT EXISTS (
          SELECT 1 FROM postgremq.messages m
          WHERE m.topic_name = g.topic_name AND m.group_key = g.group_key);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$ LANGUAGE plpgsql;

-- Run regularly, including when there are no completed deliveries (unrouted
-- publications and queue/DLQ deletion also leave unreferenced payloads).
-- Also prunes the message_groups rows of groups whose last message this call
-- deleted (bounded by the batch size; see prune_message_groups).
CREATE OR REPLACE FUNCTION postgremq.cleanup_unreferenced_messages(p_older_than_hours INT DEFAULT 24, p_batch_size INT DEFAULT 1000)
RETURNS INT AS $$
DECLARE
    deleted  INT;
    v_topics VARCHAR[];
    v_keys   VARCHAR[];
BEGIN
    IF p_older_than_hours < 0 OR p_batch_size <= 0 THEN
        RAISE EXCEPTION 'invalid retention or batch size' USING ERRCODE = 'PMQ03';
    END IF;
    WITH candidates AS (
        SELECT m.id FROM postgremq.messages m
        WHERE m.published_at < clock_timestamp() - make_interval(hours => p_older_than_hours)
          AND NOT EXISTS (SELECT 1 FROM postgremq.queue_messages qm WHERE qm.message_id = m.id)
          AND NOT EXISTS (SELECT 1 FROM postgremq.dead_letter_queue d WHERE d.message_id = m.id)
        ORDER BY m.published_at, m.id LIMIT p_batch_size FOR UPDATE SKIP LOCKED
    ), removed AS (
        DELETE FROM postgremq.messages m USING candidates c WHERE m.id = c.id
        RETURNING m.topic_name, m.group_key
    ), touched AS (
        SELECT DISTINCT r.topic_name, r.group_key FROM removed r WHERE r.group_key IS NOT NULL
    )
    SELECT (SELECT count(*) FROM removed),
           (SELECT array_agg(g.topic_name ORDER BY g.topic_name, g.group_key) FROM touched g),
           (SELECT array_agg(g.group_key ORDER BY g.topic_name, g.group_key) FROM touched g)
    INTO deleted, v_topics, v_keys;
    PERFORM postgremq.prune_message_groups(v_topics, v_keys);
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION postgremq.cleanup_completed_messages(p_older_than_hours INT DEFAULT 24, p_batch_size INT DEFAULT 1000)
RETURNS INT AS $$
DECLARE deleted INT;
BEGIN
    IF p_older_than_hours < 0 OR p_batch_size <= 0 THEN
        RAISE EXCEPTION 'invalid retention or batch size' USING ERRCODE = 'PMQ03';
    END IF;
    WITH candidates AS (
        SELECT qm.queue_name, qm.message_id FROM postgremq.queue_messages qm
        WHERE qm.status = 'completed'
          AND qm.processed_at < clock_timestamp() - make_interval(hours => p_older_than_hours)
        ORDER BY qm.processed_at LIMIT p_batch_size FOR UPDATE SKIP LOCKED
    ) DELETE FROM postgremq.queue_messages qm USING candidates c
      WHERE qm.queue_name = c.queue_name AND qm.message_id = c.message_id;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    PERFORM postgremq.cleanup_unreferenced_messages(p_older_than_hours, p_batch_size);
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;


-- Queue-state telemetry. One snapshot/cutoff for all queues, including empty
-- queues. Read-only and payload-free; no counters on the publication hot path.
-- Completed retention is intentionally excluded from this operational scan.
CREATE OR REPLACE FUNCTION postgremq.queue_metrics()
RETURNS TABLE (
    queue_name text, topic_name text, active bigint,
    ready bigint, delayed bigint, processing bigint, exhausted bigint,
    dead_letter bigint, oldest_ready_age_seconds double precision
)
LANGUAGE sql STABLE AS $$
    WITH cutoff AS MATERIALIZED (SELECT statement_timestamp() AS ts),
    live AS (
        SELECT qm.queue_name,
            count(*) FILTER (WHERE qm.vt <= c.ts AND
                (q.max_delivery_attempts = 0 OR qm.delivery_attempts < q.max_delivery_attempts)) AS ready,
            count(*) FILTER (WHERE qm.status = 'pending' AND qm.vt > c.ts) AS delayed,
            count(*) FILTER (WHERE qm.status = 'processing' AND qm.vt > c.ts) AS processing,
            count(*) FILTER (WHERE qm.vt <= c.ts AND q.max_delivery_attempts > 0
                AND qm.delivery_attempts >= q.max_delivery_attempts) AS exhausted,
            min(qm.vt) FILTER (WHERE qm.vt <= c.ts AND
                (q.max_delivery_attempts = 0 OR qm.delivery_attempts < q.max_delivery_attempts)) AS ready_since
        FROM postgremq.queue_messages qm
        JOIN postgremq.queues q ON q.name = qm.queue_name
        CROSS JOIN cutoff c
        WHERE qm.status IN ('pending', 'processing')
        GROUP BY qm.queue_name
    ), dead AS (
        SELECT d.queue_name, count(*) AS n
        FROM postgremq.dead_letter_queue d GROUP BY d.queue_name
    )
    SELECT q.name::text, q.topic_name::text,
        CASE WHEN NOT q.exclusive OR q.keep_alive_until > c.ts THEN 1::bigint ELSE 0::bigint END,
        CASE WHEN NOT q.exclusive OR q.keep_alive_until > c.ts THEN coalesce(l.ready, 0) ELSE 0 END,
        coalesce(l.delayed, 0), coalesce(l.processing, 0), coalesce(l.exhausted, 0), coalesce(d.n, 0),
        CASE WHEN NOT q.exclusive OR q.keep_alive_until > c.ts
            THEN coalesce(extract(epoch FROM c.ts - l.ready_since)::double precision, 0)
            ELSE 0::double precision END
    FROM postgremq.queues q
    CROSS JOIN cutoff c
    LEFT JOIN live l ON l.queue_name = q.name
    LEFT JOIN dead d ON d.queue_name = q.name;
$$;
