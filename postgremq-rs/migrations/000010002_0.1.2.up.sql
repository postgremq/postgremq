-- ── change: reject_null_arguments · patch ──────────────────────────────────

-- A NULL in a required argument raises PMQ03. The range checks (p_limit <= 0
-- and similar) let NULL through: a NULL consume_message batch size reached
-- LIMIT NULL and leased the whole queue, and other NULLs surfaced as raw
-- 23502 errors, a misleading PMQ01/PMQ02, or a silent no-op. NULL keeps its
-- meaning for publish_message's deliver_after and group_key, the generations,
-- get_queue_statistics(NULL) and prune_message_groups(NULL, NULL).

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
    IF p_max_attempts IS NULL OR p_max_attempts < 0 THEN
        RAISE EXCEPTION 'p_max_attempts must be >= 0 (got %)', p_max_attempts
          USING ERRCODE = 'PMQ03';
    END IF;
    IF p_topic_name IS NULL OR p_exclusive IS NULL THEN
        RAISE EXCEPTION 'p_topic_name and p_exclusive must not be NULL' USING ERRCODE = 'PMQ03';
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
    IF p_topic IS NULL OR p_payload IS NULL THEN
        RAISE EXCEPTION 'p_topic and p_payload must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
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
    -- NULL must be rejected explicitly: it passes the range checks, and a
    -- NULL p_limit would reach LIMIT NULL (no limit) and lease the whole queue.
    IF p_queue_name IS NULL THEN
        RAISE EXCEPTION 'p_queue_name must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
    IF p_vt IS NULL OR p_vt < 0 THEN
        RAISE EXCEPTION 'p_vt must be >= 0 (got %)', p_vt USING ERRCODE = 'PMQ03';
    END IF;
    IF p_limit IS NULL OR p_limit <= 0 THEN
        RAISE EXCEPTION 'p_limit must be > 0 (got %)', p_limit USING ERRCODE = 'PMQ03';
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

CREATE OR REPLACE FUNCTION postgremq.ack_message(p_queue_name VARCHAR(255), p_message_id BIGINT, p_consumer_token VARCHAR(64))
RETURNS VOID AS $$
DECLARE
  v_group_key VARCHAR(255);
BEGIN
  IF p_queue_name IS NULL OR p_message_id IS NULL OR p_consumer_token IS NULL THEN
    RAISE EXCEPTION 'Ack failed: queue name, message id and consumer token must not be NULL'
      USING ERRCODE = 'PMQ03';
  END IF;
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
    IF p_queue_name IS NULL OR p_message_id IS NULL OR p_consumer_token IS NULL THEN
        RAISE EXCEPTION 'Nack failed: queue name, message id and consumer token must not be NULL'
          USING ERRCODE = 'PMQ03';
    END IF;
    IF p_delay_until IS NULL THEN
        RAISE EXCEPTION 'Nack failed: p_delay_until must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
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

CREATE OR REPLACE FUNCTION postgremq.release_message(
    p_queue_name VARCHAR(255),
    p_message_id BIGINT,
    p_consumer_token VARCHAR(64)
)
    RETURNS VOID AS $$
BEGIN
    IF p_queue_name IS NULL OR p_message_id IS NULL OR p_consumer_token IS NULL THEN
        RAISE EXCEPTION 'Release message failed: queue name, message id and consumer token must not be NULL'
          USING ERRCODE = 'PMQ03';
    END IF;
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

CREATE OR REPLACE FUNCTION postgremq.set_vt(
    p_queue_name VARCHAR(255),
    p_message_id BIGINT,
    p_consumer_token VARCHAR(64),
    p_vt INTEGER
) RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_vt TIMESTAMPTZ;
BEGIN
    IF p_queue_name IS NULL OR p_message_id IS NULL OR p_consumer_token IS NULL THEN
        RAISE EXCEPTION 'Extend lock failed: queue name, message id and consumer token must not be NULL'
          USING ERRCODE = 'PMQ03';
    END IF;
    IF p_vt IS NULL OR p_vt < 0 THEN
        RAISE EXCEPTION 'p_vt must be >= 0 (got %)', p_vt USING ERRCODE = 'PMQ03';
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

CREATE OR REPLACE FUNCTION postgremq.extend_queue_keep_alive_multi(
    p_queue_names  VARCHAR[],
    p_intervals_ms BIGINT[],
    p_generations UUID[] DEFAULT NULL
) RETURNS TABLE (queue_name VARCHAR, keep_alive_until TIMESTAMPTZ, outcome TEXT) AS $$
DECLARE r RECORD; deadline TIMESTAMPTZ;
BEGIN
    -- Both arrays NULL would pass the length check (equal NULL cardinalities).
    IF p_queue_names IS NULL OR p_intervals_ms IS NULL
       OR cardinality(p_queue_names) IS DISTINCT FROM cardinality(p_intervals_ms)
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

CREATE OR REPLACE FUNCTION postgremq.requeue_dlq_messages(p_queue_name VARCHAR(255))
RETURNS VOID AS $$
DECLARE
    v_requeued INT;
BEGIN
    IF p_queue_name IS NULL THEN
        RAISE EXCEPTION 'p_queue_name must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
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

CREATE OR REPLACE FUNCTION postgremq.delete_topic(p_topic VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  IF p_topic IS NULL THEN
    RAISE EXCEPTION 'p_topic must not be NULL' USING ERRCODE = 'PMQ03';
  END IF;
  IF EXISTS (SELECT 1 FROM postgremq.messages WHERE topic_name = p_topic) THEN
    RAISE EXCEPTION 'Cannot delete topic "%" because messages exist. Clean up the topic first.', p_topic
      USING ERRCODE = 'PMQ03';
  END IF;
  DELETE FROM postgremq.topics WHERE name = p_topic;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION postgremq.delete_queue(p_queue VARCHAR(255))
RETURNS VOID AS $$
DECLARE
  v_dlq_count INT;
BEGIN
  IF p_queue IS NULL THEN
    RAISE EXCEPTION 'p_queue must not be NULL' USING ERRCODE = 'PMQ03';
  END IF;
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

CREATE OR REPLACE FUNCTION postgremq.delete_queue_message(p_queue_name VARCHAR(255), p_message_id BIGINT)
RETURNS VOID AS $$
DECLARE
  v_group_key VARCHAR(255);
  v_status    VARCHAR(16);
BEGIN
  IF p_queue_name IS NULL OR p_message_id IS NULL THEN
    RAISE EXCEPTION 'p_queue_name and p_message_id must not be NULL' USING ERRCODE = 'PMQ03';
  END IF;
  DELETE FROM postgremq.queue_messages
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id
  RETURNING group_key, status INTO v_group_key, v_status;
  IF v_group_key IS NOT NULL AND v_status <> 'completed' THEN
    PERFORM pg_notify('pmq:q:' || p_queue_name, '');
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION postgremq.clean_up_queue(p_queue VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  IF p_queue IS NULL THEN
    RAISE EXCEPTION 'p_queue must not be NULL' USING ERRCODE = 'PMQ03';
  END IF;
  DELETE FROM postgremq.queue_messages WHERE queue_name = p_queue;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION postgremq.clean_up_topic(p_topic VARCHAR(255))
RETURNS VOID AS $$
DECLARE
  v_dlq_count INT;
BEGIN
  IF p_topic IS NULL THEN
    RAISE EXCEPTION 'p_topic must not be NULL' USING ERRCODE = 'PMQ03';
  END IF;
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

CREATE OR REPLACE FUNCTION postgremq.set_vt_batch_multi(
    p_queue_names     VARCHAR[],
    p_message_ids     BIGINT[],
    p_consumer_tokens VARCHAR[],
    p_vts             INTEGER[]
) RETURNS TABLE (queue_name VARCHAR, message_id BIGINT, vt TIMESTAMPTZ, consumer_token VARCHAR, outcome TEXT) AS $$
DECLARE r RECORD; owned postgremq.queue_messages%ROWTYPE;
BEGIN
    -- All four arrays NULL would pass the length checks (equal NULL cardinalities).
    IF p_queue_names IS NULL OR p_message_ids IS NULL OR p_consumer_tokens IS NULL OR p_vts IS NULL
       OR cardinality(p_queue_names) IS DISTINCT FROM cardinality(p_message_ids)
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
    IF p_queue_name IS NULL THEN
        RAISE EXCEPTION 'p_queue_name must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
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
    IF p_message_id IS NULL THEN
        RAISE EXCEPTION 'p_message_id must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
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

CREATE OR REPLACE FUNCTION postgremq.get_next_visible_time(p_queue_name VARCHAR(255))
RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_next_vt TIMESTAMPTZ;
BEGIN
    IF p_queue_name IS NULL THEN
        RAISE EXCEPTION 'p_queue_name must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
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

CREATE OR REPLACE FUNCTION postgremq.cleanup_unreferenced_messages(p_older_than_hours INT DEFAULT 24, p_batch_size INT DEFAULT 1000)
RETURNS INT AS $$
DECLARE
    deleted  INT;
    v_topics VARCHAR[];
    v_keys   VARCHAR[];
BEGIN
    -- NULL must be rejected explicitly: a NULL p_batch_size would reach
    -- LIMIT NULL (no limit).
    IF p_older_than_hours IS NULL OR p_batch_size IS NULL
       OR p_older_than_hours < 0 OR p_batch_size <= 0 THEN
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
    -- NULL must be rejected explicitly: a NULL p_batch_size would reach
    -- LIMIT NULL (no limit).
    IF p_older_than_hours IS NULL OR p_batch_size IS NULL
       OR p_older_than_hours < 0 OR p_batch_size <= 0 THEN
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
