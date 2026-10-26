-- ── change: first_delivery_wait · major ────────────────────────────────────

-- consume_message returns first_delivery_wait_ms: how long a row waited for
-- its first delivery, from when it became deliverable (its distribution, or a
-- later deliver_after) to its first claim, by the database clock. The claim
-- is recorded once in queue_messages.first_delivered_at; release, nack and
-- lease expiry keep it. A DLQ requeue makes the row new (published_at, vt and
-- delivery_attempts reset), so requeue_dlq_messages clears it as well and the
-- next claim reports the wait since the requeue. A new result column changes
-- consume_message's result type, so it is recreated (keeping its owner and
-- grants).
--
-- Rows delivered before this migration (processing, or pending after a nack)
-- get the migration time: their first delivery is unknown, and their next
-- claim must not report one. A row released back to delivery_attempts 0
-- cannot be told from a fresh one; its next claim reports the wait since the
-- release. A 0.2 claim waiting for this migration's locks when it commits
-- runs the 0.2 consume_message and leaves first_delivered_at unset (its next
-- redelivery then reports a wait): consumers are stopped before migrating.
-- The backfill runs under the ALTER TABLE lock, so with many
-- in-flight or retrying rows the migration blocks queue_messages for as long
-- as it takes to rewrite them.
--
-- This migration's ALTER TABLEs lock messages, queue_messages and
-- dead_letter_queue until it commits. They are taken here, together and in
-- the order a publish takes them (messages, then queue_messages in the
-- distribution trigger): taken one by one, a publish arriving in between
-- would hold messages while waiting for queue_messages, deadlock with the
-- migration, and could leave the migration aborted and its version dirty.

LOCK TABLE postgremq.messages, postgremq.queue_messages, postgremq.dead_letter_queue
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE postgremq.queue_messages ADD COLUMN first_delivered_at TIMESTAMPTZ;

UPDATE postgremq.queue_messages SET first_delivered_at = now()
WHERE status = 'processing' OR (status = 'pending' AND delivery_attempts > 0);

ALTER FUNCTION postgremq.consume_message(VARCHAR, INTEGER, INT, UUID) RENAME TO pmq_replaced_consume_message;

CREATE FUNCTION postgremq.consume_message(
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
    vt_remaining_ms BIGINT,
    published_at TIMESTAMPTZ,
    group_key VARCHAR(255),
    group_seq BIGINT,
    first_delivery_wait_ms BIGINT
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
    -- returns zero rows with no error (the common idle case). PMQ02 is raised
    -- for an absent queue row, an expired exclusive queue, and a generation
    -- mismatch.
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
               qm.published_at,
               -- Before the update: on a row never claimed, vt is still the
               -- message's deliver_after (distribute_message sets it).
               qm.vt,
               qm.first_delivered_at
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
        -- (an O(n log n) cliff on a deep backlog). At distribution vt is the
        -- message's deliver_after, about published_at for an undelayed message,
        -- so fresh messages keep roughly FIFO order. Tradeoff: REDELIVERED
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
        -- Per-lease ownership token: a random v4 UUID, fresh per claim.
        consumer_token = gen_random_uuid()::text,
        first_delivered_at = COALESCE(qm.first_delivered_at, clock_timestamp())
    FROM next_msg qm
    WHERE queue_messages.queue_name = qm.queue_name
        AND queue_messages.message_id = qm.message_id
    RETURNING queue_messages.queue_name,
              queue_messages.message_id,
              (SELECT m.payload FROM postgremq.messages m WHERE m.id = queue_messages.message_id) AS payload,
              queue_messages.consumer_token,
              queue_messages.delivery_attempts,
              floor(extract(epoch FROM queue_messages.vt - clock_timestamp()) * 1000)::BIGINT,
              queue_messages.published_at,
              queue_messages.group_key,
              queue_messages.group_seq,
              -- The wait starts when the row became deliverable: GREATEST of
              -- its distribution and its deliver_after (a past deliver_after
              -- counts from the publish).
              CASE WHEN qm.first_delivered_at IS NULL THEN
                  GREATEST(floor(extract(epoch FROM queue_messages.first_delivered_at
                                         - GREATEST(qm.published_at, qm.vt)) * 1000), 0)::BIGINT
              END;
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
          published_at = clock_timestamp(),
          vt = clock_timestamp(),
          processed_at = NULL,
          group_key = EXCLUDED.group_key,
          group_seq = EXCLUDED.group_seq,
          first_delivered_at = NULL;

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

DO $$
DECLARE f RECORD; g RECORD;
BEGIN
    FOR f IN SELECT t.old_signature, t.new_signature, pg_get_userbyid(p.proowner) AS owner,
                    coalesce(p.proacl, acldefault('f', p.proowner)) AS acl
             FROM (VALUES ('postgremq.pmq_replaced_consume_message(VARCHAR, INTEGER, INT, UUID)',
                           'postgremq.consume_message(VARCHAR, INTEGER, INT, UUID)')) AS t(old_signature, new_signature)
             JOIN pg_proc p ON p.oid = t.old_signature::regprocedure LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO %I', f.new_signature, f.owner);
        -- Clear what CREATE gave (the current defaults), then grant the old acl.
        FOR g IN SELECT * FROM aclexplode((SELECT coalesce(p.proacl, acldefault('f', p.proowner))
                                           FROM pg_proc p WHERE p.oid = f.new_signature::regprocedure)) LOOP
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s', f.new_signature,
                           CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(g.grantee)) END);
        END LOOP;
        FOR g IN SELECT * FROM aclexplode(f.acl) LOOP
            EXECUTE format('GRANT %s ON FUNCTION %s TO %s%s', g.privilege_type, f.new_signature,
                           CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(g.grantee)) END,
                           CASE WHEN g.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
        END LOOP;
        EXECUTE format('DROP FUNCTION %s', f.old_signature);
    END LOOP;
END;
$$;

-- ── change: published_at_not_null · major ──────────────────────────────────

-- published_at was nullable on messages, queue_messages and dead_letter_queue,
-- though every insert takes its DEFAULT clock_timestamp(), so only a direct
-- table write could leave it NULL. consume_message's first-delivery wait
-- counts from queue_messages.published_at, and payload retention never
-- removes a message whose published_at is NULL, so the columns become NOT
-- NULL. A NULL left by such a write gets the migration time. SET NOT NULL
-- scans each table under its ACCESS EXCLUSIVE lock.

UPDATE postgremq.messages SET published_at = now() WHERE published_at IS NULL;
UPDATE postgremq.queue_messages SET published_at = now() WHERE published_at IS NULL;
UPDATE postgremq.dead_letter_queue SET published_at = now() WHERE published_at IS NULL;

ALTER TABLE postgremq.messages ALTER COLUMN published_at SET NOT NULL;
ALTER TABLE postgremq.queue_messages ALTER COLUMN published_at SET NOT NULL;
ALTER TABLE postgremq.dead_letter_queue ALTER COLUMN published_at SET NOT NULL;
