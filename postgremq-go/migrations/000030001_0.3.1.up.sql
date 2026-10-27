-- ── change: create_queue_concurrent_delete · patch ─────────────────────────

-- create_queue no longer reports a misleading error when its queue or topic
-- is deleted concurrently:
--
-- - A redeclaration used to insert with ON CONFLICT DO NOTHING and then lock
--   the existing row with a separate SELECT ... FOR UPDATE. Under READ
--   COMMITTED, a delete_queue committing between the two left the SELECT
--   with no row, and the call raised PMQ03 "already exists with different
--   parameters" with every existing value NULL. It now inserts or locks the
--   existing row in one statement (ON CONFLICT DO UPDATE ... WHERE false,
--   which locks without updating), so the queue is created anew. Under
--   REPEATABLE READ and SERIALIZABLE the race raises 40001, as before.
-- - A topic deleted between create_queue's existence check and its insert
--   made the insert fail with a raw 23503 FK violation. That violation is now
--   raised as PMQ02 "does not exist", the same error as a missing topic.
--
-- Same signature: CREATE OR REPLACE keeps the function's owner and grants.
-- Privileges: every call, including one that creates a new queue, now needs
-- UPDATE on queues.name (ON CONFLICT DO UPDATE checks it up front). Before,
-- creating a new queue needed no UPDATE privilege, and a redeclaration's row
-- lock accepted UPDATE on any column. The SELECT and INSERT requirements, and
-- UPDATE on keep_alive_until for an exclusive queue's redeclaration, are
-- unchanged. The runtime grants documented in mq/README.md (UPDATE on the
-- whole table) already cover it.

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
    -- Same length cap as create_topic: NOTIFY channel names are limited to
    -- 63 bytes; the settle and requeue paths emit on 'pmq:q:'||queue_name, leaving
    -- 57 bytes for the queue name.
    IF octet_length(p_queue_name) > 57 THEN
        RAISE EXCEPTION 'Queue name "%" is too long: maximum 57 bytes (limit imposed by NOTIFY channel length: 63 bytes minus the "pmq:q:" prefix)', p_queue_name
          USING ERRCODE = 'PMQ03';
    END IF;
    IF p_keep_alive_interval IS NULL OR p_keep_alive_interval <= interval '0' THEN
        RAISE EXCEPTION 'keep alive interval must be positive' USING ERRCODE = 'PMQ03';
    END IF;
    -- A negative max_delivery_attempts would silently break consume_message's
    -- filter (qm.delivery_attempts < v_max_attempts is never true, as
    -- delivery_attempts is never negative) and never retire to the DLQ
    -- (nack_message and pmq_maintenance_fast both gate on max_attempts > 0).
    -- Reject it up front so callers see PMQ03 instead of a silently broken queue.
    IF p_max_attempts IS NULL OR p_max_attempts < 0 THEN
        RAISE EXCEPTION 'p_max_attempts must be >= 0 (got %)', p_max_attempts
          USING ERRCODE = 'PMQ03';
    END IF;
    IF p_topic_name IS NULL OR p_exclusive IS NULL THEN
        RAISE EXCEPTION 'p_topic_name and p_exclusive must not be NULL' USING ERRCODE = 'PMQ03';
    END IF;
    -- A missing topic raises PMQ02 (parity with publish_message), not the raw
    -- 23503 FK violation the INSERT below would raise. This check takes no
    -- lock, so the topic can still be deleted between it and the INSERT; the
    -- block below turns the INSERT's FK violation into the same PMQ02.
    -- Locking the topic here instead (FOR KEY SHARE) would make a
    -- redeclaration hold the topic while it waits on the queue row, and a
    -- transaction that deletes the queue and then its topic would deadlock
    -- with it.
    IF NOT EXISTS (SELECT 1 FROM postgremq.topics WHERE name = p_topic_name) THEN
        RAISE EXCEPTION 'Topic "%" does not exist', p_topic_name
          USING ERRCODE = 'PMQ02';
    END IF;

    -- The rest of the function runs in a block whose handler turns the FK
    -- violation of a topic deleted after the check above into that PMQ02.
    -- topic_name is the table's only foreign key, and it is not deferrable,
    -- so the violation is raised by the INSERT itself, inside the block. The
    -- handler costs a subtransaction per call (a caller transaction with more
    -- than 64 of them overflows the backend's subtransaction cache, which
    -- slows other sessions' visibility checks while it stays open). The whole
    -- insert-or-check path is inside it (rather than the INSERT alone) so
    -- that the row lock and the keep-alive UPDATE below come from the same
    -- (sub)transaction; split across two, an exclusive queue's redeclaration
    -- would turn the row's lock into a MultiXact.
    BEGIN
        -- Insert the queue, or lock the existing one, in a single statement.
        --
        -- ON CONFLICT DO NOTHING followed by a separate SELECT ... FOR UPDATE
        -- is not safe: a concurrent delete_queue (or the reaping of an
        -- expired queue) can commit between the two statements, and the
        -- SELECT then finds no row. ON CONFLICT DO UPDATE has no such gap:
        -- PostgreSQL either inserts the row or locks the conflicting one. If
        -- that row is deleted or updated by another transaction while this
        -- one waits to lock it, PostgreSQL retries the whole insert itself
        -- under READ COMMITTED, and raises 40001 under REPEATABLE READ and
        -- SERIALIZABLE.
        --
        -- The update must not change anything: the existing row is compared
        -- with the arguments below, and a redeclaration with different
        -- parameters must fail rather than overwrite it. WHERE false skips the
        -- update, but PostgreSQL has already locked the conflicting row ("all
        -- rows will be locked when the ON CONFLICT DO UPDATE action is
        -- taken", INSERT docs), and no new row version is written. The lock
        -- lasts until the transaction ends. SET name, the key column, makes
        -- it FOR UPDATE, the lock a redeclaration has always taken; a
        -- non-key column would take FOR NO KEY UPDATE.
        --
        -- RETURNING yields a row only when the INSERT happened. For an
        -- existing queue the skipped update returns nothing and FOUND is false.
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
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name WHERE false
        RETURNING generation INTO v_generation;

        IF FOUND THEN
            RETURN v_generation;  -- inserted on the fast path
        END IF;

        -- Conflict path: the queue exists and this transaction holds its row
        -- lock, so no other transaction can delete or change it. Under READ
        -- COMMITTED this SELECT takes a new snapshot and sees exactly that
        -- row. FOR UPDATE matters under REPEATABLE READ and SERIALIZABLE: the
        -- transaction's snapshot can still show an older row of this name,
        -- deleted by a transaction that committed after the snapshot (this
        -- transaction then inserted the locked row; a second create_queue
        -- reaches here). Locking that row raises 40001, where STRICT alone
        -- would raise too_many_rows. STRICT makes any other violation an
        -- error rather than a comparison against NULLs.
        SELECT * INTO STRICT v_existing FROM postgremq.queues
        WHERE name = p_queue_name FOR UPDATE;

        -- Verify the caller's parameters match the existing row; otherwise
        -- raise so accidental config drift is caught loudly rather than
        -- silently ignored.
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
    EXCEPTION WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'Topic "%" does not exist', p_topic_name
          USING ERRCODE = 'PMQ02';
    END;
END;
$$ LANGUAGE plpgsql;
