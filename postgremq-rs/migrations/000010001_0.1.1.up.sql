-- ── change: derive_protocol_major · patch ──────────────────────────────────

-- Clients now check the mq major, read from schema_version, instead of a
-- separate protocol major. info() still reports protocol_major for the
-- clients that check it (up to 0.3.0), now derived from the version: the mq
-- major + 1, so 1 for every 0.x schema and 2 from mq 1.0.0, which those
-- clients then refuse instead of failing later.
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
    RETURN jsonb_build_object('schema_version', v_schema_version,
                              'protocol_major', coalesce(v_schema_version / 100000000, 0) + 1);
END;
$$;
