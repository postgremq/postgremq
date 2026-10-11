-- Test-only fault injection. PostgreSQL sequences survive statement rollback,
-- so the first publication aborts and the second attempt succeeds.
CREATE SEQUENCE public.metrics_retry_probe;
CREATE FUNCTION public.metrics_retry_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF nextval('public.metrics_retry_probe') = 1 THEN
        RAISE EXCEPTION 'injected serialization failure' USING ERRCODE = '40001';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER metrics_retry_once BEFORE INSERT ON postgremq.messages
FOR EACH ROW EXECUTE FUNCTION public.metrics_retry_once();
