-- Running executions is cross-organisation work, so like the outbox relay it cannot run under
-- per-tenant row-level security. The functions below are owned by a NOLOGIN BYPASSRLS role whose
-- only privileges are on action_executions. The app role calls them and cannot update the table.
DO $$
BEGIN
  CREATE ROLE kuki_executor NOLOGIN NOSUPERUSER BYPASSRLS NOCREATEDB NOCREATEROLE;
EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
END $$;

GRANT USAGE ON SCHEMA public TO kuki_executor;
GRANT SELECT, UPDATE ON action_executions TO kuki_executor;

-- The app role queues executions (INSERT) and reads them; every other change goes through the
-- execution_* functions below. Reset explicitly, because 20261001235248 granted more than that.
REVOKE ALL ON action_executions FROM kuki_app;
GRANT SELECT, INSERT ON action_executions TO kuki_app;

ALTER TABLE action_executions
  ADD COLUMN claimed_at timestamptz,
  ADD COLUMN not_before timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN last_error text;

CREATE INDEX action_executions_runnable ON action_executions (not_before, created_at, id)
  WHERE status IN ('queued', 'running');

-- Takes the oldest due executions that are not under a live lease. A 'running' row whose lease
-- has expired is taken over (its worker died), so delivery is at least once.
CREATE FUNCTION execution_claim(p_limit int, p_lease_seconds float8)
RETURNS TABLE (id uuid, organisation_id uuid, recommendation_id uuid, action_type text,
               input jsonb, adapter text, attempts int, created_by text)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  WITH due AS (
    SELECT e.id FROM action_executions e
     WHERE e.status IN ('queued', 'running')
       AND e.not_before <= now()
       AND (e.claimed_at IS NULL OR e.claimed_at < now() - make_interval(secs => p_lease_seconds))
     ORDER BY e.created_at, e.id
     LIMIT p_limit
       FOR UPDATE SKIP LOCKED
  )
  UPDATE action_executions e
     SET status = 'running', claimed_at = now(), attempts = e.attempts + 1
    FROM due
   WHERE e.id = due.id
  RETURNING e.id, e.organisation_id, e.recommendation_id, e.action_type, e.input,
            e.adapter, e.attempts, e.created_by
$$;

-- Ends a running execution. False when it is not running or belongs to another organisation.
CREATE FUNCTION execution_finish(p_org uuid, p_id uuid, p_status text, p_adapter text, p_result jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE n int;
BEGIN
  IF p_status NOT IN ('succeeded', 'failed') THEN
    RAISE EXCEPTION 'execution_finish: % is not a final status', p_status;
  END IF;
  UPDATE action_executions
     SET status = p_status, adapter = p_adapter, result = p_result,
         finished_at = now(), claimed_at = NULL
   WHERE id = p_id AND organisation_id = p_org AND status = 'running';
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n = 1;
END $$;

-- Puts a running execution back in the queue, not to be claimed before the delay has passed.
CREATE FUNCTION execution_retry(p_org uuid, p_id uuid, p_adapter text, p_error text, p_delay_seconds float8)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE n int;
BEGIN
  UPDATE action_executions
     SET status = 'queued', adapter = p_adapter, claimed_at = NULL,
         not_before = now() + make_interval(secs => greatest(p_delay_seconds, 0)),
         last_error = left(p_error, 1000)
   WHERE id = p_id AND organisation_id = p_org AND status = 'running';
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n = 1;
END $$;

ALTER FUNCTION execution_claim(int, float8)                    OWNER TO kuki_executor;
ALTER FUNCTION execution_finish(uuid, uuid, text, text, jsonb) OWNER TO kuki_executor;
ALTER FUNCTION execution_retry(uuid, uuid, text, text, float8) OWNER TO kuki_executor;

REVOKE ALL ON FUNCTION execution_claim(int, float8),
                       execution_finish(uuid, uuid, text, text, jsonb),
                       execution_retry(uuid, uuid, text, text, float8) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION execution_claim(int, float8),
                          execution_finish(uuid, uuid, text, text, jsonb),
                          execution_retry(uuid, uuid, text, text, float8) TO kuki_app;
