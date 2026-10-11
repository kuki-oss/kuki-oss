-- The relay publishes outbox rows for every organisation, so it cannot run under per-tenant
-- row-level security. Rather than give the application role BYPASSRLS, the relay reaches the
-- outbox only through the functions below, which are owned by a NOLOGIN role that has BYPASSRLS
-- and privileges on nothing but the outbox. (Creating a BYPASSRLS role needs a privileged
-- migration user, the same as creating kuki_app.)
DO $$
BEGIN
  CREATE ROLE kuki_relay NOLOGIN NOSUPERUSER BYPASSRLS NOCREATEDB NOCREATEROLE;
EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
END $$;

GRANT USAGE ON SCHEMA public TO kuki_relay;
GRANT SELECT, UPDATE ON outbox TO kuki_relay;

ALTER TABLE outbox ADD COLUMN dead_at timestamptz;
ALTER TABLE outbox ADD COLUMN last_error text;

-- Takes the oldest rows that are not delivered, not dead and not under a live lease.
CREATE FUNCTION outbox_claim(p_limit int, p_lease_seconds float8)
RETURNS TABLE (id bigint, organisation_id uuid, topic text, key text, payload jsonb,
               created_at timestamptz, attempts int)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  WITH due AS (
    SELECT o.id FROM outbox o
     WHERE o.delivered_at IS NULL
       AND o.dead_at IS NULL
       AND (o.claimed_at IS NULL OR o.claimed_at < now() - make_interval(secs => p_lease_seconds))
     ORDER BY o.id
     LIMIT p_limit
       FOR UPDATE SKIP LOCKED
  )
  UPDATE outbox o
     SET claimed_at = now(), attempts = o.attempts + 1
    FROM due
   WHERE o.id = due.id
  RETURNING o.id, o.organisation_id, o.topic, o.key, o.payload, o.created_at, o.attempts
$$;

CREATE FUNCTION outbox_mark_published(p_ids bigint[]) RETURNS bigint
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  WITH u AS (
    UPDATE outbox SET delivered_at = now()
     WHERE id = ANY (p_ids) AND delivered_at IS NULL
    RETURNING 1
  )
  SELECT count(*) FROM u
$$;

-- Makes rows claimable again immediately, so a failed row is retried before newer ones.
CREATE FUNCTION outbox_release(p_ids bigint[]) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  UPDATE outbox SET claimed_at = NULL WHERE id = ANY (p_ids) AND delivered_at IS NULL
$$;

-- A message the broker will never accept (for example, too large). Alert on dead_at IS NOT NULL.
CREATE FUNCTION outbox_mark_dead(p_id bigint, p_error text) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  UPDATE outbox SET dead_at = now(), last_error = left(p_error, 1000)
   WHERE id = p_id AND delivered_at IS NULL
$$;

ALTER FUNCTION outbox_claim(int, float8)       OWNER TO kuki_relay;
ALTER FUNCTION outbox_mark_published(bigint[]) OWNER TO kuki_relay;
ALTER FUNCTION outbox_release(bigint[])        OWNER TO kuki_relay;
ALTER FUNCTION outbox_mark_dead(bigint, text)  OWNER TO kuki_relay;

REVOKE ALL ON FUNCTION outbox_claim(int, float8), outbox_mark_published(bigint[]),
                       outbox_release(bigint[]), outbox_mark_dead(bigint, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION outbox_claim(int, float8), outbox_mark_published(bigint[]),
                          outbox_release(bigint[]), outbox_mark_dead(bigint, text) TO kuki_app;
