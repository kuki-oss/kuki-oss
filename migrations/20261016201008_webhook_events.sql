-- Status-event webhooks. Receivers are registered per organisation (webhook_subscriptions);
-- each matching Kafka event becomes one durable row per receiver (webhook_deliveries), which a
-- worker delivers with leases and backoff. The signing secret is never stored here: secret_ref
-- is an OpenBao KV path.

CREATE TABLE webhook_subscriptions (
    id              uuid PRIMARY KEY,
    organisation_id uuid NOT NULL REFERENCES organisations (id),
    name            text NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    url             text NOT NULL CHECK (url ~ '^https?://[^[:space:]]+$'),
    secret_ref      text NOT NULL
        CHECK (secret_ref ~ '^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$'),
    event_types     text[] NOT NULL DEFAULT ARRAY['recommendation.status_changed']
        CHECK (cardinality(event_types) > 0
           AND event_types <@ ARRAY['recommendation.created', 'recommendation.updated',
                                    'recommendation.status_changed']),
    enabled         boolean NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (organisation_id, name)
);

CREATE TABLE webhook_deliveries (
    id              uuid PRIMARY KEY,
    organisation_id uuid NOT NULL REFERENCES organisations (id),
    subscription_id uuid NOT NULL REFERENCES webhook_subscriptions (id) ON DELETE CASCADE,
    -- The outbox row id (kuki-outbox-id header): stable across Kafka redelivery, so the
    -- consumer is idempotent and receivers can dedupe on it.
    event_id        text NOT NULL,
    event_type      text NOT NULL,
    payload         jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'delivered', 'dead')),
    attempts        int NOT NULL DEFAULT 0,
    not_before      timestamptz NOT NULL DEFAULT now(),
    claimed_at      timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    UNIQUE (subscription_id, event_id)
);

CREATE INDEX webhook_deliveries_runnable ON webhook_deliveries (not_before, created_at, id)
  WHERE status IN ('queued', 'running');

ALTER TABLE webhook_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_subscriptions FORCE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries FORCE ROW LEVEL SECURITY;

-- Reuse the tenant-isolation predicate enforced on audit_log, as for webhook_endpoints.
DO $$
DECLARE
    q text;
    t text;
BEGIN
    SELECT qual INTO q
      FROM pg_policies
     WHERE schemaname = 'public' AND tablename = 'audit_log' AND qual IS NOT NULL
     LIMIT 1;
    IF q IS NULL THEN
        RAISE EXCEPTION 'no tenant policy found on audit_log to reuse';
    END IF;
    FOREACH t IN ARRAY ARRAY['webhook_subscriptions', 'webhook_deliveries'] LOOP
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I USING (%s) WITH CHECK (%s)', t, q, q);
    END LOOP;
END $$;

-- The app role manages subscriptions, and queues and reads deliveries. Every other change to a
-- delivery goes through the delivery_* functions below.
REVOKE ALL ON webhook_subscriptions, webhook_deliveries FROM kuki_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON webhook_subscriptions TO kuki_app;
GRANT SELECT, INSERT ON webhook_deliveries TO kuki_app;

-- Delivering is cross-organisation work, like running executions (0012): same role.
GRANT SELECT ON webhook_subscriptions TO kuki_executor;
GRANT SELECT, UPDATE ON webhook_deliveries TO kuki_executor;

-- Takes the oldest due deliveries not under a live lease, for enabled subscriptions only.
-- A 'running' row whose lease has expired is taken over, so delivery is at least once.
CREATE FUNCTION delivery_claim(p_limit int, p_lease_seconds float8)
RETURNS TABLE (id uuid, organisation_id uuid, subscription_id uuid, event_id text,
               event_type text, payload jsonb, attempts int, url text, secret_ref text)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  WITH due AS (
    SELECT d.id FROM webhook_deliveries d
      JOIN webhook_subscriptions s ON s.id = d.subscription_id AND s.enabled
     WHERE d.status IN ('queued', 'running')
       AND d.not_before <= now()
       AND (d.claimed_at IS NULL OR d.claimed_at < now() - make_interval(secs => p_lease_seconds))
     ORDER BY d.created_at, d.id
     LIMIT p_limit
       FOR UPDATE OF d SKIP LOCKED
  )
  UPDATE webhook_deliveries d
     SET status = 'running', claimed_at = now(), attempts = d.attempts + 1
    FROM due, webhook_subscriptions s
   WHERE d.id = due.id AND s.id = d.subscription_id
  RETURNING d.id, d.organisation_id, d.subscription_id, d.event_id, d.event_type, d.payload,
            d.attempts, s.url, s.secret_ref
$$;

-- Ends a running delivery. False when it is not running or belongs to another organisation.
CREATE FUNCTION delivery_finish(p_org uuid, p_id uuid, p_status text, p_error text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE n int;
BEGIN
  IF p_status NOT IN ('delivered', 'dead') THEN
    RAISE EXCEPTION 'delivery_finish: % is not a final status', p_status;
  END IF;
  UPDATE webhook_deliveries
     SET status = p_status, finished_at = now(), claimed_at = NULL,
         last_error = coalesce(left(p_error, 1000), last_error)
   WHERE id = p_id AND organisation_id = p_org AND status = 'running';
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n = 1;
END $$;

-- Puts a running delivery back in the queue, not to be claimed before the delay has passed.
CREATE FUNCTION delivery_retry(p_org uuid, p_id uuid, p_error text, p_delay_seconds float8)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE n int;
BEGIN
  UPDATE webhook_deliveries
     SET status = 'queued', claimed_at = NULL,
         not_before = now() + make_interval(secs => greatest(p_delay_seconds, 0)),
         last_error = left(p_error, 1000)
   WHERE id = p_id AND organisation_id = p_org AND status = 'running';
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n = 1;
END $$;

ALTER FUNCTION delivery_claim(int, float8)               OWNER TO kuki_executor;
ALTER FUNCTION delivery_finish(uuid, uuid, text, text)   OWNER TO kuki_executor;
ALTER FUNCTION delivery_retry(uuid, uuid, text, float8)  OWNER TO kuki_executor;

REVOKE ALL ON FUNCTION delivery_claim(int, float8),
                       delivery_finish(uuid, uuid, text, text),
                       delivery_retry(uuid, uuid, text, float8) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION delivery_claim(int, float8),
                          delivery_finish(uuid, uuid, text, text),
                          delivery_retry(uuid, uuid, text, float8) TO kuki_app;
