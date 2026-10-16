-- Webhook endpoint registry: one endpoint per (organisation, action type).
-- The signing secret is NEVER stored here. secret_ref is a path into OpenBao KV
-- (e.g. "kuki/webhooks/acme/draft_po"); the value is read at delivery time.
CREATE TABLE webhook_endpoints (
    id              uuid PRIMARY KEY,
    organisation_id uuid NOT NULL REFERENCES organisations (id),
    action_type     text NOT NULL CHECK (action_type <> ''),
    url             text NOT NULL CHECK (url ~ '^https?://[^[:space:]]+$'),
    secret_ref      text NOT NULL
        CHECK (secret_ref ~ '^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$'),
    enabled         boolean NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (organisation_id, action_type)
);

ALTER TABLE webhook_endpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_endpoints FORCE ROW LEVEL SECURITY;

-- Reuse the tenant-isolation predicate already enforced on audit_log (0010), so
-- there is a single definition of "this organisation" in the schema.
DO $$
DECLARE
    q text;
BEGIN
    SELECT qual INTO q
      FROM pg_policies
     WHERE schemaname = 'public' AND tablename = 'audit_log' AND qual IS NOT NULL
     LIMIT 1;
    IF q IS NULL THEN
        RAISE EXCEPTION 'no tenant policy found on audit_log to reuse';
    END IF;
    EXECUTE format(
        'CREATE POLICY tenant_isolation ON webhook_endpoints USING (%s) WITH CHECK (%s)',
        q, q);
END $$;

GRANT SELECT, INSERT, UPDATE, DELETE ON webhook_endpoints TO kuki_app;
