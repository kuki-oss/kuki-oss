-- Kuki OSS schema, release R0. Forward-only.
-- NOTE: Runs as the owning role (a superuser in dev and CI). The application connects as kuki_app.

CREATE EXTENSION IF NOT EXISTS ltree;

DO $$
BEGIN
    -- No password here: the operator sets one (ALTER ROLE kuki_app PASSWORD '...').
    CREATE ROLE kuki_app LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
EXCEPTION
    -- A concurrent migration (parallel tests, two replicas booting) can create it first.
    -- A lost race surfaces as unique_violation, a pre-existing role as duplicate_object.
    WHEN duplicate_object OR unique_violation THEN NULL;
END $$;

-- ───────────────────────── tenancy ─────────────────────────
CREATE TABLE organisations (
  id          uuid PRIMARY KEY,
  slug        text NOT NULL UNIQUE,
  name        text NOT NULL,
  region      text,
  config      jsonb NOT NULL DEFAULT '{}',
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE org_units (
  id               uuid PRIMARY KEY,
  organisation_id  uuid NOT NULL REFERENCES organisations(id),
  path             ltree NOT NULL,
  name             text NOT NULL,
  attrs            jsonb NOT NULL DEFAULT '{}',
  UNIQUE (organisation_id, path)
);
CREATE INDEX org_units_path_gist ON org_units USING GIST (path);

-- Outside RLS on purpose: authentication happens before the tenant is known.
-- kuki_app has no privileges on this table; it goes through auth_lookup_api_key().
CREATE TABLE api_keys (
  id                   uuid PRIMARY KEY,
  key_id               text NOT NULL UNIQUE CHECK (key_id ~ '^[a-z0-9]{12}$'),
  organisation_id      uuid NOT NULL REFERENCES organisations(id),
  name                 text NOT NULL,
  secret_hash          bytea NOT NULL CHECK (octet_length(secret_hash) = 32),
  scopes               text[] NOT NULL DEFAULT '{}',
  org_unit_scope_path  ltree,
  created_at           timestamptz NOT NULL DEFAULT now(),
  expires_at           timestamptz,
  revoked_at           timestamptz,
  last_used_at         timestamptz
);

-- ───────────────────────── session helpers ─────────────────────────
-- The store sets kuki.org_id and kuki.scope_path per transaction (set_config(..., true)).
-- An unset or empty org id yields NULL, so a connection with no context sees no rows.
CREATE FUNCTION kuki_org_id() RETURNS uuid LANGUAGE sql STABLE AS
$$ SELECT nullif(current_setting('kuki.org_id', true), '')::uuid $$;

CREATE FUNCTION kuki_scope_path() RETURNS ltree LANGUAGE sql STABLE AS
$$ SELECT nullif(current_setting('kuki.scope_path', true), '')::ltree $$;

CREATE FUNCTION kuki_unit_visible(p_unit uuid) RETURNS boolean LANGUAGE sql STABLE AS
$$ SELECT kuki_scope_path() IS NULL
       OR EXISTS (SELECT 1 FROM org_units u WHERE u.id = p_unit AND u.path <@ kuki_scope_path()) $$;

-- ───────────────────────── registry ─────────────────────────
CREATE TABLE object_types (
  organisation_id uuid NOT NULL,
  name            text NOT NULL,
  version         int  NOT NULL,
  json_schema     jsonb NOT NULL,
  PRIMARY KEY (organisation_id, name, version)
);

CREATE TABLE event_types (
  organisation_id uuid NOT NULL,
  name            text NOT NULL,
  version         int  NOT NULL,
  subject_type    text NOT NULL,
  json_schema     jsonb NOT NULL,
  PRIMARY KEY (organisation_id, name, version)
);

CREATE TABLE decision_types (
  organisation_id    uuid NOT NULL,
  name               text NOT NULL,
  pack               text NOT NULL,
  payload_schema_ref text NOT NULL,
  payload_schema     jsonb NOT NULL,
  PRIMARY KEY (organisation_id, name)
);

-- ───────────────────────── ontology ─────────────────────────
-- id is the STABLE object id; every version is a row keyed (id, valid_from).
CREATE TABLE objects (
  id              uuid NOT NULL,
  organisation_id uuid NOT NULL,
  org_unit_id     uuid,
  type            text NOT NULL,
  type_version    int  NOT NULL,
  external_ids    jsonb NOT NULL DEFAULT '{}',
  attrs           jsonb NOT NULL,
  valid_from      timestamptz NOT NULL,
  valid_to        timestamptz,
  recorded_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, valid_from)
);
CREATE UNIQUE INDEX objects_one_current ON objects (id) WHERE valid_to IS NULL;
CREATE INDEX objects_current  ON objects (organisation_id, type) WHERE valid_to IS NULL;
CREATE INDEX objects_external ON objects USING GIN (external_ids jsonb_path_ops);

-- src_id and dst_id reference stable object ids, so there is no foreign key (see notes).
CREATE TABLE links (
  id              uuid PRIMARY KEY,
  organisation_id uuid NOT NULL,
  type            text NOT NULL,
  src_id          uuid NOT NULL,
  dst_id          uuid NOT NULL,
  attrs           jsonb NOT NULL DEFAULT '{}',
  valid_from      timestamptz NOT NULL,
  valid_to        timestamptz
);
CREATE UNIQUE INDEX links_one_current ON links (organisation_id, type, src_id, dst_id) WHERE valid_to IS NULL;
CREATE INDEX links_src ON links (organisation_id, type, src_id) WHERE valid_to IS NULL;
CREATE INDEX links_dst ON links (organisation_id, type, dst_id) WHERE valid_to IS NULL;

-- Append-only ledger, partitioned monthly by occurred_at.
CREATE TABLE events (
  id              uuid NOT NULL,
  organisation_id uuid NOT NULL,
  type            text NOT NULL,
  type_version    int  NOT NULL,
  subject_id      uuid NOT NULL,
  occurred_at     timestamptz NOT NULL,
  recorded_at     timestamptz NOT NULL DEFAULT now(),
  payload         jsonb NOT NULL,
  supersedes      uuid,
  source          text NOT NULL,
  idempotency_key text,
  PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);
CREATE INDEX events_subject_time ON events (organisation_id, subject_id, occurred_at DESC);
CREATE INDEX events_idem ON events (organisation_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Rows outside every monthly partition land here. Alert if this table is ever non-empty.
CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE FUNCTION kuki_ensure_event_partitions(p_months_ahead int DEFAULT 3) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  first_day date;
  lo timestamptz;
  hi timestamptz;
  i  int;
BEGIN
  FOR i IN -1..p_months_ahead LOOP
    first_day := (date_trunc('month', now() AT TIME ZONE 'UTC') + make_interval(months => i))::date;
    lo := first_day::timestamp AT TIME ZONE 'UTC';
    hi := (first_day + interval '1 month')::timestamp AT TIME ZONE 'UTC';
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
                   'events_' || to_char(first_day, 'YYYY_MM'), lo, hi);
  END LOOP;
END $$;
SELECT kuki_ensure_event_partitions(3);

CREATE FUNCTION kuki_events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'events is append-only: % is not allowed (corrections are new events)', TG_OP;
END $$;
CREATE TRIGGER events_append_only BEFORE UPDATE OR DELETE ON events
  FOR EACH ROW EXECUTE FUNCTION kuki_events_append_only();

-- ───────────────────────── ingest ─────────────────────────
CREATE TABLE idempotency_keys (
  organisation_id uuid NOT NULL REFERENCES organisations(id),
  key             text NOT NULL,
  request_hash    bytea NOT NULL,
  state           text NOT NULL CHECK (state IN ('in_progress', 'completed')),
  status_code     int,
  response        jsonb,
  created_at      timestamptz NOT NULL DEFAULT now(),
  expires_at      timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (organisation_id, key)
);

CREATE TABLE ingest_batches (
  id              uuid PRIMARY KEY,
  organisation_id uuid NOT NULL REFERENCES organisations(id),
  kind            text NOT NULL CHECK (kind IN ('events', 'objects', 'links', 'replay')),
  idempotency_key text,
  mode            text NOT NULL DEFAULT 'live' CHECK (mode IN ('live', 'backfill')),
  backfill_id     text,
  source          text NOT NULL,
  received_at     timestamptz NOT NULL DEFAULT now(),
  accepted_count  int NOT NULL DEFAULT 0,
  rejected_count  int NOT NULL DEFAULT 0
);

CREATE TABLE ingest_quarantine (
  id              uuid PRIMARY KEY,
  organisation_id uuid NOT NULL REFERENCES organisations(id),
  batch_id        uuid NOT NULL REFERENCES ingest_batches(id),
  row_index       int  NOT NULL CHECK (row_index >= 0),
  received_at     timestamptz NOT NULL DEFAULT now(),
  code            text NOT NULL,
  message         text NOT NULL,
  field           text,
  status          text NOT NULL DEFAULT 'quarantined' CHECK (status IN ('quarantined', 'replayed')),
  raw             jsonb NOT NULL,
  replayed_at     timestamptz
);
CREATE INDEX quarantine_batch ON ingest_quarantine (organisation_id, batch_id, row_index);
CREATE INDEX quarantine_open  ON ingest_quarantine (organisation_id, id) WHERE status = 'quarantined';

CREATE TABLE backfills (
  organisation_id uuid NOT NULL REFERENCES organisations(id),
  id              text NOT NULL,
  status          text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'completing', 'completed', 'expired')),
  opened_at       timestamptz NOT NULL DEFAULT now(),
  expires_at      timestamptz NOT NULL,
  completed_at    timestamptz,
  event_count     bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (organisation_id, id)
);

-- ───────────────────────── delivery ─────────────────────────
CREATE TABLE outbox (
  id              bigserial PRIMARY KEY,
  organisation_id uuid NOT NULL,
  topic           text NOT NULL,
  key             text NOT NULL,
  payload         jsonb NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  claimed_at      timestamptz,
  delivered_at    timestamptz,
  attempts        int NOT NULL DEFAULT 0
);
CREATE INDEX outbox_undelivered ON outbox (id) WHERE delivered_at IS NULL;

-- ───────────────────────── decisions ─────────────────────────
CREATE TABLE recommendations (
  id                 uuid PRIMARY KEY,
  organisation_id    uuid NOT NULL,
  org_unit_id        uuid,
  decision_type      text NOT NULL,
  dedup_key          text NOT NULL,
  status             text NOT NULL CHECK (status IN
                       ('pending','accepted','rejected','expired','superseded',
                        'executing','executed','failed','auto_executed')),
  urgency            numeric(4,3) NOT NULL CHECK (urgency BETWEEN 0 AND 1),
  confidence         numeric(4,3) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  rationale_summary  text NOT NULL,
  body               jsonb NOT NULL,
  provenance         jsonb NOT NULL,
  revision_history   jsonb NOT NULL DEFAULT '[]',
  resolution         jsonb,
  version            int NOT NULL DEFAULT 1,
  created_at         timestamptz NOT NULL DEFAULT now(),
  last_confirmed_at  timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  resolved_at        timestamptz,
  resolved_by        text,
  expires_at         timestamptz
);
-- At most one pending recommendation per dedup key, enforced by the database.
CREATE UNIQUE INDEX recs_one_pending_per_key ON recommendations (organisation_id, dedup_key) WHERE status = 'pending';
CREATE INDEX recs_inbox   ON recommendations (organisation_id, status, urgency DESC, created_at DESC);
CREATE INDEX recs_unit    ON recommendations (organisation_id, org_unit_id, status);
CREATE INDEX recs_changes ON recommendations (organisation_id, updated_at, id);

CREATE FUNCTION kuki_recommendations_touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.version    := OLD.version + 1;
  NEW.updated_at := now();
  RETURN NEW;
END $$;
CREATE TRIGGER recommendations_touch BEFORE UPDATE ON recommendations
  FOR EACH ROW EXECUTE FUNCTION kuki_recommendations_touch();

CREATE TABLE action_executions (
  id                 uuid PRIMARY KEY,
  organisation_id    uuid NOT NULL,
  recommendation_id  uuid NOT NULL REFERENCES recommendations(id),
  action_type        text NOT NULL,
  input              jsonb NOT NULL,
  idempotency_key    text NOT NULL,
  status             text NOT NULL CHECK (status IN
                       ('queued','running','succeeded','failed','compensated','cancelled')),
  adapter            text NOT NULL,
  result             jsonb,
  attempts           int NOT NULL DEFAULT 0,
  created_by         text NOT NULL,
  created_at         timestamptz NOT NULL DEFAULT now(),
  finished_at        timestamptz,
  UNIQUE (organisation_id, idempotency_key)
);

-- ───────────────────────── authentication functions ─────────────────────────
CREATE FUNCTION auth_lookup_api_key(p_key_id text)
RETURNS TABLE (id uuid, organisation_id uuid, secret_hash bytea, scopes text[],
               org_unit_scope_path ltree, expires_at timestamptz, revoked_at timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  SELECT k.id, k.organisation_id, k.secret_hash, k.scopes, k.org_unit_scope_path, k.expires_at, k.revoked_at
    FROM api_keys k WHERE k.key_id = p_key_id
$$;

-- Throttled in SQL so a hot key cannot become a write hotspot.
CREATE FUNCTION auth_touch_api_key(p_id uuid) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  UPDATE api_keys SET last_used_at = now()
   WHERE id = p_id AND (last_used_at IS NULL OR last_used_at < now() - interval '5 minutes')
$$;
REVOKE ALL ON FUNCTION auth_lookup_api_key(text), auth_touch_api_key(uuid) FROM PUBLIC;

-- ───────────────────────── row-level security ─────────────────────────
ALTER TABLE organisations ENABLE ROW LEVEL SECURITY;
ALTER TABLE organisations FORCE ROW LEVEL SECURITY;
CREATE POLICY org_isolation ON organisations USING (id = (SELECT kuki_org_id()));

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'org_units', 'object_types', 'event_types', 'decision_types', 'objects', 'links', 'events',
    'idempotency_keys', 'ingest_batches', 'ingest_quarantine', 'backfills', 'outbox',
    'recommendations', 'action_executions'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('CREATE POLICY org_isolation ON %I USING (organisation_id = (SELECT kuki_org_id()))', t);
  END LOOP;
END $$;

-- Org-unit scoping is RESTRICTIVE, so it is ANDed with org_isolation.
CREATE POLICY unit_scope ON org_units AS RESTRICTIVE
  USING (kuki_scope_path() IS NULL OR path <@ kuki_scope_path());
CREATE POLICY unit_scope ON objects AS RESTRICTIVE
  USING (org_unit_id IS NULL OR kuki_unit_visible(org_unit_id));
CREATE POLICY unit_scope ON recommendations AS RESTRICTIVE
  USING (kuki_unit_visible(org_unit_id));

-- ───────────────────────── privileges for the application role ─────────────────────────
GRANT USAGE ON SCHEMA public TO kuki_app;
GRANT SELECT ON organisations, org_units TO kuki_app;
GRANT SELECT, INSERT ON object_types, event_types, decision_types TO kuki_app;
GRANT SELECT, INSERT, UPDATE ON objects, links, recommendations, action_executions,
      idempotency_keys, ingest_batches, ingest_quarantine, backfills, outbox TO kuki_app;
GRANT SELECT, INSERT ON events TO kuki_app;            -- no UPDATE or DELETE, ever
GRANT USAGE, SELECT ON SEQUENCE outbox_id_seq TO kuki_app;
GRANT EXECUTE ON FUNCTION auth_lookup_api_key(text), auth_touch_api_key(uuid) TO kuki_app;
