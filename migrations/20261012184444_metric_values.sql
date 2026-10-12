-- Metric values (spec 9.1 MetricValue). Append-only; a writer gets an INSERT grant when the
-- first pack that computes metrics arrives.
CREATE TABLE metric_values (
  organisation_id uuid        NOT NULL REFERENCES organisations(id),
  name            text        NOT NULL,
  version         int         NOT NULL CHECK (version >= 1),
  subject_id      uuid        NOT NULL,
  as_of           timestamptz NOT NULL,
  value           jsonb       NOT NULL,
  unit            text,
  recorded_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organisation_id, name, subject_id, as_of, version)
);
ALTER TABLE metric_values ENABLE ROW LEVEL SECURITY;
ALTER TABLE metric_values FORCE ROW LEVEL SECURITY;
CREATE POLICY org_isolation ON metric_values USING (organisation_id = (SELECT kuki_org_id()));
GRANT SELECT ON metric_values TO kuki_app;
