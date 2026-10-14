-- Append-only audit trail (kuki-extension AuditEntry). Written in the same transaction as the
-- decision it records. No UPDATE or DELETE grant, ever!
CREATE TABLE audit_log (
  id              uuid        PRIMARY KEY,
  organisation_id uuid        NOT NULL REFERENCES organisations(id),
  occurred_at     timestamptz NOT NULL,
  actor           text        NOT NULL,
  action          text        NOT NULL,
  subject         text        NOT NULL,
  details         jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_log_org_time ON audit_log (organisation_id, occurred_at DESC, id DESC);
CREATE INDEX audit_log_subject  ON audit_log (organisation_id, subject, occurred_at);
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log FORCE ROW LEVEL SECURITY;
CREATE POLICY org_isolation ON audit_log USING (organisation_id = (SELECT kuki_org_id()));
GRANT SELECT, INSERT ON audit_log TO kuki_app;
