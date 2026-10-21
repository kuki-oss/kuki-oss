-- What a generator in shadow mode would have proposed. Never read by the inbox, the review
-- budget or the API: shadow rows are only reachable through `kuki shadow report`.
CREATE TABLE shadow_proposals (
    organisation_id uuid        NOT NULL REFERENCES organisations (id),
    generator_id    text        NOT NULL CHECK (generator_id <> ''),
    dedup_key       text        NOT NULL CHECK (dedup_key <> ''),
    decision_type   text        NOT NULL,
    org_unit_id     uuid,
    rationale_summary text      NOT NULL,
    body            jsonb       NOT NULL,
    provenance      jsonb       NOT NULL,
    first_seen_at   timestamptz NOT NULL,
    last_seen_at    timestamptz NOT NULL,
    -- how many runs proposed this row
    seen_count      integer     NOT NULL DEFAULT 1 CHECK (seen_count >= 1),
    PRIMARY KEY (organisation_id, generator_id, dedup_key)
);

CREATE INDEX shadow_proposals_last_seen
    ON shadow_proposals (organisation_id, generator_id, last_seen_at);

ALTER TABLE shadow_proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE shadow_proposals FORCE ROW LEVEL SECURITY;
CREATE POLICY org_isolation ON shadow_proposals
    USING (organisation_id = (SELECT kuki_org_id()));

GRANT SELECT, INSERT, UPDATE, DELETE ON shadow_proposals TO kuki_app;
