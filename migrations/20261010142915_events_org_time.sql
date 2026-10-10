-- Unfiltered event feed: newest first within an organisation.
CREATE INDEX events_org_time ON events (organisation_id, occurred_at DESC, id DESC);
