ALTER TABLE events ADD COLUMN backfill_id text;
CREATE INDEX events_backfill ON events (organisation_id, backfill_id)
    WHERE backfill_id IS NOT NULL;
