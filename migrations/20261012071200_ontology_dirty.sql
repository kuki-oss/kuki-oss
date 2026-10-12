-- Which entities changed since the evaluator last looked. Filled by triggers, drained by the
-- evaluator service. Keyed so a bulk load collapses to one row per entity.
CREATE TABLE ontology_dirty (
  organisation_id uuid NOT NULL,
  entity_id       uuid NOT NULL,
  marked_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organisation_id, entity_id)
);
CREATE INDEX ontology_dirty_age ON ontology_dirty (marked_at);

-- kuki_app never touches the table: triggers and the functions below run as the owner.
REVOKE ALL ON ontology_dirty FROM PUBLIC;
REVOKE ALL ON ontology_dirty FROM kuki_app;

CREATE FUNCTION kuki_mark_dirty() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
  CASE TG_ARGV[0]
    WHEN 'object' THEN
      INSERT INTO ontology_dirty (organisation_id, entity_id)
      VALUES (NEW.organisation_id, NEW.id) ON CONFLICT DO NOTHING;
    WHEN 'link' THEN
      INSERT INTO ontology_dirty (organisation_id, entity_id)
      VALUES (NEW.organisation_id, NEW.src_id), (NEW.organisation_id, NEW.dst_id)
      ON CONFLICT DO NOTHING;
    WHEN 'event' THEN
      INSERT INTO ontology_dirty (organisation_id, entity_id)
      VALUES (NEW.organisation_id, NEW.subject_id) ON CONFLICT DO NOTHING;
  END CASE;
  RETURN NULL;
END
$$;

-- A new version, or a version being closed, changes the entity.
CREATE TRIGGER objects_mark_dirty AFTER INSERT OR UPDATE OF valid_to ON objects
  FOR EACH ROW EXECUTE FUNCTION kuki_mark_dirty('object');
CREATE TRIGGER links_mark_dirty AFTER INSERT OR UPDATE OF valid_to ON links
  FOR EACH ROW EXECUTE FUNCTION kuki_mark_dirty('link');
-- On the partitioned parent: every partition, present and future, inherits it.
CREATE TRIGGER events_mark_dirty AFTER INSERT ON events
  FOR EACH ROW EXECUTE FUNCTION kuki_mark_dirty('event');

-- Evaluator-side access. Claim is cross-tenant by nature (one service serves every organisation);
-- until dedicated login roles exist it is executable by kuki_app, like outbox_claim.
CREATE FUNCTION ontology_dirty_claim(p_limit int, p_min_age_secs float8)
RETURNS TABLE (org_id uuid, entity uuid)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
  WITH picked AS (
    SELECT d.organisation_id, d.entity_id
      FROM ontology_dirty d
     WHERE d.marked_at <= now() - make_interval(secs => p_min_age_secs)
     ORDER BY d.marked_at, d.organisation_id, d.entity_id
     LIMIT p_limit
       FOR UPDATE SKIP LOCKED
  )
  DELETE FROM ontology_dirty t USING picked p
   WHERE t.organisation_id = p.organisation_id AND t.entity_id = p.entity_id
  RETURNING t.organisation_id, t.entity_id
$$;

-- Puts entities back after a failed run. Only for the organisation the session is set to.
CREATE FUNCTION ontology_dirty_mark(p_org uuid, p_ids uuid[]) RETURNS void
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
  INSERT INTO ontology_dirty (organisation_id, entity_id)
  SELECT p_org, unnest(p_ids) WHERE p_org = kuki_org_id()
  ON CONFLICT DO NOTHING
$$;

-- Organisations that have configured at least one generator: the scheduled tick's worklist.
CREATE FUNCTION evaluator_organisations() RETURNS SETOF uuid
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
  SELECT o.id FROM organisations o
   WHERE jsonb_typeof(o.config -> 'decision' -> 'generators') = 'object'
     AND (o.config -> 'decision' -> 'generators') <> '{}'::jsonb
   ORDER BY o.id
$$;

REVOKE ALL ON FUNCTION ontology_dirty_claim(int, float8) FROM PUBLIC;
REVOKE ALL ON FUNCTION ontology_dirty_mark(uuid, uuid[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION evaluator_organisations() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ontology_dirty_claim(int, float8) TO kuki_app;
GRANT EXECUTE ON FUNCTION ontology_dirty_mark(uuid, uuid[]) TO kuki_app;
GRANT EXECUTE ON FUNCTION evaluator_organisations() TO kuki_app;
