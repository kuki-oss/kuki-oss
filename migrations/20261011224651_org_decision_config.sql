-- The decision-engine configuration lives under organisations.config -> 'decision'.
-- kuki_app reads it through this function, never from the table, and only for the
-- organisation set on the session (kuki.org_id). Asking for any other org yields NULL.
CREATE FUNCTION org_decision_config(p_org uuid) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
  SELECT COALESCE(o.config -> 'decision', '{}'::jsonb)
    FROM organisations o
   WHERE o.id = p_org AND p_org = kuki_org_id()
$$;

REVOKE ALL ON FUNCTION org_decision_config(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION org_decision_config(uuid) TO kuki_app;
