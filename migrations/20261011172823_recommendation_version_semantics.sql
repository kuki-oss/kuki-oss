-- version and updated_at move only when something other than a re-confirmation changes.
-- last_confirmed_at ("still true as of") and expires_at can be refreshed on every evaluation
-- without producing a new version, an updated_at bump or an event.
CREATE OR REPLACE FUNCTION kuki_recommendations_touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - 'last_confirmed_at' - 'expires_at' - 'version' - 'updated_at')
     IS DISTINCT FROM
     (to_jsonb(OLD) - 'last_confirmed_at' - 'expires_at' - 'version' - 'updated_at') THEN
    NEW.version    := OLD.version + 1;
    NEW.updated_at := now();
  ELSE
    NEW.version    := OLD.version;
    NEW.updated_at := OLD.updated_at;
  END IF;
  RETURN NEW;
END $$;
