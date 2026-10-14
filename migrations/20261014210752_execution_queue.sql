-- Accepting a recommendation queues its action execution in the same transaction.
-- The app role may insert queued rows; the dispatcher claims and finishes them
-- through SECURITY DEFINER functions (added with the dispatcher).
GRANT INSERT ON action_executions TO kuki_app;
