-- The action accepting a decision of this type queues. Packs declare it; the evaluator
-- copies it into each recommendation it creates. NULL means accepting queues nothing.
ALTER TABLE decision_types
  ADD COLUMN default_action_type text
  CHECK (default_action_type IS NULL OR default_action_type ~ '^[a-z][a-z0-9_.:-]{0,127}$');
