-- migrate:up

-- Drop the existing constraint
ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_status_check;

-- Add updated constraint that includes 'failed' status
ALTER TABLE objects ADD CONSTRAINT objects_status_check
CHECK (status = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text, 'failed'::text]));

-- migrate:down

-- Revert to original constraint (remove failed status)
ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_status_check;
ALTER TABLE objects ADD CONSTRAINT objects_status_check
CHECK (status = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text]));
