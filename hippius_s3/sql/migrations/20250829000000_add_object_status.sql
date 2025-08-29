-- migrate:up

-- Add status column to objects table
ALTER TABLE objects ADD COLUMN status TEXT DEFAULT 'publishing' CHECK (status IN ('publishing', 'pinning', 'uploaded'));

-- Add index for status queries
CREATE INDEX idx_objects_status ON objects(status);

-- migrate:down
DROP INDEX IF EXISTS idx_objects_status;
ALTER TABLE objects DROP COLUMN IF EXISTS status;
