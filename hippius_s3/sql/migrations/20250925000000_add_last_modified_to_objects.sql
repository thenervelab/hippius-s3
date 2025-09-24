-- migrate:up

-- Add last_modified column to objects to track modification time
ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS last_modified TIMESTAMPTZ DEFAULT NOW();

-- Optional index for queries ordering/filtering by last_modified
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects(last_modified);

-- migrate:down

-- Drop index and column
DROP INDEX IF EXISTS idx_objects_last_modified;
ALTER TABLE objects DROP COLUMN IF EXISTS last_modified;
