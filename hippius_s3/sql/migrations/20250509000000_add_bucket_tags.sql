-- migrate:up
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '{}'::jsonb;

-- migrate:down
ALTER TABLE buckets DROP COLUMN IF EXISTS tags;
