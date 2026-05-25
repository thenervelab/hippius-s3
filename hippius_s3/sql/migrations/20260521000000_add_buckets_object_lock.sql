-- migrate:up
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS object_lock JSONB;

-- migrate:down
ALTER TABLE buckets DROP COLUMN IF EXISTS object_lock;
