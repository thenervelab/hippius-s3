-- migrate:up

-- Add MD5 hash column for S3 ETag compatibility
ALTER TABLE objects ADD COLUMN md5_hash TEXT;

-- Create index for efficient ETag lookups
CREATE INDEX idx_objects_md5_hash ON objects(md5_hash);

-- migrate:down
DROP INDEX IF EXISTS idx_objects_md5_hash;
ALTER TABLE objects DROP COLUMN IF EXISTS md5_hash;
