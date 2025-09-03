-- migrate:up

-- Add object_id column to parts table for direct object reference
ALTER TABLE parts ADD COLUMN object_id UUID REFERENCES objects(object_id) ON DELETE CASCADE;

-- Create index for efficient lookups
CREATE INDEX idx_parts_object_id ON parts(object_id);

-- Populate object_id for existing parts by joining through multipart_uploads
UPDATE parts
SET object_id = mu.object_id
FROM multipart_uploads mu
WHERE parts.upload_id = mu.upload_id
AND mu.object_id IS NOT NULL;

-- migrate:down

-- Remove the index and column
DROP INDEX IF EXISTS idx_parts_object_id;
ALTER TABLE parts DROP COLUMN IF EXISTS object_id;
