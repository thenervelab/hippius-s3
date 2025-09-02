-- migrate:up
-- Add object_id column to multipart_uploads table to link completed uploads to final objects
ALTER TABLE multipart_uploads ADD COLUMN object_id UUID REFERENCES objects(object_id) ON DELETE SET NULL;

-- Create index for efficient lookups by object_id
CREATE INDEX idx_multipart_uploads_object_id ON multipart_uploads(object_id);

-- migrate:down
DROP INDEX idx_multipart_uploads_object_id;
ALTER TABLE multipart_uploads DROP COLUMN object_id;
