-- Add object_id column to parts table to link parts directly to objects
ALTER TABLE parts ADD COLUMN object_id UUID REFERENCES objects(object_id) ON DELETE CASCADE;

-- Create index for efficient lookups by object_id
CREATE INDEX idx_parts_object_id ON parts(object_id);
