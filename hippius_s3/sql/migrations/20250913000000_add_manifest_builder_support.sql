-- migrate:up

-- Add manifest builder support to objects table
ALTER TABLE objects
ADD COLUMN manifest_cid TEXT,
ADD COLUMN manifest_built_for_version INT,
ADD COLUMN manifest_built_at TIMESTAMPTZ,
ADD COLUMN last_append_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Create index for efficient manifest builder queries
CREATE INDEX idx_objects_manifest_builder ON objects(last_append_at, append_version)
WHERE manifest_built_for_version IS NULL OR append_version > manifest_built_for_version;

-- migrate:down

-- Remove manifest builder columns
DROP INDEX IF EXISTS idx_objects_manifest_builder;
ALTER TABLE objects
DROP COLUMN IF EXISTS last_append_at,
DROP COLUMN IF EXISTS manifest_built_at,
DROP COLUMN IF EXISTS manifest_built_for_version,
DROP COLUMN IF EXISTS manifest_cid;
