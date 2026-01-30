-- migrate:up
ALTER TABLE object_versions DROP COLUMN IF EXISTS manifest_cid;
ALTER TABLE object_versions DROP COLUMN IF EXISTS manifest_built_for_version;
ALTER TABLE object_versions DROP COLUMN IF EXISTS manifest_built_at;
ALTER TABLE object_versions DROP COLUMN IF EXISTS manifest_api_file_id;
DROP INDEX IF EXISTS idx_object_versions_manifest_builder;

-- migrate:down
ALTER TABLE object_versions ADD COLUMN manifest_cid TEXT;
ALTER TABLE object_versions ADD COLUMN manifest_built_for_version INT;
ALTER TABLE object_versions ADD COLUMN manifest_built_at TIMESTAMPTZ;
ALTER TABLE object_versions ADD COLUMN manifest_api_file_id TEXT;
CREATE INDEX idx_object_versions_manifest_builder ON object_versions(last_append_at, append_version)
WHERE (manifest_built_for_version IS NULL OR append_version > manifest_built_for_version);
