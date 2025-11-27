-- migrate:up

ALTER TABLE part_chunks ADD COLUMN api_file_id TEXT;
ALTER TABLE object_versions ADD COLUMN manifest_api_file_id TEXT;

-- migrate:down

ALTER TABLE part_chunks DROP COLUMN api_file_id;
ALTER TABLE object_versions DROP COLUMN manifest_api_file_id;
