-- migrate:up
-- Drop the default for storage_version;
ALTER TABLE objects
    ALTER COLUMN storage_version DROP DEFAULT;


-- migrate:down

ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS storage_version SMALLINT NOT NULL DEFAULT 2;
