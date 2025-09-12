-- migrate:up
-- Add append_version to objects for version-based CAS on appends
ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS append_version INTEGER NOT NULL DEFAULT 0;

-- migrate:down
ALTER TABLE objects
    DROP COLUMN IF EXISTS append_version;
