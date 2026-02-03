-- migrate:up

ALTER TABLE object_versions
ADD COLUMN IF NOT EXISTS append_etag_md5s BYTEA;

-- migrate:down

ALTER TABLE object_versions
DROP COLUMN IF EXISTS append_etag_md5s;
