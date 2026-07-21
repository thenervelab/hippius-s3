-- migrate:up

-- The MPU reaper (list_abandoned_versions.sql) scans multipart_uploads for
-- not-completed uploads older than a threshold on every cycle. A partial index on
-- initiated_at restricted to the not-completed rows matches that predicate exactly and
-- keeps the scan cheap as the table grows.
CREATE INDEX IF NOT EXISTS idx_multipart_uploads_initiated_at
    ON multipart_uploads (initiated_at)
    WHERE COALESCE(is_completed, false) = false;

-- migrate:down

DROP INDEX IF EXISTS idx_multipart_uploads_initiated_at;
