-- migrate:up

-- R2: the MPU reaper's activity-gate (list_abandoned_versions.sql) checks, per candidate
-- upload, whether any of its parts has recent `uploaded_at` activity — a correlated
-- `SELECT 1 FROM parts pr WHERE pr.upload_id = mu.upload_id AND pr.uploaded_at > ...`. The
-- existing single-column idx_parts_upload (upload_id) forces a heap probe per matching part to
-- read uploaded_at; at prod cardinality (~94M parts) that is the reaper's dominant buffer cost.
-- A composite (upload_id, uploaded_at) index makes the activity check an index-only range scan
-- per upload — the R2 gate before enabling the reaper at prod scale.
CREATE INDEX IF NOT EXISTS idx_parts_upload_uploaded_at
    ON parts (upload_id, uploaded_at);

-- migrate:down

DROP INDEX IF EXISTS idx_parts_upload_uploaded_at;
