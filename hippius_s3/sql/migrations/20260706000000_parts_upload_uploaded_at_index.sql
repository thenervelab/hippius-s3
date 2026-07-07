-- migrate:up transaction:false

-- R2: the MPU reaper's activity-gate (list_abandoned_versions.sql) checks, per candidate
-- upload, whether any of its parts has recent `uploaded_at` activity — a correlated
-- `SELECT 1 FROM parts pr WHERE pr.upload_id = mu.upload_id AND pr.uploaded_at > ...`. The
-- existing single-column idx_parts_upload (upload_id) forces a heap probe per matching part to
-- read uploaded_at; at prod cardinality (~94M parts) that is the reaper's dominant buffer cost.
-- A composite (upload_id, uploaded_at) index makes the activity check an index-only range scan
-- per upload — the R2 gate before enabling the reaper at prod scale.
--
-- CONCURRENTLY (+ `transaction:false` above) is mandatory here: a plain CREATE INDEX takes a
-- SHARE lock that blocks every INSERT/UPDATE/DELETE on `parts` for the whole build. Migrations
-- run before the API serves traffic on deploy, so a plain build at ~94M rows would stall every
-- PUT for minutes. CONCURRENTLY builds without the write lock, but Postgres forbids it inside a
-- transaction block — hence dbmate's `transaction:false` directive on the migrate:up line.
--
-- Recovery caveat: a CONCURRENTLY build that fails midway (crash, deadlock, cancelled deploy)
-- leaves an INVALID index of this name behind. `IF NOT EXISTS` then SKIPS it on the next run — it
-- will NOT rebuild an existing-but-invalid index. If this migration errors, drop the leftover
-- (`DROP INDEX CONCURRENTLY idx_parts_upload_uploaded_at;`) or `REINDEX INDEX CONCURRENTLY
-- idx_parts_upload_uploaded_at;` before re-running.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_parts_upload_uploaded_at
    ON parts (upload_id, uploaded_at);

-- migrate:down

DROP INDEX IF EXISTS idx_parts_upload_uploaded_at;
