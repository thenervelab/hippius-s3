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
-- idx_parts_upload_uploaded_at;` before re-running. This MUST stay a SINGLE statement: dbmate runs
-- a `transaction:false` migration body as one multi-command simple query, and Postgres runs any
-- multi-statement string in an IMPLICIT transaction — so adding a second statement (e.g. a
-- DROP CONCURRENTLY self-heal) makes CREATE INDEX CONCURRENTLY fail with "cannot run inside a
-- transaction block". The invalid-index recovery above stays a manual operator step.
--
-- NOTE (staging): version 20260706000000 was first added by an earlier commit as a NON-concurrent
-- index and is already recorded in `schema_migrations` on staging; dbmate keys on the version
-- number, not file content, so this edited body will NOT re-run there (staging already has the
-- index, built the write-locking way — a one-time cost already paid). A fresh apply (prod) runs
-- this final form and gets the CONCURRENTLY build. The resulting index is identical either way.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_parts_upload_uploaded_at
    ON parts (upload_id, uploaded_at);

-- migrate:down

DROP INDEX IF EXISTS idx_parts_upload_uploaded_at;
