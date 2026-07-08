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
-- Self-heal a failed build: a CONCURRENTLY build that fails midway (crash, deadlock, cancelled
-- deploy) leaves an INVALID index of this name behind, and dbmate does NOT record a failed
-- migration — so on retry a bare `CREATE ... IF NOT EXISTS` would SKIP the invalid index, record
-- the migration as applied, and leave the reaper seq-scanning ~94M rows with no error. The
-- `DROP INDEX CONCURRENTLY IF EXISTS` first removes any leftover so the CREATE always rebuilds a
-- fresh valid index on retry. On the normal first run the index does not exist, so the DROP is a
-- no-op; the migration runs once, so it never drops a good index in steady state. Both statements
-- need `transaction:false` (set above) — CONCURRENTLY cannot run inside a transaction block.
--
-- NOTE (staging): version 20260706000000 was first added by an earlier commit as a NON-concurrent
-- index and is already recorded in `schema_migrations` on staging; dbmate keys on the version
-- number, not file content, so this edited body will NOT re-run there (staging already has the
-- index, built the write-locking way — a one-time cost already paid). A fresh apply (prod) runs
-- this final form and gets the CONCURRENTLY build. The resulting index is identical either way.
DROP INDEX CONCURRENTLY IF EXISTS idx_parts_upload_uploaded_at;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_parts_upload_uploaded_at
    ON parts (upload_id, uploaded_at);

-- migrate:down

DROP INDEX IF EXISTS idx_parts_upload_uploaded_at;
