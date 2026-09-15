-- The drain no longer copies a part to the CephFS pool: it verifies the SSD part is whole,
-- records its digest, publishes the backend UploadChainRequest to the NODE-LOCAL uploader
-- (arion_upload_requests:<node>), and commits the row 'uploading'. The uploader reads the same
-- SSD and flips the row 'replicated' once every chunk has a live chunk_backend row; the
-- drain's upload sweep does the same from chunk_backend coverage (the DB-authoritative
-- backstop) and re-publishes a row that sits 'uploading' too long.
--
-- 'uploading' is the state in which the SSD copy is the ONLY copy: the evictor and the
-- reclaimer never touch it (both key on 'replicated' / no row). 'replicated' now means "on the
-- backend", which is what makes evicting a replicated part's SSD copy safe without a pool.
--
-- upload_attempts:
--   how many times the upload sweep has RE-published this row after it sat 'uploading' past
--   the re-drive window without full backend coverage (a lost queue entry, a crashed uploader
--   mid-part). Bounded: past the cap the sweep leaves the row for the DLQ/operator path and
--   counts it, so a permanently failing part (a 402, a missing chunk) cannot be re-driven
--   forever. Reset to 0 by the drain's own commit (a fresh hand-off starts a fresh budget).
--
-- This file is the transactional half. sqlx wraps a migration in one transaction unless told
-- otherwise, so the ACCESS EXCLUSIVE lock the constraint swap takes is HELD until commit —
-- which is why validating the widened CHECK (a scan of every row) is in 0021, outside a
-- transaction, and not here: held across that scan it would block every drain and uploader
-- statement on the fleet for its whole duration. The partial index below is the one scan that
-- stays: it builds in seconds at this table's cardinality (the judgement 0013/0017/0018
-- recorded for plain over CONCURRENTLY here) and rolls back cleanly if aborted.

-- lock_timeout is the load-bearing line, exactly as in 0013/0018/0019: the constraint swap and
-- ADD COLUMN each need ACCESS EXCLUSIVE, which queues behind any open reader and then blocks
-- every statement arriving after it — the whole drain fleet. migrate() runs in the allocator's
-- startup path before its liveness file is first touched (SIGKILL at ~50-65s), so failing fast
-- and retrying on the next start beats a long lock wait that CrashLoops.
SET LOCAL lock_timeout = '5s';

-- NOT VALID: adding a CHECK normally scans the whole table (~11M rows on prod) under ACCESS
-- EXCLUSIVE. NOT VALID takes the lock only for the catalog change; new and updated rows are
-- checked from this point on, which is all the drain needs. 0021 validates the existing rows.
ALTER TABLE cephor_replication_status DROP CONSTRAINT IF EXISTS cephor_replication_status_status_check;
ALTER TABLE cephor_replication_status ADD CONSTRAINT cephor_replication_status_status_check
    CHECK (status IN ('pending', 'draining', 'uploading', 'replicated', 'failed', 'corrupt')) NOT VALID;

-- Metadata-only on PG 11+ (a non-volatile default is stored in the catalog, not written to
-- every row).
ALTER TABLE cephor_replication_status ADD COLUMN IF NOT EXISTS upload_attempts INTEGER NOT NULL DEFAULT 0;

-- The upload sweep's worklist: this node's parts handed to the uploader, oldest-committed
-- first. Partial so the scan is proportional to the in-flight set, never the table.
CREATE INDEX IF NOT EXISTS cephor_replication_uploading_idx
    ON cephor_replication_status (node_id, updated_at)
    WHERE status = 'uploading';
