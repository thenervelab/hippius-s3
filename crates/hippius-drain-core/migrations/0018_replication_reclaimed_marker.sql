-- Let the DB-driven `failed` reclaim advance its cursor, and give its worklist an index.
--
-- THE BUG THIS FIXES. The walk-driven reclaim was disk-keyed: once a part's directory was
-- unlinked, the next scan simply did not find it, so the cursor advanced for free. The
-- DB-driven worklist is status-keyed, and unlinking a part changes nothing about its row — so
-- every poll re-selected the same oldest LIMIT page, re-unlinked it (idempotent, a no-op), and
-- re-counted it as reclaimed. Metrics and logs looked productive while the cursor never moved.
--
-- `gc_terminal_status_rows` does eventually delete these rows, but at a 7-day retention. So a
-- node with more than one page of aged `failed` parts would churn its oldest 512 every poll for
-- a week and never reach the rest — which then waited for the hourly walk this path exists to
-- make unnecessary. Prod carries 22,123 `failed` rows fleet-wide, ~4.4k per node against a
-- 512-row page, so this was the normal case, not an edge one.
--
-- WHY A MARKER AND NOT A DELETE. Deleting the row on unlink would also advance the cursor, and
-- is tempting because the row is inert once its disk copy is gone. It is rejected because
-- `gc_terminal_status_rows` owns that deletion on a deliberate 7-day retention — the row is the
-- only record that this part failed, and reclaiming disk is not a reason to discard the
-- diagnosis a week early. The marker takes the row off the worklist and leaves the GC's
-- semantics untouched.
-- lock_timeout is the load-bearing line, exactly as in 0013. CREATE INDEX takes SHARE, which
-- conflicts with ROW EXCLUSIVE: with no timeout it waits out any open writer AND blocks every
-- new writer on this table behind it — claim_part, release_part, mark_replicated,
-- record_landed_part, i.e. the whole drain fleet. migrate() runs in the allocator's startup path
-- BEFORE its liveness file is first touched, and that probe SIGKILLs at ~50-65s, so a long lock
-- wait means CrashLoopBackOff with each restart re-queuing the lock. Failing fast and retrying
-- on the next start is strictly better.
--
-- Set BEFORE the ALTER, not after it. `ADD COLUMN` without a default is metadata-only and
-- fast once it HOLDS the lock, but it still needs ACCESS EXCLUSIVE to take it, and that
-- acquisition queues behind any open reader and then blocks every statement arriving after
-- it. 0013 puts the guard ahead of its first DDL for this reason.
SET LOCAL lock_timeout = '5s';

ALTER TABLE cephor_replication_status
    ADD COLUMN IF NOT EXISTS reclaimed_at TIMESTAMPTZ;

-- Partial, so the index covers only the ~22k `failed` rows rather than all 11.4M. Without it the
-- worklist query is a scan on a table the drain writes to constantly, every poll, per node.
-- Ordered by updated_at to match the query's ORDER BY, so the LIMIT reads straight off the index.
--
-- Plain build, NOT CONCURRENTLY — the same judgement 0013 recorded for this table: at this
-- cardinality it builds in seconds and rolls back cleanly if aborted, whereas CONCURRENTLY needs
-- `-- no-transaction`, leaves an INVALID index on failure that `IF NOT EXISTS` then silently
-- refuses to rebuild, and waits out all older snapshots twice.
CREATE INDEX IF NOT EXISTS cephor_replication_status_failed_reclaimable
    ON cephor_replication_status (node_id, updated_at)
    WHERE status = 'failed' AND reclaimed_at IS NULL;
