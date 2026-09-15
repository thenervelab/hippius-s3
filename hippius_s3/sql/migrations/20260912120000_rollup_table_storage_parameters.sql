-- migrate:up

-- Storage parameters for the two rollup tables. Neither changes behaviour; both stop the tables
-- degrading under load, and neither can be set later without an ACCESS EXCLUSIVE moment on a table
-- that is by then hot.
--
-- Both statements take SHARE UPDATE EXCLUSIVE, which does NOT conflict with SELECT, INSERT, UPDATE
-- or DELETE -- only with other DDL, VACUUM and ANALYZE. So unlike the trigger DDL in
-- 20260910120000 this cannot stall the data plane even in principle. lock_timeout is still set,
-- because prod runs with lock_timeout = 0 and a wait behind a running autovacuum is possible.
SET LOCAL lock_timeout = '3s';

-- A QUEUE. Rows are appended at the tail and the head is claimed by DELETE ... RETURNING, so the
-- table is supposed to sit near empty. The default autovacuum_vacuum_scale_factor of 0.2 is
-- PROPORTIONAL to live tuples, which on a near-empty table means "vacuum after a handful of dead
-- rows" -- and production runs autovacuum_max_workers = 3 against 168M-row objects/object_versions,
-- so a small table claiming a worker slot every few seconds is capacity taken from where it counts.
-- Disable the proportional term and use a flat threshold instead: vacuum when there is actually
-- work, not when a percentage of nothing has changed.
ALTER TABLE storage_delta_ledger SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 10000
);

-- A HOT-UPDATED COUNTER. The compactor issues one
-- `UPDATE ... SET bytes_used = bytes_used + $n, updated_at = now()` per active bucket per fold. At
-- the default fillfactor of 100 there is no free space on the page, so each update writes the new
-- row version to a different page: the table bloats, the PK index has to be updated every time
-- (a HOT update is impossible once the tuple moves page), and the working set stops fitting in
-- cache. 70 leaves room for roughly three row versions per page before a move is forced, which is
-- ample for a table whose rows are ~40 bytes and are re-read every fold.
--
-- fillfactor applies to pages written AFTER this, so on production -- where the table is created
-- empty by 20260910120000 and seeded by the backfill afterwards -- it governs from the start.
ALTER TABLE bucket_storage_usage SET (fillfactor = 70);

-- migrate:down

SET LOCAL lock_timeout = '3s';

ALTER TABLE storage_delta_ledger RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);
ALTER TABLE bucket_storage_usage RESET (fillfactor);
