-- migrate:up

-- The Rust import's catch-up and its parity check read the notices written since a saved snapshot:
-- `txid` from the snapshot's xmin up. With only the (bucket_id, object_key) primary key, every
-- catch-up cycle walks all of `import_dirty` (24.7M rows, 13 GB on 2026-10-20), 25-30 minutes,
-- and that walk sets the length of the cutover's write freeze. With this index the Rust side pages
-- through only the new notices, in (txid, bucket_id, object_key) order, an index-only scan per
-- page (hippius-s3-internal #381): the freeze drops from about 45-60 minutes to about 15.
--
-- On production the index is built OUT OF BAND, before this deploys, and this file does nothing:
--
--   1. On the primary, with no long transaction open (check pg_stat_activity for old xact_start or
--      backend_xmin in this database), from a session with a statement_timeout:
--        SET statement_timeout = '2h';
--        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_import_dirty_txid
--            ON import_dirty (txid, bucket_id, object_key);
--   2. Check it: SELECT indisvalid, indisready FROM pg_index
--                WHERE indexrelid = 'idx_import_dirty_txid'::regclass;   -- both true
--      A build that failed leaves an INVALID index that IF NOT EXISTS then skips. Drop it with
--      DROP INDEX CONCURRENTLY idx_import_dirty_txid and run step 1 again.
--
-- Why not here: every API pod (start-api.sh) and the migrations Job run `dbmate up` at once, with
-- no migration lock. Two CONCURRENTLY builds of one index can deadlock at their end, and the loser
-- leaves an invalid index that the next run skips and records as applied. A build this size also
-- outlasts the API startup probe. A plain CREATE INDEX would block every object write (the notice
-- triggers write this table) for the whole build.
--
-- So this builds the index only on a table too small for any of that to matter: a fresh database,
-- CI, local. On a large table without the index it does nothing; the Rust side reads the notices
-- without it (slower, as before) until step 1 is done, so no order of the two is unsafe.
--
-- Cost once built: each notice upsert from a new transaction sets `txid`, an indexed column, so
-- it is not a HOT update and writes this index and the primary key's. Those upserts come per
-- separately committed chunk write (`import_dirty_chunk_row`), for every key and alias of the
-- object, not once per object write.
SET LOCAL lock_timeout = '3s';
DO $$
BEGIN
    -- A count capped at 100k: exact, and a short index scan however large the table is.
    IF to_regclass('idx_import_dirty_txid') IS NULL
       AND (SELECT count(*) FROM (SELECT 1 FROM import_dirty LIMIT 100000) t) < 100000 THEN
        CREATE INDEX idx_import_dirty_txid ON import_dirty (txid, bucket_id, object_key);
    END IF;
END $$;

-- migrate:down transaction:false

-- CONCURRENTLY: a plain DROP INDEX takes ACCESS EXCLUSIVE on import_dirty, which queues every
-- object write behind it. One statement, as a transaction:false body must be.
DROP INDEX CONCURRENTLY IF EXISTS idx_import_dirty_txid;
