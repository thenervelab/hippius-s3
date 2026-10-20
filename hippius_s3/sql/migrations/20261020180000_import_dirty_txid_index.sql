-- migrate:up transaction:false

-- The Rust import's catch-up reads the notices written since its last cycle: `txid` past a saved
-- snapshot. With only the (bucket_id, object_key) primary key, every cycle walks all of
-- `import_dirty` (24.7M rows, 13 GB on 2026-10-20) to find the few thousand new ones, which takes
-- 25-30 minutes. That scan sets the length of the cutover's write freeze, which needs one cycle and
-- one parity check inside it. With this index both read only the notices past the snapshot's
-- xmin: the freeze drops from about 45-60 minutes of failed writes to about 15.
--
-- Cost: the notice triggers upsert one row per changed key, setting `txid`, so those updates are
-- no longer HOT and maintain this index too. One small index entry per object write.
--
-- Built CONCURRENTLY: a plain CREATE INDEX takes a SHARE lock that blocks the notice triggers, and
-- so every object write, for the whole build. Postgres forbids CONCURRENTLY inside a transaction,
-- hence the directive, and it must stay a SINGLE statement: dbmate runs a transaction:false body as
-- one simple query, and a multi-statement string runs in an implicit transaction.
--
-- Recovery: a build that fails midway leaves an INVALID index, which `IF NOT EXISTS` then SKIPS on
-- the next run, leaving an index that is never read but still maintained on every write. Check it,
-- and drop the leftover before re-running:
--     SELECT indisvalid, indisready FROM pg_index
--     WHERE indexrelid = 'idx_import_dirty_txid'::regclass;   -- both must be true
--     DROP INDEX CONCURRENTLY idx_import_dirty_txid;
-- A long-running transaction elsewhere is the usual cause: check pg_stat_activity for old
-- xact_start values before retrying.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_import_dirty_txid ON import_dirty (txid);

-- migrate:down

DROP INDEX IF EXISTS idx_import_dirty_txid;
