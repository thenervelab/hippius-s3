-- Recompute ONE bucket's rollup from the objects/object_versions tables and SET it.
--
-- Used twice: by the one-shot backfill (every bucket) and by the reconciler (a rolling slice,
-- forever). SET, not ADD, so it converges on the truth rather than double-counting whatever the
-- ledger has already folded in.
--
-- All the interesting reasoning is in the function body -- see
-- 20260910120000_storage_usage_rollup.sql -- because it needs a transaction-scoped advisory lock
-- and a single-snapshot DELETE+aggregate, neither of which survives being split into statements
-- the caller drives.
--
-- Returns the counter before and after, so the caller can report drift. Expected drift is ZERO;
-- anything else means a write path is not accounted for.
--
-- Parameters: $1: bucket_id (uuid)
SELECT o_bytes_before AS bytes_before, o_bytes_after AS bytes_after
FROM recompute_bucket_storage_usage($1)
