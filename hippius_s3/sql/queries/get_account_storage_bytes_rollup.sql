-- Total billable bytes stored by an account, read from the maintained rollup. Sub-millisecond.
--
-- THE SAME NUMBER as get_account_storage_bytes.sql, which remains THE canonical definition — read
-- its header. This query does not restate that definition; it sums a counter that the triggers in
-- 20260910120000_storage_usage_rollup.sql maintain against it, and
-- tests/integration/test_storage_usage_rollup.py asserts the two agree after every write path.
--
-- Bucket liveness and OWNERSHIP are applied HERE, not in the rollup: bucket_storage_usage holds
-- per-bucket bytes regardless of buckets.deleted_at, so a bucket being soft-deleted or transferred
-- to another account needs no counter maintenance at all — it simply stops (or starts) being summed
-- by this query. That is why there is no trigger on `buckets`.
--
-- Plan: idx_buckets_main_account for the account's buckets, then a PK probe per bucket into
-- bucket_storage_usage. A few hundred buckets at worst, so no additional index is warranted.
--
-- THREE GUARD OUTPUTS, all of which the caller must act on:
--
--   `ready` is false until the one-shot backfill has run. Before that the rollup holds only
--   deltas-since-migration, which is NOT a total and would under-report every existing customer.
--   usage_service refuses to return a number at all in that state.
--
--   `negative_buckets` counts this account's buckets whose counter has gone below zero. That is
--   impossible unless the ledger has drifted, so it is a defect signal, not a condition to handle.
--   bytes_used is deliberately NOT clamped in the rollup (a clamp there would bias it permanently
--   high); the clamp is here, on the way out, where it cannot corrupt the arithmetic.
--
--   `missing_buckets` counts live buckets with no rollup row. After the backfill this only happens
--   for a bucket that has never held an object, where 0 is the right answer — so it is reported for
--   observability rather than treated as an error.
--
-- Parameters: $1: main_account_id (SS58)
SELECT
    -- Clamp PER BUCKET, not on the total. GREATEST(0, SUM(...)) let a negative bucket net against
    -- a positive one -- one at -3 GB and one at +10 GB reported 7 GB, silently UNDER-billing with
    -- the clamp never firing, so nothing indicated anything was wrong. Per bucket, a broken counter
    -- reads as 0: the account is over-reported relative to that one bucket, which is the safe
    -- direction, and `negative_buckets` below still flags it for the reconciler to repair.
    COALESCE(SUM(GREATEST(0, bsu.bytes_used)), 0)::bigint AS bytes_used,
    COALESCE(SUM(CASE WHEN bsu.bytes_used < 0 THEN 1 ELSE 0 END), 0)::bigint AS negative_buckets,
    COALESCE(SUM(CASE WHEN bsu.bucket_id IS NULL THEN 1 ELSE 0 END), 0)::bigint AS missing_buckets,
    COALESCE((SELECT s.backfilled_at IS NOT NULL FROM storage_usage_rollup_state s), false) AS ready
FROM buckets b
LEFT JOIN bucket_storage_usage bsu ON bsu.bucket_id = b.bucket_id
WHERE b.main_account_id = $1
  AND b.deleted_at IS NULL
