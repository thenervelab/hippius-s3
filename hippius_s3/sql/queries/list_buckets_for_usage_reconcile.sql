-- The reconciler's work queue: the live buckets whose rollup was recomputed longest ago.
--
-- NULLS FIRST puts never-recomputed buckets at the head, so a bucket created after the backfill is
-- verified on the first pass that reaches it rather than trusted indefinitely.
--
-- LIVE BUCKETS ONLY. A soft-deleted bucket contributes to nobody's total
-- (get_account_storage_bytes_rollup.sql filters it), so recomputing one -- potentially millions of
-- objects -- would be spending the exact cost this whole mechanism exists to avoid on a number
-- nothing reads. Its counter is still maintained by the triggers, which do not care about bucket
-- liveness; there is no path that revives a soft-deleted bucket, and if one is ever added it must
-- recompute the bucket on the way through.
--
-- Parameters: $1: how many buckets to return
SELECT b.bucket_id
FROM buckets b
LEFT JOIN bucket_storage_usage bsu ON bsu.bucket_id = b.bucket_id
WHERE b.deleted_at IS NULL
ORDER BY bsu.recomputed_at ASC NULLS FIRST, b.bucket_id
LIMIT $1
