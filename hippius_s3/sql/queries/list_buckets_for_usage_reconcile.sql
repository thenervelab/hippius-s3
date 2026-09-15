-- The reconciler's work queue: live buckets whose verification is most overdue.
--
-- ORDERED ON `attempted_at`, NOT `recomputed_at`, and that distinction is the whole point.
-- recompute_bucket_storage_usage() only stamps on success, so ordering on `recomputed_at` meant a
-- bucket whose aggregate cannot complete was never stamped and therefore stayed at the HEAD of this
-- queue forever -- re-attempted every cycle, never succeeding, while the buckets behind it were
-- never verified. Measured on prod 2026-09-15: one bucket failed 168 times in 24h and the pass
-- completed 47 of 50 buckets. Stamping the ATTEMPT rotates it out after one try.
--
-- `recompute_failures` comes back so the caller can route: a bucket that has failed the fast path
-- often enough is verified in slices instead (see sum_bucket_object_key_slice.sql). Routing on
-- observed failure rather than an object-count threshold means it needs no retuning as the estate
-- grows.
SELECT
    b.bucket_id,
    COALESCE(bsu.recompute_failures, 0) AS recompute_failures
FROM buckets b
LEFT JOIN bucket_storage_usage bsu ON bsu.bucket_id = b.bucket_id
WHERE b.deleted_at IS NULL
ORDER BY bsu.attempted_at ASC NULLS FIRST, b.bucket_id
LIMIT $1
