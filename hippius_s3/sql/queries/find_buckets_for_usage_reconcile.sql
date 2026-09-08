-- Next slice of buckets for the reconciler's rolling sweep: never-reconciled first (NULLS FIRST on
-- idx_bucket_storage_usage_reconciled), then least-recently-reconciled.
--
-- Buckets that exist but have no rollup row yet are picked up by the LEFT JOIN, so a bucket created
-- while the backfill was already past it is still reconciled rather than left at an implicit zero.
--
-- Parameters: $1: reconcile interval (seconds), $2: limit
SELECT b.bucket_id
FROM buckets b
LEFT JOIN bucket_storage_usage u ON u.bucket_id = b.bucket_id
WHERE b.deleted_at IS NULL
  AND (u.reconciled_at IS NULL OR u.reconciled_at < now() - make_interval(secs => $1))
ORDER BY u.reconciled_at ASC NULLS FIRST
LIMIT $2
