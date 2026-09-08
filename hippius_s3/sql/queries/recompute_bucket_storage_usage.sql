-- Recompute ONE bucket's rollup row from ground truth. Backfill, reconciler, and manual repair all
-- use this.
--
-- SET, not ADD. That is what makes it safe to run concurrently with live writes: a bucket written
-- mid-recompute converges on the next pass instead of double-counting, and the operation is
-- idempotent, so re-running the backfill cannot inflate anything.
--
-- The window between the aggregate and the UPSERT can lose a concurrent trigger delta (the trigger
-- fires against the pre-existing row, then this statement overwrites with a slightly stale total).
-- That is acceptable and self-correcting: the reconciler sweeps every bucket on a rolling schedule,
-- and the quota gate never denies on this number alone.
--
-- `previous_bytes_used` is captured BEFORE the overwrite and returned so the caller can emit the
-- drift metric. Without it the reconciler silently repairs drift and we never learn the triggers
-- are wrong -- with triggers, expected drift is exactly 0, so any non-zero value is a real defect
-- and not a threshold to argue about.
--
-- Keep in sync with get_account_storage_usage_authoritative.sql and usage_billable().
--
-- Parameters: $1: bucket_id (uuid)
WITH prev AS (
    SELECT bytes_used FROM bucket_storage_usage WHERE bucket_id = $1
), truth AS (
    SELECT
        b.bucket_id,
        b.main_account_id,
        COALESCE(SUM(ov.size_bytes), 0)::bigint AS bytes_used,
        COUNT(ov.object_id)::bigint            AS objects_count
    FROM buckets b
    LEFT JOIN objects o
      ON o.bucket_id = b.bucket_id
     AND o.deleted_at IS NULL
    LEFT JOIN object_versions ov
      ON ov.object_id = o.object_id
     AND ov.object_version = o.current_object_version
     AND ov.deleted_at IS NULL
     AND NOT ov.is_delete_marker
    WHERE b.bucket_id = $1
    GROUP BY b.bucket_id, b.main_account_id
)
INSERT INTO bucket_storage_usage AS bsu
    (bucket_id, main_account_id, bytes_used, objects_count, updated_at, reconciled_at, reconciled_bytes)
SELECT t.bucket_id, t.main_account_id, t.bytes_used, t.objects_count, now(), now(), t.bytes_used
FROM truth t
ON CONFLICT (bucket_id) DO UPDATE
    SET main_account_id  = EXCLUDED.main_account_id,
        bytes_used       = EXCLUDED.bytes_used,
        objects_count    = EXCLUDED.objects_count,
        updated_at       = now(),
        reconciled_at    = now(),
        reconciled_bytes = EXCLUDED.bytes_used
RETURNING bucket_id,
          bytes_used,
          objects_count,
          (SELECT p.bytes_used FROM prev p) AS previous_bytes_used
