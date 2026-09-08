-- Total billable bytes for an account, read from the trigger-maintained rollup.
--
-- This is the quota gate's hot read: O(number of buckets the account owns), index-only over
-- idx_bucket_storage_usage_account, versus the O(number of objects) SUM in
-- get_account_storage_usage_authoritative.sql. It is a CACHE of that query -- see the header of
-- migrations/20260908120000_bucket_storage_usage.sql.
--
-- Soft-deleted buckets are excluded here rather than at write time: DeleteBucket refuses a
-- non-empty bucket (409 BucketNotEmpty), so a soft-deleted bucket's row is already 0 and the join
-- is belt-and-braces against a bucket emptied by a purge that raced the rollup.
--
-- Parameters: $1: main_account_id (SS58)
SELECT COALESCE(SUM(u.bytes_used), 0)::bigint AS bytes_used,
       COALESCE(SUM(u.objects_count), 0)::bigint AS objects_count
FROM bucket_storage_usage u
JOIN buckets b ON b.bucket_id = u.bucket_id AND b.deleted_at IS NULL
WHERE u.main_account_id = $1
