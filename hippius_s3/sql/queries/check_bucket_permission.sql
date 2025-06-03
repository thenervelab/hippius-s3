-- Check if a user has permission to access a bucket
-- Parameters: $1: bucket_id, $2: main_account_id
SELECT EXISTS (
    SELECT 1 FROM buckets b
    WHERE b.bucket_id = $1 AND b.main_account_id = $2
) as has_permission
