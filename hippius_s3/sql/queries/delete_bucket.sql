-- Delete a bucket only if the user has permission
-- Parameters: $1: bucket_id, $2: main_account_id
DELETE FROM buckets
WHERE bucket_id = $1 AND main_account_id = $2
RETURNING bucket_id, bucket_name
