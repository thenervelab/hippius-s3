-- Get bucket by name
-- Parameters: $1: bucket_name
SELECT
    b.bucket_id,
    b.bucket_name,
    b.created_at,
    ba.acl_json,
    b.tags,
    b.main_account_id
FROM buckets b
LEFT JOIN bucket_acls ba ON ba.bucket_id = b.bucket_id
WHERE b.bucket_name = $1
LIMIT 1
