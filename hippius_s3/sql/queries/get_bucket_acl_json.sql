-- Get bucket ACL JSON for permission checks
-- Parameters: $1: bucket_name
SELECT acl_json
FROM bucket_acls ba
JOIN buckets b ON ba.bucket_id = b.bucket_id
WHERE b.bucket_name = $1;
