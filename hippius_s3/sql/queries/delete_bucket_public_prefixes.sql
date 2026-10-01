-- Parameters: $1: bucket_id
DELETE FROM bucket_public_prefixes
WHERE bucket_id = $1
