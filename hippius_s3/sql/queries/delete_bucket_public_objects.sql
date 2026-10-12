-- Parameters: $1: bucket_id
DELETE FROM bucket_public_objects
WHERE bucket_id = $1
