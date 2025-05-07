-- Delete an object
-- Parameters: $1: bucket_id, $2: object_key
DELETE FROM objects
WHERE bucket_id = $1 AND object_key = $2
RETURNING object_id, ipfs_cid
