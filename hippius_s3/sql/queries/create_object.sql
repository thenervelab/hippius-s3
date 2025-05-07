-- Create a new object
-- Parameters: $1: object_id, $2: bucket_id, $3: object_key, $4: ipfs_cid,
--             $5: size_bytes, $6: content_type, $7: created_at, $8: metadata
INSERT INTO objects (
    object_id, bucket_id, object_key, ipfs_cid,
    size_bytes, content_type, created_at, metadata
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING object_id, bucket_id, object_key, ipfs_cid
