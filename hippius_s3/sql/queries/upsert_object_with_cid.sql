-- Create or update an object with cid_id instead of ipfs_cid
-- Parameters: $1: object_id, $2: bucket_id, $3: object_key, $4: cid_id,
--             $5: size_bytes, $6: content_type, $7: created_at, $8: metadata, $9: md5_hash
INSERT INTO objects (
    object_id, bucket_id, object_key, cid_id,
    size_bytes, content_type, created_at, metadata, md5_hash,
    storage_version
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
ON CONFLICT (bucket_id, object_key)
DO UPDATE SET
    object_id = EXCLUDED.object_id,
    cid_id = EXCLUDED.cid_id,
    size_bytes = EXCLUDED.size_bytes,
    content_type = EXCLUDED.content_type,
    created_at = EXCLUDED.created_at,
    metadata = EXCLUDED.metadata,
    md5_hash = EXCLUDED.md5_hash,
    storage_version = EXCLUDED.storage_version
RETURNING object_id, bucket_id, object_key, cid_id, size_bytes, content_type, created_at, metadata, md5_hash
