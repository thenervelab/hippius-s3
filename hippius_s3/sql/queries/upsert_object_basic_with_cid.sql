-- Create or update an object with basic fields and cid_id (for multipart uploads)
-- Parameters: $1: object_id, $2: bucket_id, $3: object_key, $4: cid_id, $5: content_type, $6: metadata, $7: md5_hash, $8: size_bytes, $9: created_at
INSERT INTO objects (
    object_id, bucket_id, object_key, cid_id, content_type, metadata, md5_hash, size_bytes, created_at
)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
ON CONFLICT (bucket_id, object_key)
DO UPDATE SET
    object_id = EXCLUDED.object_id,
    cid_id = EXCLUDED.cid_id,
    content_type = EXCLUDED.content_type,
    metadata = EXCLUDED.metadata,
    md5_hash = EXCLUDED.md5_hash,
    size_bytes = EXCLUDED.size_bytes,
    created_at = EXCLUDED.created_at
RETURNING object_id, bucket_id, object_key, cid_id, content_type, metadata, md5_hash, size_bytes, created_at
