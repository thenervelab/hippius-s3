-- Create or update an object with basic fields for multipart uploads
-- Parameters: $1: object_id, $2: bucket_id, $3: object_key, $4: content_type, $5: metadata, $6: md5_hash, $7: size_bytes, $8: created_at
INSERT INTO objects (
    object_id, bucket_id, object_key, content_type, metadata, md5_hash, size_bytes, created_at, status, multipart, storage_version
)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, 'publishing', TRUE, 3)
ON CONFLICT (bucket_id, object_key)
DO UPDATE SET
    object_id = EXCLUDED.object_id,
    content_type = EXCLUDED.content_type,
    metadata = EXCLUDED.metadata,
    md5_hash = EXCLUDED.md5_hash,
    size_bytes = EXCLUDED.size_bytes,
    created_at = EXCLUDED.created_at,
    status = 'publishing',
    multipart = TRUE,
    storage_version = 3
RETURNING object_id, bucket_id, object_key, content_type, metadata, md5_hash, size_bytes, created_at, status, multipart, storage_version
