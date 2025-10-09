-- Parameters: $1: upload_id, $2: bucket_id, $3: object_key, $4: initiated_at, $5: content_type, $6: metadata, $7: file_mtime, $8: object_id
INSERT INTO multipart_uploads (
    upload_id, bucket_id, object_key, initiated_at, content_type, metadata, file_mtime, object_id
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (upload_id) DO UPDATE SET
    -- keep earliest initiation time
    initiated_at = LEAST(multipart_uploads.initiated_at, EXCLUDED.initiated_at),
    -- refresh non-key fields for completeness
    content_type = COALESCE(EXCLUDED.content_type, multipart_uploads.content_type),
    metadata = COALESCE(EXCLUDED.metadata, multipart_uploads.metadata),
    file_mtime = COALESCE(EXCLUDED.file_mtime, multipart_uploads.file_mtime),
    object_id = COALESCE(EXCLUDED.object_id, multipart_uploads.object_id)
RETURNING upload_id, bucket_id, object_key, object_id
