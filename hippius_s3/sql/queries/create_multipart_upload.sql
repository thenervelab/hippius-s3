-- Parameters: $1: upload_id, $2: bucket_id, $3: object_key, $4: initiated_at, $5: content_type, $6: metadata, $7: file_mtime, $8: object_id
INSERT INTO multipart_uploads (
    upload_id, bucket_id, object_key, initiated_at, content_type, metadata, file_mtime, object_id
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING upload_id, bucket_id, object_key, object_id
