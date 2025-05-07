-- Create a new multipart upload
-- Parameters: $1: upload_id, $2: bucket_id, $3: object_key, $4: initiated_at, $5: content_type, $6: metadata
INSERT INTO multipart_uploads (
    upload_id, bucket_id, object_key, initiated_at, content_type, metadata
)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING upload_id, bucket_id, object_key
