-- Mark a multipart upload as completed
-- Parameters: $1: upload_id
UPDATE multipart_uploads
SET is_completed = TRUE
WHERE upload_id = $1
RETURNING upload_id, bucket_id, object_key
