-- Delete a multipart upload and its parts
-- Parameters: $1: upload_id
DELETE FROM multipart_uploads
WHERE upload_id = $1
RETURNING upload_id
