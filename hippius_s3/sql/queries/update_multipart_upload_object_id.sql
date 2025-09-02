-- Update multipart_uploads table with the final object_id after completion
-- Parameters: $1: upload_id, $2: object_id
UPDATE multipart_uploads
SET object_id = $2, is_completed = TRUE
WHERE upload_id = $1
