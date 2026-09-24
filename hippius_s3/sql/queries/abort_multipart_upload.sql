-- Delete a multipart upload that is still open, and (by cascade) its parts
-- A completed upload's parts are its object's data, so it is never deleted here.
-- Parameters: $1: upload_id
DELETE FROM multipart_uploads
WHERE upload_id = $1
  AND is_completed = FALSE
RETURNING upload_id
