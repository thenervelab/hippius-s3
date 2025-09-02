-- Complete multipart upload and cleanup old attempts for same object
-- This should be run in a transaction
-- Parameters: $1: upload_id (completing), $2: object_id, $3: bucket_id, $4: object_key

-- First, link the successful upload to the final object
UPDATE multipart_uploads
SET object_id = $2, is_completed = TRUE
WHERE upload_id = $1;

-- Then delete all other multipart upload records for the same bucket/object_key
-- This includes their parts (CASCADE will handle parts cleanup)
DELETE FROM multipart_uploads
WHERE bucket_id = $3
  AND object_key = $4
  AND upload_id != $1;
