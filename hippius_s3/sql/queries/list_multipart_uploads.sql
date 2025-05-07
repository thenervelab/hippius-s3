-- List all multipart uploads for a bucket
-- Parameters: $1: bucket_id, $2: prefix (optional, can be NULL)
SELECT *
FROM multipart_uploads
WHERE bucket_id = $1
  AND ($2 IS NULL OR object_key LIKE $2 || '%')
  AND is_completed = FALSE
ORDER BY initiated_at DESC
