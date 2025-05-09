-- List all multipart uploads for a bucket
-- Parameters: $1: bucket_id, $2: prefix (optional, can be NULL)
SELECT *
FROM multipart_uploads
WHERE bucket_id = $1
  AND ($2::text IS NULL OR object_key LIKE $2::text || '%')
  AND is_completed = FALSE
ORDER BY initiated_at DESC
