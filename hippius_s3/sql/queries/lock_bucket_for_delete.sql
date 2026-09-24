-- Lock a live bucket row for DeleteBucket. FOR UPDATE conflicts with the FOR KEY SHARE that every
-- INSERT referencing the bucket takes through its foreign key (objects, multipart_uploads), so this
-- waits for in-flight creates to commit, and holds new ones off until the soft-delete commits. The
-- emptiness check that follows therefore sees every row that could make the bucket non-empty.
-- Parameters: $1: bucket_id (uuid)
SELECT bucket_id
FROM buckets
WHERE bucket_id = $1
  AND deleted_at IS NULL
FOR UPDATE
