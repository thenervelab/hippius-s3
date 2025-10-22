-- Parameters:
--   $1: target_storage_version (int)
--   $2: bucket_name (nullable)
--   $3: object_key (nullable)
WITH cur AS (
  SELECT o.object_id,
         o.bucket_id,
         o.object_key,
         b.bucket_name,
         b.is_public,
         b.main_account_id,
         ov.object_version,
         ov.storage_version,
         ov.content_type,
         ov.metadata
  FROM objects o
  JOIN object_versions ov
    ON ov.object_id = o.object_id
   AND ov.object_version = o.current_object_version
  JOIN buckets b ON b.bucket_id = o.bucket_id
  WHERE ov.storage_version < $1
    AND ($2 IS NULL OR b.bucket_name = $2)
    AND ($3 IS NULL OR o.object_key = $3)
)
SELECT * FROM cur
ORDER BY bucket_id, object_key
;
