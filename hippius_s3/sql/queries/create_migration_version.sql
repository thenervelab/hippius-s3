-- Create a new migration version row for an object
-- Parameters:
--   $1: object_id (uuid)
--   $2: content_type (text)
--   $3: metadata (jsonb)
--   $4: storage_version_target (int)
-- Returns: object_version (bigint)
WITH next AS (
  SELECT COALESCE(MAX(object_version), 0) + 1 AS v
  FROM object_versions
  WHERE object_id = $1
)
INSERT INTO object_versions (
  object_id,
  object_version,
  version_type,
  storage_version,
  size_bytes,
  content_type,
  metadata,
  md5_hash,
  ipfs_cid,
  cid_id,
  multipart,
  status,
  append_version,
  manifest_cid,
  manifest_built_for_version,
  manifest_built_at,
  last_append_at,
  last_modified,
  created_at
)
SELECT $1,
       v,
       'migration',
       $4,
       0,
       $2,
       $3,
       NULL,
       NULL,
       NULL,
       FALSE,
       'publishing',
       0,
       NULL,
       NULL,
       NULL,
       NOW(),
       NOW(),
       NOW()
FROM next
RETURNING object_version
;
