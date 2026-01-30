-- Create a new migration version row for an object with advisory lock and transactional cleanup
-- Parameters:
--   $1: object_id (uuid)
--   $2: content_type (text)
--   $3: metadata (jsonb)
--   $4: storage_version_target (int)
--   $5: upload_backends (text[])
-- Returns: object_version (bigint)
WITH lock AS (
  SELECT pg_advisory_xact_lock(hashtext($1::text))
), cur AS (
  SELECT current_object_version FROM objects WHERE object_id = $1::uuid FOR UPDATE
), last AS (
  SELECT COALESCE(MAX(object_version), (SELECT current_object_version FROM cur)) AS last_ver
    FROM object_versions
   WHERE object_id = $1::uuid
), ins AS (
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
    last_append_at,
    last_modified,
    created_at,
    upload_backends
  )
  SELECT $1::uuid,
         (SELECT last_ver FROM last) + 1 AS v,
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
         NOW(),
         NOW(),
         NOW(),
         $5::text[]
  RETURNING object_version
)
SELECT object_version FROM ins;
