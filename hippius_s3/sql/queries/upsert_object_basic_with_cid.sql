-- Upsert object and set cid_id on latest version
WITH upsert_object AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at)
  VALUES ($1, $2, $3, $9)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET object_id = EXCLUDED.object_id
  RETURNING object_id, current_object_version
), ensure_current AS (
  UPDATE objects o
  SET current_object_version = COALESCE(o.current_object_version, 1)
  FROM upsert_object uo
  WHERE o.object_id = uo.object_id
  RETURNING o.object_id, o.current_object_version
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
SELECT
  ec.object_id,
  ec.current_object_version,
  'user',
  $10,
  $8,
  $5,
  $6::jsonb,
  $7,
  NULL,
  $4,
  TRUE,
  'publishing',
  0,
  NULL,
  NULL,
  NULL,
  $9,
  $9,
  $9
FROM ensure_current ec
ON CONFLICT (object_id, object_version)
DO UPDATE SET
  storage_version = EXCLUDED.storage_version,
  size_bytes = EXCLUDED.size_bytes,
  content_type = EXCLUDED.content_type,
  metadata = EXCLUDED.metadata,
  md5_hash = EXCLUDED.md5_hash,
  cid_id = EXCLUDED.cid_id,
  multipart = EXCLUDED.multipart,
  status = EXCLUDED.status,
  last_modified = EXCLUDED.last_modified
RETURNING $1 AS object_id, $2 AS bucket_id, $3 AS object_key, $4 AS cid_id, $5 AS content_type, $6 AS metadata, $7 AS md5_hash, $8 AS size_bytes, $9 AS created_at
