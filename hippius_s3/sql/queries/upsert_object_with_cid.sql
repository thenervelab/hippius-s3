-- Allocate a new version and set cid_id on that version (promotes it live).
WITH upserted AS (
  INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version)
  VALUES ($1, $2, $3, $7, 1)
  ON CONFLICT (bucket_id, object_key)
  DO UPDATE SET
    object_key = EXCLUDED.object_key,
    deleted_at = NULL,
    -- Next version = GREATEST(row-locked counter, committed MAX) + 1.
    -- The ON CONFLICT row lock serializes concurrent writers on this objects row, and
    -- objects.current_object_version is re-read post-lock (EvalPlanQual), so it always
    -- reflects a sibling txn's just-committed bump. A bare MAX() subquery does NOT: its
    -- snapshot can miss the concurrent sibling's uncommitted-then-committed row, hand out a
    -- duplicate object_version, and 500 with an object_versions_pkey violation. MAX() is kept as
    -- a best-effort floor for out-of-band versions (create_migration_version.sql inserts a version
    -- without bumping current_object_version). NOTE: that floor is itself snapshot-stale under READ
    -- COMMITTED, so a migrator running concurrently with a write to the SAME object can still
    -- collide; the writer retries on object_versions_pkey (object_writer.py) to cover that.
    current_object_version = GREATEST(
      objects.current_object_version,
      (SELECT COALESCE(MAX(ov.object_version), 0) FROM object_versions ov WHERE ov.object_id = objects.object_id)
    ) + 1
  RETURNING object_id, bucket_id, object_key, created_at, current_object_version
), ins_version AS (
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
  SELECT
    u.object_id,
    u.current_object_version,
    'user',
    $10,
    $5,
    $6,
    $8::jsonb,
    $9,
    NULL,
    $4,
    FALSE,
    'publishing',
    0,
    $7,
    $7,
    $7,
    $11::text[]
  FROM upserted u
  RETURNING object_id, object_version
)
SELECT
  u.object_id,
  u.bucket_id,
  u.object_key,
  $4 AS cid_id,
  $5 AS size_bytes,
  $6 AS content_type,
  $7 AS created_at,
  $8 AS metadata,
  $9 AS md5_hash
FROM upserted u
JOIN ins_version iv ON iv.object_id = u.object_id AND iv.object_version = u.current_object_version
