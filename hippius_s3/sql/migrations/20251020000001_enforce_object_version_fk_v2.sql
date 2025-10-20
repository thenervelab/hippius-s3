-- migrate:up

-- 1) Make the FK from objects(object_id, current_object_version) to object_versions(object_id, object_version)
--    deferrable so we can insert/update both in one statement.
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_current_version_fk;

ALTER TABLE public.objects
  ADD CONSTRAINT objects_current_version_fk
  FOREIGN KEY (object_id, current_object_version)
  REFERENCES public.object_versions(object_id, object_version)
  ON DELETE RESTRICT
  DEFERRABLE INITIALLY DEFERRED;

-- 2) Ensure every object has its current version row; if missing, create it with minimal defaults
WITH missing AS (
  SELECT o.object_id, COALESCE(o.current_object_version, 1) AS v, o.created_at
  FROM public.objects o
  LEFT JOIN public.object_versions ov
    ON ov.object_id = o.object_id AND ov.object_version = COALESCE(o.current_object_version, 1)
  WHERE ov.object_id IS NULL
)
INSERT INTO public.object_versions (
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
  m.object_id,
  m.v,
  'user'::public.version_type,
  3,                                -- storage_version (default latest)
  0,                                -- size_bytes
  'application/octet-stream',       -- content_type
  '{}'::jsonb,                      -- metadata
  '',                               -- md5_hash
  NULL,                             -- ipfs_cid
  NULL,                             -- cid_id
  FALSE,                            -- multipart
  'publishing',                     -- status
  0,                                -- append_version
  NULL,                             -- manifest_cid
  NULL,                             -- manifest_built_for_version
  NULL,                             -- manifest_built_at
  now(),                            -- last_append_at
  now(),                            -- last_modified
  m.created_at
FROM missing m;

-- 3) Strengthen NOT NULLs now that backfill done (optionally; keep lenient if incompatible)
-- ALTER TABLE public.objects ALTER COLUMN current_object_version SET NOT NULL;

-- migrate:down

ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_current_version_fk;

ALTER TABLE public.objects
  ADD CONSTRAINT objects_current_version_fk
  FOREIGN KEY (object_id, current_object_version)
  REFERENCES public.object_versions(object_id, object_version)
  ON DELETE SET NULL;
