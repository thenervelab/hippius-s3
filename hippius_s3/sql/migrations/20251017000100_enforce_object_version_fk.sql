-- migrate:up

-- Make objects → object_versions FK deferrable so we can insert object and version in one statement
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_current_version_fk;

ALTER TABLE public.objects
  ADD CONSTRAINT objects_current_version_fk
  FOREIGN KEY (object_id, current_version_seq)
  REFERENCES public.object_versions(object_id, version_seq)
  ON DELETE RESTRICT
  DEFERRABLE INITIALLY DEFERRED;

-- Backfill: ensure every object has a matching version row for its current_version_seq (or 1)
WITH missing AS (
  SELECT o.object_id, COALESCE(o.current_version_seq, 1) AS vseq, o.created_at
  FROM public.objects o
  LEFT JOIN public.object_versions ov
    ON ov.object_id = o.object_id AND ov.version_seq = COALESCE(o.current_version_seq, 1)
  WHERE ov.object_id IS NULL
)
INSERT INTO public.object_versions (
  object_id,
  version_seq,
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
  m.vseq,
  'user',
  3,
  0,
  'application/octet-stream',
  '{}'::jsonb,
  '',
  NULL,
  NULL,
  FALSE,
  'publishing',
  0,
  NULL,
  NULL,
  NULL,
  now(),
  now(),
  m.created_at
FROM missing m;

-- (Default/NOT NULL will be applied in subsequent migrations to avoid pending trigger events)

-- migrate:down

-- Relax FK to non-deferrable and allow NULL again
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_current_version_fk;

ALTER TABLE public.objects
  ADD CONSTRAINT objects_current_version_fk
  FOREIGN KEY (object_id, current_version_seq)
  REFERENCES public.object_versions(object_id, version_seq)
  ON DELETE SET NULL;

ALTER TABLE public.objects
  ALTER COLUMN current_version_seq DROP NOT NULL,
  ALTER COLUMN current_version_seq DROP DEFAULT;
