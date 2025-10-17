-- migrate:up

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'version_type') THEN
        CREATE TYPE version_type AS ENUM ('user', 'migration');
    END IF;
END$$;

-- Create table for per-object versions with sequential numbering
CREATE TABLE IF NOT EXISTS public.object_versions (
    object_id uuid NOT NULL REFERENCES public.objects(object_id) ON DELETE CASCADE,
    version_seq bigint NOT NULL,
    version_type version_type NOT NULL DEFAULT 'user',

    -- per-version fields moved from objects
    storage_version int2 NOT NULL,
    size_bytes int8 NOT NULL,
    content_type text NOT NULL,
    metadata jsonb NULL,
    md5_hash text NULL,
    ipfs_cid text NULL,
    cid_id uuid NULL REFERENCES public.cids(id),
    multipart bool DEFAULT false NULL,
    status varchar(50) DEFAULT 'publishing' NULL,
    append_version int4 DEFAULT 0 NOT NULL,
    manifest_cid text NULL,
    manifest_built_for_version int4 NULL,
    manifest_built_at timestamptz NULL,
    last_append_at timestamptz DEFAULT now() NOT NULL,
    last_modified timestamptz DEFAULT now() NULL,

    created_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT object_versions_pkey PRIMARY KEY (object_id, version_seq),
    CONSTRAINT object_versions_status_check CHECK (((status)::text = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text, 'failed'::text])))
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_object_versions_object_created_desc
  ON public.object_versions (object_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_object_versions_object_type_created_desc
  ON public.object_versions (object_id, version_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_object_versions_ipfs_cid
  ON public.object_versions (ipfs_cid);

CREATE INDEX IF NOT EXISTS idx_object_versions_cid_id
  ON public.object_versions (cid_id);

CREATE INDEX IF NOT EXISTS idx_object_versions_status
  ON public.object_versions (status);

CREATE INDEX IF NOT EXISTS idx_object_versions_manifest_builder
  ON public.object_versions (last_append_at, append_version)
  WHERE (manifest_built_for_version IS NULL OR append_version > manifest_built_for_version);

-- Pointer on objects to current version sequence
ALTER TABLE public.objects
  ADD COLUMN IF NOT EXISTS current_version_seq bigint NULL;

-- Backfill: create initial version_seq=1 for each object, and set current_version_seq
WITH ins AS (
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
    o.object_id,
    1 AS version_seq,
    'user'::version_type,
    o.storage_version,
    o.size_bytes,
    o.content_type,
    o.metadata,
    o.md5_hash,
    o.ipfs_cid,
    o.cid_id,
    o.multipart,
    o.status,
    o.append_version,
    o.manifest_cid,
    o.manifest_built_for_version,
    o.manifest_built_at,
    o.last_append_at,
    COALESCE(o.last_modified, o.created_at),
    o.created_at
  FROM public.objects o
  WHERE NOT EXISTS (
    SELECT 1 FROM public.object_versions ov WHERE ov.object_id = o.object_id
  )
  RETURNING object_id, version_seq
)
UPDATE public.objects o
SET current_version_seq = 1
FROM ins
WHERE o.object_id = ins.object_id
  AND o.current_version_seq IS NULL;

-- Enforce FK from objects to object_versions (composite)
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_current_version_fk;
ALTER TABLE public.objects
  ADD CONSTRAINT objects_current_version_fk
  FOREIGN KEY (object_id, current_version_seq)
  REFERENCES public.object_versions(object_id, version_seq)
  ON DELETE RESTRICT;

-- Parts now link to object_versions
ALTER TABLE public.parts
  ADD COLUMN IF NOT EXISTS object_version_seq bigint NULL;

-- Backfill parts.object_version_seq from objects.current_version_seq
UPDATE public.parts p
SET object_version_seq = o.current_version_seq
FROM public.objects o
WHERE p.object_id = o.object_id
  AND p.object_version_seq IS NULL;

-- Add FK to composite key (object_id, version_seq)
ALTER TABLE public.parts
  DROP CONSTRAINT IF EXISTS parts_object_version_fk;
ALTER TABLE public.parts
  ADD CONSTRAINT parts_object_version_fk
  FOREIGN KEY (object_id, object_version_seq)
  REFERENCES public.object_versions(object_id, version_seq)
  ON DELETE CASCADE;

-- Replace uniqueness: (object_id, part_number) -> (object_id, object_version_seq, part_number)
ALTER TABLE public.parts
  DROP CONSTRAINT IF EXISTS parts_object_id_part_number_key;
ALTER TABLE public.parts
  ADD CONSTRAINT parts_object_version_part_unique
  UNIQUE (object_id, object_version_seq, part_number);

-- Helpful index for parts lookup by version
DROP INDEX IF EXISTS idx_parts_object_id;
CREATE INDEX IF NOT EXISTS idx_parts_object_version
  ON public.parts (object_id, object_version_seq);

-- Drop obsolete indexes on objects
DROP INDEX IF EXISTS idx_objects_ipfs_cid;
DROP INDEX IF EXISTS idx_objects_md5_hash;
DROP INDEX IF EXISTS idx_objects_cid_id;
DROP INDEX IF EXISTS idx_objects_status;
DROP INDEX IF EXISTS idx_objects_storage_version;
DROP INDEX IF EXISTS idx_objects_manifest_builder;
DROP INDEX IF EXISTS idx_objects_last_modified;

-- Drop moved columns from objects
ALTER TABLE public.objects
  DROP COLUMN IF EXISTS ipfs_cid,
  DROP COLUMN IF EXISTS size_bytes,
  DROP COLUMN IF EXISTS content_type,
  DROP COLUMN IF EXISTS metadata,
  DROP COLUMN IF EXISTS md5_hash,
  DROP COLUMN IF EXISTS cid_id,
  DROP COLUMN IF EXISTS multipart,
  DROP COLUMN IF EXISTS status,
  DROP COLUMN IF EXISTS append_version,
  DROP COLUMN IF EXISTS manifest_cid,
  DROP COLUMN IF EXISTS manifest_built_for_version,
  DROP COLUMN IF EXISTS manifest_built_at,
  DROP COLUMN IF EXISTS last_append_at,
  DROP COLUMN IF EXISTS last_modified,
  DROP COLUMN IF EXISTS storage_version;

-- Strengthen invariant: versions must exist for every object; prevent null current_version_seq
ALTER TABLE public.objects
  ALTER COLUMN current_version_seq SET DEFAULT 1;
-- Ensure no NULLs remain (backfill already set to 1)
UPDATE public.objects SET current_version_seq = 1 WHERE current_version_seq IS NULL;
ALTER TABLE public.objects
  ALTER COLUMN current_version_seq SET NOT NULL;

-- migrate:down

-- 1) Drop FK from objects to object_versions to allow column changes
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_current_version_fk;

-- 2) Re-add per-version columns on objects as NULLable for backfill
ALTER TABLE public.objects
  ADD COLUMN IF NOT EXISTS ipfs_cid text NULL,
  ADD COLUMN IF NOT EXISTS size_bytes int8 NULL,
  ADD COLUMN IF NOT EXISTS content_type text NULL,
  ADD COLUMN IF NOT EXISTS metadata jsonb NULL,
  ADD COLUMN IF NOT EXISTS md5_hash text NULL,
  ADD COLUMN IF NOT EXISTS cid_id uuid NULL,
  ADD COLUMN IF NOT EXISTS multipart bool NULL,
  ADD COLUMN IF NOT EXISTS status varchar(50) NULL,
  ADD COLUMN IF NOT EXISTS append_version int4 NULL,
  ADD COLUMN IF NOT EXISTS manifest_cid text NULL,
  ADD COLUMN IF NOT EXISTS manifest_built_for_version int4 NULL,
  ADD COLUMN IF NOT EXISTS manifest_built_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS last_append_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS last_modified timestamptz NULL,
  ADD COLUMN IF NOT EXISTS storage_version int2 NULL;

-- 3) Backfill object columns from the latest version (current_version_seq if present, else max)
WITH latest AS (
  SELECT ov.*
  FROM public.object_versions ov
  JOIN (
    SELECT o.object_id,
           COALESCE(o.current_version_seq, (SELECT MAX(version_seq) FROM public.object_versions x WHERE x.object_id = o.object_id)) AS v
    FROM public.objects o
  ) sel ON sel.object_id = ov.object_id AND sel.v = ov.version_seq
)
UPDATE public.objects o
SET ipfs_cid = l.ipfs_cid,
    size_bytes = l.size_bytes,
    content_type = l.content_type,
    metadata = l.metadata,
    md5_hash = l.md5_hash,
    cid_id = l.cid_id,
    multipart = l.multipart,
    status = l.status,
    append_version = l.append_version,
    manifest_cid = l.manifest_cid,
    manifest_built_for_version = l.manifest_built_for_version,
    manifest_built_at = l.manifest_built_at,
    last_append_at = l.last_append_at,
    last_modified = COALESCE(l.last_modified, o.created_at),
    storage_version = l.storage_version
FROM latest l
WHERE o.object_id = l.object_id;

-- 4) Enforce NOT NULLs and defaults, add back constraints
ALTER TABLE public.objects
  ALTER COLUMN size_bytes SET NOT NULL,
  ALTER COLUMN content_type SET NOT NULL,
  ALTER COLUMN append_version SET DEFAULT 0,
  ALTER COLUMN append_version SET NOT NULL,
  ALTER COLUMN last_append_at SET DEFAULT now(),
  ALTER COLUMN last_append_at SET NOT NULL,
  ALTER COLUMN storage_version SET NOT NULL,
  ALTER COLUMN multipart SET DEFAULT false,
  ALTER COLUMN status SET DEFAULT 'publishing';

-- Status check constraint (recreate)
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_status_check;
ALTER TABLE public.objects
  ADD CONSTRAINT objects_status_check CHECK (((status)::text = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text, 'failed'::text])));

-- FK on cid_id
ALTER TABLE public.objects
  DROP CONSTRAINT IF EXISTS objects_cid_id_fkey;
ALTER TABLE public.objects
  ADD CONSTRAINT objects_cid_id_fkey FOREIGN KEY (cid_id) REFERENCES public.cids(id);

-- 5) Revert parts to object-scoped uniqueness by removing version dimension
-- Keep only parts for the current version to avoid duplicates
DELETE FROM public.parts p
USING public.objects o
WHERE p.object_id = o.object_id
  AND p.object_version_seq IS NOT NULL
  AND o.current_version_seq IS NOT NULL
  AND p.object_version_seq <> o.current_version_seq;

-- Drop FK to object_versions and versioned uniqueness
ALTER TABLE public.parts
  DROP CONSTRAINT IF EXISTS parts_object_version_fk;
ALTER TABLE public.parts
  DROP CONSTRAINT IF EXISTS parts_object_version_part_unique;

-- Drop version index and column
DROP INDEX IF EXISTS idx_parts_object_version;
ALTER TABLE public.parts
  DROP COLUMN IF EXISTS object_version_seq;

-- Restore original uniqueness and index
ALTER TABLE public.parts
  ADD CONSTRAINT parts_object_id_part_number_key UNIQUE (object_id, part_number);
CREATE INDEX IF NOT EXISTS idx_parts_object_id ON public.parts (object_id);

-- 6) Restore original indexes on objects
CREATE INDEX IF NOT EXISTS idx_objects_ipfs_cid ON public.objects (ipfs_cid);
CREATE INDEX IF NOT EXISTS idx_objects_md5_hash ON public.objects (md5_hash);
CREATE INDEX IF NOT EXISTS idx_objects_cid_id ON public.objects (cid_id);
CREATE INDEX IF NOT EXISTS idx_objects_status ON public.objects (status);
CREATE INDEX IF NOT EXISTS idx_objects_storage_version ON public.objects (storage_version);
CREATE INDEX IF NOT EXISTS idx_objects_manifest_builder
  ON public.objects (last_append_at, append_version)
  WHERE ((manifest_built_for_version IS NULL) OR (append_version > manifest_built_for_version));
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON public.objects (last_modified);

-- 7) Drop object_versions and helper type, and current_version_seq column
DROP TABLE IF EXISTS public.object_versions;
ALTER TABLE public.objects DROP COLUMN IF EXISTS current_version_seq;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'version_type'
    ) THEN
        DROP TYPE version_type;
    END IF;
END $$;
