-- migrate:up
--
-- v5 envelope encryption metadata stored per object version.
-- These columns are intended to be populated when storage_version >= 5.
--
-- Note: columns are nullable for backward compatibility with existing versions.
-- A future migration can add CHECK constraints once all write paths populate them.

ALTER TABLE public.object_versions
  ADD COLUMN IF NOT EXISTS encryption_version int2 NULL;

ALTER TABLE public.object_versions
  ADD COLUMN IF NOT EXISTS enc_suite_id text NULL;

ALTER TABLE public.object_versions
  ADD COLUMN IF NOT EXISTS enc_chunk_size_bytes int4 NULL;

ALTER TABLE public.object_versions
  ADD COLUMN IF NOT EXISTS kek_id uuid NULL;

ALTER TABLE public.object_versions
  ADD COLUMN IF NOT EXISTS wrapped_dek bytea NULL;

CREATE INDEX IF NOT EXISTS idx_object_versions_kek_id
  ON public.object_versions (kek_id);


-- migrate:down
DROP INDEX IF EXISTS idx_object_versions_kek_id;

ALTER TABLE public.object_versions DROP COLUMN IF EXISTS wrapped_dek;
ALTER TABLE public.object_versions DROP COLUMN IF EXISTS kek_id;
ALTER TABLE public.object_versions DROP COLUMN IF EXISTS enc_chunk_size_bytes;
ALTER TABLE public.object_versions DROP COLUMN IF EXISTS enc_suite_id;
ALTER TABLE public.object_versions DROP COLUMN IF EXISTS encryption_version;
