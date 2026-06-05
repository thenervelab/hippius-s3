-- migrate:up

-- Drop the redundant UNIQUE on multipart_uploads (~7 GB on prod, ~1M scans
-- in prod but redundant: upload_id is already the primary key, so this
-- bucket_id+object_key+upload_id UNIQUE adds no actual constraint coverage).
-- No code references the name (see indexes.md Tier 2 grep).
--
-- This may exist as a NAMED CONSTRAINT or as a BARE INDEX depending on how
-- the original was declared. The DO block handles both. The dryrun job's
-- pg_constraint probe tells you which branch will fire.
--
-- Run CONCURRENTLY out-of-band via k8s/cleanup-indexes-staging-apply.yaml;
-- that job inserts the schema_migrations row before this file runs via dbmate,
-- so dbmate sees it applied and skips it.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'multipart_uploads_bucket_id_object_key_upload_id_key'
  ) THEN
    ALTER TABLE public.multipart_uploads
      DROP CONSTRAINT multipart_uploads_bucket_id_object_key_upload_id_key;
  ELSIF EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname  = 'multipart_uploads_bucket_id_object_key_upload_id_key'
  ) THEN
    DROP INDEX public.multipart_uploads_bucket_id_object_key_upload_id_key;
  END IF;
END $$;

-- migrate:down

-- Rollback: recreate as a constraint (the conservative default). If the
-- original was a bare index, the apply job's pg_constraint probe will have
-- told you so — and a constraint is the safer reconstruction either way.
-- Confirm no current row violations before running this.
ALTER TABLE public.multipart_uploads
  ADD CONSTRAINT multipart_uploads_bucket_id_object_key_upload_id_key
  UNIQUE (bucket_id, object_key, upload_id);
