-- migrate:up

-- Soft-delete column. DeleteBucket sets this and returns 204 in O(1) instead
-- of running a 6-table FK cascade that exceeds the API's 30s statement_timeout
-- on buckets with significant child-row residue. Hard cleanup runs async via
-- the bucket_reaper worker.
ALTER TABLE public.buckets ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Replace the global unique constraint with a partial unique index so a name
-- becomes reusable the instant the prior bucket is soft-deleted (matches S3
-- DeleteBucket semantics: name immediately reusable). Live-set uniqueness is
-- still enforced — the constraint named buckets_bucket_name_key was restored
-- in 20251126000000 specifically for S3 spec compliance, so do NOT replace
-- it with a per-account composite.
ALTER TABLE public.buckets DROP CONSTRAINT IF EXISTS buckets_bucket_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS buckets_bucket_name_active_key
    ON public.buckets (bucket_name) WHERE deleted_at IS NULL;

-- Lookup index for the reaper to find pending work without scanning live rows.
CREATE INDEX IF NOT EXISTS idx_buckets_deleted_at_pending
    ON public.buckets (deleted_at) WHERE deleted_at IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_buckets_deleted_at_pending;
DROP INDEX IF EXISTS buckets_bucket_name_active_key;
ALTER TABLE public.buckets ADD CONSTRAINT buckets_bucket_name_key UNIQUE(bucket_name);
ALTER TABLE public.buckets DROP COLUMN IF EXISTS deleted_at;
