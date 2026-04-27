-- migrate:up

-- Per-bucket descending scan by created_at, partial on active rows.
-- Required by /user/recent_uploads. Without this, heavy users (e.g. one prod
-- account has 11.8M objects in a single bucket) trigger a full per-bucket scan.
-- Built CONCURRENTLY out-of-band via k8s/migrate-recent-uploads-index-apply.yaml;
-- the apply job inserts the schema_migrations row before this file runs via
-- dbmate, so dbmate sees it applied and skips it (no write lock during deploy).
CREATE INDEX IF NOT EXISTS idx_objects_bucket_created_desc_active
    ON public.objects (bucket_id, created_at DESC)
    WHERE deleted_at IS NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_objects_bucket_created_desc_active;
