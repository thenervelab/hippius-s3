-- Supports per-account "recent uploads" feed: per-bucket descending scan by last_modified.
-- Partial on deleted_at IS NULL so soft-deleted rows don't bloat the index.
CREATE INDEX IF NOT EXISTS idx_objects_bucket_last_modified_active
    ON public.objects (bucket_id, last_modified DESC)
    WHERE deleted_at IS NULL;
