-- migrate:up

-- Per-account "recent uploads" feed scaling fix.
-- The previous global index idx_object_versions_last_modified_desc forced the planner
-- to backward-scan all object_versions rows in chronological order and only filter
-- by main_account_id at the very end. For low-volume accounts on a busy system this
-- scans millions of unrelated rows (16s+ on prod, 30s+ cold).
-- This composite partial index lets the planner go user -> buckets -> per-bucket
-- descending scan -> top-10. Work is bounded by the user's footprint.
-- The matching query change is in queries/get_recent_uploads_for_account.sql
-- (ORDER BY o.last_modified instead of ov.last_modified) so the planner can use it.
CREATE INDEX IF NOT EXISTS idx_objects_bucket_last_modified_active
    ON public.objects (bucket_id, last_modified DESC)
    WHERE deleted_at IS NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_objects_bucket_last_modified_active;
