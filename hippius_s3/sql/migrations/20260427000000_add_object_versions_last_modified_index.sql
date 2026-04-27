-- migrate:up

-- Supports the per-account "recent uploads" feed.
-- The dashboard query orders by object_versions.last_modified DESC and takes top 10.
-- A backward index scan over this index lets the planner short-circuit the LIMIT
-- without sorting the full join result for active users.
CREATE INDEX IF NOT EXISTS idx_object_versions_last_modified_desc
    ON public.object_versions (last_modified DESC);

-- migrate:down

DROP INDEX IF EXISTS idx_object_versions_last_modified_desc;
