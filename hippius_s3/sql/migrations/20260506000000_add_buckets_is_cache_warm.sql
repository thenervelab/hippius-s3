-- Adds the warm-cache flag for buckets. When true, the gateway emits a
-- WARM_PUBLIC Cache-Control header so ATS holds bodies indefinitely (30d max-age)
-- instead of revalidating on every request. Source of truth is owned by the
-- hippius-s3-cache-control service.

ALTER TABLE buckets
    ADD COLUMN is_cache_warm boolean NOT NULL DEFAULT false;

-- Partial index — the warm subset is small relative to total bucket count,
-- and the only consumer that filters on this column is the cache-control
-- sweeper enumerating warm buckets.
CREATE INDEX idx_buckets_is_cache_warm ON buckets(bucket_id) WHERE is_cache_warm = true;
