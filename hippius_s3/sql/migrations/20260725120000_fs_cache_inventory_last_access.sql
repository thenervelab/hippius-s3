-- migrate:up

-- Read-recency signal for hot-retention, replacing filesystem atime as the
-- "recently read" channel. atime refresh was an os.utime() on every chunk read:
-- dead on read-only mounts (prod api-local mounts the pool RO, so EROFS was
-- silently swallowed and NO read traffic could protect a pool-resident part
-- from eviction) and an MDS metadata WRITE from the read path everywhere else.
-- NULL = never read since this column shipped; consumers treat NULL as
-- cached_at, so the rollout degrades to the old write-recency behavior.
ALTER TABLE fs_cache_inventory ADD COLUMN IF NOT EXISTS last_access_at TIMESTAMPTZ;

-- migrate:down

ALTER TABLE fs_cache_inventory DROP COLUMN IF EXISTS last_access_at;
