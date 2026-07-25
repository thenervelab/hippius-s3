-- migrate:up

-- Read-recency signal for hot-retention, replacing filesystem atime as the
-- "recently read" channel. atime refresh was an os.utime() on every chunk read:
-- dead on read-only mounts (prod api-local mounts the pool RO, so EROFS was
-- silently swallowed and NO read traffic could protect a pool-resident part
-- from eviction) and an MDS metadata WRITE from the read path everywhere else.
-- NULL = never read since this column shipped; consumers treat NULL as
-- cached_at, so the rollout degrades to the old write-recency behavior.

-- Bounded lock wait. The ADD COLUMN is catalog-only (nullable, no default → no
-- table rewrite), but it still takes ACCESS EXCLUSIVE for an instant. On prod
-- fs_cache_inventory (387 MB / 1.3M rows) is scanned continuously by the
-- janitor's SQL eviction and updated every 30s by the api's AccessTracker, and
-- migrations run on api pod startup — i.e. concurrently with a live janitor.
-- Without a timeout the ALTER would wait behind any in-flight scan, and because
-- a pending ACCESS EXCLUSIVE blocks every request queued after it, the whole
-- table would stall until that scan finished. Failing fast is strictly better:
-- the rollout retries on the next pod start while the old pods keep serving.
-- SET LOCAL, not SET, so it cannot leak into later migrations on this session.
SET LOCAL lock_timeout = '3s';

ALTER TABLE fs_cache_inventory ADD COLUMN IF NOT EXISTS last_access_at TIMESTAMPTZ;

-- migrate:down

ALTER TABLE fs_cache_inventory DROP COLUMN IF EXISTS last_access_at;
