-- migrate:up transaction:false

-- Partial: locked versions are the rare case, and every read is "is THIS version locked".
--
-- Split out of 20260902100000 and built CONCURRENTLY because a plain CREATE INDEX takes a SHARE
-- lock that blocks every write to `object_versions` for the whole build — and on the ~152M-row /
-- ~79 GB production table the build still has to scan the entire heap to discover that (today)
-- not one row matches the predicate. A very selective partial index is cheap to STORE, never
-- cheap to BUILD. Postgres forbids CONCURRENTLY inside a transaction block, hence the directive.
--
-- Must stay a SINGLE statement: dbmate runs a transaction:false body as one simple query, and a
-- multi-statement string runs in an implicit transaction, which would break CONCURRENTLY.
--
-- Recovery: a build that fails midway leaves an INVALID index, which `IF NOT EXISTS` then SKIPS
-- on the next run — leaving an index that is never used for reads but is still maintained on
-- every write. Check with the query in docs/runbooks/object-lock-migration.md and drop the
-- leftover (`DROP INDEX CONCURRENTLY idx_object_versions_locked;`) before re-running.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_object_versions_locked ON object_versions (object_id, object_version) WHERE object_lock_retain_until IS NOT NULL OR object_lock_legal_hold;

-- migrate:down

DROP INDEX IF EXISTS idx_object_versions_locked;
