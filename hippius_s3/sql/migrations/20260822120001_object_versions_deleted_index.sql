-- migrate:up transaction:false

-- Drives the janitor's version-reap sweep (find_versions_ready_for_reap.sql), which walks a keyset
-- ring ordered by (deleted_at, object_id, object_version) over soft-deleted versions. The composite
-- shape matches that ordering exactly, so the ring scan needs no sort node; a bare (deleted_at)
-- index would work but adds an Incremental Sort at the same build cost.
--
-- Partial, so it costs nothing until versions start being deleted — no existing row has deleted_at
-- set, and on a bucket that never enables versioning none ever will.
--
-- CONCURRENTLY (+ `transaction:false` above) is mandatory: a plain CREATE INDEX takes a SHARE lock
-- that blocks every write on object_versions for the whole build, and the build must scan the full
-- ~49 GB heap to evaluate the partial predicate (a partial index shrinks the OUTPUT, not the build
-- scan). At ~146M rows that is a multi-minute stall on every PUT/GET/HEAD/LIST — migrations run on
-- API pod startup, so it would be a data-plane outage. Postgres forbids CONCURRENTLY inside a
-- transaction block, hence the directive.
--
-- Recovery caveat: a CONCURRENTLY build that fails midway (crash, deadlock, cancelled deploy)
-- leaves an INVALID index of this name behind, and `IF NOT EXISTS` then SKIPS it on the next run
-- rather than rebuilding. If this migration errors, drop the leftover
-- (`DROP INDEX CONCURRENTLY idx_object_versions_deleted;`) or `REINDEX INDEX CONCURRENTLY
-- idx_object_versions_deleted;` before re-running. This MUST stay a SINGLE statement: dbmate runs
-- a `transaction:false` body as one multi-command simple query, and Postgres runs any
-- multi-statement string in an IMPLICIT transaction — a second statement here would make
-- CREATE INDEX CONCURRENTLY fail with "cannot run inside a transaction block".
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_object_versions_deleted
    ON object_versions (deleted_at, object_id, object_version)
    WHERE deleted_at IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_object_versions_deleted;
