-- migrate:up transaction:false

-- LS-3: the ListObjects ordered range scan uses idx_objects_bucket_prefix (bucket_id, object_key)
-- but every list/get query filters `deleted_at IS NULL`, so the scan visits and discards
-- soft-deleted (tombstone) rows interleaved in the key range — churny buckets pay most, and it
-- compounds the delimiter skip-scan. A partial index over only the active rows keeps tombstones out
-- of the scanned range entirely.
--
-- Added as a pure, zero-downtime addition. The old non-partial idx_objects_bucket_prefix is left in
-- place for now; dropping it is a follow-up once this partial index is confirmed to serve every
-- consumer (audit console_list_objects.sql, which may not filter deleted_at IS NULL).
--
-- CONCURRENTLY (+ `transaction:false`) is mandatory: a plain CREATE INDEX takes a SHARE lock that
-- blocks every write on `objects` for the whole build, and migrations run before the API serves
-- traffic on deploy. Postgres forbids CONCURRENTLY inside a transaction block — hence the directive.
-- Must stay a SINGLE statement (dbmate runs a transaction:false body as one simple query; a
-- multi-statement string runs in an implicit transaction and would break CONCURRENTLY).
--
-- Recovery: a build that fails midway leaves an INVALID index; `IF NOT EXISTS` then SKIPS it. Drop
-- the leftover (`DROP INDEX CONCURRENTLY idx_objects_bucket_prefix_active;`) or REINDEX CONCURRENTLY
-- before re-running.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_bucket_prefix_active
    ON objects (bucket_id, object_key) WHERE deleted_at IS NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_objects_bucket_prefix_active;
