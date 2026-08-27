-- migrate:up

-- Columns only. The partial index that drives the janitor's reap sweep is built separately in
-- 20260822120001, CONCURRENTLY — see that file for why it cannot live here.
--
-- ROLLBACK CAVEAT: migrate:down below is mechanically correct and re-runnable, but it is NOT
-- semantically safe once delete markers exist. Dropping is_delete_marker turns every marker into a
-- live zero-byte object and every soft-deleted version back into a live one, so deleted keys
-- reappear in listings. Treat the down migration as "undo before the feature is used", not as a
-- production rollback.

-- A long-running reader can otherwise queue behind these ALTERs and make them wait while they hold
-- ACCESS EXCLUSIVE, stalling the whole data plane. Failing fast and retrying the deploy is better.
--
-- SET LOCAL, not SET: dbmate applies every pending migration over ONE connection, and a plain SET
-- is session-scoped, so it would survive this COMMIT and still be in force for the CREATE INDEX
-- CONCURRENTLY in 20260822120001. That build's wait-for-older-snapshots phases take
-- VirtualXactLock through the lock manager and ARE lock_timeout-sensitive, so any transaction
-- older than 3s would abort it — leaving an INVALID index that `IF NOT EXISTS` then silently skips
-- on retry while marking the migration applied. The reap sweep would seq-scan ~146M rows forever
-- with nothing in the logs to explain it. SET LOCAL reverts at COMMIT.
SET LOCAL lock_timeout = '3s';

-- Bucket-level versioning state. NULL means "never enabled", which is every bucket that exists
-- today — so all existing buckets keep their current behaviour and only opt in explicitly.
-- AWS forbids returning to unversioned once enabled, so there is no transition back to NULL.
ALTER TABLE buckets
  ADD COLUMN IF NOT EXISTS versioning_status text NULL;

-- `buckets` is ~40k rows / 7 MB, so validating this against existing rows is sub-second.
ALTER TABLE buckets
  DROP CONSTRAINT IF EXISTS buckets_versioning_status_check;
ALTER TABLE buckets
  ADD CONSTRAINT buckets_versioning_status_check
  CHECK (versioning_status IS NULL OR versioning_status IN ('Enabled', 'Suspended'));

-- A delete marker is an object_versions row with no data: zero size, no parts, no DEK envelope.
-- It becomes objects.current_object_version, so "this key is deleted" is a property of the
-- version chain rather than of objects.deleted_at (which keeps its whole-object meaning).
--
-- NOT NULL DEFAULT false is metadata-only on PG 11+ (attmissingval), so this does not rewrite the
-- ~146M row table.
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS is_delete_marker boolean NOT NULL DEFAULT false;

-- Per-version soft delete, matching the existing convention on objects and buckets.
--
-- Why not just DELETE the row: the unpinner resolves backend identifiers at PROCESSING time by
-- joining parts/part_chunks/chunk_backend. Dropping those rows in the request handler would leave
-- the queued unpin with nothing to find and leak the backend copy — the exact bug this change
-- exists to fix. So a version DELETE marks deleted_at, enqueues the unpin, and repoints
-- current_object_version; the janitor reaps the parts once every chunk_backend row is confirmed
-- deleted.
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz NULL;

-- migrate:down

ALTER TABLE object_versions
  DROP COLUMN IF EXISTS deleted_at;

ALTER TABLE object_versions
  DROP COLUMN IF EXISTS is_delete_marker;

ALTER TABLE buckets
  DROP CONSTRAINT IF EXISTS buckets_versioning_status_check;

ALTER TABLE buckets
  DROP COLUMN IF EXISTS versioning_status;
