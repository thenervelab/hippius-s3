-- migrate:up

-- Bucket-level versioning state. NULL means "never enabled", which is every bucket that exists
-- today — so all existing buckets keep their current behaviour and only opt in explicitly.
-- AWS forbids returning to unversioned once enabled, so there is no transition back to NULL.
ALTER TABLE buckets
  ADD COLUMN IF NOT EXISTS versioning_status text NULL;

ALTER TABLE buckets
  DROP CONSTRAINT IF EXISTS buckets_versioning_status_check;
ALTER TABLE buckets
  ADD CONSTRAINT buckets_versioning_status_check
  CHECK (versioning_status IS NULL OR versioning_status IN ('Enabled', 'Suspended'));

-- A delete marker is an object_versions row with no data: zero size, no parts, no DEK envelope.
-- It becomes objects.current_object_version, so "this key is deleted" is a property of the
-- version chain rather than of objects.deleted_at (which keeps its whole-object meaning).
--
-- DEFAULT false is metadata-only on PG 11+, so this is safe against the ~144M row table.
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS is_delete_marker boolean NOT NULL DEFAULT false;

-- Per-version soft delete, matching the existing convention on objects and buckets.
--
-- Why not just DELETE the row: the unpinner resolves backend identifiers at PROCESSING time by
-- joining parts/part_chunks/chunk_backend. Dropping those rows in the request handler would leave
-- the queued unpin with nothing to find and leak the backend copy — the exact bug this change
-- exists to fix. So a version DELETE marks deleted_at, enqueues the unpin, and repoints
-- current_object_version; the janitor reaps the row once every chunk_backend row is confirmed
-- deleted.
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz NULL;

-- Drives the janitor's reap sweep. Partial, so it costs nothing until versions start being
-- deleted (no existing row has deleted_at set).
CREATE INDEX IF NOT EXISTS idx_object_versions_deleted
  ON object_versions (deleted_at)
  WHERE deleted_at IS NOT NULL;

-- migrate:down

DROP INDEX IF EXISTS idx_object_versions_deleted;

ALTER TABLE object_versions
  DROP COLUMN IF EXISTS deleted_at;

ALTER TABLE object_versions
  DROP COLUMN IF EXISTS is_delete_marker;

ALTER TABLE buckets
  DROP CONSTRAINT IF EXISTS buckets_versioning_status_check;

ALTER TABLE buckets
  DROP COLUMN IF EXISTS versioning_status;
