-- migrate:up

-- Record, on each multipart upload, whether the destination key already served an object at the
-- moment the upload was initiated.
--
-- CompleteMultipartUpload with If-None-Match: * has to answer "did this key exist?", but by the time
-- it runs it can no longer tell: upsert_object_multipart clears objects.deleted_at at INITIATE, so a
-- key that was soft-deleted before the upload started looks live at completion. Judging on that
-- refused a create-only completion over a soft-deleted key AND left the key's pre-delete content
-- readable once the client aborted — undeleting data on a write we rejected.
--
-- The flag is the MPU's equivalent of the PutObject reserve-time baseline: existence is judged when
-- the upload starts, and the completion re-check only has to look for versions that appeared since.
--
-- ROLLOUT ORDER: this migration must apply BEFORE the code that writes the column (create_multipart_upload
-- names it explicitly). DEFAULT FALSE is a metadata-only add on PG11+, and uploads initiated by the
-- previous build simply read as "key did not exist" — harmless, since If-None-Match on MPU completion
-- is new in the same release and no in-flight upload can be using it.
-- BOUND THE LOCK WAIT. `ADD COLUMN` takes ACCESS EXCLUSIVE on multipart_uploads, and on production
-- that table is ~216.8M rows / 68 GB carrying ~8.5 writes/s (measured) -- every simple PUT creates a
-- row through ensure_upload_row, so it is squarely on the write path. The add itself is
-- metadata-only (a non-volatile DEFAULT is catalog-only on PG11+, so no 68 GB rewrite) and
-- therefore fast ONCE ACQUIRED; the risk is entirely in the acquisition. Production runs
-- `lock_timeout = 0`, so without this the statement waits behind whatever is in flight while every
-- arriving write queues behind the pending lock request -- a write-path stall with no upper bound.
--
-- Failing the migration is the correct outcome: dbmate wraps the file in one transaction including
-- its own schema_migrations row, so a timeout leaves nothing half-applied, and the db-migrations
-- Job retries (backoffLimit 3). Retry in a quieter moment beats stalling the fleet.
--
-- This repo has been bitten by exactly this before -- 20260822120001 was split out of
-- 20260822120000 for it. Pinned by tests/unit/test_migration_lock_guards.py.
SET LOCAL lock_timeout = '3s';

ALTER TABLE multipart_uploads
    ADD COLUMN IF NOT EXISTS key_existed_at_initiate BOOLEAN NOT NULL DEFAULT FALSE;

-- migrate:down

SET LOCAL lock_timeout = '3s';

ALTER TABLE multipart_uploads
    DROP COLUMN IF EXISTS key_existed_at_initiate;
