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
ALTER TABLE multipart_uploads
    ADD COLUMN IF NOT EXISTS key_existed_at_initiate BOOLEAN NOT NULL DEFAULT FALSE;

-- migrate:down

ALTER TABLE multipart_uploads
    DROP COLUMN IF EXISTS key_existed_at_initiate;
