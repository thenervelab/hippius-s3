-- migrate:up

-- s3-2.1 PR-7: the main-account address (SS58) for the drain-gated upload promoter.
--
-- The arion-uploader / ovh-backup workers re-derive everything they need about an
-- object from the DB by object_id EXCEPT the main-account address (the Arion/S3
-- upload identity). Today the api carries it in the enqueued UploadChainRequest. The
-- drain-gated promoter (PR-7d) builds that request itself once a part replicates, so
-- it needs the address persisted. The api writes it on the promoter path only
-- (PR-7e), so the column is NULL for legacy/non-gated rows — harmless, the promoter
-- only runs when the feature is enabled.
ALTER TABLE object_versions ADD COLUMN IF NOT EXISTS address TEXT;

-- migrate:down

ALTER TABLE object_versions DROP COLUMN IF EXISTS address;
