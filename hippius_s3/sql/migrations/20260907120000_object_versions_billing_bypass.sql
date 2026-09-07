-- migrate:up
-- Records, per version, that the VERIFIED CALLER who wrote it was a service account.
--
-- The uploader charges `object_versions.address`, which is the storage-attribution account — the
-- BUCKET OWNER, not the writer. Deriving the billing exemption from that address alone would
-- exempt anything written into a service-account-owned bucket, including by a third party holding
-- a WRITE grant on it. The gateway's decision is made from the authenticated caller and cannot be
-- reconstructed in the worker (since drain-direct the API does not build the UploadChainRequest —
-- the Rust drain-agent does), so it is persisted here instead.
--
-- ONLINE SAFETY: `object_versions` is ~152M rows / ~79 GB in production and migrations run on
-- deploy against the live primary. ADD COLUMN with a non-volatile DEFAULT has not rewritten the
-- heap since PG11 (the default lives in pg_attribute.attmissingval), so this is metadata-only
-- even with NOT NULL. No constraint, no index: the column is only ever read by object_id +
-- object_version, which the primary key already covers.
--
-- DEFAULT FALSE is the fail-closed direction: every pre-existing row, and every row written by a
-- version of the api that predates this column, is billed.
ALTER TABLE object_versions
    ADD COLUMN IF NOT EXISTS billing_bypass BOOLEAN NOT NULL DEFAULT FALSE;

-- migrate:down
ALTER TABLE object_versions
    DROP COLUMN IF EXISTS billing_bypass;
