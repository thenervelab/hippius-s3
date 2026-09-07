-- migrate:up
-- Records, per version, that the VERIFIED CALLER who wrote it was a service account.
--
-- OBSERVABILITY, NOT AUTHORIZATION. The billing exemption itself follows `address` (the bucket
-- owner), because Arion charges the owner for storage in their bucket whoever wrote the bytes —
-- owner-pays. What that leaves invisible is WHICH unmetered uploads into our own buckets we made
-- ourselves and which a guest made through a WRITE grant. This column is the only thing that can
-- tell them apart: the caller's identity cannot be reconstructed in the worker, because since
-- drain-direct the API does not build the UploadChainRequest — the Rust drain-agent does.
--
-- Deliberately not used to gate the upload. Refusing the exemption for a guest write would not
-- move the cost (Arion bills the owner regardless), it would 402 the upload into the DLQ against
-- a service account that carries no credit — breaking shared buckets to protect nothing.
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
