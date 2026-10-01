-- migrate:up

-- Anonymous GET/HEAD prefixes for one bucket. A row publishes the current version of
-- keys strictly under that prefix (the key starts with prefix || '/'). It does not
-- publish the rest of the bucket, and it is deliberately not a bucket ACL grant:
-- AllUsers READ on the bucket publishes every key. Soft-deleted buckets are excluded
-- by the read query; a hard delete cascades.
--
-- The table is empty, so the FK lock on buckets is brief. lock_timeout still bounds
-- the wait because prod runs with lock_timeout = 0, and a pending ACCESS EXCLUSIVE
-- blocks every request queued behind it.
SET LOCAL lock_timeout = '3s';

CREATE TABLE bucket_public_prefixes (
    bucket_id UUID NOT NULL REFERENCES buckets (bucket_id) ON DELETE CASCADE,
    prefix    TEXT NOT NULL,
    PRIMARY KEY (bucket_id, prefix),
    CONSTRAINT ck_bucket_public_prefixes_shape CHECK (
        octet_length(prefix) BETWEEN 1 AND 1024
        AND position('*' IN prefix) = 0
    )
);

-- migrate:down

DROP TABLE IF EXISTS bucket_public_prefixes;
