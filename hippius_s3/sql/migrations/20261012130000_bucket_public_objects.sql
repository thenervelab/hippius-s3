-- migrate:up

-- Anonymous GET/HEAD of every current key, without ListBucket.
--
-- PutBucketPolicy of s3:GetObject on arn:aws:s3:::<bucket>/* used to write the
-- bucket ACL canned public-read grant. AllUsers READ is what authorizes listing,
-- so a policy that names only GetObject made the keyspace enumerable. A row here
-- is the object-read grant and nothing else: the list check has no key, and this
-- table is not consulted for it. An object ACL row still seals that key.
--
-- The table is empty, so the FK lock on buckets is brief. lock_timeout still
-- bounds the wait because prod runs with lock_timeout = 0.
SET LOCAL lock_timeout = '3s';

CREATE TABLE bucket_public_objects (
    bucket_id UUID PRIMARY KEY REFERENCES buckets (bucket_id) ON DELETE CASCADE
);

-- migrate:down

SET LOCAL lock_timeout = '3s';

DROP TABLE IF EXISTS bucket_public_objects;
