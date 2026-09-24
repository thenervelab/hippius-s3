-- Whether DeleteBucket must refuse with BucketNotEmpty: true while the bucket still holds ANY
-- version S3 would count, not just a listable current one. list_objects answered this before and
-- saw only keys whose newest version is live content, so a versioned bucket whose keys were all
-- hidden behind delete markers read as empty — and deleting it orphaned every non-current version,
-- Object-Locked ones included, with nothing left that could address them.
--
-- Two arms, either of which makes the bucket non-empty:
--
-- 1. A live object with at least one live completed version or delete marker. That is AWS's
--    definition (versions AND delete markers must all be gone). The serveable predicate is the one
--    list_objects uses: it skips the zero-byte placeholder an aborted multipart upload retains on a
--    brand-new key, which no client can see or delete and would otherwise wedge the bucket forever.
-- 2. A locked version ANYWHERE in the bucket, even under a soft-deleted object or itself
--    soft-deleted. The SQL gates keep those bytes on the backends until the lock ends; the bucket
--    must outlive them so they stay attributable and addressable. The predicate is
--    LOCKED_VERSION_SQL_PREDICATE (object_lock_enforcement.py) spelled out, as in every gated query.
--
-- EXISTS stops at the first hit, so a non-empty bucket answers from its first rows. Only a bucket
-- that really is empty of live objects pays for arm 2's walk over its soft-deleted object rows,
-- and those are drained continuously by the hard-delete ring.
--
-- Parameters: $1: bucket_id (uuid)
SELECT EXISTS (
    SELECT 1
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id
    WHERE o.bucket_id = $1
      AND o.deleted_at IS NULL
      AND ov.deleted_at IS NULL
      AND (ov.is_delete_marker OR ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
) OR EXISTS (
    SELECT 1
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id
    WHERE o.bucket_id = $1
      AND (ov.object_lock_legal_hold
           OR (ov.object_lock_retain_until IS NOT NULL AND ov.object_lock_retain_until > now()))
) AS has_versions
