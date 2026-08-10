-- Is this bucket marked public? Parameters: $1: bucket_name
--
-- Used by the anonymous /public/... router, which is the one api entry point the gateway cannot
-- authorize for: it parses the request as bucket `public` + key `<real-bucket>/<real-key>`, and
-- because `public` is a reserved segment no such bucket exists, so the ACL middleware takes its
-- "bucket not found, let the backend answer" path and performs no permission check. The router
-- therefore has to establish for itself that the bucket it is about to read really is public.
--
-- Returns no row for a missing or deleted bucket, which the caller must treat as "not public"
-- rather than as an error, so a probe cannot tell the two apart.
SELECT b.is_public
FROM buckets b
WHERE b.bucket_name = $1
  AND b.deleted_at IS NULL
LIMIT 1
