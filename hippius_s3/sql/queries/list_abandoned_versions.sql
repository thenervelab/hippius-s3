-- Abandoned multipart uploads: a version whose parts landed but whose
-- object_versions.address was never written (the api writes it at PUT/MPU-complete),
-- and whose upload was initiated more than $1 seconds ago. A completed or simple
-- upload always has a non-NULL address, so it is never matched; the NULL-address +
-- age pair is what distinguishes a genuinely abandoned upload (the drain would defer
-- its enqueue as not-ready forever) from a still-in-flight one. is_completed=false is
-- a belt-and-suspenders guard so a completed upload can never be selected.
-- Returns one row per (upload_id, object_id, object_version) to reap.
-- Parameters: $1: stale_seconds (int)
SELECT DISTINCT mu.upload_id, p.object_id, p.object_version
FROM multipart_uploads mu
JOIN parts p ON p.upload_id = mu.upload_id
LEFT JOIN object_versions ov
       ON ov.object_id = p.object_id AND ov.object_version = p.object_version
WHERE COALESCE(mu.is_completed, false) = false
  AND (ov.address IS NULL OR ov.object_id IS NULL)
  AND mu.initiated_at < now() - make_interval(secs => $1)
ORDER BY mu.upload_id
LIMIT 200
