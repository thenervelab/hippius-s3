-- Abandoned multipart uploads: a version whose parts landed but whose
-- object_versions.address was never written (the api writes it at PUT/MPU-complete),
-- initiated more than $1 seconds ago AND with no part uploaded in the last $1 seconds.
-- A completed or simple upload always has a non-NULL address, so it is never matched;
-- the NULL-address + age pair distinguishes a genuinely abandoned upload (the drain
-- would defer its enqueue as not-ready forever) from a still-in-flight one.
-- The last-activity gate is the safety valve: a user may initiate an MPU, upload parts
-- slowly, pause for days, then resume + complete. Keying on initiated_at alone would
-- reap that upload mid-flight; requiring NO part in the last $1 seconds means any part
-- upload resets the clock, so only a genuinely inactive upload is reaped.
-- is_completed=false is a belt-and-suspenders guard so a completed upload can't be
-- selected. Returns one row per (upload_id, object_id, object_version) to reap, plus
-- age_seconds (how long ago the upload was initiated) so the reaper can report its lag.
-- Parameters: $1: stale_seconds (int)
SELECT DISTINCT mu.upload_id, p.object_id, p.object_version,
       EXTRACT(EPOCH FROM (now() - mu.initiated_at))::float8 AS age_seconds
FROM multipart_uploads mu
JOIN parts p ON p.upload_id = mu.upload_id
LEFT JOIN object_versions ov
       ON ov.object_id = p.object_id AND ov.object_version = p.object_version
WHERE COALESCE(mu.is_completed, false) = false
  AND (ov.address IS NULL OR ov.object_id IS NULL)
  AND mu.initiated_at < now() - make_interval(secs => $1)
  AND NOT EXISTS (
        SELECT 1 FROM parts pr
        WHERE pr.upload_id = mu.upload_id
          AND pr.uploaded_at >= now() - make_interval(secs => $1)
      )
ORDER BY mu.upload_id
LIMIT 2000
