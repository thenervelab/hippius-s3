-- Claim the next purge job: oldest queued, or a running job whose lease expired
-- (worker crash / pod kill — heartbeat_at stopped advancing). FOR UPDATE SKIP LOCKED
-- makes concurrent claimers race-safe; started_at survives a reclaim so the original
-- start time is preserved.
-- Parameters: $1: lease seconds (float)
UPDATE purge_jobs
SET state = 'running', started_at = COALESCE(started_at, now()), heartbeat_at = now()
WHERE job_id = (
    SELECT job_id
    FROM purge_jobs
    WHERE state = 'queued'
       OR (state = 'running' AND heartbeat_at < now() - make_interval(secs => $1))
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING job_id, account_id, deleted_objects, deleted_bytes
