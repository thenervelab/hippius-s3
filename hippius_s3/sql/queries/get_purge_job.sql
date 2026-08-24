-- Parameters: $1: job_id (UUID)
SELECT job_id, account_id, state, deleted_objects, deleted_bytes, error,
       created_at, started_at, finished_at
FROM purge_jobs
WHERE job_id = $1
