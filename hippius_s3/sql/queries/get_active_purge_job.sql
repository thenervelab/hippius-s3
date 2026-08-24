-- Parameters: $1: account_id (SS58)
SELECT job_id, state
FROM purge_jobs
WHERE account_id = $1 AND state IN ('queued', 'running')
