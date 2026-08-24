-- Parameters: $1: job_id, $2: deleted_objects, $3: deleted_bytes, $4: progress (jsonb text)
UPDATE purge_jobs
SET deleted_objects = $2, deleted_bytes = $3, progress = $4::jsonb, heartbeat_at = now()
WHERE job_id = $1
