-- Create a purge job; the partial unique index uq_purge_jobs_account_active makes
-- concurrent creates race-safe — the loser gets zero rows and re-reads the winner.
-- Parameters: $1: job_id (UUID), $2: account_id (SS58)
INSERT INTO purge_jobs (job_id, account_id)
VALUES ($1, $2)
ON CONFLICT (account_id) WHERE state IN ('queued', 'running') DO NOTHING
RETURNING job_id
