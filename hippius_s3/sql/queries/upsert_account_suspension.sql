-- Suspend an account (idempotent). Mode transitions (full <-> read_only) just update.
-- Parameters: $1: account_id (SS58), $2: mode ('full' | 'read_only')
INSERT INTO account_suspensions (account_id, mode)
VALUES ($1, $2)
ON CONFLICT (account_id) DO UPDATE SET mode = EXCLUDED.mode
RETURNING account_id, mode
