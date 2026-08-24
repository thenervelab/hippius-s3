-- Parameters: $1: account_id (SS58)
SELECT account_id, mode, created_at, updated_at
FROM account_suspensions
WHERE account_id = $1
