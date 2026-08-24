-- Reactivate an account (idempotent — zero rows when it was not suspended).
-- Parameters: $1: account_id (SS58)
DELETE FROM account_suspensions
WHERE account_id = $1
RETURNING account_id
