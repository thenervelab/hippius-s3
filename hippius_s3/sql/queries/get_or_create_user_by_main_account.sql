-- Get or create a user by main_account_id (simplified - main_account_id is the primary key)
-- Parameters: $1: main_account_id, $2: created_at
WITH new_user AS (
    INSERT INTO users (main_account_id, created_at)
    VALUES ($1, $2)
    ON CONFLICT (main_account_id) DO NOTHING
    RETURNING main_account_id
)
SELECT main_account_id FROM new_user
UNION ALL
SELECT main_account_id FROM users WHERE main_account_id = $1
LIMIT 1
