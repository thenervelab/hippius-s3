-- Get a user by main_account_id
-- Parameters: $1: main_account_id
SELECT user_id FROM users WHERE main_account_id = $1
