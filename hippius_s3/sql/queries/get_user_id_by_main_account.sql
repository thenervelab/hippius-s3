-- Read-only existence check for a user by main_account_id (the primary key)
-- Parameters: $1: main_account_id
SELECT main_account_id FROM users WHERE main_account_id = $1
