-- Get a user by seed phrase
-- Parameters: $1: seed_phrase
SELECT user_id FROM users WHERE seed_phrase = $1
