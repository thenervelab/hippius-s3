-- Get or create a user by seed phrase
-- Parameters: $1: user_id, $2: seed_phrase, $3: created_at
WITH new_user AS (
    INSERT INTO users (user_id, seed_phrase, created_at)
    VALUES ($1, $2, $3)
    ON CONFLICT (seed_phrase) DO NOTHING
    RETURNING user_id
)
SELECT user_id FROM new_user
UNION ALL
SELECT user_id FROM users WHERE seed_phrase = $2
LIMIT 1
