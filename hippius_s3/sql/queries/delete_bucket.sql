-- Delete a bucket only if the user has permission
-- Parameters: $1: bucket_id, $2: user_id
DELETE FROM buckets
WHERE bucket_id = $1
AND (
    -- User is the owner
    owner_user_id = $2
    OR
    -- User is a sub-account of the owner
    EXISTS (
        SELECT 1 FROM sub_accounts sa
        WHERE sa.parent_user_id = owner_user_id
        AND sa.sub_user_id = $2
    )
)
RETURNING bucket_id, bucket_name
