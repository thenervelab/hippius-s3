-- Check if a user has permission to access a bucket
-- Parameters: $1: bucket_id, $2: user_id
SELECT EXISTS (
    SELECT 1 FROM buckets b
    WHERE b.bucket_id = $1 AND (
        -- User is the owner
        b.owner_user_id = $2
        OR
        -- User is a sub-account of the owner
        EXISTS (
            SELECT 1 FROM sub_accounts sa
            WHERE sa.parent_user_id = b.owner_user_id
            AND sa.sub_user_id = $2
        )
    )
) as has_permission
