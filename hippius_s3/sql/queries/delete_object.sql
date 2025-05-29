-- Delete an object only if the user has permission on the bucket
-- Parameters: $1: bucket_id, $2: object_key, $3: user_id
DELETE FROM objects
WHERE bucket_id = $1
AND object_key = $2
AND (
    -- Check if the user has permission on the parent bucket
    EXISTS (
        SELECT 1 FROM buckets b
        WHERE b.bucket_id = $1
        AND (
            -- User is the owner
            b.owner_user_id = $3
            OR
            -- User is a sub-account of the owner
            EXISTS (
                SELECT 1 FROM sub_accounts sa
                WHERE sa.parent_user_id = b.owner_user_id
                AND sa.sub_user_id = $3
            )
        )
    )
)
RETURNING object_id, ipfs_cid
