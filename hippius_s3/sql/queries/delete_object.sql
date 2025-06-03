-- Delete an object only if the user has permission on the bucket
-- Parameters: $1: bucket_id, $2: object_key, $3: main_account_id
DELETE FROM objects
WHERE bucket_id = $1
AND object_key = $2
AND (
    -- Check if the user has permission on the parent bucket
    EXISTS (
        SELECT 1 FROM buckets b
        WHERE b.bucket_id = $1
        AND b.main_account_id = $3
    )
)
RETURNING object_id, ipfs_cid
