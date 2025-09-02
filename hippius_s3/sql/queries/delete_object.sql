-- Delete an object only if the user has permission on the bucket
-- Parameters: $1: bucket_id, $2: object_key, $3: main_account_id
WITH deleted_object AS (
    DELETE FROM objects o
    WHERE o.bucket_id = $1
    AND o.object_key = $2
    AND (
        -- Check if the user has permission on the parent bucket
        EXISTS (
            SELECT 1 FROM buckets b
            WHERE b.bucket_id = $1
            AND b.main_account_id = $3
        )
    )
    RETURNING o.object_id, o.cid_id, o.ipfs_cid
)
SELECT d.object_id, COALESCE(c.cid, d.ipfs_cid) as ipfs_cid
FROM deleted_object d
LEFT JOIN cids c ON d.cid_id = c.id
