WITH target AS (
    SELECT
        o.object_id,
        o.bucket_id,
        o.object_key,
        ov.cid_id,
        COALESCE(c.cid, ov.ipfs_cid) AS ipfs_cid
    FROM objects o
    JOIN buckets b ON b.bucket_id = o.bucket_id AND b.main_account_id = $3
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
    LEFT JOIN cids c ON ov.cid_id = c.id
    WHERE o.bucket_id = $1
      AND o.object_key = $2
), deleted_object AS (
    DELETE FROM objects o
    USING target t
    WHERE o.object_id = t.object_id
    RETURNING o.object_id
)
SELECT t.object_id, t.ipfs_cid
FROM target t
JOIN deleted_object d ON d.object_id = t.object_id
