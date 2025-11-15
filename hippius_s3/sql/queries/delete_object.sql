WITH target AS (
    SELECT
        o.object_id,
        o.bucket_id,
        o.object_key,
        o.current_object_version,
        ov.cid_id,
        COALESCE(c.cid, ov.ipfs_cid) AS ipfs_cid
    FROM objects o
    JOIN buckets b ON b.bucket_id = o.bucket_id
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
    LEFT JOIN cids c ON ov.cid_id = c.id
    WHERE o.bucket_id = $1
      AND o.object_key = $2
), all_cids AS (
    SELECT DISTINCT t.object_id, COALESCE(c.cid, ov.ipfs_cid) AS cid
    FROM target t
    JOIN object_versions ov ON ov.object_id = t.object_id AND ov.object_version = t.current_object_version
    LEFT JOIN cids c ON ov.cid_id = c.id
    WHERE COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
      AND COALESCE(c.cid, ov.ipfs_cid) != ''
      AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'

    UNION

    SELECT DISTINCT t.object_id, COALESCE(c.cid, p.ipfs_cid) AS cid
    FROM target t
    JOIN parts p ON p.object_id = t.object_id AND p.object_version = t.current_object_version
    LEFT JOIN cids c ON p.cid_id = c.id
    WHERE COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
      AND COALESCE(c.cid, p.ipfs_cid) != ''
      AND COALESCE(c.cid, p.ipfs_cid) != 'pending'

    UNION

    SELECT DISTINCT t.object_id, pc.cid
    FROM target t
    JOIN parts p ON p.object_id = t.object_id AND p.object_version = t.current_object_version
    JOIN part_chunks pc ON pc.part_id = p.part_id
    WHERE pc.cid IS NOT NULL
      AND pc.cid != ''
      AND pc.cid != 'pending'
), deleted_object AS (
    DELETE FROM objects o
    USING target t
    WHERE o.object_id = t.object_id
    RETURNING o.object_id
)
SELECT
    t.object_id,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT ac.cid), NULL) AS all_cids
FROM target t
JOIN deleted_object d ON d.object_id = t.object_id
LEFT JOIN all_cids ac ON ac.object_id = t.object_id
GROUP BY t.object_id
