-- $1 object_id, $2 object_version
-- Did the VERIFIED CALLER who wrote this version hold a billing exemption? Read by the uploader
-- to label the bypass metric owner-vs-guest; it does NOT decide the exemption (that follows the
-- bucket owner, owner-pays). COALESCE so a missing or pre-migration version reads FALSE, which
-- labels as "guest" — the conservative direction for a signal you alert on.
SELECT COALESCE((
    SELECT ov.billing_bypass
    FROM object_versions ov
    WHERE ov.object_id = $1 AND ov.object_version = $2
), FALSE) AS billing_bypass
