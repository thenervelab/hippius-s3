-- $1 object_id, $2 object_version
-- Did the VERIFIED CALLER who wrote this version hold a billing exemption?
-- COALESCE so a missing version reads FALSE (billed), never NULL.
SELECT COALESCE((
    SELECT ov.billing_bypass
    FROM object_versions ov
    WHERE ov.object_id = $1 AND ov.object_version = $2
), FALSE) AS billing_bypass
