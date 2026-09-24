-- B4: the version was made serveable before its drain address landed. Put it back in the
-- reserved-row shape so reads skip it. Do not touch the lock columns. Clearing them would drop a
-- retention or legal hold an admin set after the tail committed and before this revert, and a
-- legal hold cleared here would release an object nobody asked to release.
-- Parameters: $1 object_id (UUID), $2 object_version (BIGINT)
UPDATE object_versions
SET size_bytes = 0,
    md5_hash = ''
WHERE object_id = $1
  AND object_version = $2
