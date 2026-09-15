-- The drain's hand-off rows for the parts of one upload request: what state each part is in and
-- the digest the drain recorded when it handed the part over. The node-local uploader reads this
-- before it touches a part (a request for a part no longer 'uploading' is stale — re-driven,
-- already confirmed, or retired — and is dropped) and compares the digest to what it uploaded
-- before it writes any chunk_backend row.
--
-- Params: $1 object_id (text — cephor stores it as text), $2 version (bigint),
-- $3 part_numbers (bigint[]).
SELECT part_number, status, content_sha256
FROM cephor_replication_status
WHERE object_id = $1 AND version = $2 AND part_number = ANY($3::bigint[])
