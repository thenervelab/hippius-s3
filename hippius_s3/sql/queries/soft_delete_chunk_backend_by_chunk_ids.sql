-- $1 backend (TEXT), $2 chunk_ids (BIGINT[])
-- One statement for a slice of ids (the unpinner caps the slice at 1000). DISTINCT so a
-- repeated id cannot update the same row twice in this statement. Empty RETURNING is
-- success: AND NOT deleted skips a row a previous attempt already marked, and that retry
-- must still ack.
UPDATE chunk_backend AS cb
SET deleted = true,
    deleted_at = now()
FROM (SELECT DISTINCT chunk_id FROM unnest($2::bigint[]) AS ids(chunk_id)) AS ids
WHERE cb.chunk_id = ids.chunk_id
  AND cb.backend = $1
  AND NOT cb.deleted
RETURNING cb.chunk_id;
