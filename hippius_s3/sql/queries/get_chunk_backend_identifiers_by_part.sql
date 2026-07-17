-- Batch backend-identifier lookup for every chunk of an object/version on one backend.
-- Returns (part_number, chunk_index, backend_identifier) so the downloader can build a map once per
-- DownloadChainRequest instead of a 3-table-join fetchrow per chunk (RD-1).
-- $1 backend (TEXT), $2 object_id (UUID), $3 object_version (BIGINT)
SELECT p.part_number, pc.chunk_index, cb.backend_identifier
FROM chunk_backend cb
JOIN part_chunks pc ON pc.id = cb.chunk_id
JOIN parts p ON pc.part_id = p.part_id
WHERE cb.backend = $1
  AND p.object_id = $2
  AND p.object_version = $3
  AND NOT cb.deleted
  AND cb.backend_identifier IS NOT NULL;
