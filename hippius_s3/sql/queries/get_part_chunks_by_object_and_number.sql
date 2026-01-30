-- Parameters: $1: object_id (UUID), $2: object_version (INT), $3: part_number (INT)
SELECT
    pc.chunk_index,
    -- Backend identifiers now live in chunk_backend. Prefer active IPFS backend identifiers,
    -- but keep pc.cid as a legacy fallback for older rows.
    COALESCE(cb.backend_identifier, pc.cid) AS cid,
    pc.cipher_size_bytes,
    pc.plain_size_bytes
FROM parts p
JOIN part_chunks pc ON pc.part_id = p.part_id
LEFT JOIN chunk_backend cb
  ON cb.chunk_id = pc.id
 AND cb.backend = 'ipfs'
 AND NOT cb.deleted
WHERE p.object_id = $1 AND p.object_version = $2 AND p.part_number = $3
ORDER BY pc.chunk_index ASC;
