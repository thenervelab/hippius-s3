-- Parameters: $1: object_id (UUID), $2: part_number (INT)
SELECT pc.chunk_index, pc.cid, pc.cipher_size_bytes, pc.plain_size_bytes
FROM parts p
JOIN part_chunks pc ON pc.part_id = p.part_id
WHERE p.object_id = $1 AND p.part_number = $2
ORDER BY pc.chunk_index ASC;
