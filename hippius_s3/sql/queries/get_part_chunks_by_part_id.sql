-- Parameters: $1: part_id (UUID)
SELECT chunk_index, cid, cipher_size_bytes, plain_size_bytes
FROM part_chunks
WHERE part_id = $1
ORDER BY chunk_index ASC;
