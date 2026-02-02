-- $1 part_id (UUID), $2 chunk_indexes INT[], $3 cipher_sizes BIGINT[]
INSERT INTO part_chunks (part_id, chunk_index, cipher_size_bytes)
SELECT $1, t.chunk_index, t.cipher_size_bytes
FROM unnest($2::int[], $3::bigint[]) AS t(chunk_index, cipher_size_bytes)
ON CONFLICT (part_id, chunk_index) DO NOTHING;
