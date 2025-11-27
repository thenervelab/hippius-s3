-- Parameters: $1 part_id (UUID), $2 chunk_index (INT), $3 cid (TEXT), $4 cipher_size_bytes (INT), $5 plain_size_bytes (INT), $6 checksum (BYTEA), $7 api_file_id (TEXT)
INSERT INTO part_chunks (part_id, chunk_index, cid, cipher_size_bytes, plain_size_bytes, checksum, api_file_id)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (part_id, chunk_index)
DO UPDATE SET
  cid = EXCLUDED.cid,
  cipher_size_bytes = EXCLUDED.cipher_size_bytes,
  plain_size_bytes = COALESCE(EXCLUDED.plain_size_bytes, part_chunks.plain_size_bytes),
  checksum = COALESCE(EXCLUDED.checksum, part_chunks.checksum),
  api_file_id = COALESCE(EXCLUDED.api_file_id, part_chunks.api_file_id);
