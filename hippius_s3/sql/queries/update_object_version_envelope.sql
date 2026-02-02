UPDATE object_versions
   SET encryption_version = 5,
       enc_suite_id = $1,
       enc_chunk_size_bytes = $2,
       kek_id = $3,
       wrapped_dek = $4
 WHERE object_id = $5
   AND object_version = $6
