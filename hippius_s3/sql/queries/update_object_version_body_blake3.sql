-- Persist the in-flight BLAKE3 of the object plaintext onto the version.
-- Parameters: $1: digest (TEXT), $2: object_id (UUID), $3: object_version (BIGINT)
UPDATE object_versions
   SET body_blake3 = $1
 WHERE object_id = $2
   AND object_version = $3
