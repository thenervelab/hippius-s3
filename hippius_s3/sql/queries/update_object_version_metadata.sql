-- Update object_versions row with final metadata after streaming upload
UPDATE object_versions
   SET size_bytes = $1,
       md5_hash = $2,
       content_type = $3,
       metadata = $4,
       last_modified = $5
 WHERE object_id = $6 AND object_version = $7
