-- Update object_versions row with final metadata after streaming upload.
-- body_blake3 rides along here rather than in its own UPDATE: this is the only caller, the tail
-- transaction already has the digest in hand, and folding it in keeps the PUT hot path at the
-- same round-trip count it had before the digest existed.
UPDATE object_versions
   SET size_bytes = $1,
       md5_hash = $2,
       content_type = $3,
       metadata = $4,
       last_modified = $5,
       body_blake3 = $8
 WHERE object_id = $6 AND object_version = $7
