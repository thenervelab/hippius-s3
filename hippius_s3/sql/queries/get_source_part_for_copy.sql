SELECT ipfs_cid
  FROM parts
 WHERE object_id = $1
   AND object_version = $2
   AND part_number = 1
 LIMIT 1
