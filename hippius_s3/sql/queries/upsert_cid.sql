-- Upsert a CID and return the cid_id
-- Parameters: $1: cid (TEXT)
INSERT INTO cids (id, cid)
VALUES (gen_random_uuid(), $1)
ON CONFLICT (cid)
DO UPDATE SET cid = EXCLUDED.cid
RETURNING id
