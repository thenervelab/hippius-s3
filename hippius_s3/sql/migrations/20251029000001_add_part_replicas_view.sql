-- migrate:up

-- Optional clarity view to separate replicas rows from parity
CREATE OR REPLACE VIEW part_replicas AS
SELECT
  ppc.part_id,
  ppc.policy_version,
  ppc.stripe_index AS chunk_index,
  ppc.parity_index AS replica_index,
  ppc.cid,
  ppc.created_at
FROM part_parity_chunks ppc
JOIN part_ec pec
  ON pec.part_id = ppc.part_id AND pec.policy_version = ppc.policy_version
WHERE pec.scheme = 'rep-v1';

-- migrate:down
DROP VIEW IF EXISTS part_replicas;
