-- F4: a per-claim fencing token for cephor_replication_status.
--
-- claim_part stamps a fresh, globally-monotonic value (nextval) on each claim and
-- returns it in ClaimedPart; mark_replicated commits only if the row STILL carries
-- the same token. Without it, a `draining` claim re-won after lease expiry (the
-- crash-recovery path) leaves the row `status='draining'` under the new claimer — so
-- the stale original claimer's commit would match the bare `status='draining'` guard,
-- mark the part replicated, and unlink the SSD copy out from under the live drain.
-- The token makes that stale commit match zero rows -> StoreError::PartClaimLost.
--
-- Nullable with no backfill: any pre-deploy `draining` row carries NULL and is simply
-- re-claimed (fresh token) by an agent after the lease, since agents restart on deploy
-- and hold no in-flight claims across it.
CREATE SEQUENCE IF NOT EXISTS cephor_claim_seq;
ALTER TABLE cephor_replication_status ADD COLUMN IF NOT EXISTS claim_seq BIGINT;
