-- Record that a reconcile ATTEMPT happened and failed.
--
-- Must run in its own transaction: the recompute's transaction has already rolled back by the time
-- the caller gets here, which is exactly why `attempted_at` cannot be stamped inside
-- recompute_bucket_storage_usage() for the failure case.
--
-- UPDATE-ONLY, deliberately -- there is no INSERT arm. A bucket with no counter row has never been
-- seeded, and `get_account_storage_bytes_rollup.sql` reports that as `missing_buckets`. Inserting a
-- row here to carry the timestamp would set bytes_used = 0, converting "we do not know this bucket's
-- size" into "this bucket stores nothing" -- a silent, wrong billing answer in exchange for a
-- bookkeeping field. The case does not arise in practice: a bucket with no counter row has no
-- objects to aggregate, so its recompute is trivial and does not fail. Pinned by
-- tests/integration/test_rollup_verify_giant_buckets.py.
UPDATE bucket_storage_usage AS bsu
   SET attempted_at = now(),
       recompute_failures = bsu.recompute_failures + 1
 WHERE bsu.bucket_id = $1
RETURNING bsu.recompute_failures
