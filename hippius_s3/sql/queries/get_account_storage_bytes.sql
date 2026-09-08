-- Total billable bytes stored by an account. THE definition of "storage used" in this codebase.
--
-- O(number of objects the account owns): measured ~90-190ms for a 1k-object account and ~0.2-1.0s
-- for a 1M-object account, depending on the plan the planner picks. The largest prod account holds
-- 11.8M objects in one bucket, where this is seconds and admin.py already degrades to a null count
-- on timeout. So it must never run on a routine request path.
--
-- ONE caller: the plans-cacher, in the background, once per poll per PLAN account (a few tens of
-- accounts, so a few seconds a cycle with nobody waiting on it). Nothing on the request path runs
-- this -- the quota gate reads the cached result and does not re-check, not even to confirm a
-- denial, which is why the refresh interval is the enforcement lag in both directions.
--
-- Keep in sync with get_admin_account_stats.sql and console_list_buckets.sql -- those two disagreed
-- with each other until 2026-09, and they are the numbers an operator and a customer each see.
--
-- Parameters: $1: main_account_id (SS58)
SELECT COALESCE(SUM(ov.size_bytes), 0)::bigint AS bytes_used
FROM buckets b
JOIN objects o
  ON o.bucket_id = b.bucket_id
 AND o.deleted_at IS NULL
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = o.current_object_version
 AND ov.deleted_at IS NULL
 AND NOT ov.is_delete_marker
WHERE b.main_account_id = $1
  AND b.deleted_at IS NULL
