# Runbook: the storage-usage rollup

Operating the maintained per-bucket byte counter — the number a plan customer is billed against.

Mechanism and design reasoning live in
[`20260910120000_storage_usage_rollup.sql`](../../hippius_s3/sql/migrations/20260910120000_storage_usage_rollup.sql)
and [`hippius_s3/sql/CLAUDE.md`](../../hippius_s3/sql/CLAUDE.md). This file is only about what to do
when something is wrong.

## The one-paragraph model

Five row triggers on `objects` / `object_versions` append `(bucket_id, delta_bytes)` to an
insert-only `storage_delta_ledger`. The single-replica `usage-rollup` worker folds the ledger into
`bucket_storage_usage` every 5s, and separately recomputes a rolling slice of live buckets from
ground truth and reports the correction as **drift**. An account total is a `SUM` over its live
buckets. Nothing serves a number until `storage_usage_rollup_state.backfilled_at` is set.

**A frozen counter is a wrong bill, not an outage.** Nothing in the request path notices, which is
why the alerts below exist.

## Alerts and what to do

Rules live in [`k8s/otel/values/prometheus.yaml`](../../k8s/otel/values/prometheus.yaml) under
`serverFiles."alerting_rules.yml"` and are pinned by `tests/unit/test_rollup_alerting.py`.

| Alert | Means | First action |
|---|---|---|
| `StorageRollupWorkerSilent` | No cycles for 15m. **Usage is frozen estate-wide.** | `kubectl -n <ns> logs deploy/usage-rollup -c usage-rollup --tail=100`. It never raises out of its loop, so silence means the pod is down, crash-looping, or stuck on an init container. |
| `StorageRollupDrift` | A write path moved bytes without emitting a delta | Grep `STORAGE_ROLLUP_DRIFT` for bucket ids. Then read [Diagnosing drift](#diagnosing-drift). |
| `StorageRollupLedgerLagging` | Compacting but behind | Usually a long recompute holding the global advisory lock. Check for a `recompute_bucket_storage_usage` in `pg_stat_activity`. Self-clears. |
| `StorageRollupNegativeCounter` | A decrement without its increment | Real trigger defect. The reconciler repairs the value, not the cause. |
| `PlansCacheStale` / `PlansCacherSilent` | The quota gate's cache is frozen | Last-known-good keeps serving. Check the upstream plans endpoint and `StorageRollupNotBackfilled`. |
| `PlanGateUnavailable` | The gate could not resolve a plan | **Customer-visible.** Plan accounts fall through to the credit path, which they cannot satisfy → 402. Check `redis-accounts`. |

## ⚠️ The kill switch, and why it is not a switch

`hippius_s3/sql/CLAUDE.md` rule 1 tells you to `ALTER TABLE ... DISABLE TRIGGER` before a bulk
migration. **Understand the cost before you type it.**

`ALTER TABLE ... DISABLE TRIGGER` takes **ACCESS EXCLUSIVE** on `objects` or `object_versions`. That
blocks **reads as well as writes** — every GET, HEAD and ListObjects on the whole estate, not just
writes. Production runs `lock_timeout = 0`, so if a long query is in flight the statement **waits**,
and every request arriving behind it queues too. On 166M-row `objects` that is a full data-plane
stall for as long as the wait lasts.

**Always bound it yourself:**

```sql
BEGIN;
SET LOCAL lock_timeout = '3s';
ALTER TABLE object_versions DISABLE TRIGGER object_versions_storage_delta_upd;
COMMIT;
```

If it times out, that is the correct outcome — retry when the primary is quieter. Do not remove the
timeout to "get it done".

**Before you reach for it, ask whether you need it at all.** The triggers cost one ledger INSERT per
statement that actually moves billable bytes; prod inserts ~1–3 rows/s total. A bulk job is only
worth disabling them for if it will move millions of rows.

**If you do disable them, you MUST recompute afterwards** or the counter is silently wrong forever:

```sql
-- re-enable first, then repair
ALTER TABLE object_versions ENABLE TRIGGER object_versions_storage_delta_upd;
SELECT recompute_bucket_storage_usage(bucket_id) FROM buckets WHERE ...;  -- the affected buckets
```

`TRUNCATE` does not fire row triggers **at all** and there is no way to make it. After any truncate
of either table every counter is stale with nothing to correct it — recompute the whole estate.

## Diagnosing drift

Drift is expected to be **exactly zero** once backfilled. Non-zero means bytes moved without a
delta. In order of likelihood:

1. **Is the estate actually backfilled?** `SELECT backfilled_at FROM storage_usage_rollup_state`. If
   NULL, every recompute legitimately moves a counter off zero and is logged as
   `STORAGE_ROLLUP_SEEDING` at INFO, not drift.
2. **Did a bulk job run with triggers disabled and no recompute?** See above.
3. **A new write path.** Any statement that moves `objects.current_object_version` or changes
   `object_versions.size_bytes` must be covered by the trigger set. The
   `objects`/`object_versions` `WHEN` clauses are deliberately narrow — a new column that affects
   truth needs adding to them.
4. **A transaction that creates an `objects` row and its first version in SEPARATE statements.**
   `objects_current_version_fk` is `DEFERRABLE INITIALLY DEFERRED` so this is *possible*, and it
   emits **nothing**. All four current allocators are single-statement; a fifth would not be.
5. **A hard delete of a LIVE object racing a finalize** over-counts. Known and accepted: the
   `objects` DELETE trigger is BEFORE DELETE, which runs before the statement locks the objects row,
   so a version lock there would be the forbidden order. Unreachable from the janitor
   (`hard_delete_object.sql` requires `deleted_at IS NOT NULL`); reachable from
   `delete_legacy_object_versions.py`. If drift appears right after an operator purge, this is why.

**Repairing one bucket** is safe at any time, concurrently with live traffic:

```sql
SELECT * FROM recompute_bucket_storage_usage('<bucket-uuid>');
```

It SETS the counter to truth and discards that bucket's pending ledger rows in the same snapshot, so
it converges rather than double-counting. It holds the rollup's **global** advisory lock while it
runs, which pauses the compactor — harmless (the ledger is insert-only) but it is why the reconciler
is rate-limited.

## The lock order you must not break

> **ALWAYS LOCK `objects` BEFORE `object_versions`. NEVER THE REVERSE.**

And the trap: a transaction reaches the `objects` row through **any `object_id` foreign key** —
`INSERT INTO parts` and `INSERT INTO multipart_uploads` each take an implicit
`objects ... FOR KEY SHARE`. So the rule applies even when no SQL in your transaction names
`objects`. Getting this wrong deadlocked **24% of concurrent same-key PUTs** at concurrency 32.
Call `lock_object_row_by_id.sql` first. Guarded by `tests/unit/test_storage_usage_lock_order.py`.

## Backfill

Manual, deliberately — it is in no kustomization and no deploy workflow runs it.

```bash
# 1. pin the image to what the target environment is running
kubectl -n <ns> get ds api-local -o jsonpath='{.spec.template.spec.containers[0].image}'
# 2. edit k8s/backfill-bucket-storage-usage-job.yaml: namespace + that image
# 3. dry run, read the output, then apply
kubectl create -f k8s/backfill-bucket-storage-usage-job.yaml
```

Safe under live traffic and safe to re-run: one bucket per transaction, recompute SETS rather than
adds, and `backfilled_at` is written **only after a complete pass** — so an interrupted backfill
degrades to the pre-rollup behaviour, never to a wrong bill.

Expect **~15 minutes** for ~48k buckets. Every changed bucket should move `0 -> truth`; a bucket
moving from a non-zero value to a different one means the maintained path had it wrong, which is
worth stopping for.
