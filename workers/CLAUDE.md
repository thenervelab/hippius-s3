# workers/

Worker entry points — the `run_*.py` scripts that actually run as pod processes in Kubernetes. Each file wraps shared logic from [../hippius_s3/workers/](../hippius_s3/workers/).

## Worker inventory

| Entry point | Purpose | Scaling |
|---|---|---|
| [run_arion_uploader_in_loop.py](run_arion_uploader_in_loop.py) | Drains `arion_upload_requests`, uploads chunks to Arion, publishes to chain. | Horizontally scalable (`replicas: 10` in production) |
| [run_arion_unpinner_in_loop.py](run_arion_unpinner_in_loop.py) | Drains `unpin_requests`, soft-deletes `chunk_backend` rows, calls Arion delete. | Horizontally scalable; per-pod request concurrency (`HIPPIUS_UNPINNER_MAX_INFLIGHT`) + shared Arion-DELETE semaphore (`HIPPIUS_UNPINNER_PARALLELISM`) |
| [run_janitor_in_loop.py](run_janitor_in_loop.py) | FS cache GC with replication gate, hot retention, and pressure modes. | Single instance |
| [run_account_cacher_in_loop.py](run_account_cacher_in_loop.py) | Warms account credit cache from Substrate. | Single instance |
| [run_plans_cacher_in_loop.py](run_plans_cacher_in_loop.py) | Scrapes the S3 billing-plan catalog + account→plan map from api.hippius.com into `redis-accounts`. | Single instance (**must stay `replicas: 1`**) |
| [run_usage_rollup_in_loop.py](run_usage_rollup_in_loop.py) | Folds the storage delta ledger into `bucket_storage_usage`; reconciles it and exports drift. | Single instance (**must stay `replicas: 1`**) |
| [cachet_health_check.py](cachet_health_check.py) | Pushes status to the external Cachet status page. | CronJob |

Each `run_*_in_loop.py` is a thin wrapper that imports the shared logic and provides backend-specific parameters (`backend_name`, `queue_name`, `fetch_fn`, etc.). See [../hippius_s3/workers/CLAUDE.md](../hippius_s3/workers/CLAUDE.md) for the core loop internals.

## Janitor (FS cache GC)

[run_janitor_in_loop.py](run_janitor_in_loop.py). Read the top-of-file docstring ([lines 1-22](run_janitor_in_loop.py)) — it spells out the invariants.

### Core invariant

**Replication is an absolute gate.** A chunk that has NOT been replicated to every required backend (`config.upload_backends` ∪ `config.backup_backends`, pinned in code as `STORAGE_BACKENDS`) is **never** deleted — under any conditions, including a full disk. The critical-pressure path still honors this: if nothing is replicated and disk is at 95%+, the janitor logs ERROR and deletes nothing. Operator paging, not data loss.

### Pressure modes

[run_janitor_in_loop.py:125-146](run_janitor_in_loop.py):

- **Normal** (<85% disk): honor `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` (default 4h). Evict replicated + aged + cold.
- **Elevated** (85-95%): halve the hot-retention window. Evict replicated + cold regardless of age.
- **Critical** (≥95%): hot retention disabled. Evict replicated + cold aggressively. If nothing replicated → log ERROR, do nothing.

### Cycle order (durability first) + walk bounding

The **DB-only durability phases run FIRST**, before the FS walks — the replication-gate sentinel and the A21 aged-orphan gauge. Deliberate: the FS cache is a single flat CephFS directory of millions of object dirs, and a full walk is metadata-latency bound (~40 objects/s serial on prod → ~20h a pass). Before this ordering those two ran LAST, behind two full-tree walks that never finished, so on prod they never ran at all. They must not be gated on the cache walk.

The FS-walk phases are **parallel, sharded, and budgeted** so a cycle always completes:
- `iter_part_dirs` fans the per-object descent across a thread pool (`HIPPIUS_JANITOR_WALK_CONCURRENCY`, default 8) so many CephFS metadata roundtrips are in flight at once — the single-threaded event-loop walk was the bottleneck, not the DB (per-part queries are 0.1–0.7ms, indexed).
- Each cycle covers one hash-shard (`HIPPIUS_JANITOR_WALK_SHARDS`, default 64) of the tree; a full sweep takes `shards` cycles. Under ELEVATED pressure a smaller rotation (`HIPPIUS_JANITOR_ELEVATED_WALK_SHARDS`, default 8) keeps the budget-truncated walk from restarting at the same readdir head every cycle; CRITICAL forces `shards=1` (whole tree every cycle).
- Each walk phase stops at `HIPPIUS_JANITOR_WALK_BUDGET_SECONDS` (default 480s); **lifted to unbounded under CRITICAL pressure** so freeing space is never capped by a clock.

### Cleanup passes

- `cleanup_stale_parts` — delete parts whose mtime > `MPU_STALE_SECONDS` (orphan-with-no-DB-row reap + terminally-abandoned reclaim). DLQ protection via `get_all_dlq_object_ids` — scans every upload + unpin DLQ per `config.upload_backends`. Fail-closed if the DLQ set is unavailable.
- Age-based GC (`cleanup_old_parts_by_mtime`) — classify by age bucket (0-1h / 1-6h / 6-24h / 1-3d / 3-7d / 7d+), gate on replication, honor hot retention. The census (parts/age-buckets/hot) is accumulated across a full sharded sweep and published only when the sweep completes untruncated, so the gauges reflect the whole cache, not one shard.
- Orphan `.tmp.*` cleanup — delete if older than `TMP_FILE_MAX_AGE_SECONDS=3600` (1h). Same sharded parallel descent as the GC walk (was a full-tree `rglob` that also blocked the loop for hours).
- Hard-delete for soft-deleted objects whose unpins have been confirmed on every backend (DB-bound, batch-capped).

### Metrics (OTel observable gauges + counters)

- `fs_store_parts_on_disk`
- `fs_store_oldest_age_seconds`
- `fs_cache_disk_used_bytes` / `fs_cache_disk_total_bytes`
- `fs_cache_hot_parts`
- `fs_cache_pressure_mode` (0/1/2)
- `fs_cache_age_bucket_parts{age_bucket=...}`
- `fs_janitor_deleted_total` / `fs_janitor_tmp_deleted_total`

## Account cacher

[run_account_cacher_in_loop.py](run_account_cacher_in_loop.py). Polls Substrate for account state (free/reserved balance, credits, bandwidth) and mirrors into `redis-accounts`. Cache TTL set by the cacher, not clients. `CACHER_LOOP_SLEEP=60`.

## Plans cacher

[run_plans_cacher_in_loop.py](run_plans_cacher_in_loop.py). One poll loop against one endpoint:

```
GET /api/s3/plans/accounts/?page=1&page_size=500     every HIPPIUS_PLANS_LOOP_SLEEP (120s)
```

It carries both halves — `plans` is the catalog, `results` is the paginated account roll — and is
split across two hashes on `redis-accounts`:

| Redis key | Field | Value |
|---|---|---|
| `hippius_s3_plan_accounts` | account SS58 | `{"plan", "storage_limit_bytes", "used_bytes"}` |
| `hippius_s3_billing_inactive` | account SS58 | `{"reason": "expired_plan"\|"payg_inactive", ...}` |
| `hippius_s3_plans` | plan name | `{"h256": ..., "storage_bytes": ...}` |
| `hippius_s3_plans:meta` | — | `{fetched_at, accounts, plans}` |

The account row carries everything the quota gate needs, so the request path is ONE `HGET` and a
comparison — no catalog lookup, no database. The two halves come from different places:

- **`storage_limit_bytes`** is the account's MAX QUOTA, from upstream. `results[].storage_bytes`
  wins over the plan's list price, so a negotiated limit is not silently overwritten.
- **`used_bytes`** is what the account actually stores, **read by this worker** from the maintained
  rollup — upstream reports no usage figure. One indexed SUM per account, over
  `HIPPIUS_PLANS_USAGE_CONCURRENCY` (4) connections.

### Where `used_bytes` comes from

`usage_service.get_account_storage_bytes` is one indexed SUM over `bucket_storage_usage` — a
MAINTAINED counter, sub-millisecond, and flat in the number of objects an account owns. See the
usage-rollup worker below, and
[hippius_s3/sql/CLAUDE.md](../hippius_s3/sql/CLAUDE.md#storage-usage-rollup-the-only-triggers-in-this-schema-that-move-a-billed-number)
for the trigger set that keeps it right.

It used to compute the number outright. Two designs, both retired:

1. **One aggregate per account.** Cost is driven by OBJECTS PER BUCKET, not by bucket fan-out and
   not by account count. One plan account's objects are concentrated in a single bucket holding a
   filesystem-style workload of **millions of small objects**; that aggregate takes over a minute. Both applicable ceilings are 30s — `HIPPIUS_PLANS_USAGE_TIMEOUT_SECONDS`
   and the replica's `max_standby_streaming_delay` — so it was cancelled every cycle, and because
   one account's failure fails the whole cycle, the roll was never published at all.
2. **The same aggregate, chunked into keyset pages.** That fixed the cancellation — no single
   statement got near either ceiling — at ~165s per cycle for that one account, forever, to
   rediscover a number that had moved by a handful of objects. It was O(objects) and that bucket
   only grows.

**Account cardinality was never the problem; objects-per-account was.** The "only a few tens of plan
accounts" premise both designs rested on was true and irrelevant: one account with one 7.8M-object
bucket breaks a recount design no matter how few accounts exist. The full scan now happens **once**,
as a backfill (`hippius_s3/scripts/backfill_bucket_storage_usage.py`), and never again.

**Before that backfill has run, the usage read RAISES.** `storage_usage_rollup_state.backfilled_at`
is NULL until a complete pass finishes, and until then the rollup holds deltas-since-migration
rather than totals. A raise fails the cycle, which keeps the previous roll serving — the same
degradation as any other failed cycle, and far better than publishing a small number as a
customer's usage.

**The read runs against a REPLICA** (`DATABASE_READONLY_URL`, falling back to `DATABASE_URL`), with
`jit=off` on the pool. The counter is WRITTEN on the primary by the usage-rollup worker, so replica
lag is one more small increment of staleness on a figure that is already a
`HIPPIUS_PLANS_LOOP_SLEEP`-old estimate by design.

⚠️ **The refresh interval IS the enforcement lag, in both directions.** An account can overshoot its
quota by one cycle's worth of uploads, and a customer who deletes data to get back under stays
refused until the next cycle sees it. Nothing on the request path recomputes.

A read failing for ANY account fails the whole cycle and keeps the previous roll: publishing a
partial answer would write `used_bytes=0` for the accounts we could not read, silently handing them
unlimited headroom.

**Only an *active* plan is written to the quota hash.** A row needs `billing == "plan"`, a plan
name, **and** `active` true. Ordinary PAYG is simply absent from both hashes, which is exactly what
the request path already reads as pay-as-you-go.

**Expired plans and inactive PAYG go on `hippius_s3_billing_inactive`.** Request-path waterfall
(gated on `HIPPIUS_ENABLE_BILLING_PLANS`):

1. Active plan → quota gate (skips credits / `can_upload`).
2. Else if PAYG is marked inactive → 402 `AccountInactive`.
3. Else try PAYG (credits + `can_upload`).
4. If PAYG also fails and the account had an expired plan → 402 `PlanExpired`.

Pinned by `tests/unit/test_plans_cacher_worker.py::test_an_inactive_plan_row_falls_through_to_payg`
and the account-middleware tests for `PlanExpired` / `AccountInactive`.

The shrink guard still refuses a truncated scrape. Accounts this scrape classified as
`expired_plan` are accounted for, so a real cancellation publishes rather than wedging the cacher
on the old allowance. Pinned by `test_expired_plans_are_dropped_from_the_live_map`.

Every cycle logs `upstream_active=N` (every row, not just plans) plus `expired_plan=` and
`payg_inactive=`. A drop back to `upstream_active=0` is the signal that the field has gone dark
again — the condition that made this check a silent no-op in PR #502.

```
{namespace="hippius-s3-prod",app="plans-cacher"} |= "Published plan roll"
```

**Caching is unconditional.** This worker does not read `HIPPIUS_ENABLE_BILLING_PLANS` and is not
deployed with it, so the maps stay warm and observably correct long before enforcement is switched
on — flipping the flag on the api is then a config change, not a cold-cache event.

**This pod being down is not an outage.** Neither hash has a TTL and `redis-accounts` is
`noeviction` + AOF, so the last known good roll keeps serving through an api.hippius.com outage and
across a Redis restart. Alert on `plans_cache_age_seconds`, not on pod restarts. CAVEAT: a value threshold on that gauge cannot fire for the failure it is named for -- a worker that is DOWN emits nothing at all, and `depth` is sampled right after a drain so it reads ~0 by construction. The alert that works is ABSENCE: no `storage_rollup_cycles_total` samples for N minutes. There are currently zero alert rules loaded cluster-wide, so this is a promise with nothing behind it.

Three invariants, all in [hippius_s3/services/plans_cache.py](../hippius_s3/services/plans_cache.py),
each of which exists to stop the same failure — silently demoting plan customers to pay-as-you-go
and 402ing them on their next upload:

1. **Publication is a whole-hash build-then-`RENAME`.** `refresh_plan_roll_once` fetches EVERY page
   before publishing; a failure on page 7 of 20 leaves the live hash untouched.
2. **An empty or heavily-shrunk roll is refused** (`MAX_ACCOUNT_MAP_SHRINK_RATIO`, 50%). One bad
   upstream deploy returning a truncated-but-valid list must not wipe the fleet's plans. Accounts
   this scrape classified as `expired_plan` are accounted for and do not count as a shrink.
3. **No TTL, ever.** A TTL would delete the last-known-good map during exactly the outage it exists
   to survive.

Pagination is bounded by `MAX_PAGES` so a self-referential `next` cursor cannot spin the worker
forever without publishing. Upstream returns `next` as an ABSOLUTE url; only its path and query are
followed, re-homed on our own configured host — otherwise the e2e cacher would walk out of
mock-hippius-api and into production.

`replicas` must stay 1 — two replicas would not corrupt anything (last `RENAME` wins) but would
double the upstream load for nothing.

## Usage rollup (storage counter)

[run_usage_rollup_in_loop.py](run_usage_rollup_in_loop.py). The only thing that turns the
trigger-fed delta ledger into the per-bucket counter the plans-cacher reads.

```
objects / object_versions  --5 row triggers, INSERT only-->  storage_delta_ledger
                                                                     |
                        COMPACT: DELETE ... RETURNING, fold, add     v
                                                              bucket_storage_usage
                                                                     |
                             account total = SUM over live buckets  <-+
```

Two jobs in one loop:

| Job | Interval | What it does |
|---|---|---|
| **Compact** | `HIPPIUS_USAGE_ROLLUP_LOOP_SLEEP` (5s) | Drains the ledger **one bucket at a time**, oldest work first, `HIPPIUS_USAGE_ROLLUP_BATCH_SIZE` (5000) rows per claim, with `DELETE ... RETURNING` under that bucket's advisory lock. Also accumulates `churn_bytes`. |
| **Reconcile** | `HIPPIUS_USAGE_RECONCILE_INTERVAL_SECONDS` (300s) | Recomputes `HIPPIUS_USAGE_RECONCILE_BUCKETS_PER_CYCLE` (50) live buckets, **least-recently-ATTEMPTED** first, and exports the correction as **drift**. ~5.6h for a full sweep over prod's ~3,350 live buckets, which is the only bound on how long a bucket can carry a wrong number. These are the heaviest aggregates in the schema and they run on the PRIMARY — a cold 200-bucket pass measured 35.9s / 9.8 GiB of buffer traffic, which is why the rate is 50 and not 200. Bounded per bucket by `HIPPIUS_USAGE_RECONCILE_TIMEOUT_SECONDS` (**20s**, far below the interval on purpose). Read the justification in [config.py](../hippius_s3/config.py) before changing any of them. |
| **Verify (sliced)** | same pass as Reconcile | A bucket that fails `HIPPIUS_USAGE_VERIFY_SLICE_AFTER_FAILURES` (2) recomputes is instead summed in indexed `object_key` ranges — `HIPPIUS_USAGE_VERIFY_SLICES_PER_CYCLE` (4) × `HIPPIUS_USAGE_VERIFY_SLICE_OBJECTS` (50k) per cycle — carrying a cursor across cycles. **It MEASURES only and never writes the counter.** Drift is claimed only when the gap exceeds the `churn_bytes` that moved during the sweep, which makes it a one-sided test that cannot false-positive on live traffic. |

**Least-recently-ATTEMPTED, not least-recently-recomputed.** `recompute_bucket_storage_usage()` only
stamps on success, so ordering the queue on that stamp meant a bucket whose aggregate can never
complete kept a NULL timestamp, sat at the head of the queue forever, and starved everything behind
it — prod 2026-09-15: 168 failures in 24h on one bucket and 47 of 50 slots used per pass. The
failure is recorded in its own transaction (the recompute's has already rolled back) so the bucket
rotates out after one try.

**Compaction is exactly-once by construction.** The rows leave the ledger in the same transaction
that adds them to the counter, so a crash puts them back and a second compactor can only see rows
nobody has taken. There is no `GREATEST(0, ...)` anywhere in the fold: clamping an upsert breaks
decrements, because `EXCLUDED.bytes_used` carries the clamped value into the `DO UPDATE` arm and
floors every decrement at zero. The counter is allowed to go negative; the READ path clamps and
reports.

⚠️ **Expected drift is ZERO.** The triggers are maintained against `get_account_storage_bytes.sql`
and asserted against it write-path by write-path in
`tests/integration/test_storage_usage_rollup.py`. So a non-zero
`storage_rollup_drifted_buckets_total` is not noise to tune out — it means a write path is moving
bytes without emitting a delta, or a statement is repointing `current_object_version` and editing
the outgoing version at the same time. **Alert on it.** Same for
`storage_rollup_negative_buckets`, which is only reachable if a decrement was recorded without its
increment.

**A recompute and a compaction of the SAME bucket must not overlap**, or the recompute's `SET` can
silently discard a delta the compactor has already consumed — permanently, and in a way that keeps
the drift metric non-zero forever, destroying the one signal that says the ledger is wrong. A
**per-bucket** advisory lock enforces it — `pg_advisory_xact_lock(rollup_key, bucket_key)` — where
the compactor uses `pg_try_` and skips just that bucket while the recompute waits. The recompute
itself drains the bucket's pending ledger rows and aggregates the truth in ONE statement, therefore
in ONE snapshot, so a write landing mid-recompute is counted exactly once.

⚠️ **The second lock argument used to be a hardcoded `0`, i.e. one key for the whole estate.** A
recompute of any single bucket therefore stalled compaction for every other bucket for its entire
timeout. On prod (2026-09-15) one 136M-object bucket — 80.7% of the `objects` table, so a 167 GB seq
scan that can never finish in a cycle — failed 168 times in 24h and aged the ledger's oldest row to
**412s** against a normal 2s. Keying the lock per bucket is what makes an un-aggregatable bucket a
local problem instead of an estate-wide one. Pinned by
`test_a_recompute_does_not_block_an_unrelated_buckets_compaction`.

**Why a separate worker rather than a second loop in the plans-cacher.** The plans-cacher's pool is
`DATABASE_READONLY_URL`, a read replica, because its work must not run on the primary. Compaction
WRITES. And the rollup's freshness wants seconds while the scrape wants minutes. Splitting them
also means the ledger keeps draining while api.hippius.com is down.

**This pod being down is not an outage, but it IS a silently frozen billing number.** Nothing on the
request path reads the rollup and the ledger is insert-only, so it accumulates losslessly. What
stops is the counter moving: plan accounts keep the usage figure from the last fold, so where
enforcement is on, a customer who deletes data stays refused. Alert on
`storage_rollup_ledger_lag_seconds`, not on pod restarts.

`replicas` must stay 1. Two would not corrupt anything — the claim is transactional — but they
would contend on the same batch for no throughput and reconcile the same buckets twice.

### Backfill

[../hippius_s3/scripts/backfill_bucket_storage_usage.py](../hippius_s3/scripts/backfill_bucket_storage_usage.py),
k8s Job at [../k8s/backfill-bucket-storage-usage-job.yaml](../k8s/backfill-bucket-storage-usage-job.yaml).
Defaults to a dry run. Safe to run concurrently with live traffic and safe to run twice: one bucket
per transaction, each recompute SETS rather than adds, and `backfilled_at` is only set after a
complete pass — so a run that dies part way through degrades to the pre-existing behaviour (the
plans-cacher keeps its previous roll) rather than to a wrong bill.

## Cachet health check

[cachet_health_check.py](cachet_health_check.py). Pushes service status to the public Cachet status page via `CACHET_API_KEY` and `CACHET_COMPONENT_ID`. Cron-scheduled.

## Worker-specific gotchas

- **Pool size**: uploader/unpinner use their own asyncpg pools inside the worker loop (min 2; per-worker max differs — uploader `HIPPIUS_UPLOADER_DB_POOL_MAX=12` ([config.py:163](../hippius_s3/config.py)), unpinner `HIPPIUS_UNPINNER_DB_POOL_MAX=16` ([config.py:188](../hippius_s3/config.py)) — mind the aggregate against Postgres `max_connections`). Do NOT share the API's pool.
- **Fatal reconnection**: if an inflight task raises a Redis or asyncpg connection error, the main loop flags the client for rebuild on the next iteration (see the uploader loop). This prevents continued failures against a dead connection.
- **Graceful shutdown**: on SIGTERM / KeyboardInterrupt, workers cancel inflight tasks and gather-with-exceptions before closing DB + Redis. See `run_worker()` in [run_arion_uploader_in_loop.py](run_arion_uploader_in_loop.py).
- **Retry mover runs on every pod**: `_retry_mover` ([run_arion_uploader_in_loop.py:133](run_arion_uploader_in_loop.py)) polls `{backend}_upload_retries` every 2s on each of the 10 uploader replicas. `move_due_upload_retries` claims due members with a server-side Lua `ZREM`-then-`LPUSH`, so exactly one pod re-enqueues each member; changing it back to a read-then-move re-introduces N-fold retry amplification. The unpin and download movers still have that race.
- **Uploader retry budget**: `HIPPIUS_UPLOADER_MAX_ATTEMPTS=7`, `HIPPIUS_UPLOADER_BACKOFF_BASE_MS=500`, `HIPPIUS_UPLOADER_BACKOFF_MAX_MS=60000` — shipped in both [.env.defaults](../.env.defaults) and [k8s/base/configmap-defaults.yaml](../k8s/base/configmap-defaults.yaml), matching the [config.py](../hippius_s3/config.py) defaults. That is ~63s of tolerance (0.5, 1, 2, 4, 8, 16, 32s) before the request goes to the upload DLQ, which is manual-recovery only. This queue is the **only** retry layer for transport errors — `retry_on_error` in [arion_service.py](../hippius_s3/services/arion_service.py) deliberately does not catch them, because retrying in both layers multiplies into ~24 requests at an already-failing backend.
