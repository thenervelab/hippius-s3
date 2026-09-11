# workers/

Worker entry points — the `run_*.py` scripts that actually run as pod processes in Kubernetes. Each file wraps shared logic from [../hippius_s3/workers/](../hippius_s3/workers/).

## Worker inventory

| Entry point | Purpose | Scaling |
|---|---|---|
| [run_arion_uploader_in_loop.py](run_arion_uploader_in_loop.py) | Drains `arion_upload_requests`, uploads chunks to Arion, publishes to chain. | Horizontally scalable (`replicas: 10` in production) |
| [run_arion_downloader_in_loop.py](run_arion_downloader_in_loop.py) | Drains `arion_download_requests`, fetches chunks from Arion, fills FS cache, notifies streamers. | Horizontally scalable |
| [run_arion_unpinner_in_loop.py](run_arion_unpinner_in_loop.py) | Drains `unpin_requests`, soft-deletes `chunk_backend` rows, calls Arion delete. | Horizontally scalable; per-pod request concurrency (`HIPPIUS_UNPINNER_MAX_INFLIGHT`) + shared Arion-DELETE semaphore (`HIPPIUS_UNPINNER_PARALLELISM`) |
| [run_janitor_in_loop.py](run_janitor_in_loop.py) | FS cache GC with replication gate, hot retention, and pressure modes. | Single instance |
| [run_orphan_checker_in_loop.py](run_orphan_checker_in_loop.py) | Periodically scans the Hippius chain for orphaned files and enqueues cleanup. | Single instance |
| [run_account_cacher_in_loop.py](run_account_cacher_in_loop.py) | Warms account credit cache from Substrate. | Single instance |
| [run_plans_cacher_in_loop.py](run_plans_cacher_in_loop.py) | Scrapes the S3 billing-plan catalog + account→plan map from api.hippius.com into `redis-accounts`. | Single instance (**must stay `replicas: 1`**) |
| [run_usage_rollup_in_loop.py](run_usage_rollup_in_loop.py) | Folds the storage delta ledger into `bucket_storage_usage`; reconciles it and exports drift. | Single instance (**must stay `replicas: 1`**) |
| [run_migrator_once.py](run_migrator_once.py) | One-shot data migration (e.g., v4→v5). Invoked as a K8s Job. | Job |
| [cachet_health_check.py](cachet_health_check.py) | Pushes status to the external Cachet status page. | CronJob |

Each `run_*_in_loop.py` is a thin wrapper that imports the shared logic and provides backend-specific parameters (`backend_name`, `queue_name`, `fetch_fn`, etc.). See [../hippius_s3/workers/CLAUDE.md](../hippius_s3/workers/CLAUDE.md) for the core loop internals.

## Janitor (FS cache GC)

[run_janitor_in_loop.py](run_janitor_in_loop.py). Read the top-of-file docstring ([lines 1-22](run_janitor_in_loop.py)) — it spells out the invariants.

### Core invariant

**Replication is an absolute gate.** A chunk that has NOT been replicated to every required backend (`HIPPIUS_UPLOAD_BACKENDS` ∪ `HIPPIUS_BACKUP_BACKENDS`) is **never** deleted — under any conditions, including a full disk. The critical-pressure path still honors this: if nothing is replicated and disk is at 95%+, the janitor logs ERROR and deletes nothing. Operator paging, not data loss.

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

## Orphan checker

[run_orphan_checker_in_loop.py](run_orphan_checker_in_loop.py). Scans Substrate for files that exist on-chain but have no corresponding entry in our DB — these are orphans from past incidents or test accounts. Enqueues unpin.

Config:
- `ORPHAN_CHECKER_LOOP_SLEEP=7200` (2h) — how often to run.
- `ORPHAN_CHECKER_BATCH_SIZE=500` — files per API call.
- `HIPPIUS_ORPHAN_WORKER_ACCOUNT_WHITELIST` — optional whitelist; if set, only those accounts are scanned. Safety valve for staging.

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

**Only accounts billed as a plan are written.** A row needs `billing == "plan"` and a plan name.
Everything else is simply absent from the hash, which is exactly what the request path already reads
as pay-as-you-go.

⚠️ **`active` is NOT consulted, and that is a deliberate concession to the real payload.** The
original filter also required `active` true, on the reading that a lapsed subscription keeps
`billing: "plan"` and its old plan name and is distinguished only by that flag. The live data
contradicted it: upstream returns `active: false` on **every** row it serves — 3069 at the last
check, zero exceptions across the two days it has been up (2026-09-08 to -09) — including the one
genuine subscriber, which carries a real `subscription_id` and a `next_charge` a month in the
future. A cancelled subscription does not have a future charge date, so the field is not carrying
that meaning; on present evidence it is simply unpopulated.

Requiring it admitted nobody, which is the worse failure: the gate could never engage, so the
feature was unobservable even in shadow mode and enforcement would have been a permanent silent
no-op.

**Measured blast radius: exactly ONE row in 3069 carries `billing == "plan"`** (checked twice, a day
apart). Re-measure before assuming otherwise — every risk below scales with that number, and so does
the "few tens of accounts" cost model this worker's design rests on.

The risks now accepted, **in both directions** — admission is not purely generous:

1. A cancelled subscriber keeps their allowance until the check is restored.
2. Admission also **imposes a cap** and removes the pay-as-you-go path. An admitted account that is
   over its plan size but holds substrate credits used to upload fine via `can_upload`; with
   enforcement on it is refused 402 until it deletes data *and* a cacher cycle re-counts.
3. An admitted account whose quota is unknown — plan absent from the catalog, or a null/0/negative
   `storage_bytes` — resolves to `catalog_miss`, which allows the write **and** skips `has_credits`
   and `can_upload`. That is unmetered storage, not merely an unenforced quota.

None of the three is reachable while `HIPPIUS_ENABLE_BILLING_PLANS` is off, which is how prod ships.
Staging has it ON, so staging is where 2 and 3 would first appear.

**When upstream confirms what `active` means, restore the check** — or switch to `next_charge` in
the future, which is the field that actually tracked reality here. Pinned by
`tests/unit/test_plans_cacher_worker.py::test_the_active_flag_is_not_consulted`, which is the test
to invert.

🚨 **A rollback needs `DEL hippius_s3_plan_accounts` on redis-accounts first.** Restoring the check
admits ~nobody, so the new roll is empty over a live hash, the shrink guard refuses it, `run_cycle`
swallows the raise — and the OLD wide roll keeps serving with `used_bytes` frozen at the moment of
the revert. The deploy looks clean and nothing changes. Pinned by
`test_restoring_the_active_check_is_wedged_by_the_shrink_guard`.

**How you would know it is time:** every cycle logs `upstream_active=N` counted over every row in
the payload. Nothing else reads the field, so that line is the only signal that upstream has started
writing it. Alert on it going non-zero.

```
{namespace="hippius-s3-prod",app="plans-cacher"} |= "Published plan roll" != "upstream_active=0"
```

**Caching is unconditional.** This worker does not read `HIPPIUS_ENABLE_BILLING_PLANS` and is not
deployed with it, so the maps stay warm and observably correct long before enforcement is switched
on — flipping the flag on the api is then a config change, not a cold-cache event.

**This pod being down is not an outage.** Neither hash has a TTL and `redis-accounts` is
`noeviction` + AOF, so the last known good roll keeps serving through an api.hippius.com outage and
across a Redis restart. Alert on `plans_cache_age_seconds`, not on pod restarts.

Three invariants, all in [hippius_s3/services/plans_cache.py](../hippius_s3/services/plans_cache.py),
each of which exists to stop the same failure — silently demoting plan customers to pay-as-you-go
and 402ing them on their next upload:

1. **Publication is a whole-hash build-then-`RENAME`.** `refresh_plan_roll_once` fetches EVERY page
   before publishing; a failure on page 7 of 20 leaves the live hash untouched.
2. **An empty or heavily-shrunk roll is refused** (`MAX_ACCOUNT_MAP_SHRINK_RATIO`, 50%). One bad
   upstream deploy returning a truncated-but-valid list must not wipe the fleet's plans.
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
| **Compact** | `HIPPIUS_USAGE_ROLLUP_LOOP_SLEEP` (5s) | Claims `HIPPIUS_USAGE_ROLLUP_BATCH_SIZE` (5000) ledger rows with `DELETE ... RETURNING` and adds them to the counter. |
| **Reconcile** | `HIPPIUS_USAGE_RECONCILE_INTERVAL_SECONDS` (300s) | Fully recomputes `HIPPIUS_USAGE_RECONCILE_BUCKETS_PER_CYCLE` (200) live buckets, oldest-recomputed first, and exports the correction as **drift**. ~19h for a full sweep, which is the only bound on how long a bucket can carry a wrong number — see the rate justification in [config.py](../hippius_s3/config.py). |

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

**A recompute and a compaction must not overlap**, or the recompute's `SET` can silently discard a
delta the compactor has already consumed — permanently, and in a way that keeps the drift metric
non-zero forever, destroying the one signal that says the ledger is wrong. A global advisory lock
enforces it: the compactor uses `pg_try_advisory_xact_lock` and skips the cycle, the recompute waits.
The recompute itself drains the bucket's pending ledger rows and aggregates the truth in ONE
statement, therefore in ONE snapshot, so a write landing mid-recompute is counted exactly once.

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

## Migrator

[run_migrator_once.py](run_migrator_once.py). Subprocess wrapper around [../hippius_s3/scripts/migrate_objects.py](../hippius_s3/scripts/migrate_objects.py). Runs as a K8s Job; exits on completion.

## Cachet health check

[cachet_health_check.py](cachet_health_check.py). Pushes service status to the public Cachet status page via `CACHET_API_KEY` and `CACHET_COMPONENT_ID`. Cron-scheduled.

## Worker-specific gotchas

- **Pool size**: uploader/downloader/unpinner use their own asyncpg pools inside the worker loop (min 2; per-worker max differs — downloader `HIPPIUS_DOWNLOADER_DB_POOL_MAX=20` ([config.py:293](../hippius_s3/config.py)), uploader `HIPPIUS_UPLOADER_DB_POOL_MAX=12` ([config.py:163](../hippius_s3/config.py)), unpinner `HIPPIUS_UNPINNER_DB_POOL_MAX=16` ([config.py:188](../hippius_s3/config.py)) — mind the aggregate against Postgres `max_connections`). Do NOT share the API's pool.
- **Fatal reconnection**: if an inflight task raises a Redis or asyncpg connection error, the main loop flags the client for rebuild on the next iteration ([downloader.py:423-435](../hippius_s3/workers/downloader.py)). This prevents continued failures against a dead connection.
- **Graceful shutdown**: on SIGTERM / KeyboardInterrupt, workers cancel inflight tasks and gather-with-exceptions before closing DB + Redis. See [downloader.py:496-508](../hippius_s3/workers/downloader.py).
- **Retry mover runs on every pod**: `_retry_mover` ([run_arion_uploader_in_loop.py:133](run_arion_uploader_in_loop.py)) polls `{backend}_upload_retries` every 2s on each of the 10 uploader replicas. `move_due_upload_retries` claims due members with a server-side Lua `ZREM`-then-`LPUSH`, so exactly one pod re-enqueues each member; changing it back to a read-then-move re-introduces N-fold retry amplification. The unpin and download movers still have that race.
- **Uploader retry budget**: `HIPPIUS_UPLOADER_MAX_ATTEMPTS=7`, `HIPPIUS_UPLOADER_BACKOFF_BASE_MS=500`, `HIPPIUS_UPLOADER_BACKOFF_MAX_MS=60000` — shipped in both [.env.defaults](../.env.defaults) and [k8s/base/configmap-defaults.yaml](../k8s/base/configmap-defaults.yaml), matching the [config.py](../hippius_s3/config.py) defaults. That is ~63s of tolerance (0.5, 1, 2, 4, 8, 16, 32s) before the request goes to the upload DLQ, which is manual-recovery only. This queue is the **only** retry layer for transport errors — `retry_on_error` in [arion_service.py](../hippius_s3/services/arion_service.py) deliberately does not catch them, because retrying in both layers multiplies into ~24 requests at an already-failing backend.
