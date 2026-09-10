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
GET /api/s3/plans/accounts/?page=1&page_size=500     every HIPPIUS_PLANS_LOOP_SLEEP (600s)
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
- **`used_bytes`** is what the account actually stores, **counted by this worker** — upstream
  reports no usage figure. Counted per bucket, in keyset pages, over
  `HIPPIUS_PLANS_USAGE_CONCURRENCY` (4) connections.

### Why the count is chunked

The obvious form is one aggregate per account, and that is what this used to do. It cannot finish.

Cost is driven by OBJECTS PER BUCKET, not by bucket fan-out and not by account count. One plan
account's objects are concentrated in a single bucket holding a filesystem-style workload of
**millions of small objects**; the single-statement aggregate for it takes over a minute. Both ceilings that apply are 30s — `HIPPIUS_PLANS_USAGE_TIMEOUT_SECONDS` and the
replica's `max_standby_streaming_delay` — so it was cancelled every cycle, and because one account's
failure fails the whole cycle, the roll was never published at all.

`usage_service.py` therefore lists the account's live buckets, then walks each bucket in keyset
pages of `HIPPIUS_PLANS_USAGE_PAGE_SIZE` objects (`get_bucket_storage_bytes_page.sql`). Same
definition, same bytes, spread over N statements — pinned against the canonical query in
`tests/integration/test_usage_service_chunked.py`. It does not make the total work smaller; it makes
no single STATEMENT long enough to be cancelled, and stops the count pinning the xmin horizon for a
minute at a time.

Measured cold on the prod replica, each page read from an un-warmed region of that bucket's key
space:

| page size | offset | cold | per row |
|---|---|---|---|
| 100k | 5.0M | 1.88s | 18.8 µs |
| **200k** | 6.5M | **2.91s** | **14.6 µs** ← default |
| **200k** | 1.2M | **5.31s** | **26.6 µs** ← worst observed, **5.7x margin** |
| 500k | 0 | 20.2s | 40.0 µs — 1.5x margin, do not |

Only 200k has two samples. Read the 100k and 500k rows as single points from one region each — by
the second property below, either could be ~2x off in another part of the bucket, so 100k is not
established as cheaper per row than 200k.

Two properties, each of which cost a wrong default once:

**Per-row cost is not linear in page size.** Past a few hundred thousand rows the random heap
fetches stop fitting cache and the page falls off a cliff — 500k is 2.7x the per-row cost of 200k.
Maximising this to save round trips is how you get it cancelled.

**Per-row cost also varies ~2x by REGION at a fixed page size**, from heap locality. A single sample
is not a margin: the first 200k measurement said 2.91s and a second at a different depth said 5.31s.
Judge a page size by the WORST observed page, not the mean.

Whole-account projection for that account: ~40 pages at ~4.1s average ≈ **165s**, none of them near
the ceiling. That is slower in total than the ~64s single statement would have been — but that
statement never completed. Bounded-and-finishing beats fast-and-cancelled.

**The counts run against a REPLICA** (`DATABASE_READONLY_URL`, falling back to `DATABASE_URL` when
unset), with `jit=off` on the pool — JIT is pure overhead for an index-probe-bound query and cost
107ms of a 326ms count for a 1,300-object account. This cluster's primary has been stalled by a
read-storm before.

A maintained counter — a delta ledger folded into a per-bucket rollup — is the real long-term answer
and is written up in todo.md. Chunking is what makes the current design work until then; recounting
is O(objects) forever, and that bucket only grows.

⚠️ **The refresh interval IS the enforcement lag, in both directions.** An account can overshoot its
quota by one cycle's worth of uploads, and a customer who deletes data to get back under stays
refused until the next cycle sees it. Nothing on the request path recomputes.

A count failing for ANY account fails the whole cycle and keeps the previous roll: publishing a
partial answer would write `used_bytes=0` for the accounts we could not count, silently handing them
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

