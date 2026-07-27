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

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 20, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #2804 | 12:43 AM | 🔵 | Uploader workers process backend-specific upload queues with retry management | ~525 |
| #2802 | " | 🔵 | Account cacher worker periodically synchronizes Substrate blockchain account data to Redis | ~533 |
| #2796 | 12:42 AM | 🔵 | Orphan checker worker periodically scans blockchain files and enqueues cleanup requests | ~559 |
| #2792 | " | 🔵 | IPFS downloader worker implements minimal backend-specific fetch wrapping shared downloader infrastructure | ~502 |
| #2791 | " | 🔵 | Unpinner workers handle backend cleanup via dedicated queue processing loops | ~427 |
| #2787 | " | 🔵 | Arion uploader worker implements retry logic with exponential backoff and DLQ for failed uploads | ~586 |
| #2786 | " | 🔵 | IPFS uploader worker implements retry logic and dead letter queue for failed uploads | ~538 |

### Apr 21, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6801 | 11:01 AM | 🟣 | Committed comprehensive PR #146 code review fixes addressing critical safety issues | ~1390 |
| #6779 | 10:48 AM | ✅ | Janitor documentation strengthened to emphasize absolute replication safety | ~726 |
| #6761 | 10:40 AM | 🔵 | Critical s3-backup PR #7 parity review reveals data loss risk | ~878 |

### May 12, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #7721 | 9:50 AM | 🔵 | Complete Sentry integration pattern documented for hippius-s3 | ~629 |
| #7720 | 9:49 AM | 🔵 | Sentry configuration pattern in hippius-s3 repository | ~506 |

### Jun 25, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #9377 | 7:07 PM | 🔵 | Application Pool Configuration Exceeds Database max_connections by 96 Connections | ~723 |
</claude-mem-context>
