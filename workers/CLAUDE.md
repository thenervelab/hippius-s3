# workers/

Worker entry points — the `run_*.py` scripts that actually run as pod processes in Kubernetes. Each file wraps shared logic from [../hippius_s3/workers/](../hippius_s3/workers/).

## Worker inventory

| Entry point | Purpose | Scaling |
|---|---|---|
| [run_arion_uploader_in_loop.py](run_arion_uploader_in_loop.py) | Drains `arion_upload_requests`, uploads chunks to Arion, publishes to chain. | Single instance (rate-limited; scale via parallelism config) |
| [run_arion_downloader_in_loop.py](run_arion_downloader_in_loop.py) | Drains `arion_download_requests`, fetches chunks from Arion, fills FS cache, notifies streamers. | Horizontally scalable |
| [run_arion_unpinner_in_loop.py](run_arion_unpinner_in_loop.py) | Drains `unpin_requests`, soft-deletes `chunk_backend` rows, calls Arion delete. | Single instance |
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

- **Normal** (<85% disk): honor `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` (default 3h). Evict replicated + aged + cold.
- **Elevated** (85-95%): halve the hot-retention window. Evict replicated + cold regardless of age.
- **Critical** (≥95%): hot retention disabled. Evict replicated + cold aggressively. If nothing replicated → log ERROR, do nothing.

### Cleanup passes

- `cleanup_stale_parts` ([run_janitor_in_loop.py:253](run_janitor_in_loop.py)) — delete parts whose mtime > `MPU_STALE_SECONDS`. DLQ protection via `get_all_dlq_object_ids` ([line 220](run_janitor_in_loop.py)) — scans every upload DLQ + unpin DLQ dynamically per `config.upload_backends` so new backends automatically get protected.
- Age-based GC — classify by age bucket (0-1h / 1-6h / 6-24h / 1-3d / 3-7d / 7d+), gate on replication, honor hot retention.
- Orphan `.tmp.*` cleanup — delete if older than `TMP_FILE_MAX_AGE_SECONDS=3600` (1h). Catches crashed atomic writes.
- Hard-delete for soft-deleted objects whose unpins have been confirmed on every backend.

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

- **Pool size**: uploader/downloader/unpinner use their own asyncpg pools (min 2, max 20) inside the worker loop. Do NOT share the API's pool.
- **Fatal reconnection**: if an inflight task raises a Redis or asyncpg connection error, the main loop flags the client for rebuild on the next iteration ([downloader.py:423-435](../hippius_s3/workers/downloader.py)). This prevents continued failures against a dead connection.
- **Graceful shutdown**: on SIGTERM / KeyboardInterrupt, workers cancel inflight tasks and gather-with-exceptions before closing DB + Redis. See [downloader.py:496-508](../hippius_s3/workers/downloader.py).

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 20, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #2804 | 2:43 AM | 🔵 | Uploader workers process backend-specific upload queues with retry management | ~525 |
| #2802 | " | 🔵 | Account cacher worker periodically synchronizes Substrate blockchain account data to Redis | ~533 |
| #2796 | 2:42 AM | 🔵 | Orphan checker worker periodically scans blockchain files and enqueues cleanup requests | ~559 |
| #2792 | " | 🔵 | IPFS downloader worker implements minimal backend-specific fetch wrapping shared downloader infrastructure | ~502 |
| #2791 | " | 🔵 | Unpinner workers handle backend cleanup via dedicated queue processing loops | ~427 |
| #2787 | " | 🔵 | Arion uploader worker implements retry logic with exponential backoff and DLQ for failed uploads | ~586 |
| #2786 | " | 🔵 | IPFS uploader worker implements retry logic and dead letter queue for failed uploads | ~538 |

### Apr 21, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6801 | 1:01 PM | 🟣 | Committed comprehensive PR #146 code review fixes addressing critical safety issues | ~1390 |
| #6779 | 12:48 PM | ✅ | Janitor documentation strengthened to emphasize absolute replication safety | ~726 |
</claude-mem-context>
