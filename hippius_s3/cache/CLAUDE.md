# hippius_s3/cache/

Chunk cache. Backed by a shared filesystem volume; Redis is used only for pub/sub readiness notifications — **not** for chunk storage (that changed 2026-04-21 with the FS-cache migration).

## Files

| File | Purpose |
|---|---|
| [fs_store.py](fs_store.py) | `FileSystemPartsStore` — the actual on-disk cache. Atomic writes, meta-gated reads, hot-retention touch. |
| [object_parts.py](object_parts.py) | `RedisObjectPartsCache` — facade composing `FileSystemPartsStore` + `ChunkNotifier`. Name retained for compat; chunk I/O is FS-backed. |
| [notifier.py](notifier.py) | `ChunkNotifier` — Redis pub/sub wrapper for chunk-ready notifications. |
| [dual_fs_store.py](dual_fs_store.py) | `DualFileSystemPartsStore` — primary + fallback read-only store for migration. |
| [__init__.py](__init__.py) | `create_fs_store(config)` factory — picks `DualFileSystemPartsStore` if `HIPPIUS_OBJECT_CACHE_FALLBACK_DIR` is set. |

## On-disk layout

```
<HIPPIUS_OBJECT_CACHE_DIR>/            # default: /var/lib/hippius/object_cache
└── <object_id_uuid>/
    └── v<version>/
        └── part_<part_number>/
            ├── chunk_0.bin
            ├── chunk_1.bin
            ├── ...
            ├── meta.json              # Presence = "part is known" signal
            └── *.tmp.<uuid4>          # In-flight atomic write; janitor cleans if >1h old
```

- Every file is written atomically: unique tmp name → `os.replace` ([fs_store.py:92, 123-131](fs_store.py)). Two workers writing the same chunk path each use their own tmp — last rename wins, content is deterministic, no corruption.
- **`meta.json` is the readiness gate**: `get_chunk` returns None unless `meta.json` exists AND the specific chunk file exists ([fs_store.py:168-173](fs_store.py)). `chunks_exist_batch` same.
- Uploaders write meta **last** (after all chunks). Downloaders write meta **first** (eager from DB parts row) so partial-range fills become readable as chunks land.

## Atomicity on CephFS / NVMe

`os.replace` is atomic on both. No `flock` (unreliable on CephFS). Writes go through `fsync` on the file before rename and `fsync` on the parent directory after ([fs_store.py:335-346, _fsync_dir_async at 473](fs_store.py)) — guarantees durability of both the content and the rename even under a hard pod kill.

## UUID coercion

`_safe_object_id` ([fs_store.py:48-62](fs_store.py)) accepts:

- `UUID` instances (asyncpg returns these for UUID columns depending on codec config).
- Strings (whitespace-stripped, then validated via `UUID(...)` parse).

Anything else → `ValueError`. This is both a security guard (path traversal) and a correctness one — an earlier bug where asyncpg handed back a `UUID` instead of a string caused `AttributeError: 'UUID' object has no attribute 'strip'` at runtime. See commit `80e304a` (2026-04-21).

## Hot retention

Every successful read calls `os.utime` on the chunk file AND the meta file ([fs_store.py:183-186](fs_store.py)). Janitor uses mtime/atime to keep "recently read" parts regardless of age. `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` (default 10800 = 3h) defines the window.

`touch_part(...)` ([fs_store.py:276](fs_store.py)) bulk-touches every file in a part dir — used by the uploader after a successful backend upload to extend the part's "hotness".

## `RedisObjectPartsCache`

[object_parts.py:59](object_parts.py). Misnomer — the class name is legacy. Actual composition:

- `self._fs`: `FileSystemPartsStore` (created lazily from config if not injected). All chunk/meta I/O goes here.
- `self._notifier`: `ChunkNotifier` backed by `queues_client` (= `redis_queues_client`, port 6382).
- `self.redis`: still retained for a narrow purpose — the download-coalescing lock `download_in_progress:...` uses `SET NX EX` / `DELETE` on this client ([object_parts.py:78](object_parts.py) comment). Not used for data.

Key methods:

- `get_chunk` / `set_chunk` / `chunks_exist_batch` → delegate to `self._fs`.
- `set_chunks(..., start_index=N)` loops `set_chunk` ([object_parts.py:148-160](object_parts.py)). **This is the second FS write on upload** — see [todo.md](../../todo.md) P1.
- `get_meta` / `set_meta` → delegate to `self._fs`.
- `get(...)` / `set(...)` — whole-part legacy API, assembled from chunks ([object_parts.py:190-245](object_parts.py)).
- `expire(...)` → `fs.touch_part` (was Redis TTL extension before the migration).
- `notify_chunk(oid, v, pn, ci)` → `self._notifier.notify(...)` publishes to `notify:{chunk_key}`.
- `wait_for_chunk(oid, v, pn, ci)` → `self._notifier.wait_for_chunk(..., fetch_fn=self.fs.get_chunk, timeout=cache_ttl_seconds)`.

## `ChunkNotifier`

[notifier.py:35](notifier.py). Pub/sub pattern:

- **Key format**: `f"obj:{object_id}:v:{version}:part:{part_number}:chunk:{chunk_index}"` ([notifier.py:26-32](notifier.py)).
- **Channel**: `f"notify:{chunk_key}"`.
- `notify(...)` publishes `"1"` on the channel.
- `wait_for_chunk(...)` flow ([notifier.py:61](notifier.py)):
  1. Fast-path call `fetch_fn` (typically `fs_store.get_chunk`); return if present.
  2. Subscribe to the channel.
  3. Re-check `fetch_fn` once (handles the race where the worker notified between step 1 and step 2).
  4. Block on `pubsub.listen()` until a message arrives or `timeout` expires.
  5. Fetch again. On transient miss (janitor delete or CephFS replication lag), sleep 100ms and retry once ([notifier.py:117-124](notifier.py)).
  6. If still missing, raise `RuntimeError`.

## Dead / removed

- **`RedisDownloadChunksCache`** — the separate 32GB Redis download cache. Removed 2026-04-21 along with the `redis-download-cache` StatefulSet, `REDIS_DOWNLOAD_CACHE_URL`, and the `DOWNLOAD_CACHE_TTL` env var. If you see any reference to these, it's stale.
- **`set_download_chunk`** shim — removed.
- **Manifest-CID machinery** (`manifest_service`) — replaced by `chunk_backend` long ago.

## Disk pressure

Writes to a full disk raise `OSError(ENOSPC)`. The API has [fs_cache_pressure_middleware](../api/middlewares/fs_cache_pressure.py) that returns 503 + Retry-After on PUT when disk usage exceeds the threshold, BEFORE reading the body. Janitor also has three pressure modes (normal / elevated / critical) — see [workers/CLAUDE.md](../../workers/CLAUDE.md).

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 14, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1900 | 9:22 PM | ⚖️ | System Already Supports Variable Chunk Sizes with Full Backward Compatibility | ~614 |
| #1899 | 9:21 PM | 🔵 | Redis Cache Stores chunk_size in Metadata Payload | ~415 |

### Apr 21, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6828 | 1:36 PM | 🔴 | Fixed FileSystemPartsStore to handle UUID objects from asyncpg | ~531 |
| #6801 | 1:01 PM | 🟣 | Committed comprehensive PR #146 code review fixes addressing critical safety issues | ~1390 |
| #6786 | 12:49 PM | 🔄 | Removed Redis download cache classes from cache module public API | ~477 |
</claude-mem-context>
