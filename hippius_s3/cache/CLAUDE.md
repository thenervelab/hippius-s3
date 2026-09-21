# hippius_s3/cache/

Chunk cache. Backed by filesystem volumes (node-local NVMe, peers, the pool) with the backend itself as the lowest read tier; Redis holds no chunk data and, since the direct-to-Arion read path, no chunk-ready pub/sub either.

## Files

| File | Purpose |
|---|---|
| [fs_store.py](fs_store.py) | `FileSystemPartsStore` — the actual on-disk cache. Atomic writes, meta-gated reads, read-recency tracking (`note_read` → `fs_cache_inventory.last_access_at`). |
| [object_parts.py](object_parts.py) | `RedisObjectPartsCache` — facade over `FileSystemPartsStore`. Name retained for compat; chunk I/O is FS-backed. |
| [dual_fs_store.py](dual_fs_store.py) | `DualFileSystemPartsStore` — the tiered read path: node-local NVMe → peer node → CephFS pool. Optionally promotes a pool/peer-served chunk onto local flash. |
| [peers.py](peers.py) | `PeerRegistry` (self-registration of pod IPs in Redis, TTL'd) + `PeerChunkFetcher` (resolve which node holds a part, fetch one chunk from it). |
| [read_recency.py](read_recency.py) | `ReadRecencyRecorder` — stamps `cephor_ssd_residency.last_read_at` on a local hit (sampled), so the drain evictor orders on USE rather than arrival. |
| [residency.py](residency.py) | `ResidencyRecorder` — claims a promoted part for this node in `cephor_ssd_residency` so this node's evictor can reclaim it. |
| [part_memo.py](part_memo.py) | `PartMemo` — bounded, TTL'd per-part memo, so per-part facts are not recomputed per chunk. |
| [__init__.py](__init__.py) | `create_fs_store(config, on_promote=..., peer_fetch=...)` factory — picks `DualFileSystemPartsStore` if `HIPPIUS_OBJECT_CACHE_FALLBACK_DIR` is set. |

## Read tiers (`DualFileSystemPartsStore`)

`get_chunk` tries three tiers in order, recording which one served the read into
`chunk_reads_by_tier_total{tier=local|peer|pool}`:

1. **local** — this node's NVMe (`HIPPIUS_OBJECT_CACHE_DIR` on an ingest node is node-local).
2. **peer** — the node that holds the part on flash, via `GET /internal/parts/...` on its
   `api-local` pod. Resolved per PART (not per request) from `cephor_ssd_residency`, memoised
   in a `PartMemo`. Off unless `HIPPIUS_PEER_FETCH_ENABLED`.
3. **pool** — the shared CephFS volume. Authoritative and always present for a replicated part,
   so every tier above it is an optimisation and must never be able to fail a read.

With `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ`, a peer- or pool-served chunk is claimed in
`cephor_ssd_residency` — so the drain-agent's evictor owns it — and only then copied onto local
flash. **Claim first, and a failed claim cancels the copy.** An unclaimed copy has no owner in
either process (the evictor is scoped to that table, and `ssd_reclaim` skips replicated parts as
the read tier), so writing first would leak one unreclaimable copy per promoted chunk for the
whole of a residency-DB outage — on the disk whose filling makes the api 503 every PUT. Failing
closed on an optimisation costs a cache warm, counted as
`promotion_skipped_total{reason=residency_failed}`. Two caps bound
peer fanout — `HIPPIUS_PEER_FETCH_MAX_INFLIGHT` per (pod, peer) on the client, and
`HIPPIUS_PEER_SERVE_MAX_INFLIGHT` on the serving pod, which sheds with 503. Both shed to the
pool rather than queueing. The client is a replaceable `httpx.AsyncClient` (`ReplaceablePeerClient`):
a streak of `PoolTimeout`s rebuilds it, the same recovery as the Arion fetch client. Without that,
cancelled `wait_for`s leak CLOSE_WAIT and the pod stops asking peers (api-local-xrbng, 2026-09-21).

**The client cap must be ≥ `HTTP_STREAM_PREFETCH_CHUNKS`.** Every chunk of one PART resolves to
the same peer, so a lower cap makes a single reader shed its own prefetch window to the pool and
book it as `client_cap` — contention that does not exist. `effective_max_inflight` floors it at
the prefetch depth at wiring time, so a stale config degrades to a startup warning rather than a
silently halved peer tier. It does **not** add peer capacity: the semaphore is shared across
readers on the pod, so under concurrency shedding just moves to the peer's `server_busy`.

Promotion is gated on free space (`HIPPIUS_PROMOTE_MIN_FREE_RATIO`, default 0.175) because it
shares the ingest mount with PUTs — `HIPPIUS_OBJECT_CACHE_DIR` is the drain agent's
`CEPHOR_SSD_ROOT` and the mount `fs_cache_pressure` measures. The floor must sit strictly inside
the evictor's band, above its reserve (0.150) and below its target (0.200), or promotion either
chatters or deadlocks permanently; `validate_promotion_band` enforces that at startup.

## Invalidating a chunk that fails AEAD

Promotion copies peer/pool bytes onto local flash, and the local tier is read FIRST — so a bad
copy (a version-skewed peer, a torn write, bit rot) would be a permanent, retry-immune read
failure for that object on that node until the evictor happened to reclaim the part.
`invalidate_local_chunk` is what makes it transient: `stream_plan` calls it when a chunk's
plaintext fails to authenticate, then re-fetches and decrypts **exactly once**, counted as
`chunk_aead_failures_total{tier,outcome}`.

Three things about it are load-bearing. It removes one chunk file, never the part or `meta.json` —
a part with meta and a hole is a normal partial-promotion state, so the hole falls
through a tier while its siblings still serve. It is gated on a durable copy existing elsewhere —
a live `chunk_backend` row (`durable_elsewhere`, passed by the streamer from the resolved
locations) or, for pool-era parts, a pool copy — because a freshly ingested part is on SSD alone
until the backend acks it and a DEK fault fails those chunks too; without the gate a key error
would become data loss. And it lives on
`DualFileSystemPartsStore` alone — with no fallback dir the single store's root IS the pool, so
the same call there would delete the authoritative copy.

The retry being straight-line rather than a loop is the other half. A DEK fault fails every chunk
of every object, and with promotion on a looping retry re-warms the copy it just dropped, so it
would never run out of things to invalidate.

**The evictor runs in a different process** (`drain-agent`) and deletes both the part directory
and its residency row. Nothing in this package may cache "I already recorded/wrote this" across
that boundary — it cannot be invalidated when the row disappears. Check on-disk state instead;
see the `_promote_chunk` in-flight guard and the note at the top of `residency.py`.

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
            ├── *.tmp.<uuid4>          # In-flight atomic write; janitor cleans if >1h old
            └── chunk_<i>.bin.staged.<attempt>   # UploadPart attempt's private copy, pre-publish
```

A staged chunk lives for the WHOLE UploadPart (a multi-GB part on a slow link can hold one for
hours) and is renamed onto `chunk_<i>.bin` only when `publish_part` promotes the attempt's full
set. Deliberately NOT `.tmp.`-shaped: the write-temp sweepers assume temps live for milliseconds
and would delete it mid-upload. A SIGKILL'd attempt's leftovers are swept by the drain agent's
orphan sweep at its 24h grace; readers and the drain's completeness gate never count staged names.

- Every file is written atomically: unique tmp name → `os.replace` ([fs_store.py:92, 123-131](fs_store.py)). Two workers writing the same chunk path each use their own tmp — last rename wins, content is deterministic, no corruption.
- **`meta.json` is the readiness gate**: `get_chunk` returns None unless `meta.json` exists AND the specific chunk file exists ([fs_store.py:168-173](fs_store.py)). `chunks_exist_batch` same — and it must stay a two-part test: a promoted part is filled chunk by chunk, so meta alone would report in-flight chunks as present.
- Ingest writes meta **last** (after all chunks). Promotion writes meta eagerly so partially-promoted parts are readable chunk by chunk.
- **The read path parses the part directory listing.** `chunks_exist_batch` answers per part with one `os.scandir` rather than a stat per chunk: on the pool tier every stat is an MDS round trip (~6ms), so the per-chunk form made read TTFB O(total_chunks) and cost 10-20s before the first byte of a 5 GB object. Two consequences before you change anything on disk: the staged/tmp naming above is now a **read-path correctness dependency** (any name parsing as `chunk_<i>.bin` counts as published, which is why the parser round-trips the index back through the filename), and scans run on a dedicated `fs-scan` pool sized by `HIPPIUS_FS_STORE_SCAN_CONCURRENCY` — off asyncio's default executor so a fan-out cannot starve the `get_chunk`/`set_chunk` IO living there, with a per-request cap so one large object cannot queue ahead of everyone else.

## Atomicity on CephFS / NVMe

`os.replace` is atomic on both. No `flock` (unreliable on CephFS). Writes go through `fsync` on the file before rename and `fsync` on the parent directory after ([fs_store.py:335-346, _fsync_dir_async at 473](fs_store.py)) — guarantees durability of both the content and the rename even under a hard pod kill.

## UUID coercion

`_safe_object_id` ([fs_store.py:48-62](fs_store.py)) accepts:

- `UUID` instances (asyncpg returns these for UUID columns depending on codec config).
- Strings (whitespace-stripped, then validated via `UUID(...)` parse).

Anything else → `ValueError`. This is both a security guard (path traversal) and a correctness one — an earlier bug where asyncpg handed back a `UUID` instead of a string caused `AttributeError: 'UUID' object has no attribute 'strip'` at runtime. See commit `80e304a` (2026-04-21).

## Hot retention

Reads no longer `os.utime` the chunk/meta files — the per-read atime touch was removed (it was silently dead on read-only mounts and an MDS metadata write everywhere else). Every successful read instead records recency via `tracker.note_read(...)` into `fs_cache_inventory.last_access_at` ([fs_store.py:180-195](fs_store.py)); the tracker is a sampled no-op in processes that never initialize it (workers/janitor). Janitor uses `last_access_at` to keep "recently read" parts regardless of age. `HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS` (default 14400 = 4h) defines the window.

`touch_part(...)` ([fs_store.py:276](fs_store.py)) bulk-touches every file in a part dir — used by the uploader after a successful backend upload to extend the part's "hotness".

## `RedisObjectPartsCache`

[object_parts.py](object_parts.py). Misnomer — the class name is legacy. It is a thin facade over
`FileSystemPartsStore` (`self.fs`, created lazily from config if not injected); all chunk/meta I/O
goes there. `get_chunk` / `set_chunk` / `chunks_exist_batch` / `get_meta` / `set_meta` delegate;
`expire(...)` → `fs.touch_part`. The chunk-ready pub/sub (`ChunkNotifier`, `wait_for_chunk`,
`notify_chunk`) and the download-coalescing lock went with the downloader: a chunk on no local tier
is fetched from the backend into memory by the read path itself
([../reader/backend_fetch.py](../reader/backend_fetch.py)), so nothing waits on a notification.

## Dead / removed

- **`RedisDownloadChunksCache`** — the separate 32GB Redis download cache. Removed 2026-04-21 along with the `redis-download-cache` StatefulSet, `REDIS_DOWNLOAD_CACHE_URL`, and the `DOWNLOAD_CACHE_TTL` env var. If you see any reference to these, it's stale.
- **`set_download_chunk`** shim — removed.
- **`ChunkNotifier`** (`notifier.py`), **`ReplicationSuspectProbe`** (`replication_probe.py`) and the downloader worker — removed with the direct-to-Arion read path; reads pull a missing chunk from the backend in-process.
- **Manifest-CID machinery** (`manifest_service`) — replaced by `chunk_backend` long ago.

## Disk pressure

Writes to a full disk raise `OSError(ENOSPC)`. The API has [fs_cache_pressure_middleware](../api/middlewares/fs_cache_pressure.py) that returns 503 + Retry-After on PUT when disk usage exceeds the threshold, BEFORE reading the body. Janitor also has three pressure modes (normal / elevated / critical) — see [workers/CLAUDE.md](../../workers/CLAUDE.md).

