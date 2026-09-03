# hippius_s3/cache/

Chunk cache. Backed by a shared filesystem volume; Redis is used only for pub/sub readiness notifications — **not** for chunk storage (that changed 2026-04-21 with the FS-cache migration).

## Files

| File | Purpose |
|---|---|
| [fs_store.py](fs_store.py) | `FileSystemPartsStore` — the actual on-disk cache. Atomic writes, meta-gated reads, read-recency tracking (`note_read` → `fs_cache_inventory.last_access_at`). |
| [object_parts.py](object_parts.py) | `RedisObjectPartsCache` — facade composing `FileSystemPartsStore` + `ChunkNotifier`. Name retained for compat; chunk I/O is FS-backed. |
| [notifier.py](notifier.py) | `ChunkNotifier` — Redis pub/sub wrapper for chunk-ready notifications. |
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
pool rather than queueing.

**The client cap must be ≥ `HTTP_STREAM_PREFETCH_CHUNKS`.** Every chunk of one PART resolves to
the same peer, so a lower cap makes a single reader shed its own prefetch window to the pool and
book it as `client_cap` — contention that does not exist. `effective_max_inflight` floors it at
the prefetch depth at wiring time, so a stale config degrades to a startup warning rather than a
silently halved peer tier. It does **not** add peer capacity: the semaphore is shared across
readers on the pod, so under concurrency shedding just moves to the peer's `server_busy`.

Four things on the peer tier exist because of the locality rollout ([docs/locality-routing.md](../../docs/locality-routing.md)),
which sends every request for a key to one node and so turns "many readers of one fresh
object" from a rare case into the normal one. **Singleflight**: concurrent readers of one
`(object, version, part, chunk)` on a pod join a single leader fetch instead of each taking a
peer slot; a failure is not cached, so followers get `None` and fall through. **Unreplicated
wait**: a part the drain has not replicated yet has no pool copy, so shedding it "to the pool"
used to mean a 25 s first-chunk 503 or a 300 s mid-stream wait. For those parts the fetcher waits
for a slot and retries `server_busy` with backoff inside
`HIPPIUS_PEER_FETCH_UNREPLICATED_WAIT_SECONDS` (0 restores the shed); replicated parts still shed
immediately. **Promote-notify**: `_promote_chunk` publishes `notify:` after `set_chunk`, so a
reader blocked in `wait_for_chunk` wakes when a neighbour lands the chunk rather than at the
timeout. **Serve-path recency**: the internal parts endpoint reads through `read_local_chunk`,
which stamps `_on_local_read` like a client-facing hit — without it the owner's copy of a part
that is only ever peer-served looks cold to the evictor.

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
a part with meta and a hole is the downloader's normal partial-fill state, so the hole falls
through a tier while its siblings still serve. It is gated on the pool holding the chunk, because
a freshly ingested part is on SSD alone until the drain replicates it and a DEK fault fails those
chunks too; without the gate a key error would become data loss. And it lives on
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
- **`meta.json` is the readiness gate**: `get_chunk` returns None unless `meta.json` exists AND the specific chunk file exists ([fs_store.py:168-173](fs_store.py)). `chunks_exist_batch` same — and it must stay a two-part test, because the two writers below mean different things by `meta.json`: meta alone would report a downloader's in-flight chunks as present and break partial-range fills.
- Uploaders write meta **last** (after all chunks). Downloaders write meta **first** (eager from DB parts row) so partial-range fills become readable as chunks land.
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

[object_parts.py:59](object_parts.py). Misnomer — the class name is legacy. Actual composition:

- `self._fs`: `FileSystemPartsStore` (created lazily from config if not injected). All chunk/meta I/O goes here.
- `self._notifier`: `ChunkNotifier` backed by `queues_client` (= `redis_queues_client`, port 6382).
- `self.redis`: still retained for a narrow purpose — the download-coalescing lock `download_in_progress:...` uses `SET NX EX` / `DELETE` on this client ([object_parts.py:78](object_parts.py) comment). Not used for data.

Key methods:

- `get_chunk` / `set_chunk` / `chunks_exist_batch` → delegate to `self._fs`.
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
- **`stream_subscription(object_id, object_version, fetch_fn=...)`** (RQ-1) — an async context manager that opens ONE pattern subscription (`notify:obj:{oid}:v:{v}:part:*:chunk:*`) for a whole stream and demuxes notifications to per-chunk `asyncio.Event`s. Replaces the per-chunk subscribe/unsubscribe churn of `wait_for_chunk` on cold multi-chunk reads. Gated by `HIPPIUS_STREAM_SINGLE_SUBSCRIPTION` (default off); `stream_plan` falls back to per-chunk `wait_for_chunk` when off. Keeps the post-subscribe FS re-check race guard and adds a periodic FS re-check (`_STREAM_RECHECK_INTERVAL_SECONDS`) so a missed wakeup degrades to a bounded poll instead of hanging.
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

