### Filesystem-backed parts store and write-through plan

#### Problem

- Multipart uploads can span long durations. Early parts stored only in Redis may be evicted/expired before the uploader processes them, causing data loss.

#### High-level solution

- Introduce a shared-volume, filesystem-backed parts store used as the authoritative staging area for incoming parts and appends.
- Writers perform write-through: write to FS (mandatory) and to Redis (best-effort). Readers remain unchanged (Redis-first only). The uploader reads from FS. The substrate pinner deletes from FS after confirmed pins.

#### Non-goals (for this change)

- No Redis read fallback from FS for GET paths.
- No Redis write-back on read misses.
- No change to downloader hydration logic.

---

### Architecture

- Keep `ObjectPartsCache` (Redis) as-is; it remains the interface used by readers and the downloader.
- Add a new `PartsStore` abstraction for filesystem operations.
- Add a `WriteThroughPartsWriter` used by upload/append code paths to write to FS first (fatal on error) and to Redis second (log-only on error).
- Uploader switches to read-only from `PartsStore` instead of Redis.
- Substrate pinner becomes responsible for deletion from FS once pin confirmations are in place.

#### Interfaces

```python
# New FS-only interface
class PartsStore(Protocol):
    def part_path(self, object_id: str, object_version: int, part_number: int) -> str: ...

    async def set_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None: ...

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> bytes | None: ...

    async def chunk_exists(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> bool: ...

    async def set_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        size_bytes: int,
    ) -> None: ...

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> dict | None: ...

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None: ...

    async def delete_object(self, object_id: str, object_version: int | None = None) -> None: ...
```

```python
# Writer used by upload/append code paths
class WriteThroughPartsWriter:
    def __init__(self, fs: PartsStore, redis_cache: ObjectPartsCache, ttl_seconds: int): ...

    async def write_meta(...):
        # FS first (fatal on failure), then Redis (log-only on failure)
        ...

    async def write_chunks(...):
        # FS first (fatal on failure), then Redis (log-only on failure)
        ...
```

Notes:

- FS write failures are fatal and must abort the request.
- Redis write failures are logged but do not fail the request.
- Readers (GET) and downloader continue to use `ObjectPartsCache` only.

---

### Filesystem layout and atomicity

- Root directory: `object_cache_dir` (configurable; shared volume).
- Layout per object/version/part:

```
<object_cache_dir>/<object_id>/v<object_version>/part_<part_number>/
  chunk_<index>.bin      # ciphertext chunk files
  meta.json              # written last; indicates part is complete
```

- Atomic writes:
  - Write each chunk to `chunk_<index>.bin.tmp`, then `rename` → `chunk_<index>.bin`.
  - Write `meta.json.tmp`, then `rename` → `meta.json` (complete marker).
  - The presence of `meta.json` indicates the part directory is fully written and safe to read.

Concurrency:

- New parts can be added at any time (append or multipart). Per-part atomicity ensures readers/uploader either see a complete part or ignore incomplete ones (missing `meta.json`).

---

### Write path semantics

- All uploads and appends write through `WriteThroughPartsWriter`:

  1. FS: `set_chunk` for each chunk; then `set_meta` (fatal if any FS op fails)
  2. Redis: `set_chunk` for each chunk; then `set_meta` (log-only on error)

- TTL is applied only to Redis keys. FS has no TTL; cleanup is explicit (see deletion and janitor).

---

### Read path semantics

- Readers (GET) remain Redis-only and unchanged. No FS fallback.
- Downloader logic remains unchanged and populates Redis per current behavior.
- Uploader reads from `PartsStore` (FS) exclusively.

---

### Uploader behavior

- For each part to upload:
  - Load `meta.json`; iterate `chunk_<index>.bin` files and upload each chunk to IPFS to obtain per-chunk CIDs.
  - Upsert CID metadata in DB as today.
  - Do not delete FS data.

---

### Substrate pinner cleanup

- Pinning is per chunk CID. A part can be deleted from FS only when all of its chunk CIDs are confirmed pinned.
- Deletion algorithm (race-resistant with concurrent append):

  1. Verify all chunk CIDs for the part are pinned.
  2. Delete files in `part_<part_number>/` and remove the directory.
  3. Attempt to remove parent directories bottom-up if empty: `.../v<object_version>/`, then `.../<object_id>/`.
  4. Ignore “not empty” errors; this avoids races with newly appended parts.

- Optionally, when a whole object version is confirmed pinned, perform a bulk delete of all parts and prune empty parent directories.

---

### Aborted multipart uploads

- Add a janitor task which:
  - Scans `multipart_uploads` for stale entries (no progress beyond `mpu_stale_seconds`).
  - Removes their on-disk part directories from `PartsStore`.
  - Skips active uploads; use both DB timestamps and FS mtimes to reduce false positives.

---

### DLQ behavior

- Do not duplicate bytes. Only the message moves into the Redis DLQ queue.
- The bytes remain in FS under the standard parts directory layout.
- Existing DLQ filesystem persistence logic can be removed or left as a no-op wrapper around `PartsStore` reads if needed for tooling.

---

### Configuration

- Add to `hippius_s3/config.py`:
  - `object_cache_dir: str = env("HIPPIUS_OBJECT_CACHE_DIR:/var/lib/hippius/object_cache")`
    - `fs_cache_gc_max_age_seconds: int = env("HIPPIUS_FS_CACHE_GC_MAX_AGE_SECONDS:172800", convert=int)` # 48h
    - `mpu_stale_seconds: int = env("HIPPIUS_MPU_STALE_SECONDS:86400", convert=int)` # 24h

Permissions and ownership:

- The shared volume must be writable by API, uploader, downloader, and pinner containers. Prefer a dedicated user and group with consistent UID/GID mapping across containers.

---

### Deployment

- Add a shared volume mount (e.g., Docker Compose and k8s) for `HIPPIUS_OBJECT_CACHE_DIR` across:

  - API container (writers)
  - Uploader worker (reads FS)
  - Pinner/substrate worker (deletes FS)
  - Downloader (not required, but harmless)

- Ensure disk space monitoring and alerting for the volume.

---

### Telemetry and logging

- Log levels:

  - FS write failures: error (fatal to request).
  - Redis write failures: warning (best-effort).
  - Pinner deletions: info (per-part/summary), warnings on partial pin evidence.
  - Janitor actions: info for deletions, debug for scans.

- Metrics:
  - Counters for fs writes, fs deletes, fs janitor deletions.
  - Gauges for FS used bytes (optional), number of parts on disk.
  - Counters for Redis write failures during write-through.

---

### Testing plan

Unit tests:

- `FileSystemPartsStore` read/write/delete; atomic renames; meta presence gating.
- `WriteThroughPartsWriter`: FS-fatal, Redis-best-effort semantics; ordering (meta last).
- Deletion helper: rmdir-if-empty behavior; ignore non-empty parent errors.

E2E (minimal, critical path):

1. Upload (multipart or append) large enough to exercise multiple chunks.
2. Clear Redis keys for the object parts.
3. Verify uploader still completes using FS bytes and publishes CIDs.
4. Verify GET works (downloader hydrates Redis from IPFS post-publish, unchanged).

Optional E2E:

- Simulate failed Redis writes during upload; ensure success via FS and correct uploader behavior.
- Simulate partial pin states; ensure part deletion waits for all chunk CIDs pinned.

---

### Rollout strategy

1. Deploy code with FS disabled (feature flag default config present but unused) to verify no regressions.
2. Provision and mount shared volume; set `HIPPIUS_OBJECT_CACHE_DIR` and enable write-through in writers.
3. Switch uploader to FS reads; keep Redis writes best-effort.
4. Enable pinner-side deletions; closely monitor disk usage.
5. Add janitor after initial stability period.

Rollback:

- Disable FS usage by config and revert writer to Redis-only (temporary) if needed.

---

### Future: Erasure Coding (EC)

- Parity shards will be FS-only and not written to Redis. Store alongside chunk files with a distinct prefix:
  - `parity_<index>.bin`
- Uploader collects and uploads both data and parity shards. `PartsStore` already encapsulates FS access; parity-specific APIs can be added later or treated as auxiliary files not referenced by `meta.json`.

---

### Implementation checklist

- Config

  - [ ] Add `object_cache_dir`, `fs_cache_gc_max_age_seconds`, `mpu_stale_seconds`.

- FS store

  - [ ] New `hippius_s3/cache/fs_store.py` implementing `PartsStore` with atomic chunk/meta writes and safe deletions.

- Writer

  - [ ] New `hippius_s3/writer/write_through_writer.py` with FS-first + Redis-best-effort semantics.
  - [ ] Update `hippius_s3/writer/object_writer.py` and MPU endpoints to use `WriteThroughPartsWriter` instead of the current `CacheWriter` for incoming data.

- Uploader

  - [ ] Update `hippius_s3/workers/uploader.py` to read chunks/meta from `PartsStore` instead of Redis.

- Pinner

  - [ ] Add deletion hook in pinner after confirming all chunk CIDs pinned per part (or per object-version) to call `PartsStore.delete_part` (or bulk delete).

- DLQ

  - [ ] Remove DLQ file duplication; keep messages only. Optionally adapt DLQ tooling to read from `PartsStore` if needed.

- Janitor

  - [ ] Add background task to remove stale FS parts for aborted MPUs.

- Deployment
  - [ ] Add shared volume mount for `HIPPIUS_OBJECT_CACHE_DIR` across relevant services; set ownership/permissions.

---

### Risk & mitigation

- Disk pressure: monitor FS volume usage; add GC and alerts.
- Partial writes: atomic rename guarantees; `meta.json` as completeness marker.
- Race with append: rmdir-if-empty after per-part delete avoids removing active directories.
- Redis outages: harmless; FS remains authoritative for writer/uploader paths.
