# hippius_s3/reader/

Download pipeline. Given a planned set of chunks, stream decrypted bytes back to the client.

The orchestration layer above this is [`services/object_reader.py`](../services/object_reader.py) — it decides cache vs pipeline, unwraps the DEK, and enqueues to the downloader if needed. This dir is purely the streaming mechanics.

## Files

| File | Purpose |
|---|---|
| [planner.py](planner.py) | `build_chunk_plan` — given parts list and optional Range, emit `[ChunkPlanItem]` with per-chunk slice bounds. |
| [streamer.py](streamer.py) | `stream_plan` — yields decrypted bytes for each planned chunk with optional prefetch. |
| [decrypter.py](decrypter.py) | `decrypt_chunk_if_needed`, `maybe_slice`. |
| [types.py](types.py) | `ChunkPlanItem(part_number, chunk_index, slice_start, slice_end_excl)`, `RangeRequest`. |
| [db_meta.py](db_meta.py) | `read_parts_list` — loads part rows (size, chunk_size, etag) for the plan. |

## `build_chunk_plan`

[planner.py:12](planner.py). For each part in range, emit one `ChunkPlanItem` per chunk that intersects the range. The first and last items of a range request get `slice_start` / `slice_end_excl` to trim the plaintext after decryption.

Chunk size is read per-part from the DB (`parts.chunk_size_bytes`), not from config — supports legacy variable-chunk objects.

## `stream_plan`

[streamer.py:18](streamer.py). Two modes:

### `prefetch_chunks=0` (sequential, default)

Trivial loop: for each item, `await obj_cache.wait_for_chunk(...)` → `decrypt_chunk_if_needed(...)` → `yield maybe_slice(pt, slice_start, slice_end_excl)`. Preserves strict ordering and back-pressure.

### `prefetch_chunks>0` (pipelined)

Scheduling loop ([streamer.py:74-144](streamer.py)):

- Schedule at least one chunk. Pre-schedule up to `prefetch` additional chunks as asyncio tasks.
- Pop the next pending task, await its bytes, schedule one more to keep the pipeline full, decrypt, slice, yield.
- On client disconnect or early exit, cancel any pending tasks in `finally`.

**Why default is 0**: prefetch=0 preserves the original sequential behavior exactly. Any prefetch >0 must handle async exception propagation carefully — a failing prefetch task shouldn't kill the stream until we actually reach it. The current scheduler does this correctly (each task's exception is re-raised when it's `await`ed), but the "default 0" is a conservative guard.

Opportunity: for large sequential GETs, enabling prefetch=4 or so would overlap FS fetch (or Arion fetch for cold chunks) with decrypt+IO. Measure before committing. Listed as P2 in [todo.md](../../todo.md).

## `decrypt_chunk_if_needed`

[decrypter.py:9](decrypter.py). Single function, delegates to `CryptoService.decrypt_chunk` ([../services/crypto_service.py](../services/crypto_service.py)). Handles:

- Storage version check.
- AAD reconstruction: `hippius-dek:{bucket_id}:{object_id}:{object_version}` plus chunk context.
- AES-256-GCM decryption with authentication tag check. Failure raises (no silent bypass).

`maybe_slice(pt, slice_start, slice_end_excl)` ([decrypter.py:40](decrypter.py)) trims plaintext for Range requests.

## `wait_for_chunk`

Called by `stream_plan` via `obj_cache`. Implementation in [../cache/object_parts.py:275](../cache/object_parts.py) delegating to [../cache/notifier.py:61](../cache/notifier.py).

- Fast path: `fs_store.get_chunk` returns bytes → yield immediately.
- Slow path: subscribe to `notify:{chunk_key}` pub/sub, re-check (race guard), wait on message, re-fetch, retry once on transient miss.
- Timeout: `cache_ttl_seconds` (default 3600) — if nothing publishes within that, raises, handled by API's global exception handler as 503 SlowDown.

## Interactions with the downloader worker

When `build_stream_context` in [../services/object_reader.py](../services/object_reader.py) determines `source="pipeline"`, it enqueues a `DownloadChainRequest` on `arion_download_requests`. The downloader ([../workers/downloader.py:94](../workers/downloader.py)) fulfills it chunk-by-chunk, publishing notifications after each write. The streamer here just waits on those notifications via `wait_for_chunk`.

**Coalescing**: multiple simultaneous GETs on the same cold part only enqueue once thanks to the `download_in_progress:...` lock in `build_stream_context`. Readers that lose the race still receive chunks via the shared pub/sub.

## Gotchas

- **Range requests still fetch full chunks from Arion** — see [todo.md](../../todo.md) section 3.3 for the range-aware backend fetch idea.
- **Client disconnect mid-stream** is handled correctly — the `finally` block in `stream_plan` cancels pending prefetch tasks. But if the client disconnects while waiting on pub/sub for a slow backend, the downloader still finishes its write (good — the chunk stays in cache for the next reader).
- **`key_bytes` is None for legacy unencrypted objects** — `decrypt_chunk_if_needed` passes the ciphertext through unchanged in that case. New writes are always v5 encrypted (enforced by `require_supported_storage_version`).

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 14, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1900 | 9:22 PM | ⚖️ | System Already Supports Variable Chunk Sizes with Full Backward Compatibility | ~614 |
| #1892 | 9:19 PM | 🔵 | Reader DB Meta Retrieves Chunk Size from Database Metadata Per Part | ~367 |
| #1888 | 9:17 PM | 🔵 | S3 Gateway Chunk Planning Logic Uses Dynamic Per-Part Chunk Sizes | ~413 |
</claude-mem-context>
