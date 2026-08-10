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

> **Runtime default is 16, not 0.** `prefetch_chunks=0` is only the `stream_plan` function-parameter
> fallback. The wired config default is `HTTP_STREAM_PREFETCH_CHUNKS=16` ([config.py:296](../config.py)),
> so production runs the pipelined branch below. The `=0` mode is what the sequential tests exercise.

### `prefetch_chunks=0` (sequential; function-param fallback, not the runtime default)

Trivial loop: for each item, `await obj_cache.wait_for_chunk(...)` → `decrypt_chunk_if_needed(...)` → `yield maybe_slice(pt, slice_start, slice_end_excl)`. Preserves strict ordering and back-pressure.

### `prefetch_chunks>0` (pipelined)

Scheduling loop ([streamer.py:74-144](streamer.py)):

- Schedule at least one chunk. Pre-schedule up to `prefetch` additional chunks as asyncio tasks.
- Pop the next pending task, await its bytes, schedule one more to keep the pipeline full, decrypt, slice, yield.
- On client disconnect or early exit, cancel any pending tasks in `finally`.

**Why the function-param fallback is 0**: it preserves the original sequential behavior exactly for callers that don't pass a value. Any prefetch >0 must handle async exception propagation carefully — a failing prefetch task shouldn't kill the stream until we actually reach it. The current scheduler does this correctly (each task's exception is re-raised when it's `await`ed). Callers in `object_reader.py` pass the config value (16), so prod uses the pipelined path.

Opportunity: for large sequential GETs, enabling prefetch=4 or so would overlap FS fetch (or Arion fetch for cold chunks) with decrypt+IO. Measure before committing. Listed as P2 in [todo.md](../../todo.md).

## `decrypt_chunk_if_needed`

[decrypter.py:20](decrypter.py). Single function, delegates to `CryptoService.decrypt_chunk` ([../services/crypto_service.py](../services/crypto_service.py)) on the crypto pool (RD-2). Handles:

- Storage version check.
- AAD reconstruction: `hippius-dek:{bucket_id}:{object_id}:{object_version}` plus chunk context.
- AES-256-GCM decryption with authentication tag check.

An authentication failure is **not** a plain raise any more. The decrypter defines
`CIPHERTEXT_UNUSABLE` ([decrypter.py:17](decrypter.py)) — `InvalidTag`, plus the
`CryptoError("ciphertext_too_short")` a version-skewed peer serves — and the streamer's
`_decrypt_reloading_once` ([streamer.py:45](streamer.py)) acts on it: drop THIS node's copy via
`DualFileSystemPartsStore.invalidate_local_chunk`, re-fetch from the next tier, and decrypt again
**exactly once** — then raise if it still fails. Never a silent bypass: every path out of a failed
decrypt either yields authenticated plaintext or raises, counted as
`chunk_aead_failures_total{tier=local|remote, outcome=recovered|unrecovered}`.

Two gates bound the retry. The invalidation only happens when the **pool holds the chunk** — a
freshly ingested part lives on SSD alone until the drain replicates it, and a DEK fault fails those
chunks too, so an ungated unlink would turn a key error into data loss. And the retry is
straight-line, not a loop: a DEK-level fault fails every chunk, and with promotion on, a looping
retry would re-warm the copy it just dropped and never run out of things to invalidate. When
nothing local held the bytes (peer/pool served them, or no lower tier exists), the failure raises
immediately — re-fetching would return the same bytes, and a pool fault is a genuine error. Full
rationale: [../cache/CLAUDE.md](../cache/CLAUDE.md) "Invalidating a chunk that fails AEAD".

`maybe_slice(pt, slice_start, slice_end_excl)` ([decrypter.py:55](decrypter.py)) trims plaintext for Range requests.

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
