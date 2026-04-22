# hippius_s3/writer/

Upload pipeline. Takes an incoming `AsyncIterator[bytes]` body and lands it in the FS cache + DB, encrypting on the way.

Entry point is [`ObjectWriter`](object_writer.py); the heavy lifting is in [`put_simple_stream_full`](object_writer.py).

## Files

| File | Purpose |
|---|---|
| [object_writer.py](object_writer.py) | Orchestrator. Simple PUT, MPU part upload, MPU completion, append. |
| [chunker.py](chunker.py) | `stream_encrypt_to_chunks` — buffers plaintext, yields ciphertext one chunk at a time. |
| [write_through_writer.py](write_through_writer.py) | `WriteThroughPartsWriter` — FS writes (fatal), Redis writes (best-effort). |
| [cache_writer.py](cache_writer.py) | **Dead code** — `CacheWriter` not referenced anywhere. Delete candidate in [todo.md](../../todo.md). |
| [db.py](db.py) | `upsert_object_basic`, `ensure_upload_row` — atomic DB reserves. |
| [queue.py](queue.py) | `enqueue_upload` — write chunks to `{backend}_upload_requests` Redis queue. |
| [types.py](types.py) | Dataclasses: `PutResult`, `PartResult`, `CompleteResult`, `AppendPreconditionFailed`, etc. |

## The core path: `put_simple_stream_full`

[object_writer.py:169](object_writer.py). A streaming single-part upload. Detailed flow:

1. **Reserve version** ([line 210](object_writer.py)) — `upsert_object_basic` inserts or bumps `object_versions` with placeholder `size=0, md5=""`. Returns authoritative `object_id` and `current_object_version`. **Trust the DB's object_id**, not the caller's candidate ([line 222-227](object_writer.py)).
2. **Generate DEK** via [`envelope_service.generate_dek`](../services/envelope_service.py).
3. **Wrap DEK** with the bucket KEK via [`kek_service.get_or_create_active_bucket_kek`](../services/kek_service.py) + [`envelope_service.wrap_dek`](../services/envelope_service.py). AAD is `f"hippius-dek:{bucket_id}:{object_id}:{object_version}"`.
4. **Write envelope to DB immediately** ([line 245-261](object_writer.py)) — a later `UPDATE object_versions SET kek_id, wrapped_dek, enc_suite_id, enc_chunk_size_bytes`. This is **critical**: it closes the race window where a concurrent GET could hit the reserved row with NULL envelope columns and 500 with `v5_missing_envelope_metadata`. Before this fix, that was the root cause of 200k+ broken rows in prod (see [analysis.md](../../analysis.md)).
5. **Producer/consumer pipeline**:
   - Producer (main coroutine, [line 338-394](object_writer.py)): drains `body_iter`, accumulates into `pt_buf`, encrypts full chunks with the **global** chunk index (AEAD AAD binds to it, so chunks MUST be encrypted in order), enqueues onto `write_queue` (maxsize 16).
   - Consumer ([line 308-333](object_writer.py)): dequeues, calls `fs_store.set_chunk` (fatal on failure), appends to `redis_chunks` for best-effort batched Redis write ([line 330-331](object_writer.py) flushes every 16).
6. **Flush final Redis batch** ([line 285-306 `_flush_redis_batch`](object_writer.py)).
7. **Write FS meta** via `WriteThroughPartsWriter.write_meta` ([line 420](object_writer.py)). `meta.json` is the "part is complete" signal for readers. Must land AFTER every chunk is safely on disk — otherwise readers could see a completed part with missing chunks.
8. **Update object_versions** with final `size_bytes`, `md5_hash`, `content_type`, `metadata`, `updated_at` ([line 442](object_writer.py)). **Until this runs, the version is invisible to downloads** (the download query filters `size_bytes=0 AND md5=""` to avoid serving reserved-but-incomplete rows).
9. **Upsert upload row + part placeholder** ([line 455-474](object_writer.py)) — links the object_version to a multipart_uploads row (used for append and MPU; simple PUT still creates one for structural consistency).
10. **Return `PutResult`**. The endpoint then enqueues to `arion_upload_requests`.

## Multipart upload

- [`mpu_upload_part_stream`](object_writer.py) — same producer/consumer pattern, per-part.
- [`mpu_complete`](object_writer.py) — composites part ETags into a final MD5 (`md5("".join(bytes.fromhex(part_etag) for part in parts))` + `-{part_count}`), marks object as `publishing`.

## Append (S4)

- [`append_stream`](object_writer.py) / [`append`](object_writer.py) — reserves the next part number, calls into `mpu_upload_part_stream` style path, updates `append_version` and composite MD5 atomically on success. CAS via `append-if-version`, idempotency via `append-id`.

## Known issues

### Double FS writes

See [todo.md](../../todo.md) P1. The same chunk is written to FS twice per upload today:

- Once by the consumer ([object_writer.py:317-323](object_writer.py) → `fs_store.set_chunk`).
- Once by `_flush_redis_batch` → `obj_cache.set_chunks` → which internally loops `fs.set_chunk` ([../cache/object_parts.py:159-160](../cache/object_parts.py)). Same pattern in `WriteThroughPartsWriter.write_chunks` at [write_through_writer.py:95-103](write_through_writer.py).

Safe (deterministic content + atomic rename) but wasteful. The "Redis path" from before the FS migration is now just another FS write. Remove `set_chunks` from the write path; benchmark.

### Meta.json visibility race (handled)

Uploader writes meta AFTER all chunks. If a concurrent downloader worker races through the cache-miss path, it writes meta EAGERLY for its own consumption. Atomic rename means both writes are safe but we do it twice. [Downloader.py:256-269](../workers/downloader.py) already skips when meta exists.

### DB visibility gating

**Until `update_object_version_metadata` sets non-empty `size_bytes` and `md5_hash`, the object version is not serveable.** The download query uses this as the "complete" signal. Writer therefore must NOT enqueue to the Arion uploader before the object_versions UPDATE lands — otherwise the uploader could read the still-empty row.

### Crypto binding

AEAD suites bind to `(bucket_id, object_id, part_number, chunk_index, upload_id)`. For simple PUT, `upload_id` is `""` ([object_writer.py:122-124](object_writer.py)). For MPU, it's the real upload_id. Any code that reassigns chunk indices or shuffles parts must honor this — otherwise decryption fails.

### Variable chunk sizes per part

The `parts.chunk_size_bytes` column is per-part; simple PUT uses `config.object_chunk_size_bytes` (default 4 MiB), but legacy objects may have smaller/larger chunks. Readers use the DB value, not the config — see [../reader/planner.py](../reader/planner.py) and the `count_chunk_backends.sql` query (DB-driven, not hardcoded to 4MiB).

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 4, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #304 | 4:25 PM | 🔵 | Object Writer Sets Status to 'publishing' on Multipart Upload Completion | ~338 |

### Feb 14, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1900 | 9:22 PM | ⚖️ | System Already Supports Variable Chunk Sizes with Full Backward Compatibility | ~614 |
| #1896 | 9:20 PM | 🔵 | Object Writer Retrieves Chunk Size from Config and Persists to Database | ~436 |
| #1895 | " | 🔵 | Write-Through Writer Stores Chunk Size in Both FS and Redis Metadata | ~384 |
| #1893 | 9:19 PM | 🔵 | Chunker Streaming Function Uses Parameterized Chunk Size for Buffering | ~375 |

### Feb 16, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #2431 | 10:57 PM | 🔵 | upsert_object_basic wrapper executes upsert_object_basic.sql query | ~307 |
| #2429 | 10:56 PM | 🔵 | put_simple_stream_full reserves version via upsert_object_basic | ~319 |
| #2424 | 10:55 PM | 🔵 | ObjectWriter updates object_versions table after multipart completion | ~308 |
</claude-mem-context>
