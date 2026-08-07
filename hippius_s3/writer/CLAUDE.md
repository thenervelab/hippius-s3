# hippius_s3/writer/

Upload pipeline. Takes an incoming `AsyncIterator[bytes]` body and lands it in the FS cache + DB, encrypting on the way.

Entry point is [`ObjectWriter`](object_writer.py); the heavy lifting is in [`put_simple_stream_full`](object_writer.py).

## Files

| File | Purpose |
|---|---|
| [object_writer.py](object_writer.py) | Orchestrator. Simple PUT, MPU part upload, MPU completion, append. |
| [chunker.py](chunker.py) | `stream_encrypt_to_chunks` — buffers plaintext, yields ciphertext one chunk at a time. |
| [write_through_writer.py](write_through_writer.py) | `WriteThroughPartsWriter` — FS writes (fatal), then the landed-part announcement (best-effort). |
| [landed.py](landed.py) | `LandedPartPublisher` — tells this node's drain agent a part is complete, so discovery is a queue pop instead of a whole-disk walk. |
| [cache_writer.py](cache_writer.py) | **Dead code** — `CacheWriter` not referenced anywhere. Delete candidate in [todo.md](../../todo.md). |
| [db.py](db.py) | `upsert_object_basic`, `ensure_upload_row` — atomic DB reserves. |
| [types.py](types.py) | Dataclasses: `PutResult`, `PartResult`, `CompleteResult`, `AppendPreconditionFailed`, etc. |

_(`queue.py` was removed: its `enqueue_upload` was the dead PUT-path upload producer. Since the s3-2.1 drain-direct cutover the Rust drain is the sole producer — it enqueues to `{backend}_upload_requests` only after replicating a part to the pool.)_

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
7. **Write FS meta** via `WriteThroughPartsWriter.write_meta` ([line 420](object_writer.py)). `meta.json` is the "part is complete" signal for readers. Must land AFTER every chunk is safely on disk — otherwise readers could see a completed part with missing chunks. `write_meta` then **announces the part** to this node's drain agent ([landed.py](landed.py)) — strictly after meta, since meta is what makes the part claimable. It is the one choke point every upload path funnels through, which is why the hook lives there rather than at the four call sites. Best-effort: a dropped announcement costs discovery latency, and the agent's reconciler still finds the part on disk.
8. **Update object_versions** with final `size_bytes`, `md5_hash`, `content_type`, `metadata`, `updated_at` ([line 442](object_writer.py)). **Until this runs, the version is invisible to downloads** (the download query filters `size_bytes=0 AND md5=""` to avoid serving reserved-but-incomplete rows).
9. **Upsert upload row + part placeholder** ([line 455-474](object_writer.py)) — links the object_version to a multipart_uploads row (used for append and MPU; simple PUT still creates one for structural consistency).
10. **Return `PutResult`**. The endpoint does NOT enqueue the backend upload (drain-direct cutover) — it persists the version address (`set_object_version_address`); the Rust drain replicates the part to Ceph and LPUSHes the `UploadChainRequest` to `{backend}_upload_requests` itself, as the sole producer (see line 18).

## Multipart upload

- [`mpu_upload_part_stream`](object_writer.py) — same producer/consumer pattern, per-part.
- [`mpu_complete`](object_writer.py) — composites part ETags into a final MD5 (`md5("".join(bytes.fromhex(part_etag) for part in parts))` + `-{part_count}`), marks object as `publishing`.

## Append (S4)

- [`append_stream`](object_writer.py) / [`append`](object_writer.py) — reserves the next part number, calls into `mpu_upload_part_stream` style path, updates `append_version` and composite MD5 atomically on success. CAS via `append-if-version`, idempotency via `append-id`.

## Known issues

### Double FS writes (fixed)

Historically each chunk and each `meta.json` was written to FS twice per upload: once directly
(`fs_store.set_chunk` / `fs_store.set_meta`) and again via the post-migration `obj_cache` /
`redis_cache` mirror, which just looped back to `fs_store`. The mirror has been removed — the
streaming consumer's `fs_store.set_chunk` is the sole chunk write, and `WriteThroughPartsWriter`
now only writes meta/chunks to FS (its `redis_cache` arg is retained for call-site compatibility
but unused). `RedisObjectPartsCache.set_chunks` / `set_meta` remain for the downloader/read paths.

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

*No recent activity*
</claude-mem-context>
