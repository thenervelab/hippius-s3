# Upload & Download Flows

Step-by-step walkthrough of every code path a client object goes through, as of branch `feat/cdn` (PR #146). Post FS-cache migration: chunks live on a shared filesystem volume, Redis carries only pub/sub notifications and a per-part coalescing lock.

---

## 0. Primer — building blocks

### Filesystem layout

```
<HIPPIUS_OBJECT_CACHE_DIR>/
  └── <object_id>/
      └── v<object_version>/
          └── part_<part_number>/
              ├── meta.json                 # readiness marker (num_chunks, chunk_size, size_bytes)
              ├── chunk_0.bin               # ciphertext (AES-GCM, per-chunk)
              ├── chunk_1.bin
              └── chunk_<i>.bin.tmp.<uuid>  # transient, atomic-rename staging
```

- **Meta.json is the readiness marker.** `fs_store.get_chunk` and `chunk_exists` both gate on it — no meta, no read.
- **Atomic writes** use `.tmp.<uuid4>` suffix + `os.replace()` so two workers racing on the same chunk can never interleave bytes (`hippius_s3/cache/fs_store.py:76-134`).

### Redis surfaces

| Client                     | Use                                                           |
|----------------------------|---------------------------------------------------------------|
| `redis_client`             | auth/ACL/banhammer/rate-limit; coalescing lock (`SET NX EX`). |
| `redis_queues_client`      | upload/download queues + `notify:{chunk_key}` pub/sub.        |
| `redis_accounts_client`    | account credits.                                              |
| `redis_chain_client`       | substrate state.                                              |
| `redis_rate_limiting_client` | gateway rate limiting.                                      |
| `redis_acl_client`         | ACL cache.                                                    |

### Pub/sub channel format

```
notify:obj:<object_id>:v:<object_version>:part:<part_number>:chunk:<chunk_index>
```

### Coalescing lock

```
download_in_progress:<object_id>:v:<object_version>:part:<part_number>
```

TTL `DOWNLOAD_COALESCE_LOCK_TTL` (default 120 s). Set by the streamer in `build_stream_context`, deleted by the downloader after the part is processed.

### Replication gate (janitor)

`count_chunk_backends.sql` must return `total_chunks == replicated_chunks == expected_chunks` across `config.upload_backends ∪ config.backup_backends`. A part is **never** deleted from FS before this holds — that's the user's absolute rule, enforced at `workers/run_janitor_in_loop.py:556-562`.

---

## 1. Upload flow

All upload paths funnel into one of three `ObjectWriter` methods (`hippius_s3/writer/object_writer.py`) and use the same streaming encryption + FS write pipeline.

### 1.1 PutObject (single-part, non-append)

Entry: `hippius_s3/api/s3/objects/put_object_endpoint.py::handle_put_object`.

1. **Gateway → API.** Gateway validated auth + ACL, forwarded with `X-Hippius-*` headers. API's `parse_internal_headers_middleware` populates `request.state.account`.
2. **User + bucket resolution.** `get_or_create_user_by_main_account`, `get_bucket_by_name`. 404 if bucket missing.
3. **Branch on `x-amz-meta-append`.** If present → jump to §1.4 (Append). Else continue.
4. **Delegate to `ObjectWriter.put_simple_stream_full`** (`object_writer.py:170`).
5. **Reserve version.** `upsert_object_basic` creates/updates the `objects` + `object_versions` rows with placeholder md5/size. DB is authoritative for `object_id` under concurrent creates.
6. **Envelope encryption setup.** Generate per-version DEK, wrap with bucket KEK, write `(enc_suite_id, kek_id, wrapped_dek)` to `object_versions` immediately. This prevents concurrent GETs from seeing a NULL envelope during the write window.
7. **Streaming encrypt + write loop** (`object_writer.py:335-399`):
   - Producer reads `body_iter`, buffers to `chunk_size` (4 MiB default), encrypts one chunk at a time with chunk-index-bound AEAD nonce, pushes onto a 16-slot `asyncio.Queue`.
   - Consumer task drains the queue and calls `fs_store.set_chunk(oid, v, part_number=1, chunk_idx, ct)`. Each write is atomic (temp + rename).
   - Consumer also pipelines chunks into a Redis best-effort batch flush (dead code post-migration; logs warnings if Redis errors but never fails the request).
8. **Write meta.json AFTER all chunks on disk.** `writer.write_meta(...)` via `WriteThroughPartsWriter` (`write_through_writer.py`). Until meta exists, readers don't see the part.
9. **Finalise DB metadata.** `update_object_version_metadata` sets final `size_bytes`, `md5`, `content_type`, `metadata`. Only *now* does the new version become visible to `get_object_for_download_with_permissions` (it filters out rows with size=0 / md5=''). **This is the atomicity boundary of a PutObject.**
10. **Insert `parts` + `part_chunks` rows.** `ensure_upload_row` + `upsert_part_placeholder` with per-chunk cipher sizes.
11. **Enqueue upload.** `writer.queue.enqueue_upload` fans out one `UploadChainRequest` to each backend queue (`{backend}_upload_requests`, e.g. `arion_upload_requests`).
12. **Return 200** with ETag = md5 hash.

### 1.2 Multipart upload

Entry: `hippius_s3/api/s3/multipart.py`.

- **1.2.1 CreateMultipartUpload** (`multipart.py` handler): insert `multipart_uploads` + `object_versions` rows, no FS writes. Return `upload_id`.
- **1.2.2 UploadPart**: same streaming pipeline as PutObject, but keyed by `(upload_id, part_number)`. Calls `ObjectWriter.mpu_upload_part_stream` (`object_writer.py:768`). Writes chunks + meta to `part_<part_number>/`. Returns part ETag = md5 of ciphertext.
  - **Does NOT enqueue** a backend upload — enqueue happens at Complete.
- **1.2.3 CompleteMultipartUpload**: validates the client-supplied ETag list against DB, calls `ObjectWriter.mpu_complete` to finalise `object_versions.status → 'published'` with a combined ETag (`md5(concat(part_etags))-N`). **No chunk writes** — UploadPart already wrote them. Then enqueues ONE `UploadChainRequest` covering all parts. Idempotent on re-invocation.
- **1.2.4 AbortMultipartUpload**: calls `fs_store.delete_part(...)` for each part, tombstones the DB row, returns 204.

### 1.3 CopyObject

Entry: `hippius_s3/api/s3/copy_helpers.py::handle_streaming_copy`.

The v5 "fast path" (`copy_service_v5.py`) is disabled — `should_use_v5_fast_path` always returns False. The active path:

1. Resolve source + destination bucket/object rows.
2. **Stream the source via the READER path.** `object_reader.stream_object(...)` returns a plaintext async iterator. This triggers the full download flow (§2) including cache-hit / pipeline / pub/sub — exactly as if a client had GET'd the source.
3. **Feed that iterator into `ObjectWriter.put_simple_stream_full`** on the destination. Fresh DEK, re-encrypted per the destination object_id (AAD binds to dest oid + version).
4. Same FS-write + enqueue tail as §1.1.

Net effect: a CopyObject is one GET and one PUT chained in-process, never materialised in memory beyond a chunk at a time.

### 1.4 AppendObject (S4 extension)

Entry: `hippius_s3/api/s3/extensions/append.py::handle_append`.

1. Parse `x-amz-meta-append-if-version` (CAS token). Missing / non-integer → 400.
2. `ObjectWriter.append_stream` (`object_writer.py:961`):
   - Take a row-level lock on the current `object_versions` row.
   - CAS: if `append_version` doesn't match the client's token → 412 Precondition Failed.
   - Reserve `part_number = MAX(existing) + 1`.
   - Ensure a multipart `upload_id` exists (create if needed).
   - **Delegate to `mpu_upload_part_stream`** (same machinery as §1.2.2).
   - Bump `append_version` atomically in `object_versions`.
3. Enqueue upload for the new part.
4. Return 200 with the new `x-amz-meta-append-version` header.

**Key property:** append appends to the *current* object_version — it does NOT bump the version. It's a new *part*, not a new *version*.

### 1.5 Uploader worker

Entry: `workers/run_arion_uploader_in_loop.py` → `hippius_s3/workers/uploader.py::Uploader.process_upload`.

1. `BRPOP arion_upload_requests`.
2. Check that `object_versions.status != 'deleted'`; skip if gone.
3. For each part in the request:
   - `fs_store.get_meta(...)` → read `num_chunks` + `chunk_size_bytes`.
   - For each `chunk_<i>.bin`: `fs_store.get_chunk(...)` (touches atime as a side-effect).
   - `backend_client.upload_file_and_get_cid(...)` — sends ciphertext to Arion, gets back a CID / backend identifier.
   - `INSERT INTO chunk_backend (chunk_id, backend, backend_identifier)`.
4. `obj_cache.expire(part)` — now a pure FS touch of every file in the part dir (post-migration, no Redis TTL to refresh). Signals the janitor "recent activity".
5. **Transient errors** → exponential-backoff retry (3–5 attempts). **Terminal errors** → push to `arion_upload_requests:dlq`; `object_versions.status → 'failed'`.
6. After the uploader rows are in place, the janitor's replication check can start returning True for this part.

### 1.6 Janitor (cleanup side of the upload lifecycle)

Entry: `workers/run_janitor_in_loop.py::run_janitor_loop`. Three phases per cycle:

1. **`cleanup_stale_parts`** — remove parts whose `meta.json` mtime is older than `mpu_stale_seconds` AND no recent DB `parts.uploaded_at` activity AND not in any DLQ.
2. **`cleanup_old_parts_by_mtime`** — replication-gated GC:
   - Never deletes unless `is_replicated_on_all_backends` returns True (required backends = upload_backends ∪ backup_backends).
   - Protects "hot" parts (atime within `fs_cache_hot_retention_seconds`, default 3 h). Under elevated disk pressure (≥ 85%) the hot window halves; under critical (≥ 95%) hot-retention disables but the replication gate still holds — and if nothing is replicated, janitor logs `JANITOR_CRITICAL_PRESSURE_BLOCKED` ERROR and refuses to delete.
3. **`cleanup_orphan_tmp_files`** — sweeps `.tmp.<uuid>` files older than 1 h (crashed atomic writes).
4. **`gc_soft_deleted_objects`** — hard-deletes `objects` rows whose `chunk_backend` rows are all `deleted=true` AND at least one chunk_backend row ever existed (closes the "empty set = all deleted" false positive).

---

## 2. Download flow

### 2.1 GetObject entry

Entry: `hippius_s3/api/s3/objects/get_object_endpoint.py::handle_get_object`.

1. Early-exit branches on query params:
   - `?tagging` → object tagging handler (returns XML tags, no chunk I/O).
   - `?uploadId` → `list_parts_internal` (multipart parts listing, no chunk I/O).
   - `?versionId` → specific version fetch via `get_object_for_download_with_permissions_by_version`.
2. Otherwise: `get_object_for_download_with_permissions`. Returns NoSuchKey / NoSuchVersion 404 where appropriate.
3. Parse `Range: bytes=s-e` header, validate against effective size. Invalid range → 416.
4. Resolve storage_version, prepare `info` dict including encryption envelope (`kek_id`, `wrapped_dek`).
5. **Call `read_response`** → `build_stream_context` + `stream_plan`. Wrap the async generator in a FastAPI `StreamingResponse` with status 200 (full) or 206 (range).

### 2.2 `build_stream_context` — the brain

Location: `hippius_s3/services/object_reader.py:47-236`. **See the deep-dive in `/Users/radumutilica/.claude/plans/jiggly-jumping-hickey.md` for full coalescing analysis + edge cases.** In summary:

1. `read_parts_list` + `build_chunk_plan` → `ChunkPlanItem[]` covering only in-range (part, chunk) pairs.
2. `obj_cache.chunks_exist_batch` → parallel `list[bool]`. All True → `source="cache"`, skip to step 5.
3. **Coalescing** (pipeline branch): per missing part, `SET NX EX download_in_progress:{oid}:v:{ov}:part:{pn} <ray> 120`.
   - Acquired → this streamer owns that part's enqueue.
   - Held by peer → skip enqueue, just wait via pub/sub later.
   - Redis error → fail open (enqueue anyway; downloader deduplicates).
4. If any part was acquired: build one `DownloadChainRequest` covering all acquired parts, `enqueue_download_request` onto `{backend}_download_requests`.
5. Envelope fallback: if current version has NULL `kek_id/wrapped_dek` (mid-write race), fall back to `object_version - 1` with its intact envelope. This is a one-shot fallback, no recursion.
6. Decrypt DEK (`unwrap_dek(kek, wrapped_dek, aad=f"hippius-dek:{bucket_id}:{object_id}:{version}")`).
7. Return `StreamContext(plan, object_version, storage_version, source, key_bytes, suite_id, bucket_id, upload_id)`.

### 2.3 `stream_plan` — the HTTP generator

Location: `hippius_s3/reader/streamer.py:18-144`.

- For each `ChunkPlanItem` in the plan:
  - `obj_cache.wait_for_chunk(oid, v, pn, ci)` — returns ciphertext.
  - `decrypt_chunk_if_needed(...)` — AES-GCM per chunk.
  - `maybe_slice(pt, slice_start, slice_end_excl)` — trim partial chunks at range boundaries.
  - `yield` bytes.
- Prefetch window (`HTTP_STREAM_PREFETCH_CHUNKS`) overlaps wait_for_chunk for chunk N+1 with decrypt+send of chunk N.
- `finally:` cancels pending fetch tasks if the client disconnects or we error out. Important for freeing pub/sub subscriptions.

### 2.4 `wait_for_chunk` — fast + slow paths

Location: `hippius_s3/cache/notifier.py:61-126` (via `hippius_s3/cache/object_parts.py:275-284`).

1. **Fast path:** `fs_store.get_chunk(oid, v, pn, ci)` → gates on `meta.json` existence, reads `chunk_<i>.bin`, `os.utime`s both files (atime refresh for janitor's hot retention). Hit returns bytes immediately.
2. **Slow path:**
   - `pubsub.subscribe("notify:{chunk_key}")`.
   - **Race-safe re-check**: call `fs.get_chunk` again (a worker may have finished between fast-path and subscribe).
   - If still missing, `asyncio.wait_for(_listen(), timeout=cache_ttl_seconds)`.
   - On message, fetch again. Single 100 ms retry for transient misses (janitor/replication lag). Else `RuntimeError`.
3. `unsubscribe` + `aclose` in the context manager `finally`.

### 2.5 Downloader worker

Entry: `workers/run_arion_downloader_in_loop.py` → `hippius_s3/workers/downloader.py::run_downloader_loop`.

1. `BRPOP arion_download_requests`.
2. For each `PartToDownload` in the request:
   - **Eager `set_meta` from DB.** `_write_part_meta_from_db` reads `parts.size_bytes` + `parts.chunk_size_bytes`, computes `num_chunks`, writes `meta.json` **before any chunks land**. Idempotent (deterministic content). This unlocks per-chunk reads as each chunk arrives.
   - **Gather-fetch** with bounded semaphore (`downloader_semaphore`, default 20):
     - `chunk_exists` skip check (deduplicates against other workers / earlier fetches).
     - DB lookup for backend identifier (`get_chunk_backend_identifier.sql`). No row → backend doesn't hold it → skip.
     - `fetch_fn(identifier, subaccount)` → ciphertext. Retry up to `downloader_chunk_retries` (default 3) with exponential backoff + jitter.
     - `fs_store.set_chunk(...)` — atomic temp+rename.
     - `obj_cache.notify_chunk(...)` — `PUBLISH` on `notify:{chunk_key}`.
   - **Lock release (best effort).** `redis.delete(download_in_progress:…:part:{pn})` in `suppress(Exception)`. Always runs after `asyncio.gather` completes, whether all chunks succeeded or not.
3. Metrics: `record_downloader_operation(backend, main_account, success, duration, num_chunks)`, TTFB, throughput.
4. DB/Redis reconnect loops on transient errors. DLQ semantics deferred to the metrics-only layer (download path is retriable by re-enqueue).

### 2.6 HeadObject

Entry: `hippius_s3/api/s3/objects/head_object_endpoint.py::handle_head_object`.

- Fetches minimal `object_info` (lightweight query).
- **Never touches chunks**. Only probes `fs_store.chunk_exists(...,part_number=1,chunk_index=0)` to populate the `x-hippius-source` diagnostic header ("cache" / "pipeline").
- Never enqueues a download.
- Builds response headers (ETag, Content-Length, Last-Modified, user metadata, `x-amz-meta-append-version`) and returns 200 body-less.

---

## 3. Observability quick reference

| Signal                                          | Where                                             |
|-------------------------------------------------|---------------------------------------------------|
| `cache_hits_total{operation="get_chunk"}`        | Streamer fast-path hits                           |
| `cache_hits_total{operation="chunk_exists"}`    | `build_stream_context` batch checks              |
| `downloader_duration_seconds` (histogram)       | per chunk fetch latency                           |
| `downloader_operations_total{success}`          | per `DownloadChainRequest`                        |
| `fs_cache_hot_parts`                            | janitor gauge                                    |
| `fs_cache_pressure_mode` (0/1/2)                | normal / elevated / critical                      |
| `fs_janitor_deleted_total`                      | parts evicted                                    |
| `fs_janitor_tmp_deleted_total`                  | orphan tmp files swept                           |
| `x-hippius-source` response header              | `cache` (FS hit) or `pipeline` (went through the downloader) |
| OTel spans                                      | `put_object.*`, `get_object.*`, `downloader.*`, `build_stream_context.*` |

---

## 4. One-page flow diagram

```
                                                         ┌───────────────────┐
                          ┌──────────────────────────────│  Client (boto3)   │
                          │                              └──────┬────────────┘
                          ▼                                     ▼
                   ┌─────────────┐                       ┌─────────────┐
                   │  HAProxy    │                       │  HAProxy    │
                   └──────┬──────┘                       └──────┬──────┘
                          ▼                                     ▼
                   ┌─────────────┐      (GET / HEAD)     ┌─────────────┐
                   │   Gateway   │◀──auth+ACL+ratelimit─ │   Gateway   │
                   └──────┬──────┘                       └──────┬──────┘
                          ▼                                     ▼
   ┌──── PutObject ──────────────────┐    ┌─── GetObject ───────────-──────┐
   │                                 │    │                                │
   │  ObjectWriter.put_simple_       │    │  build_stream_context          │
   │     stream_full                 │    │    │                           │
   │    │                            │    │    ├─ chunks_exist_batch (FS)  │
   │    ├─ reserve object_version    │    │    │                           │
   │    ├─ wrap DEK                  │    │    ├─ [all hit] source=cache   │
   │    ├─ stream encrypt → consumer │    │    │                           │
   │    │     └─ fs_store.set_chunk  │    │    └─ [miss] source=pipeline:  │
   │    │         (atomic tmp+rename)│    │         SET NX coalesce lock   │
   │    ├─ fs_store.set_meta (last!) │    │         enqueu_download_request│
   │    ├─ update_object_version_    │    │                                │
   │    │     metadata (now visible) │    │  stream_plan                   │
   │    └─ enqueue_upload_request    │    │    └─ wait_for_chunk (notifier)│
   │                                 │    │         ├─ fast: fs_store.get  │
   └──────────────┬──────────────────┘    │         └─ slow: subscribe     │
                  ▼                       │               notify:{key},    │
          arion_upload_requests           │               re-check, wait   │
                  │                       │                                │
                  ▼                       └──────────────────┬─────────────┘
         ┌───────────────┐                                    ▲
         │  Uploader     │                                    │ PUBLISH notify:{key}
         │    │          │                                    │
         │    ├─ fs_store.get_meta/chunk                      │
         │    ├─ arion.upload_file_and_get_cid                │
         │    └─ chunk_backend INSERT                         │
         └───────┬───────┘                                    │
                 ▼                                            │
           ┌──────────┐                              ┌────────┴────────┐
           │  Arion   │◀──────────────── fetch ──────│   Downloader    │
           └──────────┘                              │    │            │
                                                     │    ├─ fs_store  │
                                                     │    │    .set_   │
                                                     │    │    chunk   │
                                                     │    ├─ notify    │
                                                     │    │    _chunk  │
                                                     │    └─ delete    │
                                                     │         lock    │
                                                     └─────────────────┘

                             ┌──────────────┐
                             │   Janitor    │──── touch atime on read
                             │              │──── replication-gated GC
                             │              │──── hot-retention window
                             └──────────────┘
```

---

## 5. What CAN'T happen (invariants)

- **No chunk is served from FS before `meta.json` exists.** `fs_store.get_chunk` / `chunk_exists` gate on it. Readers never see a partial part.
- **No chunk is written in-place.** Every write is `tmp.<uuid>` → `os.replace`. Concurrent writers can't interleave bytes.
- **No part is deleted by the janitor unless replication check returns True** on every required backend (upload + backup). Disk-pressure modes tighten the hot-window but never bypass the replication gate.
- **No PutObject is visible to readers until `update_object_version_metadata` runs** — this is the atomicity boundary. Upstream concurrent overwrites can temporarily read the previous version via the envelope-fallback path, never an in-progress one.
- **At most one enqueued `DownloadChainRequest` per (object_id, version, part)** within the 120 s lock window (unless Redis is flaky and we fail-open — then the downloader's chunk_exists skip prevents duplicate backend fetches).

---

## 6. Known hardening follow-ups (not blockers)

- `cache_ttl_seconds` default (3600 s) → streamer chunk-wait ceiling is too long. Recommend 120–300 s so hung downloaders surface as 5xx quickly.
- Coalescing lock TTL (120 s) shorter than large-object download time → duplicate backend bandwidth. Recommend heartbeat-extend from the downloader, or raise default.
- No periodic re-probe in `wait_for_chunk` → a lost pub/sub message (Redis reconnect) forces a 1 h timeout. Cheap to fix with an N-second poll inside `_listen`.
- No negative-path signal for terminally failed chunks → waiters time out rather than fail fast. A `failed:{chunk_key}` sentinel channel would let waiters short-circuit.
