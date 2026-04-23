# hippius_s3/workers/

Core worker logic. The ENTRY points that actually run in pods live in [/workers/](../../workers/) (top-level); this package has the shared implementations.

## Files

| File | Purpose |
|---|---|
| [uploader.py](uploader.py) | `BackendClient` ABC + `Uploader` class. Drains upload queue, chunks, retries with DLQ fallback. |
| [downloader.py](downloader.py) | Per-backend shared downloader loop. Drains download queue, writes chunks to FS, releases coalesce locks, publishes notifications. |
| [unpinner.py](unpinner.py) | `Unpinner` — drains unpin queue, calls backend `delete_file`, soft-deletes `chunk_backend` rows. |

Each backend ([workers/run_arion_uploader_in_loop.py](../../workers/run_arion_uploader_in_loop.py), etc.) passes a concrete `BackendClient` / `UnpinBackendClient` / `fetch_fn` into these shared loops.

## `BackendClient` ABC

[uploader.py:37-63](uploader.py):

```python
class BackendClient(ABC):
    async def upload_file_and_get_cid(self, file_path, ...) -> str: ...
    async def download_file(self, identifier, ...) -> bytes: ...
    async def delete_file(self, identifier, ...) -> None: ...
```

Concrete: `ArionClient` ([../services/arion_service.py](../services/arion_service.py)). Only one backend in production today.

## Uploader

`Uploader` processes `UploadChainRequest` from `arion_upload_requests`. Reads chunks from FS, uploads to Arion, records the returned identifier in `chunk_backend`. Retry config ([../config.py](../config.py)):

- `HIPPIUS_UPLOADER_MAX_ATTEMPTS=5`
- `HIPPIUS_UPLOADER_BACKOFF_BASE_MS=500`, `_MAX_MS=60000`
- `HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY=5` (per-part parallelism within an upload)
- `HIPPIUS_UPLOADER_PIN_PARALLELISM=5` (concurrent Arion calls).

Error classification ([uploader.py:78-123](uploader.py)):

- `507` → permanent.
- Timeouts / `503` / `429` → transient.
- Errors mentioning `"pin"`/`"unpin"` → permanent.

Transient failures go back to the queue with backoff; permanent failures go to the upload DLQ ([../dlq/upload_dlq.py](../dlq/upload_dlq.py)) for manual intervention.

**Single-instance design**: Only one uploader pod per backend per region — ensures deterministic CID assignment and controlled rate-limit usage against Arion. Scale via `pin_parallelism` / `multipart_max_concurrency`, not replicas.

## Downloader

`process_download_request` at [downloader.py:94](downloader.py). Fulfills a `DownloadChainRequest` (list of parts, each with a list of `PartChunkSpec(index, cid?, cipher_size_bytes?)`). Flow:

1. **Eager meta write** ([downloader.py:49-91](downloader.py)): for each part, check if `meta.json` exists; if not, write it from the DB `parts` row. Chunk size defaults to `4 MiB` if DB says 0 ([downloader.py:82](downloader.py)). This makes partial fills readable.
2. **Chunk-batched processing** ([downloader.py:279-283](downloader.py)): within a part, chunks are batched by `config.downloader_semaphore` (default 20) so a pathological 5 GiB part (up to ~1280 chunks) doesn't spawn thousands of tasks on the semaphore.
3. **Per-chunk flow** ([downloader.py:143-242](downloader.py)):
   - Check FS: `fs_store.chunk_exists(...)`. Skip if cached.
   - Look up `backend_identifier` via `get_chunk_backend_identifier.sql`. Skip if null (this backend doesn't hold the chunk).
   - Call `fetch_fn(identifier, subaccount)` → ciphertext bytes.
   - `fs_store.set_chunk(...)` atomically.
   - `obj_cache.notify_chunk(...)` → publishes `notify:{chunk_key}`.
   - Retry with exponential backoff + jitter on failure (`DOWNLOADER_CHUNK_RETRIES`, `DOWNLOADER_RETRY_BASE_SECONDS`, `DOWNLOADER_RETRY_JITTER_SECONDS`).
4. **Release coalesce lock** ([downloader.py:286-292](downloader.py)):
   ```python
   lock_key = f"download_in_progress:{download_request.object_id}:v:{int(download_request.object_version)}:part:{part_number}"
   await obj_cache.redis.delete(lock_key)
   ```
   Key format **must** match [../services/object_reader.py:87](../services/object_reader.py) — otherwise streamers hang until the TTL expires.
5. **Reap + reconnect** ([downloader.py:423-435](downloader.py)): if an inflight task died from a Redis or asyncpg infra error, flag for client rebuild on the next loop iteration. Prevents continued failures against a stale connection.

Key config ([../config.py](../config.py)):

- `DOWNLOADER_SEMAPHORE=20` — concurrent chunk fetches per DCR.
- `DOWNLOADER_MAX_INFLIGHT=10` — concurrent DCRs per pod. Range-heavy reads produce many single-part DCRs; parallelizing them is what delivers real backend throughput.
- `DOWNLOADER_CHUNK_RETRIES=3`.
- `DOWNLOAD_COALESCE_LOCK_TTL=120` — upper bound on how long a streamer can wait for a crashed downloader.

## Unpinner

Delete pin on backend → mark `chunk_backend.deleted = true, deleted_at = now()` (soft delete). Retries with backoff (`HIPPIUS_UNPINNER_MAX_ATTEMPTS=5`, `HIPPIUS_UNPINNER_BACKOFF_BASE_MS=1000`, `_MAX_MS=60000`). Failures go to [../dlq/unpin_dlq.py](../dlq/unpin_dlq.py).

## Tracing

All worker operations emit OTel spans with `hippius.ray_id`, `hippius.account.main`, and backend-specific attributes. See [downloader.py:114-123](downloader.py) for the standard span shape.

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 13, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1773 | 11:15 AM | ✅ | Corrected Ty Ignore Error Code for spec.index Attribute Access | ~388 |
| #1770 | 11:07 AM | ✅ | Suppressed Redis aclose() Unresolved Attribute Errors | ~393 |
| #1768 | " | ✅ | Added Ty Ignore for Redis aclose() Method Call | ~376 |

### Feb 14, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1900 | 9:22 PM | ⚖️ | System Already Supports Variable Chunk Sizes with Full Backward Compatibility | ~614 |
| #1891 | 9:19 PM | 🔵 | Downloader Worker Uses Backend-Agnostic Chunk Retrieval via Chunk Backend Table | ~455 |

### Feb 20, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #2798 | 2:42 AM | 🔵 | Unpinner worker handles deletion from distributed storage with retry logic and soft-delete database tracking | ~584 |
| #2785 | 2:41 AM | 🔵 | Downloader worker architecture reveals queue processing with concurrency limits and retry logic | ~639 |
</claude-mem-context>
