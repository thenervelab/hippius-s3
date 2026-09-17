# hippius_s3/workers/

Core worker logic. The ENTRY points that actually run in pods live in [/workers/](../../workers/) (top-level); this package has the shared implementations.

## Files

| File | Purpose |
|---|---|
| [uploader.py](uploader.py) | `BackendClient` ABC + `Uploader` class. Drains upload queue, chunks, retries with DLQ fallback. |
| [unpinner.py](unpinner.py) | `Unpinner` — drains unpin queue, calls backend `delete_file`, soft-deletes `chunk_backend` rows. |

Each backend ([workers/run_arion_uploader_in_loop.py](../../workers/run_arion_uploader_in_loop.py), etc.) passes a concrete `BackendClient` / `UnpinBackendClient` into these shared loops. There is no download worker: the read path fetches straight from the backend into memory ([../reader/backend_fetch.py](../reader/backend_fetch.py)).

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

- `HIPPIUS_UPLOADER_MAX_ATTEMPTS=7`
- `HIPPIUS_UPLOADER_BACKOFF_BASE_MS=500`, `_MAX_MS=60000`
- `HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY=5` (per-part parallelism within an upload)

These are the values shipped in both [.env.defaults](../../.env.defaults) and
[k8s/base/configmap-defaults.yaml](../../k8s/base/configmap-defaults.yaml); the code defaults in
[../config.py](../config.py) match, so a pod with no overrides behaves the same. 7 attempts at
`500ms · 2^(n-1)` is ~63s of tolerance (0.5, 1, 2, 4, 8, 16, 32s) before the DLQ.

**This queue is the only retry layer for transport failures.** `retry_on_error` in
[../services/arion_service.py](../services/arion_service.py) deliberately does not catch
`httpx.ConnectError` and friends: they are classified `transient`, so re-driving them here gives
exponential backoff with jitter, durability across pod restarts, and — unlike the decorator, whose
sleep is held inside `_put_semaphore` — no cost to upload concurrency while waiting. Adding them to
both layers multiplies the budgets (7 × 4 ≈ 24 requests) at a backend that is already failing.

Per-pod request concurrency and the shared Arion-POST ceiling are covered under **Concurrency model** below.

Error classification lives in [errors.py](errors.py) — three path-specific classifiers sharing one rule engine (`classify_upload_error`, `classify_download_error`, `classify_unpin_error`). The key divergence is 404: permanent on upload/download, transient on unpin (pin commit pending upstream). Upload-only: `402` → `billing`. Layers: custom exception class → boto `Error.Code` → HTTP status → exception class/errno → keyword fallback → chained `__cause__`. Unmatched errors return `"unknown"` and go to the DLQ.

Transient failures go back to the queue with backoff; permanent failures go to the upload DLQ ([../dlq/upload_dlq.py](../dlq/upload_dlq.py)) for manual intervention.

**Concurrency model**: the uploader runs many replicas, and each pod processes up to `HIPPIUS_UPLOADER_MAX_INFLIGHT` upload requests concurrently (bounded-dispatch loop). Total concurrent Arion POSTs **per pod** are capped by a single shared `HIPPIUS_ARION_UPLOAD_CONCURRENCY` semaphore on the `Uploader` instance (the one throttle on the scarce resource). CID assignment is content-deterministic and `insert_chunk_backend` is idempotent (`ON CONFLICT`), so cross-request/cross-pod concurrency is safe. Scale aggregate throughput by raising `MAX_INFLIGHT` + `ARION_UPLOAD_CONCURRENCY` (watch Arion 429/5xx), not just replicas. Transient Arion errors fall back to the existing per-request exponential-backoff retry — ramp concurrency cautiously.

## Unpinner

Delete pin on backend → mark `chunk_backend.deleted = true, deleted_at = now()` (soft delete). Retries with backoff (`HIPPIUS_UNPINNER_MAX_ATTEMPTS=5`, `HIPPIUS_UNPINNER_BACKOFF_BASE_MS=1000`, `_MAX_MS=60000`). Failures go to [../dlq/unpin_dlq.py](../dlq/unpin_dlq.py).

**Concurrency model**: runs many replicas; each pod processes up to `HIPPIUS_UNPINNER_MAX_INFLIGHT` unpin requests concurrently (bounded-dispatch loop mirroring the uploader). One request expands to N chunk identifiers — their backend DELETEs run concurrently bounded by a single shared per-pod `HIPPIUS_UNPINNER_PARALLELISM` semaphore (the one throttle on Arion DELETEs). Atomic dequeue + idempotent DELETE (404 tolerated) + idempotent soft-delete make cross-pod/cross-request concurrency safe. Scale via `MAX_INFLIGHT` + `PARALLELISM`, not just replicas.

## Tracing

All worker operations emit OTel spans with `hippius.ray_id`, `hippius.account.main`, and backend-specific attributes. See [uploader.py](uploader.py) for the standard span shape.

