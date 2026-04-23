# hippius_s3/api/

Internal FastAPI app on port 8000. **Not exposed to the internet** — only the gateway talks to it. Trusts `X-Hippius-*` headers injected by [gateway/services/forward_service.py](../../gateway/services/forward_service.py).

See [../../CLAUDE.md](../../CLAUDE.md) for the full request lifecycle; this file covers the API layer.

## Entry

- [hippius_s3/main.py:237 `factory()`](../main.py) — FastAPI factory.
- [hippius_s3/main.py:85 `lifespan`](../main.py) — async lifespan that creates:
  - Postgres pool via [postgres_create_pool](../main.py)
  - Redis clients (general, accounts, chain, rate-limiting, queues)
  - Queue/chain/cache client singletons ([main.py:118-128](../main.py))
  - KMS client via [init_kms_client](../services/kek_service.py) — fail-fast in `required` mode, no-op in `disabled`
  - `app.state.fs_store` (FS cache) via [create_fs_store](../cache/__init__.py)
  - `app.state.obj_cache` = `RedisObjectPartsCache(redis_client, queues_client=redis_queues_client, fs_store=fs_store)` ([main.py:137-141](../main.py))
  - Background metrics collector
- AES-NI warning: on Linux, reads `/proc/cpuinfo` and warns if AES HW acceleration isn't advertised ([main.py:43-62](../main.py)). Heuristic — don't rely on it for performance decisions, but a missing flag is a red flag.

## Middleware chain

Registered at [main.py:293-299](../main.py). Reverse order means on the request path: `metrics → tracing → parse_internal_headers → ip_whitelist → fs_cache_pressure` (outer → inner).

- **`metrics_middleware`** ([hippius_s3/api/middlewares/metrics.py](middlewares/metrics.py)) — OTel request-level metrics.
- **`tracing_middleware`** ([hippius_s3/api/middlewares/tracing.py](middlewares/tracing.py)) — OTel spans.
- **`parse_internal_headers_middleware`** ([hippius_s3/api/middlewares/parse_internal_headers.py](middlewares/parse_internal_headers.py)) — reads `X-Hippius-*` from the gateway, populates `request.state.account`, `request.state.ray_id`, `request.state.seed_phrase`, etc. The API **assumes these are trustworthy** because it only listens inside the cluster.
- **`ip_whitelist_middleware`** — if `API_IP_WHITELIST` is configured, only allows those IPs. Defence-in-depth for a misconfigured k8s network policy.
- **`fs_cache_pressure_middleware`** — returns **503 + Retry-After BEFORE reading the body** when the cache disk is above the configured usage threshold. Saves RAM/disk on doomed PUTs. See [middlewares/CLAUDE.md](middlewares/CLAUDE.md).
- **`SpeedscopeProfilerMiddleware`** ([hippius_s3/api/middlewares/profiler.py](middlewares/profiler.py)) — optional, only added when `ENABLE_REQUEST_PROFILING=true`.

## Routers

Registered at [main.py:357-359](../main.py):

- `/user` prefix — [hippius_s3/api/user/](user/) — user-management endpoints (unban, etc.).
- Public router (no prefix) — [hippius_s3/api/s3/public_router.py](s3/public_router.py) — public-read GET for buckets that allow it.
- S3 router (no prefix) — [hippius_s3/api/s3/router.py](s3/router.py) — full S3 surface.

Plus: `/robots.txt` ([main.py:317-350](../main.py)) blocks crawlers, `/health` ([main.py:352-355](../main.py)) reports simple status, `/static/...` serves the FastAPI favicon.

## Global exception handler

[main.py:301-315](../main.py) handles two specific exceptions before falling through:

- `DownloadNotReadyError` or literal `"initial_stream_timeout"` → **503 SlowDown** with "Object not ready for download yet. Please retry."
- `UnsupportedStorageVersionError` → **501 NotImplemented** with a message asking to migrate to v5.

Everything else re-raises and uvicorn handles it (usually 500).

## S3 surface

[hippius_s3/api/s3/](s3/) is organized by resource:

- [s3/buckets/](s3/buckets/) — CreateBucket, ListBuckets, HeadBucket, DeleteBucket, PutBucketTagging, PutBucketLifecycle (**acknowledged but not stored**, see [todo.md](../../todo.md)), GetBucketPolicy/PutBucketPolicy, CORS (ignored with a 200), ListObjects/ListObjectsV2.
- [s3/objects/](s3/objects/) — PutObject, GetObject, HeadObject, DeleteObject, CopyObject, DeleteObjects (bulk), tagging. Details in [s3/objects/CLAUDE.md](s3/objects/CLAUDE.md).
- [s3/multipart.py](s3/multipart.py) — InitiateMultipartUpload, UploadPart (streaming), CompleteMultipartUpload, AbortMultipartUpload, ListParts.
- [s3/extensions/append.py](s3/extensions/append.py) — S4 atomic append. Triggered by `x-amz-meta-append: true` on PutObject ([s3/objects/put_object_endpoint.py:68-79](s3/objects/put_object_endpoint.py)).
- [s3/common.py](s3/common.py) — response builders, headers.
- [s3/errors.py](s3/errors.py) — `s3_error_response(code, message, status_code, **xml_attrs)` returns an XML error body in AWS format.
- [s3/copy_helpers.py](s3/copy_helpers.py) — shared helpers for CopyObject fast-path + streaming fallback. The v5 fast-path is at [hippius_s3/services/copy_service_v5.py](../services/copy_service_v5.py).

## SigV4 handling

The API does **not** do SigV4. The gateway verifies signatures and passes the authenticated account in `X-Hippius-Request-User`. The API trusts that header.

Implication: if anything except the gateway could reach the API's port, it could impersonate any account. Network isolation is load-bearing.

## Gotchas

- **`DownloadNotReadyError` is a plain `Exception` subclass** (see [object_reader.py:31-32](../services/object_reader.py)). The global handler ([main.py:303](../main.py)) also matches the literal string `"initial_stream_timeout"` from another code path — retained for backward compat with tests that raised strings.
- **PutObject pre-check race**: see [s3/objects/CLAUDE.md](s3/objects/CLAUDE.md) and the `TODO` at [put_object_endpoint.py:105-108](s3/objects/put_object_endpoint.py).
- **CORS on PutBucket**: returns 200 OK for `?cors` query, logs an "Ignored" line, but doesn't store anything. Added in commit `afc0a94` to avoid `BucketAlreadyExists` errors from AWS SDKs attempting to configure CORS on existing buckets.
- **Lifecycle XML parsed then discarded** — same pattern for `?lifecycle` at [bucket_create_endpoint.py:78](s3/buckets/bucket_create_endpoint.py). See [todo.md](../../todo.md) P2.

## Where things live

| Want to change... | File |
|---|---|
| A specific S3 endpoint | [s3/objects/*.py](s3/objects/) or [s3/buckets/*.py](s3/buckets/) |
| Multipart flow | [s3/multipart.py](s3/multipart.py) |
| S4 append semantics | [s3/extensions/append.py](s3/extensions/append.py) |
| Request-level middleware | [middlewares/](middlewares/) |
| Encryption / chunking | [../writer/CLAUDE.md](../writer/CLAUDE.md) |
| Stream / decrypt | [../reader/CLAUDE.md](../reader/CLAUDE.md) |
| Cache layer | [../cache/CLAUDE.md](../cache/CLAUDE.md) |
