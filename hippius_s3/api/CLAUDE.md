# hippius_s3/api/

Internal FastAPI app on port 8000. **Not exposed to the internet** — only the gateway talks to it. Trusts `X-Hippius-*` headers injected by [gateway/services/forward_service.py](../../gateway/services/forward_service.py).

See [../../CLAUDE.md](../../CLAUDE.md) for the full request lifecycle; this file covers the API layer.

## Entry

- [hippius_s3/main.py:248 `factory()`](../main.py) — FastAPI factory.
- [hippius_s3/main.py:87 `lifespan`](../main.py) — async lifespan that creates:
  - Postgres pool via [postgres_create_pool](../main.py)
  - Redis clients (general, accounts, rate-limiting, queues) ([main.py:104-113](../main.py))
  - Queue/cache client singletons ([main.py:119-122](../main.py))
  - KMS client via [init_kms_client](../services/kek_service.py) — fail-fast in `required` mode, no-op in `disabled`
  - `app.state.fs_store` (FS cache) via [create_fs_store](../cache/__init__.py)
  - `app.state.obj_cache` = `RedisObjectPartsCache(redis_client, queues_client=redis_queues_client, fs_store=fs_store)` ([main.py:137-141](../main.py))
  - Background metrics collector
- AES-NI warning: on Linux, reads `/proc/cpuinfo` and warns if AES HW acceleration isn't advertised ([main.py:43-62](../main.py)). Heuristic — don't rely on it for performance decisions, but a missing flag is a red flag.

## Middleware chain

Registered in [main.py](../main.py) together with the gateway middlewares (single merged app). Full request-path order: `cors → ray_id → cache_control → ats_purge → cache_invalidation → [read_only] → fs_cache_pressure → input_validation → auth_router → trailing_slash → account → acl → request_context → frontend_hmac → tracing → metrics → [audit_log] → auth_probe → acl_subresource → routers`.

- **`metrics_middleware`** ([hippius_s3/api/middlewares/metrics.py](middlewares/metrics.py)) — OTel request-level metrics.
- **`tracing_middleware`** ([hippius_s3/api/middlewares/tracing.py](middlewares/tracing.py)) — OTel spans.
- **`request_context_middleware`** ([middlewares/request_context.py](middlewares/request_context.py)) — derives `request.state.account` (main_account = bucket owner) and caller ids from what the auth/account/acl middlewares resolved. Replaced the header-parsing `parse_internal_headers` after the gateway merge.
- **`fs_cache_pressure_middleware`** — returns **503 + Retry-After BEFORE reading the body** when the cache disk is above the configured usage threshold. Saves RAM/disk on doomed PUTs. See [middlewares/CLAUDE.md](middlewares/CLAUDE.md).
- **`SpeedscopeProfilerMiddleware`** ([hippius_s3/api/middlewares/profiler.py](middlewares/profiler.py)) — optional, only added when `ENABLE_REQUEST_PROFILING=true`.

## Routers

Registered at [main.py:362-365](../main.py):

- `/user` prefix — [hippius_s3/api/user/](user/) — user-management endpoints (unban, etc.).
- `/user/sub-tokens` prefix — sub-token scopes router.
- Public router (no prefix) — [hippius_s3/api/s3/public_router.py](s3/public_router.py) — public-read GET for buckets that allow it.
- S3 router (no prefix) — [hippius_s3/api/s3/router.py](s3/router.py) — full S3 surface.

Plus: `/robots.txt` ([main.py:322](../main.py)) blocks crawlers, `/health` ([main.py:357](../main.py)) reports simple status, `/static/...` serves the FastAPI favicon.

## Global exception handler

[main.py:312-320](../main.py) delegates to `s3_errors.map_read_path_exception(exc)` — a testable pure function in [errors.py](s3/errors.py) that maps the read path (not-ready → 503 SlowDown, pool saturation → 503, key/crypto → 503/500, unsupported storage/suite → 501 NotImplemented, terminal chunk miss → 503 SlowDown). A recognized failure returns a well-formed S3 error; anything else re-raises to uvicorn's 500.

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
- **PutObject object identity is DB-atomic**: the old pre-check `SELECT` was removed (WU-3); the endpoint always passes a fresh `candidate_object_id` and trusts the DB-returned id ([put_object_endpoint.py:137-142](s3/objects/put_object_endpoint.py)). See [s3/objects/CLAUDE.md](s3/objects/CLAUDE.md).
- **CORS on PutBucket**: returns 200 OK for `?cors` query, logs an "Ignored" line, but doesn't store anything. Added in commit `afc0a94` to avoid `BucketAlreadyExists` errors from AWS SDKs attempting to configure CORS on existing buckets.
- **Lifecycle XML parsed then discarded** — same pattern for `?lifecycle` at [bucket_create_endpoint.py:78](s3/buckets/bucket_create_endpoint.py). See [todo.md](../../todo.md) P2.
- **Client XML goes through `parse_untrusted_xml`** ([xml_helpers.py](../xml_helpers.py)), never a bare `ET.fromstring` — a default parser loads DTDs and expands entities. Responses are built with `create_element`/`add_subelement`/`to_xml_bytes` so values are escaped: a key containing `&` is legal and an f-string template produces a document no client can parse. Match on `local-name()` when reading, since SDKs disagree about namespacing the body.

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


<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Apr 23, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6965 | 2:08 PM | 🔵 | Code reuse analysis identifies test fixture duplication and typing improvements | ~864 |
</claude-mem-context>
