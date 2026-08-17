# gateway/

**This is no longer a standalone service.** The gateway merged into the api (one FastAPI
app, one uvicorn): this package now holds the edge-facing middleware chain (auth, ACL,
validation, audit, purge, CORS), its services (auth orchestrator, ACLService, auth cache,
ATS purge client) and the `?acl` subresource handlers — all composed into the single app
by [hippius_s3/main.py `factory()`](../hippius_s3/main.py).

What disappeared in the merge: `gateway/main.py` (the second app factory),
`ForwardService` (the HTTP relay to `api:8000`), the `X-Hippius-*` trusted-header
contract (replaced by [hippius_s3/api/middlewares/request_context.py](../hippius_s3/api/middlewares/request_context.py)
mapping `request.state` directly), the docs proxy, and the api's `ip_whitelist` /
`parse_internal_headers` middlewares. There is no internal HTTP hop anymore; auth is
structural — enforced by middleware order, pinned by
[tests/unit/gateway/test_middleware_order.py](../tests/unit/gateway/test_middleware_order.py).

## Middleware chain

Registered in [hippius_s3/main.py](../hippius_s3/main.py) (search for `acl_subresource_middleware`). FastAPI's `@app.middleware("http")` stacks in reverse order (last-registered = outermost). On the request path, the chain runs: cors → ray_id → cache_control → ats_purge → cache_invalidation → [read_only] → fs_cache_pressure → input_validation → auth_router → trailing_slash → account → acl → request_context → frontend_hmac → tracing → metrics → [audit_log] → auth_probe → acl_subresource → routers.

| # | Middleware | Purpose | Short-circuit |
|---|-----------|---------|---------------|
| 1 (outermost) | `cors_middleware` | Adds CORS headers to every response (including error paths). | — |
| 2 | `read_only_middleware` | If `HIPPIUS_READ_ONLY_MODE=true`, block writes. | 403 |
| 3 | `input_validation_middleware` | Bucket name / object key / metadata validation. | 400 |
| 4 | `auth_router_middleware` | Dispatches to `auth_orchestrator` (see below), populates `request.state.auth_method`, `.account_id`, `.token_type`, etc. | 403 |
| 5 | `trailing_slash_normalizer` | Harmonizes `/path` vs `/path/`. | — |
| 6 | `account_middleware` | For seed-phrase auth, fetches account info from Arion + Redis cache. | 503 on Arion error |
| 7 | `acl_middleware` | Checks bucket/object permission via `ACLService`. Master tokens bypass. | 403 |
| 8 | `verify_frontend_hmac_middleware` | If `FRONTEND_HMAC_SECRET` is set, verify HMAC on internal frontend requests. | 403 |
| 9 | `tracing_middleware` | OTel span attachment. | — |
| 10 | `metrics_middleware` | Request latency, status codes, account attribution. | — |
| 11 | `audit_log_middleware` | Comprehensive operation audit (when `ENABLE_AUDIT_LOGGING=true`). | — |
| 12 (innermost) | `ray_id_middleware` | Generates or propagates `X-Ray-ID`. Populates `request.state.ray_id`. | — |

**Not wired today**: `rate_limit` and `banhammer`. The modules at [gateway/middlewares/rate_limit.py](middlewares/rate_limit.py) and [gateway/middlewares/banhammer.py](middlewares/banhammer.py) exist but `main.py` doesn't register them — see the log line `"Rate limiting and banhammer disabled"` at [main.py:94](main.py). See [todo.md](../todo.md) P2.

## Routing

Routes now live on the merged app ([hippius_s3/main.py](../hippius_s3/main.py)); this
package contributes no routers. The `?acl` subresource is handled by
[gateway/middlewares/acl_subresource.py](middlewares/acl_subresource.py) — the innermost
middleware: it answers `GET|PUT` with `?acl`, validates canned-ACL headers before writes,
materializes an object ACL after a successful `PutObject` carrying `x-amz-acl`, and passes
everything else through to the S3 routers.

## Authentication at a glance

Orchestrator: [gateway/services/auth_orchestrator.py:39 `authenticate_request`](services/auth_orchestrator.py). Detection order:

1. **Presigned URL** — query params `X-Amz-Algorithm=AWS4-HMAC-SHA256` + `X-Amz-Credential` + `X-Amz-Signature`.
2. **Bearer** — `Authorization: Bearer hip_...`.
3. **Access key SigV4** — `Authorization: AWS4-HMAC-SHA256 Credential=hip_...` in header.
4. **Seed phrase SigV4** — Authorization header present but credential doesn't start `hip_`.
5. **Anonymous** — GET/HEAD on public buckets, no Authorization.

Detail: [gateway/services/CLAUDE.md](services/CLAUDE.md).

## Gotchas

- **Payload hash for presigned URLs** defaults to `UNSIGNED-PAYLOAD` ([gateway/middlewares/sigv4.py](middlewares/sigv4.py)). For streaming uploads with a presigned URL, the SHA256 is effectively of an empty body. This matches AWS S3 behavior but catches people off guard.
- **Master token ACL bypass** at [gateway/middlewares/acl.py:126-130](middlewares/acl.py). Master tokens skip the ACL check entirely when the authenticated account owns the bucket — we trust Arion to have already enforced token scope.
- **Sub-token scope not enforced**. [sub_token_scope.py](services/sub_token_scope.py) implements scope evaluation but isn't wired and its `TokenAcl` import is dead. See [todo.md](../todo.md).
- **Bucket name validation** accepts SS58 addresses as a special case ([gateway/middlewares/input_validation.py](middlewares/input_validation.py)). That lets users create `s3://5Grw...abc/` as a bucket matching their account address.
- **Object key validation** rejects `\ { } ^ % ` [ ] " < > ~ # |` and all non-printable ASCII. AWS discourages these, we enforce strictly.
- **Constant-time HMAC** comparison via `hmac.compare_digest` throughout.

## Tests

Unit tests for gateway-specific logic: [tests/unit/gateway/](../tests/unit/gateway/). ACL scope tests live there despite `sub_token_scope` being dormant ([tests/unit/gateway/test_acl_scope.py](../tests/unit/gateway/test_acl_scope.py)).

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 4, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #86 | 10:04 AM | 🔵 | Gateway Application Architecture with Middleware Pipeline | ~537 |

### Feb 13, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1669 | 10:14 AM | 🔵 | Comprehensive Authentication Architecture Analysis - No STS/OAuth Implementation Exists | ~806 |
| #1659 | 10:11 AM | 🔵 | Hippius S3 Gateway Middleware Stack and Service Architecture | ~688 |

### Feb 20, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #2763 | 1:46 AM | 🔵 | Complete gateway configuration reveals database pooling and Loki logging settings | ~497 |
| #2748 | 1:39 AM | 🔵 | Gateway banhammer configuration shows 200 infraction limit for authenticated clients | ~510 |

### Apr 20, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6615 | 6:35 PM | 🔵 | Gateway Application Initializes ACLService with Database and Redis | ~451 |

### May 12, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #7721 | 11:50 AM | 🔵 | Complete Sentry integration pattern documented for hippius-s3 | ~629 |
| #7720 | 11:49 AM | 🔵 | Sentry configuration pattern in hippius-s3 repository | ~506 |

### Jun 25, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #9377 | 9:07 PM | 🔵 | Application Pool Configuration Exceeds Database max_connections by 96 Connections | ~723 |
</claude-mem-context>
