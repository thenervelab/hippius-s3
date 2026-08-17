# gateway/

Public-facing FastAPI service on port 8080. **This is the only hippius-s3 component exposed to the internet.** Its job is to authenticate, authorize, rate-limit (when enabled), audit, and **forward** to the internal API.

See [../CLAUDE.md](../CLAUDE.md) for the full architectural map. This file covers gateway internals.

## Entry

- [gateway/main.py:43 `factory()`](main.py) — FastAPI factory. Launched by uvicorn with `factory=True`.
- Startup ([main.py:52](main.py)) creates the Postgres pool, four Redis clients (general, accounts, rate-limiting, ACL), the `ForwardService` / `ACLService` / `DocsProxyService` / `ArionClient`, and a background task that exports Postgres pool metrics every 60s.
- Shutdown ([main.py:131](main.py)) closes all of them in reverse order.

There is **no** business logic here — the gateway never touches chunk data. It proxies.

## Middleware chain

Registered at the bottom of [main.py](main.py). FastAPI's `@app.middleware("http")` stacks in reverse order (last-registered = outermost). On the request path, this runs top-to-bottom:

| # | Middleware | Purpose | Short-circuit |
|---|-----------|---------|---------------|
| 1 (outermost) | `cors_middleware` | Adds CORS headers to every response (including error paths). | — |
| 2 | `ray_id_middleware` | Generates or propagates ray id + stamps `gateway_start_time`; runs early so every inner middleware logs a real ray_id. | — |
| 3 | `cache_control_middleware` | Cache-Control stamping for anonymous-readable objects (ATS/browser caching). | — |
| 4 | `ats_purge_middleware` | Fans PURGE requests out to ATS cache endpoints. | — |
| 5 | `cache_invalidation_middleware` | Invalidates ATS cache on writes. | — |
| 6 | `read_only_middleware` | Only when `HIPPIUS_READ_ONLY_MODE=true`: block all writes. | 405 |
| 7 | `input_validation_middleware` | Bucket name / object key / metadata validation; rejects CreateBucket on reserved names. | 400 |
| 8 | `auth_router_middleware` | Dispatches to `auth_orchestrator`, populates `request.state.auth_method`, `.account_address`, `.account_id`, `.token_type`. | 403 |
| 9 | `suspension_middleware` | Account-level suspend/read_only gate (issue #421), keyed on `account_address`. Must stay inner to auth_router and outer to account/acl (master tokens bypass ACL). | 403 |
| 10 | `trailing_slash_normalizer` | Harmonizes `/path` vs `/path/`. | — |
| 11 | `account_middleware` | Credit / can_upload checks for mutating requests (redis-accounts + Arion). | 402/503 |
| 12 | `acl_middleware` | Bucket/object permission via `ACLService`; also blocks access to a suspended owner's buckets. Master tokens bypass the permission check. | 403 |
| 13 | `verify_frontend_hmac_middleware` | HMAC gate for `/user/*` (`FRONTEND_HMAC_SECRET`). | 401/403 |
| 14 | `verify_admin_hmac_middleware` | HMAC gate for `/admin/*` (`HIPPIUS_ADMIN_HMAC_SECRET`, fail-closed when unset). | 401/403 |
| 15 | `tracing_middleware` | OTel span attachment. | — |
| 16 | `metrics_middleware` | Request latency, status codes, account attribution. | — |
| 17 | `audit_log_middleware` | Operation audit (when `ENABLE_AUDIT_LOGGING=true`). | — |
| 18 (innermost) | `auth_probe_middleware` | ATS auth-probe short-circuit; MUST stay innermost (see warning in main.py). | 200 |

**Not wired today**: `rate_limit` and `banhammer`. The modules at [gateway/middlewares/rate_limit.py](middlewares/rate_limit.py) and [gateway/middlewares/banhammer.py](middlewares/banhammer.py) exist but `main.py` doesn't register them — see the log line `"Rate limiting and banhammer disabled"` at [main.py:94](main.py). See [todo.md](../todo.md) P2.

## Routing

[main.py:171-179](main.py):

- `GET /docs`, `GET /redoc`, `GET /openapi.json`, `DELETE /docs/cache` — via [gateway/routers/docs.py](routers/docs.py).
- **`gateway/routers/acl.py` has NO `/acl` prefix.** `router = APIRouter()` is included bare ([main.py:190](main.py)), so it registers `GET|PUT /{bucket}` and `GET|PUT /{bucket}/{key:path}` **at the root** — every S3 request matches one of these first. Each handler takes `acl: str | None = Query(default=None)` and calls `forward_service.forward_request(request)` when `?acl` is absent, so normal traffic is forwarded from inside the ACL handler rather than from the catch-all. There is no `/acl` path in the gateway, and no `/static` mount either.
- `/{path:path}` — catch-all for methods the ACL router doesn't declare (POST, DELETE, HEAD, PATCH), forwarding through `ForwardService.forward_request` ([gateway/services/forward_service.py:67](services/forward_service.py)).
- `GET /health` — simple 200 `{"status": "healthy", "service": "gateway"}`. Does **not** check downstream deps (noted as a P1 improvement in [ha.md](../ha.md)).

## Forwarding model (important)

`ForwardService` uses a single shared `httpx.AsyncClient` with `Timeout(300, connect=10)`, `max_connections=100`, `max_keepalive_connections=20` ([forward_service.py:60-64](services/forward_service.py)). Requests are:

1. Client headers cleaned up: any `X-Hippius-*` stripped to prevent header injection ([forward_service.py:71-74](services/forward_service.py)).
2. Trusted headers injected from `request.state`:
   - `X-Hippius-Ray-ID`, `X-Hippius-Request-User`, `X-Hippius-Bucket-Owner`, `X-Hippius-Main-Account`, `X-Hippius-Seed` (if seed auth), `X-Hippius-Has-Credits`, `X-Hippius-Can-Upload`, `X-Hippius-Can-Delete`, `X-Hippius-Gateway-Time-Ms`.
3. Hop-by-hop headers (`host`, `x-forwarded-for`, `x-forwarded-host`) removed.
4. Body is **streamed** (`request.stream()`), not buffered — never load a full PUT into gateway RAM.
5. Response is also streamed back via `StreamingResponse`, with hop-by-hop headers filtered ([forward_service.py:28-54](services/forward_service.py)).
6. If the upstream closes early and `bytes_sent < content-length`, log a warning ([forward_service.py:148-157](services/forward_service.py)).

Implication: **there is no duplicate request body read**, but there's a full TCP hop per request. See [todo.md](../todo.md) P2 for the "merge gateway + API" discussion.

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
