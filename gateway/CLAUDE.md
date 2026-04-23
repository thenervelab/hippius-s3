# gateway/

Public-facing FastAPI service on port 8080. **This is the only hippius-s3 component exposed to the internet.** Its job is to authenticate, authorize, rate-limit (when enabled), audit, and **forward** to the internal API.

See [../CLAUDE.md](../CLAUDE.md) for the full architectural map. This file covers gateway internals.

## Entry

- [gateway/main.py:35 `factory()`](main.py) — FastAPI factory. Launched by uvicorn with `factory=True`.
- Startup ([main.py:52](main.py)) creates the Postgres pool, five Redis clients (general, accounts, chain, rate-limiting, ACL), the `ForwardService` / `ACLService` / `DocsProxyService` / `ArionClient`, and a background task that exports Postgres pool metrics every 60s.
- Shutdown ([main.py:131](main.py)) closes all of them in reverse order.

There is **no** business logic here — the gateway never touches chunk data. It proxies.

## Middleware chain

Registered at [main.py:181-197](main.py). FastAPI's `@app.middleware("http")` stacks in reverse order (last-registered = outermost). On the request path, this runs top-to-bottom:

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

[main.py:171-179](main.py):

- `GET /docs` — proxied to the docs service via [gateway/routers/docs.py](routers/docs.py).
- `/acl/...` — ACL management endpoints via [gateway/routers/acl.py](routers/acl.py).
- `/{path:path}` — **catch-all** forwards everything else to the internal API through `ForwardService.forward_request` ([gateway/services/forward_service.py:67](services/forward_service.py)).
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
</claude-mem-context>
