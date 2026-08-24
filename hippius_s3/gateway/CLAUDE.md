# gateway/

**This is no longer a standalone service.** The gateway merged into the api (one FastAPI
app, one uvicorn): this package now holds the edge-facing middleware chain (auth, ACL,
validation, audit, purge, CORS), its services (auth orchestrator, ACLService, auth cache,
ATS purge client) — all composed into the single app
by [hippius_s3/main.py `factory()`](../main.py).

What disappeared in the merge: `gateway/main.py` (the second app factory),
`ForwardService` (the HTTP relay to `api:8000`), the `X-Hippius-*` trusted-header
contract (replaced by [hippius_s3/api/middlewares/request_context.py](../api/middlewares/request_context.py)
mapping `request.state` directly), the docs proxy, and the api's `ip_whitelist` /
`parse_internal_headers` middlewares. There is no internal HTTP hop anymore; auth is
structural — enforced by middleware order, pinned by
[tests/unit/gateway/test_middleware_order.py](../../tests/unit/gateway/test_middleware_order.py).

## Middleware chain

Registered in [hippius_s3/main.py](../main.py). FastAPI's `@app.middleware("http")` stacks in reverse order (last-registered = outermost). On the request path, the chain runs: cors → ray_id → cache_control → ats_purge → cache_invalidation → [read_only] → fs_cache_pressure → input_validation → auth_router → suspension → trailing_slash → account → acl → request_context → frontend_hmac → admin_hmac → tracing → metrics → [audit_log] → auth_probe → routers.

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

**Not wired today**: `rate_limit` and `banhammer`. The modules at [gateway/middlewares/rate_limit.py](middlewares/rate_limit.py) and [gateway/middlewares/banhammer.py](middlewares/banhammer.py) exist but `main.py` doesn't register them — see the log line `"Rate limiting and banhammer disabled"` at [main.py:94](main.py). See [todo.md](../../todo.md) P2.

## Routing

Routes now live on the merged app ([hippius_s3/main.py](../main.py)); this
package contributes no routers. The `?acl` subresource is handled by
[hippius_s3/api/s3/acl_endpoints.py](../api/s3/acl_endpoints.py), dispatched from
the bucket/object routers' query-param branches exactly like `tagging` — the acl MIDDLEWARE
still maps `?acl` to READ_ACP/WRITE_ACP and enforces permission before those branches run.

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
- **Sub-token scope not enforced**. [sub_token_scope.py](services/sub_token_scope.py) implements scope evaluation but isn't wired and its `TokenAcl` import is dead. See [todo.md](../../todo.md).
- **Bucket name validation** accepts SS58 addresses as a special case ([gateway/middlewares/input_validation.py](middlewares/input_validation.py)). That lets users create `s3://5Grw...abc/` as a bucket matching their account address.
- **Object key validation** rejects `\ { } ^ % ` [ ] " < > ~ # |` and all non-printable ASCII. AWS discourages these, we enforce strictly.
- **Constant-time HMAC** comparison via `hmac.compare_digest` throughout.

## Tests

Unit tests for gateway-specific logic: [tests/unit/gateway/](../../tests/unit/gateway/). ACL scope tests live there despite `sub_token_scope` being dormant ([tests/unit/gateway/test_acl_scope.py](../../tests/unit/gateway/test_acl_scope.py)).

