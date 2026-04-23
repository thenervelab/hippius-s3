# gateway/services/

Business-logic services used by the gateway middleware. No HTTP framing here — these are called from the middleware chain.

## [auth_orchestrator.py](auth_orchestrator.py) — `authenticate_request`

Single entry point that picks the right auth method based on request shape. Priority order ([auth_orchestrator.py:39](auth_orchestrator.py)):

1. `_authenticate_presigned_url` ([line 100](auth_orchestrator.py)) — for `X-Amz-Algorithm=AWS4-HMAC-SHA256` query auth.
2. `_authenticate_bearer` ([line 169](auth_orchestrator.py)) — for `Authorization: Bearer hip_...`.
3. `_authenticate_access_key_header` ([line 236](auth_orchestrator.py)) — for `Authorization: AWS4-HMAC-SHA256 Credential=hip_...`.
4. `_authenticate_seed_phrase` ([line 292](auth_orchestrator.py)) — Authorization header present but credential doesn't start `hip_`.
5. Anonymous ([line 67](auth_orchestrator.py)) — GET/HEAD without Authorization.

Returns an `AuthResult(is_valid, auth_method, access_key, account_address, token_type, seed_phrase, ...)`. The middleware adapter attaches these to `request.state`.

**Line 75**: mutations without any Authorization header get a 403 short-circuit before attempting auth — only GET/HEAD get the anonymous fallthrough.

## [auth_service.py](auth_service.py) — `decrypt_secret`

ChaCha20-Poly1305 decryption of the `encrypted_secret` field returned by Arion's token auth endpoint. The KEK is `HIPPIUS_AUTH_ENCRYPTION_KEY` (64-hex). Used by all access-key SigV4 verification paths.

## [auth_cache.py](auth_cache.py) — `cached_auth`

Redis-cached wrapper around Arion `/objectstore/tokens/auth/`. The cache key is the credential itself; TTL is configured on the cache service side (not visible in this module). On miss, makes a single HTTP call and writes through.

**Failure mode**: if Arion is down, this bubbles up as 503 to the client. There's no stale-while-revalidate today.

## [acl_service.py](acl_service.py) — `ACLService`

Per-request permission evaluator. Called from `acl_middleware`. Reads ACL rows from the main DB + ACL cache (`redis-acl`). Evaluates grant matching against the request's `(account, bucket, key, permission)` tuple.

Bucket-level ACLs and object-level ACLs both supported. Public buckets are modeled as explicit ACL grants (`AllUsers` grantee) rather than a boolean flag — migration script at [hippius_s3/scripts/migrate_public_buckets_to_acl.py](../../hippius_s3/scripts/migrate_public_buckets_to_acl.py) converted legacy `is_public` bools.

## [account_service.py](account_service.py) — `fetch_account`

For seed-phrase auth: given a seed, derive the SS58 subaccount ID via substrateinterface (called in an executor thread to avoid blocking — [account_service.py:29](account_service.py)), then fetch cached account data from `redis-accounts`. If missing, call the Substrate node.

Populates `request.state.account` with main_account, has_credits, upload/delete flags. These propagate to the internal API as `X-Hippius-Main-Account` / `X-Hippius-Has-Credits` / `X-Hippius-Can-Upload` / `X-Hippius-Can-Delete`.

## [forward_service.py](forward_service.py) — `ForwardService`

The proxy. See [../CLAUDE.md](../CLAUDE.md) for the streaming model. Key invariants:

- Single shared `httpx.AsyncClient`, 100 max connections, 20 keepalive, 300s overall timeout, 10s connect timeout ([forward_service.py:60-64](forward_service.py)).
- `_filter_hop_by_hop_raw_headers` ([forward_service.py:28-54](forward_service.py)) handles RFC 7230 hop-by-hop headers plus any named in the `Connection` header, and dedupes `date`/`server` (prevents `server: uvicorn, uvicorn`).
- **Request body is streamed** via `request.stream()` ([forward_service.py:113-126](forward_service.py)). Never buffers — supports arbitrary-sized PUTs.
- **Response body is streamed** via `iter_upstream()` ([forward_service.py:132-165](forward_service.py)). Records `bytes_sent` vs upstream `content-length` and warns on mismatch ([forward_service.py:148-157](forward_service.py)).
- `raw_headers` on the response preserves multi-value headers (Set-Cookie, etc).

**Not here**: retry logic. If the upstream API returns 5xx, gateway forwards it. If the upstream closes the connection mid-response, client sees truncation. See [todo.md](../../todo.md) P0 (postmortem) for the proposed 503+Retry-After work.

## [sub_token_scope.py](sub_token_scope.py) — DORMANT

Pure functions for evaluating a sub-token's ACL against a request: `required_s3_action`, `sub_token_allows`, `prefix_matches`, `bucket_in_scope`, `ip_allowed`, `evaluate_sub_token`. Good tests at [tests/unit/gateway/test_acl_scope.py](../../tests/unit/gateway/test_acl_scope.py).

**Broken**: [sub_token_scope.py:3](sub_token_scope.py) imports `TokenAcl` from `hippius_s3.services.hippius_api_service` where no such symbol exists. The import would fail at runtime — but since no production code references this module, the failure never surfaces.

**Status**: not wired. Master-vs-sub token distinction today is binary: master bypasses ACL entirely ([acl.py:126-130](../middlewares/acl.py)), sub goes through normal ACL check. The fine-grained scope evaluation here is not enforced. See [todo.md](../../todo.md) P2.

## [docs_proxy_service.py](docs_proxy_service.py) — `DocsProxyService`

Proxies `/docs` to the internal Swagger UI when `ENABLE_API_DOCS=true`. Redis-cached for short TTL.

## Tests

- [tests/unit/gateway/](../../tests/unit/gateway/) — everything gateway-specific.
- [tests/unit/gateway/test_acl_scope.py](../../tests/unit/gateway/test_acl_scope.py) — exercises `sub_token_scope.py` with mocked `TokenAcl`. Useful even though the production code is dormant.

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 4, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #71 | 10:03 AM | 🔵 | Multi-Method Authentication Orchestrator Implementation | ~521 |

### Feb 13, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1671 | 10:17 AM | 🔵 | Forward Service Proxies Requests with Authenticated Context Headers | ~713 |
| #1669 | 10:14 AM | 🔵 | Comprehensive Authentication Architecture Analysis - No STS/OAuth Implementation Exists | ~806 |
| #1666 | 10:13 AM | 🔵 | ACL Service Permission Resolution and Grant Matching Logic | ~709 |
| #1661 | 10:11 AM | 🔵 | Account Service Implementation for Seed Phrase and Access Key Authentication | ~694 |
| #1657 | 10:10 AM | 🔵 | Hippius S3 Gateway Authentication Architecture | ~562 |

### Apr 20, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6606 | 6:33 PM | 🔵 | Authentication Flow Retrieves token_type from Hippius API | ~473 |

### Apr 21, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6659 | 10:20 AM | 🔴 | Added token_acl Field to AuthResult Dataclass | ~385 |
</claude-mem-context>
