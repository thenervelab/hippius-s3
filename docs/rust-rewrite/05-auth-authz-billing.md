# 05 — Auth / Authorization / ACL / Billing-Gate

> **Framing:** §0–§6 trace the **current Python** request gate (accurate, code-anchored) as the
> behavioral reference the rewrite must match; §7 is the harvest/build guidance. Rewrite deltas to
> keep in mind: **FOUR** auth methods (seed-phrase removed), **no general IAM engine** (bucket policy
> v1 = public-read subset only, §2.5), sub-tokens **enforced fail-closed**, and the **billing gate is
> inherited from HCFS** (per-tenant 402 + usage→chain) rather than rebuilt (§7b). Authorities:
> `decisions-register.md`, `IMPLEMENTATION-PLAN.md`.

Implementation-grade description of the **request gate** every S3 call passes through in
`hippius-s3`, as the behavioral contract for the rewrite. This is the subsystem that decides *who*
is calling, *whether they may*, and *whether the write is allowed to store bytes*.

Everything here is traced to the current Python source (not to READMEs or CLAUDE.md, several of
which are stale — see [§8 Open questions](#8-open-questions)). File/line refs are to
`hippius_s3/…` unless noted. Security-sensitive behaviours are flagged **⚠ SECURITY**.

---

## 0. The request gate at a glance

A single merged FastAPI app (`hippius_s3/main.py factory()`) composes the whole chain; there is
no separate gateway process anymore (`gateway/CLAUDE.md`). Auth is **structural** — enforced by
middleware ordering, pinned by `tests/unit/gateway/test_middleware_order.py`.

```
                          REQUEST  (outermost → innermost)
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  cors ─ ray_id ─ path_normalization ─ cache_control ─ ats_purge ─              │
  │  cache_invalidation ─ [read_only] ─ fs_cache_pressure ─ input_validation ─     │
  │  auth_router ─ suspension ─ trailing_slash ─ account ─ acl ─                    │
  │  frontend_hmac ─ admin_hmac ─ [audit_log] ─ request_context ─                  │
  │  tracing ─ metrics ─ auth_probe ─► ROUTERS (S3 handlers)                        │
  └──────────────────────────────────────────────────────────────────────────────┘
       ▲ identity resolved here (auth_router)   ▲ authZ here (acl)   ▲ billing here (account)
```

**Layers that matter to this subsystem and what each may short-circuit:**

| Order | Middleware | File | Short-circuits |
|------:|-----------|------|----------------|
| 7 | `input_validation` | `gateway/middlewares/input_validation.py` | 400 (bad bucket/key/`%`/`?`/`#`/reserved name) |
| 8 | `auth_router` | `gateway/middlewares/auth_router.py` | 403 (bad/absent credential) |
| 9 | `suspension` | `gateway/middlewares/suspension.py` | 403 (account suspended) |
| 11 | `account` | `gateway/middlewares/account.py` | 402 (no credit / over quota) / 503 (billing backend) |
| 12 | `acl` | `gateway/middlewares/acl.py` | 403 (AccessDenied) |
| 13 | `frontend_hmac` | `gateway/middlewares/frontend_hmac.py` | 401/403 (`/user/*`) |
| 14 | `admin_hmac` | `gateway/middlewares/admin_hmac.py` | 401/403 (`/admin/*`) |
| 18 | `auth_probe` | `gateway/middlewares/auth_probe.py` | 200 (ATS probe; MUST stay innermost) |

> **⚠ Ordering is load-bearing** (`main.py:511-534`):
> - `account` runs **before** `acl` — so `acl` has not yet resolved `bucket_owner_id` when the
>   plan gate runs. The plan gate re-resolves the owner itself (see [§5](#5-billing--plan-gate)).
> - `suspension` sits **outer** to `acl` because master tokens bypass `acl`, so a suspension
>   check inside `acl` would never see them.
> - `auth_probe` MUST stay innermost — it answers `200` to a valid probe and would otherwise be
>   reachable by unauthenticated callers.

The exact registration (Starlette stacks last-registered = outermost), `main.py:535-570`:

```python
app.middleware("http")(auth_probe_middleware)      # innermost
app.middleware("http")(metrics_middleware)
app.middleware("http")(tracing_middleware)
app.middleware("http")(request_context_middleware)
if config.enable_audit_logging:
    app.middleware("http")(audit_log_middleware)
app.middleware("http")(verify_admin_hmac_middleware)
app.middleware("http")(verify_frontend_hmac_middleware)
app.middleware("http")(acl_middleware)
app.middleware("http")(account_middleware)
app.middleware("http")(trailing_slash_normalizer)
app.middleware("http")(suspension_middleware)
app.middleware("http")(auth_router_middleware)
app.middleware("http")(input_validation_middleware)
app.middleware("http")(fs_cache_pressure_middleware)
if config.read_only_mode:
    app.middleware("http")(read_only_middleware)
app.middleware("http")(cache_invalidation_middleware)
app.middleware("http")(ats_purge_middleware)
app.middleware("http")(cache_control_middleware)
app.middleware("http")(path_normalization_middleware)
app.middleware("http")(ray_id_middleware)
app.middleware("http")(cors_middleware)             # outermost
```

> **Not wired today**: `rate_limit` and `banhammer` modules exist but are never registered — the
> Python gate has **no rate limiting** on the request path (contrast the Rust branch, which does;
> see [§7](#7-rust-implementation-notes)).

---

## 1. The authentication methods (FOUR live; seed-phrase removed)

`auth_router_middleware` (`auth_router.py:50`) is the entry point; it delegates the whole decision
to `authenticate_request()` in `gateway/services/auth_orchestrator.py:37`. Detection order and
the identity each resolves to are below.

### Detection / routing (`auth_orchestrator.authenticate_request`)

```
1. Presigned URL   — query has X-Amz-Algorithm=AWS4-HMAC-SHA256 + X-Amz-Credential + X-Amz-Signature
2. Authorization header:
     "Bearer …"     → bearer token
     "AWS4-…"       → extract credential:
                         hip_*      → access-key SigV4
                         non-hip_*  → seed-phrase shape → REJECT (deprecation pointer)
3. No Authorization header:
     GET/HEAD and path != "/"  → anonymous
     otherwise                 → 403 InvalidAccessKeyId
```

`AuthResult` (auth_orchestrator.py:26) carries `auth_method ∈ {"access_key","bearer_access_key",
"anonymous"}`, `access_key`, `account_address`, `account_id`, `token_type`. On success
`auth_router` stamps `request.state.auth_method / .access_key / .account_address / .account_id /
.token_type` (auth_router.py:82-99).

Two helpers exported from `sigv4.py` gate credential shape:
`ACCESS_KEY_PATTERN = ^hip_[a-zA-Z0-9_-]{1,240}$` and `SS58_PATTERN = ^[1-9A-HJ-NP-Za-km-z]{47,48}$`
(`models/sub_token.py:18-19`).

### Common back-end: token lookup + secret decryption

All key-based methods resolve the secret the same way:

1. **`cached_auth(access_key, redis, api_client)`** (`gateway/services/auth_cache.py:17`) — Redis
   cache `hippius_auth:{key}`, TTL **60 s**. Miss → `HippiusApiClient.auth(key)` which POSTs
   `{"accessKeyId": key}` to **`POST /objectstore/tokens/auth/`** on `api.hippius.com`
   (`services/hippius_api_service.py:403-430`) and caches the `TokenAuthResponse`.
   `TokenAuthResponse` fields (hippius_api_service.py:70-80):
   `valid: bool`, `status`, `account_address`, `token_type`, `encrypted_secret`, `nonce`.
2. **Validity gates** (access_key_auth.py:85-103): `valid == True`, `status == "active"`,
   `account_address` present and matches `SS58_PATTERN`, `token_type ∈ {"master","sub"}`
   (`ALLOWED_TOKEN_TYPES`), `encrypted_secret` and `nonce` present.
3. **Secret decryption** (`gateway/services/auth_service.py:14 decrypt_secret`): NaCl
   `SecretBox` over base64(`encrypted_secret`) with a 32-byte hex key
   `HIPPIUS_AUTH_ENCRYPTION_KEY` (config `hippius_secret_decryption_material`). The `nonce`
   param is accepted for API compatibility but unused (the nonce is embedded in the SecretBox
   message).

> **⚠ SECURITY — identity is upstream-asserted.** Unlike a classic S3 server, hippius does **not**
> store the secret. The account SS58 (`account_address`) and token type come from the Hippius
> API's `auth` response, cached for 60 s. Revocation lags by up to 60 s. A `master` token is
> AUDIT-logged on every use (access_key_auth.py:154-155, 310-314).

---

### 1a. Access-key SigV4 (header) — `auth_method = "access_key"`

**Route**: `Authorization` header whose extracted `Credential=` starts `hip_`
(auth_orchestrator.py:92). Verified by `verify_access_key_signature`
(`gateway/middlewares/access_key_auth.py:58`).

**Exact algorithm:**

1. `ACCESS_KEY_PATTERN` check on the key.
2. Require `authorization` **and** `x-amz-date` headers (else `AccessKeyAuthError`).
3. Parse header (`sigv4.py`): `Signature=([a-f0-9]+)`, `SignedHeaders=([^,]+)` split on `;`,
   `Credential=hip_…/{date}/{region}/{service}/aws4_request`.
4. `cached_auth` + validity gates + decrypt secret (above).
5. **Canonical request** (`sigv4.create_canonical_request`, sigv4.py:80):
   ```
   {METHOD}\n{CANONICAL_PATH}\n{CANONICAL_QUERY}\n{CANONICAL_HEADERS}\n\n{SIGNED_HEADERS}\n{PAYLOAD_HASH}
   ```
   - `CANONICAL_PATH` = `canonical_path_from_scope` = **raw** `scope["raw_path"]` decoded ASCII
     (sigv4.py:57) — must match the client's percent-encoding exactly; raises if `raw_path` absent.
   - `CANONICAL_HEADERS`: signed header names lowercased+sorted; value whitespace-collapsed
     (`" ".join(v.strip().split())`). For `host`, prefers `x-forwarded-host` → `x-original-host`
     → `host` (sigv4.py:107-115).
   - `CANONICAL_QUERY` = `canonicalize_query_string`: `parse_qsl(keep_blank_values=True)`, sort by
     key, re-encode `urlencode(quote_via=quote, safe="-_.~")`.
   - `PAYLOAD_HASH` = `x-amz-content-sha256` header; **missing header raises `AuthParsingError`**
     for a signed (non-presigned) request → surfaced as `400 MissingSecurityHeader`
     (auth_orchestrator.py:264-277). `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` → empty-body SHA256
     `e3b0c442…b7852b855`.
6. **String-to-sign** (access_key_auth.py:134-136):
   ```
   AWS4-HMAC-SHA256\n{x-amz-date}\n{date}/{region}/{service}/aws4_request\n{hex(sha256(canonical_request))}
   ```
7. **Signing key** (`sigv4.calculate_signature`, sigv4.py:160):
   `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")`, then
   `HMAC(k_signing, string_to_sign)` hex.
8. `hmac.compare_digest(calculated, provided)` — constant-time. Mismatch →
   `403 SignatureDoesNotMatch`.

**Identity**: `TokenAuth(access_key, account_address, token_type)` → `account_id = account_address`
(the SS58).

### 1b. Presigned URL — `auth_method = "access_key"`

**Route**: query has all three `X-Amz-Algorithm=AWS4-HMAC-SHA256` + `X-Amz-Credential` +
`X-Amz-Signature` (auth_orchestrator.py:54-59). Credential's first `/`-segment must start `hip_`
(else `403 InvalidAccessKeyId`). Verified by `verify_access_key_presigned_url`
(access_key_auth.py:163).

**Differences from 1a:**

- Params read from query: `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
  `X-Amz-SignedHeaders`, `X-Amz-Signature`.
- `credential_id` (segment 0) must equal the header access key; `date_scope` (segment 1) must
  equal `X-Amz-Date[:8]`.
- **Expiry**: `X-Amz-Expires ∈ [1, 604800]` (7 days); `signed_at = strptime(amz_date,
  "%Y%m%dT%H%M%SZ")`; reject if `now > signed_at + expires` (`403 SignatureDoesNotMatch`, logged
  "Presigned URL expired").
- `X-Amz-SignedHeaders` **must include `host`** (else invalid).
- **Canonical query** = `canonicalize_presigned_query_string` — same as header but **excludes
  `X-Amz-Signature`** and sorts by `(name, value)` (sigv4.py:208).
- **Payload hash**: no `x-amz-content-sha256` header → defaults to `UNSIGNED-PAYLOAD`
  (sigv4.py:139-142). **⚠ For a streaming presigned upload the body is effectively unauthenticated
  (hash of empty body).** Matches AWS but a footgun.
- Same string-to-sign, signing key, constant-time compare.

**Identity**: same `TokenAuth` shape → `account_id = account_address`.

### 1c. Bearer token — `auth_method = "bearer_access_key"`

**Route**: `Authorization: Bearer …` (auth_orchestrator.py:76 → `_authenticate_bearer:168`).

**Algorithm** (no signature — it is a bearer credential):

1. Strip `"Bearer "`; token must start `hip_` (else `403`).
2. `cached_auth(token, …)`; require `valid` and `status == "active"` and non-empty
   `account_address`.
3. **No SigV4 verification, no secret decryption** — possession of the token is the proof.

**Identity**: `account_address = account_id = token_response.account_address`,
`token_type = token_response.token_type`.

> **⚠ SECURITY** — bearer skips signature verification entirely; it is a plaintext capability.
> Only the 60 s auth cache + upstream `status=="active"` gate it. `acl_middleware` treats
> `bearer_access_key` identically to `access_key` for sub-token/master handling
> (acl.py:246-248, 332).

### 1d. Seed-phrase SigV4 — **REMOVED / rejected**

A well-formed `AWS4-HMAC-SHA256` header whose credential does **not** start `hip_` is the shape the
old seed-phrase auth used. It is now rejected with `403 InvalidAccessKeyId` and a deprecation
pointer (auth_orchestrator.py:98-105):

```python
message="Seed phrase authentication is deprecated. Use https://docs.hippius.com/storage/s3/integration ..."
```

There is **no** seed-phrase → keypair → signature path in the current code. The Rust rewrite should
implement it only if product wants it back; otherwise reproduce the rejection. (The gateway CLAUDE.md
still lists it as method #4 — stale.)

### 1e. Anonymous — `auth_method = "anonymous"`

No `Authorization` header, `GET`/`HEAD`, path `!= "/"` → `AuthResult(is_valid=True,
auth_method="anonymous")` (auth_orchestrator.py:64-65). Any other method/path with no credential →
`403 InvalidAccessKeyId`.

**Identity**: none. `account_middleware` stamps `account_id = "anonymous"`, an all-false
`HippiusAccount` (account.py:588-599). Anonymous callers carry the **sentinel** account id, which
`acl` and `acl_service` explicitly exclude from owner/grant matches
(`is_sentinel_account_id`, see [§2](#2-authorization--acl-model)).

### Auth exemptions (skip auth entirely)

`auth_router._is_exempt` (auth_router.py:34): first path segment in
`EXEMPT_SEGMENTS = {docs, openapi.json, robots.txt, metrics, health}` (any depth), or in
`{admin, user}` **only with a subpath**. Judged on `routing_path` (the collapsed path the app
will act on) — **not** the as-sent path — to defeat `/docs/../anybucket/key` traversal that reads
as exempt `docs` but forwards as `anybucket` (prod incident 2026-08-03). Every exempt segment must
be unusable as a bucket name (`test_every_auth_exempt_segment_is_a_reserved_bucket_name`).
`OPTIONS` and valid peer fetches ([§4c](#4c-internal-peer-fetch)) also bypass.

---

## 2. Authorization / ACL model

Two orthogonal authorization systems run in `acl_middleware` (acl.py:195). Enforcement is
**at evaluation time**, not only at write time.

### 2.1 Required-permission mapping

`get_required_permission(method, query_params, has_key)` (acl.py:129) maps a request to one of
`Permission ∈ {READ, WRITE, READ_ACP, WRITE_ACP, FULL_CONTROL}` (`models/acl.py:18`):

- `?acl` → `READ_ACP` (GET) / `WRITE_ACP` (else)
- `?policy`, `?tagging`, `?versioning`, `?object-lock`/`?retention`/`?legal-hold` → `READ_ACP`
  (GET/HEAD) / `WRITE_ACP` (else). **⚠** These are deliberately graded `_ACP` (not `WRITE`) so a
  write-only grantee cannot e.g. `PUT ?policy` to publish a bucket to AllUsers, or turn on
  versioning/Object-Lock (comments at acl.py:150-177 record the exact past bugs).
- `?uploads`/`?uploadId` → `WRITE`
- else `GET/HEAD` → `READ`; `PUT/POST/DELETE` → `WRITE`.

### 2.2 Bucket ownership / `main_account_id`

Ownership is a column, not an ACL: `buckets.main_account_id` (the SS58 storage-attribution owner).
`ACLService.get_bucket_owner_and_id(bucket)` (acl_service.py:153) returns
`BucketLookup{owner_id, bucket_id, is_cache_warm, object_lock}` in one query, **Redis-cached** per
bucket **name** at `hippius_acl:bucketmeta:{bucket}`, TTL `cache_ttl` (600 s).

> **⚠ SECURITY — name-reuse cache poisoning.** The key is the bucket **name**, which outlives the
> bucket (uniqueness is over `deleted_at IS NULL` rows). A stale entry hands the previous owner the
> master-token bypass and the "private" owner match on someone else's bucket. `cache_invalidation`
> purges on both DeleteBucket **and** CreateBucket (cache_invalidation.py:19-45,
> `invalidate_bucket_meta` acl_service.py:95).

`get_object_owner` inherits the bucket owner (acl_service.py:196). `get_effective_acl`
(acl_service.py:280): object ACL row → else bucket ACL row → else synthesized `private` canned ACL
owned by `main_account_id`.

### 2.3 Canned ACLs vs grants

`ACL{owner: Owner, grants: [Grant]}` (`models/acl.py:64`). `Grant{grantee, permission}`;
`Grantee.type ∈ {CanonicalUser, Group, AmazonCustomerByEmail, AccessKey}` (models/acl.py:26).
Canned ACLs expand in `services/acl_helper.py:18 canned_acl_to_acl`:

| Canned ACL | Grants (beyond owner FULL_CONTROL) |
|-----------|-------------------------------------|
| `private` | owner FULL_CONTROL only |
| `public-read` | AllUsers READ |
| `public-read-write` | AllUsers READ + WRITE |
| `authenticated-read` | AuthenticatedUsers READ |
| `log-delivery-write` | LogDelivery WRITE + READ_ACP |
| `aws-exec-read` | EC2 READ |
| `bucket-owner-read` | bucket owner READ (needs `bucket`+`db`) |
| `bucket-owner-full-control` | bucket owner FULL_CONTROL |

Well-known group URIs in `WellKnownGroups` (models/acl.py:9): `AllUsers`, `AuthenticatedUsers`,
`LogDelivery`, `AmazonEC2`.

### 2.4 `check_permission` evaluation order

`ACLService.check_permission(account_id, bucket, key, permission, access_key, bucket_owner_id)`
(acl_service.py:207):

1. `get_effective_acl`.
2. **Owner match** → `FULL_CONTROL` (return True) iff `account_id` is non-sentinel and
   `== acl.owner.id`. **⚠** Sentinel excluded on both sides — a legacy ownerless row would
   otherwise match every anonymous caller (acl_service.py:229-237).
3. **Service-account write ban**: if `permission ∈ WRITE_PERMISSIONS = {WRITE, WRITE_ACP,
   FULL_CONTROL}` and `protected_owner` (prefer `bucket_owner_id`) is a service account →
   **deny**, even if a grant exists (acl_service.py:251-259). Placed *after* the owner match, so
   the service account writes to its own bucket fine; scoped to writes so public READ still works.
4. **Grant scan**: `_grant_matches` (acl_service.py:118):
   - `AccessKey` grantee → matches `grant.grantee.id == access_key`.
   - `CanonicalUser` → matches `== account_id` (sentinel excluded).
   - `Group AllUsers` → always matches (public).
   - `Group AuthenticatedUsers` → matches any non-sentinel account.
   `_permission_implies`: `FULL_CONTROL` implies anything, else exact match.

### 2.5 Public-access semantics

A bucket/object is "public" iff its stored ACL contains a `Group/AllUsers` grant (READ for public
read). `bucket_policy_endpoint` is a **thin façade over the bucket ACL**: `PUT ?policy` only
accepts a policy that is exactly `Allow *  s3:GetObject  arn:aws:s3:::{bucket}/*` and translates it
into the `public-read` canned ACL (`bucket_policy_endpoint.py:130 _validate_public_policy`,
`:108-119`); `GET ?policy` synthesizes that JSON back iff the ACL has AllUsers READ. There is **no
real IAM policy engine.**

`request.state.anonymous_read_allowed` is computed once for GET/HEAD object requests when ATS
caching is on (acl.py:440-449), so master-token and presigned reads of public objects still warm
the CDN cache. Anonymous responses get header `x-hippius-access-mode: anon` (acl.py:491-492).

> **⚠ PER-PREFIX POLICY GAP.** Buckets are public **or** private at the **bucket grain**. The
> policy surface accepts only a single wildcard `…/*` GetObject statement; there is no way to make
> `photos/public/*` public while keeping the rest private, and no `Deny`, no `Condition`, no
> principal other than `*`. Object-level ACLs exist (`object_acls` table) but the policy/console
> surface does not drive them per-prefix. The Rust rewrite should treat "bucket public flag" as
> the real model and design a proper prefix/policy layer only as new work.

### 2.6 Sub-token scopes (R2-style) — **LIVE**

For `token_type == "sub"` and **not** cross-account, the sub-token branch is authoritative
(acl.py:332-399) and short-circuits the ACL grant scan. Scope is loaded from `sub_token_scopes`
via `get_cached_sub_token_scope` (`gateway/services/sub_token_scope_cache.py:31`, Redis
`hippius_subscope:{key}`, TTL 60 s, **fail-closed** — any Redis/PG error → `None` → default-deny).

`SubTokenScope{access_key_id, account_id, permission, bucket_scope, bucket_ids}`
(`models/sub_token.py:58`). Four permission tiers × bucket list, **no prefix/IP restrictions**
(`sub_token_scope.py`):

| Tier | Ops granted |
|------|-------------|
| `admin_read_write` | all 9 ops |
| `admin_read` | read_object, list_bucket, list_buckets, read_bucket_meta |
| `object_read_write` | read/write/delete_object, list_bucket |
| `object_read` | read_object, list_bucket |

`required_op(method, has_key, query_params)` (sub_token_scope.py:121) maps the request to one `Op`;
`_BUCKET_META_SUBRESOURCES` (acl/tagging/policy/versioning/… listing at :70) turn a bucket request
into `*_bucket_meta`; `POST ?delete` → `delete_object`; `evaluate()` (:160) checks
`permission_allows` then `bucket_in_scope` (`BucketScope.all` or `bucket_id ∈ bucket_ids`).
`create_bucket` requires `bucket_scope == all`. **CopyObject/UploadPartCopy** additionally require
the **source** bucket be in scope with `read_object` (acl.py:379-395).

> Cross-account (`bucket_owner_id != account_id`) sub-tokens fall through to the bucket-ACL grant
> scan — the "contractor" pattern (acl.py:399).

### 2.7 Master-token bypass

`token_type == "master"`, non-sentinel account, and `bucket_owner_id == account_id` → **skip the
ACL check entirely** (acl.py:451-459). The comment: "we trust Arion to have already enforced token
scope." **⚠** A poisoned bucket-meta cache ([§2.2](#22-bucket-ownership--main_account_id)) directly
widens this bypass.

### 2.8 Copy-source authorization

Because every other check derives from the request **path** (destination only), the copy **source**
is authorized separately and **ahead of all bypasses** (acl.py:303-326): `parse_copy_source`
percent-decodes → strips `/` → splits (must mirror the handlers exactly, or a `victim%2Fkey` split
view lets a write grant read any object). Source requires `READ` via `check_permission`.

### 2.9 The `?acl` / grant write path

`api/s3/acl_endpoints.py` handles `?acl` GET/PUT for buckets and objects (dispatched from routers;
the ACL **middleware** already enforced READ_ACP/WRITE_ACP before the handler runs). PUT accepts
`x-amz-acl` canned, `x-amz-grant-*` headers, or an XML body; owner is forced to the immutable
bucket/object owner (acl_endpoints.py:414-418, 564-568); max 100 grants;
`validate_grant_grantees` enforces grantee id/uri shape (models/acl.py:114). **⚠** Write grants on
a service-account bucket are refused with 403 at write time too
(`service_account_grant_response`, acl_endpoints.py:590 → `forbidden_write_grants`,
`services/service_accounts.py:30`). ACL rows live in `bucket_acls`/`object_acls`
(`repositories/acl_repository.py`), Redis-cached via `CachedACLRepository`.

---

## 3. HMAC admin / frontend auth

Two separate HMAC gates protect the non-S3 surfaces. Both scheme:
`X-HMAC-Signature = hex(HMAC-SHA256(secret, METHOD + PATH [+ "?" + QUERY]))`, compared with
`hmac.compare_digest`.

### 3a. Frontend HMAC — `/user/*`

`frontend_hmac.py:27`. Gate condition uses `routing_path(request).startswith("/user/")`; the
**signed message** deliberately uses `request.url.path` (must be byte-identical to what the
frontend signs). Secret: `FRONTEND_HMAC_SECRET` (config `frontend_hmac_secret`). Missing header →
`401`; bad signature → `403`. **⚠** An empty secret would silently verify against `""` (unlike
admin, which fails closed).

### 3b. Admin HMAC — `/admin/*`

`admin_hmac.py:19`. Secret: `HIPPIUS_ADMIN_HMAC_SECRET` (config `admin_hmac_secret`) — **dedicated**
because `/admin/*` can suspend/destroy whole accounts. **Fail-closed**: empty/unset secret →
`403 "Admin API is not enabled"` for every request (admin_hmac.py:41-46). Target `account_id`
(SS58) always travels in the **signed path**, never the body (`docs/admin-api.md`). Missing header
→ `401`; bad signature → `403`.

Admin endpoints (docs/admin-api.md): `POST /admin/accounts/{ss58}/suspend` (body `{mode:
full|read_only}`), `.../reactivate`, `GET .../status`, `DELETE .../data` (async purge → `202
{job_id}`, implies `full` suspension), `GET /admin/purge-jobs/{job_id}`.

---

## 4. Suspension & read-only enforcement

### 4a. Account suspension (issue #421)

State: `account_suspensions` table (`row present = suspended`, `mode ∈ {full, read_only}`), Redis
`hippius_suspension:{ss58}`, TTL **30 s**, write-through from the admin endpoints
(`gateway/services/suspension.py:28 get_account_suspension`).

> **⚠ FAILS OPEN** on any DB/Redis error (suspension.py:44-61) — it is a **billing** control, not
> a security one; a suspended account slipping through during a DB blip is cheaper than 500ing all
> traffic. This is the deliberate opposite of the sub-token scope cache (fail-closed).

`suspension_blocks(mode, method, query_params, has_key)` (suspension.py:77): `full` blocks
everything; `read_only` blocks anything the ACL matrix grades `WRITE`/`WRITE_ACP` (reuses
`get_required_permission`); unknown methods count as writes.

Two enforcement points:
- **`suspension_middleware`** (suspension.py, order 9) — keyed on `request.state.account_address`
  (the main SS58; identical across master/sub/bearer/presigned). Blocks the **suspended caller**.
  Skips `/health`, `/user/*`, `/admin/*`, `OPTIONS`, and anonymous (no `account_address`). Returns
  `403 AccessDenied`.
- **Bucket-owner check inside `acl_middleware`** (acl.py:278-294) — when
  `bucket_owner_id != account_id`, look up the **owner's** suspension so anonymous public reads and
  cross-account access to a suspended owner's buckets are blocked too (`full` = all, `read_only` =
  writes only).

### 4b. Global read-only mode

`read_only_middleware` (read_only.py, order 6, registered only if `HIPPIUS_READ_ONLY_MODE=true`):
`PUT/POST/DELETE/PATCH` (except `/health`) → `405 MethodNotAllowed`.

### 4c. Internal peer fetch (bypass)

`peer_auth.py:90 is_authorized_peer_fetch`: `GET /internal/parts/…` carrying a valid
`X-Hippius-Peer-Auth` header (constant-time compare against `HIPPIUS_INTERNAL_PEER_SECRET`,
64-hex, `peer_serve_enabled`). Bypasses input_validation, auth_router, account, and acl. Fail-closed:
unset/empty secret → never authorizes; `internal` is otherwise a reserved bucket segment rejected by
input_validation.

---

## 5. Billing / plan gate

Runs inside `account_middleware` (account.py:451), only for `auth_method == "access_key"`
mutating methods (`PUT/POST/DELETE`). Reads (`GET/HEAD`) get a lightweight all-false account and
skip everything (account.py:528-535). There are **two parallel billing systems**:

```
        mutating access_key write
                  │
     ┌────────────┼─────────────────────────┐
service account?  │  on a billing plan?      │  neither
     │YES         │  (plan gate)             │  (pay-as-you-go)
  BYPASS all      │                          │
  (permissive)    │  quota check on          │  substrate has_credits
                  │  stored bytes            │  + Arion can_upload
```

### 5a. Service-account bypass

`is_service_account(account_address, config.service_account_ids)`
(`services/service_accounts.py:15`, exact case-sensitive SS58 membership of
`HIPPIUS_SERVICE_ACCOUNT_IDS`). Skips redis-accounts fetch **and** both gates
(account.py:536-544), emits `BILLING_BYPASS` metric. Keyed on the **verified** address, so only the
holder of the service account's own credentials lands here.

### 5b. Plan gate (quota on stored bytes)

`_check_plan_quota` (account.py:311) → `gateway/services/plan_gate.py`.

1. **Whose quota** (`_billed_account`, account.py:240): **the bucket owner**, via
   `acl_service.get_bucket_owner_and_id` (the same Redis-cached lookup acl uses), falling back to
   the caller when there is no bucket / bucket doesn't exist yet (CreateBucket) / lookup fails.
   > **⚠ NOTE — this contradicts todo.md.** `todo.md` P1 "the quota gate keys on the CALLER"
   > describes the *old* behaviour; the current code resolves the **owner** in
   > `_billed_account`. See [§8](#8-open-questions).
2. **Plan lookup** (`resolve_plan` → `services/plans_cache.py:170 get_plan_for_account`): one
   Redis `HGET hippius_s3_plan_accounts {ss58}` on the **redis-accounts** client. Returns
   `PlanQuota{plan_id, storage_bytes (max quota, from upstream), used_bytes (computed by us)}` or
   `None` (pay-as-you-go). No DB work on the request path.
   - `None` → not on a plan → return `(handled=False)` → fall through to pay-as-you-go.
   - Lookup error → `PlanLookupUnavailable` → fall through to pay-as-you-go (never a 500).
3. If `config.enable_billing_plans` is **off**: `_log_plan_shadow` records what it *would* have
   done (`BILLING_PLAN_SHADOW`, `would_deny` outcome) and falls through — the request is still
   billed pay-as-you-go (account.py:341-343).
4. If the op doesn't add bytes (`_adds_storage`, account.py:155): allow (no quota check).
5. `evaluate_quota(quota, incoming_bytes)` (plan_gate.py:69):
   - `not quota.enforceable` (missing/0/negative limit) → `catalog_miss` → **ALLOW loudly** — a
     paying customer must never be blocked by a cold cache. **⚠ also skips `has_credits` +
     `can_upload` → unmetered storage** (todo.md P1).
   - `used_bytes + incoming_bytes <= limit` → `allow`.
   - else → `deny` → `402 QuotaExceeded` with `quota_exceeded_message` (binary units, plan_gate.py:92).

`incoming_bytes` = `_incoming_bytes` (account.py:280): declared length
(`x-amz-decoded-content-length` or `content-length`, else 0), except CopyObject/UploadPartCopy which
resolve the **source object's current size** from PG.

**`_adds_storage`** (account.py:155): `required_op == write_object` **minus** CompleteMultipartUpload
(`POST ?uploadId` without `partNumber`) and object metadata subresources
(`{acl, tagging, retention, legal-hold}`). `POST ?delete` frees space → not gated.

**Known todo.md bugs (all still real except where noted):**
- **Quota keyed on caller-not-owner** — *documented as fixed in code* via `_billed_account`; the
  todo entry is stale. `can_upload` (pay-as-you-go) still keys on `request.state.account.main_account`
  = the caller (account.py:394).
- **Enforcement lags a refresh** — `used_bytes` is only recomputed every `HIPPIUS_PLANS_LOOP_SLEEP`
  (**120 s**) by the plans-cacher; nothing on the request path recomputes. An account can overshoot
  by one cycle's uploads, and a customer who deletes data stays refused until the next cycle
  (plan_gate.py docstring; todo.md P2).
- **Nothing accumulates between refreshes** — every request compares against the same cached
  `used_bytes`, so within a window an account at 9.9/10 TiB can issue unbounded 100 GiB PUTs that
  each individually "fit" (todo.md P1 "nothing accumulates").
- **Overwrite charged as additive** — `used + incoming <= limit`, but `used` already contains the
  version being replaced, so re-uploading an unchanged file (an `aws s3 sync` re-run) is refused
  for a net-zero write (todo.md P2).
- **`active` flag ignored** — plans-cacher admits on `billing=="plan"` alone, so a cancelled
  subscriber keeps their allowance (`plans_cache.get_plan_for_account` docstring; todo.md P1).
- **MPU parts** gated individually, never against the accumulating total (todo.md P2).

### 5c. Pay-as-you-go path

For an access-key write that is neither service-account nor plan-handled (account.py:560-580):
1. `fetch_account_by_main_address` (`gateway/services/account_service.py:18`) — reads
   `hippius_main_account_credits:{ss58}` from redis-accounts; `has_credits` **defaults True** when
   the cache is cold. `not has_credits` → `402 InsufficientAccountCredit`.
2. `_check_can_upload` (account.py:380) — `PUT/POST` only. Redis short-circuit `can_upload:{main}`
   (`b"1"`, TTL `can_upload_cache_ttl_seconds`=10 s; **positives only**, so denials retry). Miss →
   Arion `can_upload(main_account, content_length)`. Transient billing-backend failures (string-
   sniffed via `_TRANSIENT_BILLING_ERROR_MARKERS`) retry `can_upload_transient_retries`=2 then
   surface `503 SlowDown`; genuine denial → `402 UploadNotPermitted`. 429/507/4xx handled specially
   (account.py:105-137).

### 5d. Usage / rollup accounting a successful write records

Usage is **not** recorded on the request path. `bucket_storage_usage` is a maintained per-bucket
counter kept by **PostgreSQL triggers** that write an insert-only delta ledger
(`storage_delta_ledger`), folded into the rollup by the compactor
(`services/storage_rollup_service.py`, migration `20260910120000_storage_usage_rollup.sql`).
`services/usage_service.py:58 get_account_storage_bytes` reads the rollup (one indexed SUM,
`get_account_storage_bytes_rollup.sql`), and **raises until backfilled** (deltas ≠ totals). Its
**only** caller is the plans-cacher (background), which every `HIPPIUS_PLANS_LOOP_SLEEP` computes
each plan account's `used_bytes` and publishes the whole `hippius_s3_plan_accounts` hash via an
**atomic swap** (build `:building`, RENAME), refusing a roll that shrank > 50%
(`plans_cache.py:109 publish_plan_roll`, `MAX_ACCOUNT_MAP_SHRINK_RATIO`). Neither hash has a TTL
(redis-accounts is `noeviction`+AOF) so last-known-good survives an upstream outage.

> **⚠ The triggers have NO kill switch** (todo.md) — `HIPPIUS_ENABLE_BILLING_PLANS` gates the quota
> *decision*, not the trigger writes; a bug in a trigger is a data-plane outage on the write path.

---

## 6. Full middleware order (reference)

| # | Middleware | May short-circuit with |
|--:|-----------|------------------------|
| 1 (outer) | `cors` | — |
| 2 | `ray_id` | — (stamps `gateway_start_time`) |
| 3 | `path_normalization` | — (one collapsed path view for all layers) |
| 4 | `cache_control` | — |
| 5 | `ats_purge` | — (fans PURGE to ATS) |
| 6 | `cache_invalidation` | — (post-response ACL/meta cache purge) |
| 7 | `read_only` *(if `HIPPIUS_READ_ONLY_MODE`)* | 405 |
| 8 | `fs_cache_pressure` | 503 (load shedding, deliberately outside auth) |
| 9 | `input_validation` | 400 |
| 10 | `auth_router` | 403 (→ populates identity) |
| 11 | `suspension` | 403 |
| 12 | `trailing_slash` | — |
| 13 | `account` | 402 / 503 (credit + plan gate) |
| 14 | `acl` | 403 (authZ; sub-token + master + grant) |
| 15 | `frontend_hmac` | 401/403 (`/user/*`) |
| 16 | `admin_hmac` | 401/403 (`/admin/*`, fail-closed) |
| 17 | `audit_log` *(if `ENABLE_AUDIT_LOGGING`)* | — (attributes to caller) |
| 18 | `request_context` | — (binds `main_account_id` from acl's `bucket_owner_id`) |
| 19 | `tracing` | — |
| 20 | `metrics` | — |
| 21 (inner) | `auth_probe` | 200 (ATS probe; MUST stay innermost) |

(Numbering is the request-path order; the table in `gateway/CLAUDE.md` is stale — it omits
`path_normalization` and mis-places `request_context`. This table is derived from the actual
`main.py:535-570` registration.)

---

## 7. Rust implementation notes

### 7a. Already covered by the hcfs `service` branch (`hcfs-server/src/s3/auth/`) — HARVEST

The `origin/service` branch already contains a complete, self-contained SigV4/SigV2 verifier that
maps almost 1:1 onto §1a/§1b. Reuse it wholesale; it is better-hardened than the Python in a few
places.

- **`signing.rs`**:
  - `verify_sigv4` (header) — full pipeline: `parse_auth_header` → require `host` + `x-amz-date`/
    `date` in signed headers → **reject unsigned `x-amz-content-sha256`** (MITM guard the Python
    lacks) → `check_clock_skew` (**±15 min**, `RequestTimeTooSkewed` — Python has NO clock-skew
    check on header SigV4) → `validate_credential_scope` (`YYYYMMDD/region/s3/aws4_request`) →
    `get_credentials` → `build_canonical_request` → `build_string_to_sign`
    (`AWS4-HMAC-SHA256\n{date}\n{scope}\n{sha256(canonical)}`) → `derive_signing_key`
    (`HMAC(HMAC(HMAC(HMAC("AWS4"+secret,date),region),"s3"),"aws4_request")`) → `constant_time_eq`.
    Returns a `SigningContext` for chunked-upload per-chunk verification.
  - `verify_sigv4_query_string` (presigned) — `X-Amz-*` params, `X-Amz-Expires ≤ 604800`,
    future-date skew reject, `build_presigned_canonical_query_string` (drops `X-Amz-Signature`),
    `UNSIGNED-PAYLOAD`, and an **AWS-compatible HEAD-with-GET-signed-URL retry** (Python lacks this).
  - `verify_sigv2_query_string` (legacy SigV2 presigned: `AWSAccessKeyId`/`Expires`/`Signature`,
    HMAC-SHA1, `build_sigv2_canonical_resource` with sorted sub-resources) — **Python has no SigV2
    at all**; keep if AWS-SDK-v2 / legacy clients matter, else drop.
  - Helpers to lift directly: `hmac_sha256`, `hex_hmac_sha256`, `derive_signing_key`,
    `build_canonical_request`, `build_string_to_sign`, `canonical_uri_encode`, `constant_time_eq`.
- **`mod.rs`**: `authenticate_full` / `try_authenticate` dispatch (header → SigV4; `X-Amz-Signature=`
  → presigned SigV4; `AWSAccessKeyId=` → SigV2; else anonymous `(None, None)`); **rejects STS
  `x-amz-security-token`** with `NotImplemented`; `check_rate_limit` / `record_failed_attempt`
  (per-access-key rate limiting — a gap in the Python gate); `require_auth`;
  **`billing_user_id(user, owner_id)`** — already encodes "bill the owner, fall back to caller",
  the exact fix §5b needs.
- **`credentials.rs`**: `S3Credentials{access_key_id, secret_access_key, user_id, expires_at}` in
  Sled with a `user_id → access_key_id` secondary index; credential-expiry checks.

### 7b. hippius-specific — MUST be built new (the Rust branch's model differs)

- **Credential source & secret handling.** The Rust branch stores the raw secret locally in Sled
  and derives the signing key from it. hippius does **not** store the secret: it calls
  `POST /objectstore/tokens/auth/` on api.hippius.com, caches the `TokenAuthResponse` in Redis
  (`hippius_auth:`, 60 s), and **decrypts a NaCl SecretBox** (`HIPPIUS_AUTH_ENCRYPTION_KEY`, 32-byte
  hex) to recover the secret per verification. Rewrite must reproduce: the upstream auth client,
  the 60 s auth cache, `token_type ∈ {master, sub}`, `status=="active"`, and NaCl decryption.
- **Bearer token method** (§1c) — plaintext-capability auth, no branch equivalent.
- **Anonymous routing rules** (§1e) + sentinel account handling (`is_sentinel_account_id`) across
  owner/grant matching.
- **The whole authorization layer** (§2): canned ACLs, `bucket_acls`/`object_acls` in Postgres,
  `check_permission` order (owner match → service-account write ban → grant scan), copy-source
  authZ, the bucket-name-keyed ACL/meta Redis caches and their name-reuse invalidation.
- **Sub-token scopes** (§2.6): `sub_token_scopes` table, R2 4-tier matrix, `required_op` mapping,
  fail-closed scope cache, cross-account contractor fallthrough.
- **Master-token bucket-owner bypass** (§2.7).
- **HMAC admin/frontend gates** (§3) with separate secrets, admin fail-closed.
- **Suspension** (§4) — 30 s write-through cache, **fail-open**, caller + bucket-owner enforcement.
- **Billing gate — mostly INHERITED from HCFS, not rebuilt.** The greenfield decision (register:
  "HCFS owns per-tenant usage→chain + the 402 credit gate; we inherit it") means the rewrite does
  **not** reproduce the Python substrate-credit scrape + plans-cacher + Arion `can_upload` apparatus
  in §5. It keeps a thin **fail-open** credit check (HCFS's 402 at store time is the backstop) and
  stores bytes under the real ss58 so HCFS attributes + gates. Service-account bypass stays as config.
- **~~Peer-fetch bypass~~ (§4c) — DROPPED** (no peer/internal-parts tier in the rewrite; reads go
  straight to HCFS). Keep only the input-validation reserved-name / traversal / double-decode
  defenses (§0).

### 7c. Crate suggestions

| Need | Crate |
|------|-------|
| SigV4 HMAC chain | `hmac` + `sha2` (already used by the branch) |
| SigV2 | `hmac` + `sha1` |
| Constant-time compare | branch's `constant_time_eq`, or `subtle::ConstantTimeEq` |
| NaCl SecretBox (secret decryption) | `crypto_secretbox` / `dryoc` / `sodiumoxide` (XSalsa20-Poly1305) |
| SS58 address validate/decode | `sp-core` (`Ss58Codec`), or `base58` + blake2b checksum |
| sr25519 / ed25519 (if seed-phrase auth is revived) | `schnorrkel` (sr25519), `ed25519-dalek`, `bip39` |
| Redis caches (auth, scope, suspension, plans, acl-meta) | `redis` / `fred` / `deadpool-redis` |
| HTTP to api.hippius.com | `reqwest` (branch already uses it) |
| Time / clock skew | `chrono` (branch already uses it) |
| URL percent-encoding (canonical query/path) | branch's own `s3_uri_encode`; else `percent-encoding` |

---

## 8. Open questions

1. **RESOLVED — four live methods.** access-key SigV4 header, presigned SigV4, bearer, anonymous;
   **seed-phrase stays removed** (rejected with a deprecation pointer, §1d — register G1). The rewrite
   reproduces the rejection; it does **not** revive seed-phrase (drop the `schnorrkel`/`bip39` "if
   revived" crate row in §7c).
2. **Quota-owner bug is stale in todo.md.** `todo.md` P1 says the plan gate keys on the caller, but
   the current `account.py:_billed_account` resolves the **bucket owner**. Confirm which is
   authoritative before porting; the pay-as-you-go `can_upload` path *does* still key on the caller
   (`account.py:394`).
3. **`gateway/CLAUDE.md` middleware table and "sub-token scope not enforced" note are both stale.**
   Sub-token scope **is** enforced (acl.py:332-399; confirmed by `todo.md` "Sub-token scope
   enforcement is live, not dead code"). §6 table here is derived from the real `main.py`
   registration.
4. **No rate limiting in the Python gate.** `rate_limit`/`banhammer` exist but are unwired. The Rust
   branch has per-key rate limiting. Is rate limiting desired in the rewrite (and at what layer)?
5. **SigV2 support.** Python has none; the Rust branch does. Keep SigV2 for legacy SDKs, or drop?
6. **`account_service.fetch_account_by_main_address` defaults `has_credits=True` on a cold cache**
   (account_service.py:52) — a fail-open credit posture. Intentional to port, or tighten?
7. **Presigned streaming uploads are effectively unauthenticated in body** (`UNSIGNED-PAYLOAD`
   default, §1b). Preserve AWS-compatible behaviour or require a signed payload hash?
