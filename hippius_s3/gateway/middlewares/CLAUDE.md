# gateway/middlewares/

Per-middleware reference. Registration order and purpose table in [../CLAUDE.md](../CLAUDE.md); detail here.

## Each middleware

### [ray_id.py](ray_id.py) — `ray_id_middleware`

Innermost on the chain. Reads `X-Ray-ID` from the request (if set), or generates a new one. Attaches to `request.state.ray_id` and echoes on the response as `X-Ray-ID`. Used for cross-service correlation — `X-Hippius-Ray-ID` header propagates to the internal API and into every structured log / span / metric.

### [audit_log.py](audit_log.py) — `audit_log_middleware`

Gated by `ENABLE_AUDIT_LOGGING`. Records every request with method, path, account, status, latency. Delegates to [hippius_s3/services/audit_service.py](../../services/audit_service.py) (shared with the internal API).

Metrics and tracing for the merged app live in [hippius_s3/api/middlewares/](../../api/middlewares/) — the gateway-side `metrics.py`/`tracing.py` shadows were deleted with the merge.

### [frontend_hmac.py](frontend_hmac.py) — `verify_frontend_hmac_middleware`

If `FRONTEND_HMAC_SECRET` is configured, verifies an HMAC header on frontend-internal endpoints (used by console.hippius.com). Public S3 traffic isn't HMAC-signed — this middleware no-ops for most requests.

### [acl.py](acl.py) — `acl_middleware`

**Permission mapping** ([acl.py:36-67](acl.py)): HTTP verb + query params → `Permission.READ | WRITE | READ_ACP | WRITE_ACP`.

Flow ([acl.py:70-171](acl.py)):
1. Parse path → `(bucket, key)`.
2. Fetch `bucket_owner` via `ACLService.get_bucket_owner` (Redis-cached).
3. **Master-token bypass** ([acl.py:126-130](acl.py)): if `auth_method=="access_key"` AND `token_type=="master"` AND the authenticated account owns the bucket, skip. We trust Arion to enforce master-token constraints.
4. Otherwise call `ACLService.check_permission`. 403 on denial.
5. **CreateBucket carve-out** ([acl.py:98-110](acl.py)): PUT with no key and no query params bypasses permission checks (you can always create your own bucket), but we reject if `x-amz-acl` is present (AWS BucketOwnerEnforced semantics).

### [account.py](account.py) — `account_middleware`

For seed-phrase auth: fetches subaccount role/credits from Arion via [gateway/services/account_service.py](../services/account_service.py). Results cached in `redis-accounts`. Populates `request.state.account` (upload/delete/credits flags, main account SS58).

**Gotcha**: If Arion is down, this returns 503. There's no graceful degradation — non-seed-phrase auth methods (bearer, access key) don't hit this middleware's hot path, but seed-phrase auth blocks on Arion.

**Service accounts**: an access-key caller whose `account_address` is in `HIPPIUS_SERVICE_ACCOUNT_IDS`
(comma-separated SS58, a GitHub secret in prod) skips both mutating-path gates — the
redis-accounts credit fetch and Arion `can_upload` — and gets `request.state.service_account = True`
for the audit log. The predicate is [services/service_accounts.py](../../services/service_accounts.py);
the allowlist is parsed and SS58-validated at config time, so a typo fails startup rather than
silently demoting an internal account back to billed. Keyed only on the VERIFIED `account_address`,
never on a header, and never on the bucket owner — the gate bills the caller. Reads set the flag
but change nothing else.

**The gateway is the only place we apply this.** The backend upload path needs nothing: Arion
whitelists our service accounts on `/upload` itself, so that exemption is applied upstream. The
uploader deliberately sends no `X-Billing-Bypass` for them — a second list of the same accounts,
maintained in two systems, would be free to drift apart. `payload.bypass_billing` in
[hippius_s3/workers/uploader.py](../../workers/uploader.py) is unrelated: the operator escape
(`dlq_requeue --bypass-billing`) for re-driving an ordinary account's 402'd uploads, which predates
service accounts.

What Arion's whitelist cannot cover is the gate above it. `has_credits` is read from **our**
`redis-accounts` cache and checked BEFORE `can_upload`, so a service account with no cached credit
would be refused `InsufficientAccountCredit` before Arion is consulted at all. That is what this
branch exists for.

**Nobody but the owner writes to a service-account bucket.** Enforced in two places, and the
evaluation-time one is the control: [acl_service.py](../services/acl_service.py) `check_permission`
refuses WRITE / WRITE_ACP on any bucket whose owner is allowlisted, after the owner match and
before the grant loop. That makes the ban retroactive over grants that already exist and total over
any path that reaches the acl tables another way. The write-time refusals (`?acl` bucket/object,
`x-amz-acl` on CreateBucket, canned object ACLs) are the loud half — without them the write
succeeds, a later `GET ?acl` reports a grant that does nothing, and the operator believes they
configured something they did not. READ and READ_ACP are untouched: publishing our own datasets
publicly is the point of several of these buckets. The predicate is `forbidden_write_grants` in
[services/service_accounts.py](../../services/service_accounts.py).

**Billing plans (parallel to pay-as-you-go).** A mutating access-key request now resolves the
caller's plan before the credit gates. The branch order inside `account_middleware` is:

```
reads (GET/HEAD)      -> lightweight account, no gates
service account       -> bypass everything                       (unchanged)
on a billing plan     -> storage-quota gate                      (NEW)
everything else       -> has_credits + Arion can_upload          (unchanged)
```

An account on a plan skips BOTH pay-as-you-go gates. That is not an optimisation: a plan customer
holds no substrate credits, so leaving `has_credits` in place would 402 every one of them before the
quota check ran. Deletes short-circuit to allow — a customer who downgraded below their usage has to
be able to dig themselves out.

Plan membership is read from two `redis-accounts` hashes populated by the `plans-cacher` worker;
nothing is added to the auth path and `/objectstore/tokens/auth/` is untouched. Decision logic is
[services/plan_gate.py](../services/plan_gate.py); the caches are
[hippius_s3/services/plans_cache.py](../../services/plans_cache.py).

Two failure postures, deliberately different:

- **The lookup fails** (Redis down, malformed cached JSON) -> fall through to the pay-as-you-go
  path. That is exactly what the code did before plans existed, so a Redis blip can never be a new
  failure mode.
- **The lookup succeeds but the quota is unknown** (cold catalog, unknown plan id) -> ALLOW, loudly.
  A positively identified paying customer is never blocked because our cache has not warmed up.

**Where the numbers come from.** Both the quota and the usage sit on one cached row published by
the plans-cacher — quota from upstream, usage counted by that worker in the background. The gate is
therefore a single Redis `HGET` and a pure comparison, with no database work on the request path.

The cost, stated plainly: usage is only as fresh as the last refresh
(`HIPPIUS_PLANS_LOOP_SLEEP`, 10 min), and a denial is **not** re-checked live. A customer who
deletes data to get back under quota stays refused until the next cycle. The refresh interval is the
only lever on that, and the 402 message says so.

**`HIPPIUS_ENABLE_BILLING_PLANS` is the master switch, and it ships OFF.** With it false every
account takes the pay-as-you-go path exactly as before — but the plan caches are still consulted on
writes, and any account that HAS a plan gets a `BILLING_PLAN_SHADOW` line recording what we would
have charged them against. That is how the whole chain is proven working in prod logs before it can
cost anyone an upload; flipping it on is then a config change, not a code change.

```
{namespace="hippius-s3-prod",app="api"} |= "BILLING_PLAN_SHADOW"
{namespace="hippius-s3-prod",app="api"} |= "BILLING_PLAN_SHADOW" |= "would=would_deny"
```

The shadow path is cheaper than the real gate on purpose: it reads the cached rollup and stops,
never running the authoritative SUM. It sits on the pay-as-you-go path of every write, and an
unbounded query there for the sake of a log line would be a self-inflicted latency regression — the
cost is that a shadow `would_deny` is UNVERIFIED and should be cross-checked before it is trusted.
Nothing in the shadow path can fail the request.

The flag is held as **two GitHub secrets**, so the environments move independently:

| Secret | Read by |
|---|---|
| `HIPPIUS_ENABLE_BILLING_PLANS_STAGING` | pods with `ENVIRONMENT=staging` |
| `HIPPIUS_ENABLE_BILLING_PLANS_PROD` | pods with `ENVIRONMENT=production` |
| `HIPPIUS_ENABLE_BILLING_PLANS` | local dev and tests (fallback) |

Each deploy workflow seeds only its own key into that cluster's Secret, so staging's Secret never
contains the production value at all. `config.py::_parse_enable_billing_plans` then selects by the
pod's OWN `ENVIRONMENT`, which means even a mis-seeded Secret cannot let production read staging's
flag. Note `production` maps to the `_PROD` suffix — an uppercase of `ENVIRONMENT` would look for
`_PRODUCTION`, find nothing, and silently leave the feature off, so the mapping is explicit.

Values are parsed by `_parse_bool` (true/True/1/yes/on); a typo fails the pod loudly rather than
silently leaving the feature off, and an environment secret that is present-but-empty falls through
to the fallback rather than pinning the feature off. An empty `hippius_s3_plan_accounts` hash is the
other kill switch.

### [trailing_slash.py](trailing_slash.py) — `trailing_slash_normalizer`

Keeps `/foo/bar/` and `/foo/bar` equivalent for S3 operations.

### [auth_router.py](auth_router.py) — `auth_router_middleware`

Calls `auth_orchestrator.authenticate_request` and attaches the result to `request.state`. 403s on invalid auth (except for anonymous on public buckets).

### [input_validation.py](input_validation.py) — `input_validation_middleware`

- Bucket names: length bounds from config, lowercase, no dots, no consecutive hyphens — EXCEPT valid SS58 addresses, which bypass format validation ([input_validation.py:84-86](input_validation.py)). That allows `s3://5Grw...abc/` if the caller's account is that address.
- Object keys: rejects `\ { } ^ % ` [ ] " < > ~ # |` and non-printable ASCII (0-31, 127) — see [input_validation.py:30-34](input_validation.py).
- Metadata: size bound from `MAX_METADATA_SIZE`.

### [read_only.py](read_only.py) — `read_only_middleware`

Controlled by `HIPPIUS_READ_ONLY_MODE`. When true, returns 403 on any non-GET/HEAD request. Used during maintenance windows or incident response.

### [cors.py](cors.py) — `cors_middleware`

Outermost. Adds CORS headers to every response, including error responses generated by inner middleware short-circuits. Standard CORS wildcard for the public S3 endpoint.

### [sigv4.py](sigv4.py) — `SigV4Verifier`

Not a middleware — a class used by `auth_orchestrator` and `access_key_auth`. Core SigV4 calculation:

- **Canonical path**: built from `request.scope["raw_path"]` (bytes) rather than `request.url.path` ([sigv4.py:66-86](sigv4.py)) to preserve exact client percent-encoding.
- **Host header fallback chain**: `x-forwarded-host` → `x-original-host` → `host` ([sigv4.py:118-124](sigv4.py)).
- **Presigned URL payload hash** defaults to `UNSIGNED-PAYLOAD` ([sigv4.py:149-162](sigv4.py)); for streaming, falls back to SHA256 of empty body.
- **Seed phrase extraction**: Authorization credential is base64-decoded to the 12-word seed ([sigv4.py:200-220](sigv4.py)). Malformed base64 → bare 403. Minor UX paper cut.

### [access_key_auth.py](access_key_auth.py) — helpers

`verify_access_key_signature(request)` ([access_key_auth.py:35](access_key_auth.py)) and `verify_access_key_presigned_url(request)` ([access_key_auth.py:139](access_key_auth.py)) do SigV4 for header-based and URL-based access keys respectively. Flow:

1. Extract credential (regex).
2. `cached_auth(credential)` → Arion `/objectstore/tokens/auth/` (Redis-cached).
3. Validate `token_response.valid` and `status=="active"` and account_address is a valid SS58 ([access_key_auth.py:75](access_key_auth.py)).
4. Decrypt stored secret via ChaCha20-Poly1305 ([auth_service.py](../services/auth_service.py) using `HIPPIUS_AUTH_ENCRYPTION_KEY`).
5. Build canonical request, compute signature, constant-time compare ([access_key_auth.py:125](access_key_auth.py)).

## What's NOT registered today

- `rate_limit.py` and `banhammer.py` were REMOVED in the gateway/api merge PR: both were parked (never registered, even pre-merge — the old `gateway/main.py` held only a commented-out banhammer registration) and their only config consumer was the deleted `GatewayConfig`. Recover from git history if the features are revived; the user-facing unban endpoint in [hippius_s3/api/user.py](../../api/user.py) still clears the `hippius_banhammer:*` Redis keys.

See [todo.md](../../../todo.md) P2 for re-enablement discussion.

