# 15 — Account / Credential / Identity model (greenfield Rust S3)

**Status:** design draft for review. Every ⚑ is a decision that needs a human sign-off.
**Date:** 2026-09-15.
**Companions:** [`05-auth-authz-billing.md`](./05-auth-authz-billing.md) (the live auth gate, traced to Python),
[`11-hcfs-prerequisites.md`](./11-hcfs-prerequisites.md) (the *if-hcfs-changed* scoped-credential design, superseded as a build item but the reference for §4),
[`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md) (the chosen **zero-hcfs-change** integration + admin-bearer facts),
[`12-schema-design.md`](./12-schema-design.md) (the greenfield Postgres schema this extends).

> **Why this doc is foundational.** Two independent constraints meet in the identity model:
> 1. **Auth** — who is calling, resolved from a SigV4/bearer/presigned credential.
> 2. **Billing** — per-tenant usage→chain attribution only works if each tenant's bytes are stored in
>    hcfs under that tenant's **real ss58** and that ss58 is **non-exempt**
>    ([`13`](./13-hcfs-as-is-integration.md) §4, §5; [`11`](./11-hcfs-prerequisites.md) §6). The ss58 that
>    authenticates the request is the *same* ss58 that must be attributed to hcfs. So auth and billing are
>    the same identity, resolved once, and everything downstream keys on it.
>
> All `hippius_s3/…` refs are to this repo; all `hcfs-server/…` / `gates.rs` refs are to
> `/Users/camden/Source/hcfs`.

---

## 0. TL;DR and decision list

**The identity spine, end to end:**

```
hip_ access key ──(api.hippius.com token-auth, §1)──► account_address (SS58) + token_type{master,sub}
        │                                                        │
   SigV4/presigned/bearer verify (§2)                    the SS58 *is* the tenant identity
        │                                                        │
   authZ: master bypass / sub-token scope / bucket ACL (§2.4)    │
        │                                                        ▼
   bucket.account_id (owner SS58) ── the BILLED subject ──► credit gate (keyed SS58)
        │                                                        │
        ▼                                                        ▼
   hcfs store/delete with account_ss58 = owner SS58 (§4,§5) ──► user_summaries bare row ──► chain-reporter (per-tenant)
```

The tenant is **not** a new abstraction we invent — it is the SS58 that api.hippius.com already binds to
each `hip_` key. Our Postgres keys tenancy on that SS58 (`accounts.account_id`, [`12`](./12-schema-design.md) §2.1).

### Decisions (⚑)

- **⚑ D1 — Keep remote identity assertion in v1; do NOT mint our own `hip_` keys.** api.hippius.com stays
  the credential + SS58 + `token_type` authority (`POST /objectstore/tokens/auth/`,
  `hippius_s3/services/hippius_api_service.py:401`). We reproduce the 60 s Redis auth cache and NaCl
  secret decryption (§1). Rationale: the SS58 binding is the billing key and must come from the same
  authority the console/product mints keys with; owning a second credential store would fork that
  authority and add secret-at-rest blast radius for no v1 benefit.
- **⚑ D2 — Own a durable `accounts` (tenant) row in *our* Postgres, upserted on first successful auth.**
  Billing attribution, suspension, and admin ops must not depend on a live api.hippius.com call. The row
  is keyed on SS58 and **stores no secret** (§1.3, §5).
- **⚑ D3 — Harvest the hcfs `service`-branch SigV4/SigV2 verifier**, but replace its local-secret source
  (Sled) with the remote-assert-then-NaCl-decrypt flow ([`05`](./05-auth-authz-billing.md) §7a/§7b). It is
  better hardened than the Python (clock skew, unsigned-hash rejection) — directly relevant to
  CVE-2026-54330 (§2.3).
- **⚑ D4 — Enforce sub-token scopes in v1 (port `sub_token_scope.py`), OR fail-closed reject
  `token_type=="sub"`.** A sub token resolves to the *same* SS58 as its master; if we accept it without
  the scope gate we silently grant it full-account access — a privilege escalation. The R2 4-tier matrix
  is ~190 lines of pure logic and a frozen contract; **recommend porting it** (§2.4). Do **not** ship
  "sub accepted, scope ignored."
- **⚑ D5 — The SS58 sent to hcfs is *always* derived from our own auth result, never from a client field.**
  This is the primary compensating control for the admin-bearer blast radius (§4). The billed/attributed
  subject is the **bucket owner** SS58 (`buckets.account_id`), matching Python's `_billed_account` and
  the branch's `billing_user_id(user, owner_id)` ([`05`](./05-auth-authz-billing.md) §5b, §7a).
- **⚑ D6 — Push for the doc-11 *scoped* hcfs service token (`HCFS_S3_SERVICE_TOKEN` + `X-HCFS-Account`)
  as the one worthwhile hcfs change, even under "zero-change".** v1 ships on the admin bearer
  (`HCFS_ADMIN_BEARER_TOKEN`, `gates.rs:82`) because doc 13 chose zero hcfs changes, but the admin bearer
  can act as *any* SS58 on *every* hcfs endpoint (§4). The scoped token shrinks that to the blob surface
  ([`11`](./11-hcfs-prerequisites.md) §3). This is the single largest residual risk in the design.
- **⚑ D7 — Give the S3 product its own `HCFS_STORAGE_S3_PREFIX`** so its content-addressed blobs never
  share a storage key with a Drive file, whose unconditional delete would otherwise reclaim shared bytes
  ([`11`](./11-hcfs-prerequisites.md) §1.5; [`13`](./13-hcfs-as-is-integration.md) §6).
- **⚑ D8 — `accounts.account_id == SS58` (no internal opaque tenant id) in v1.** Add a surrogate
  `tenant_id uuid` only if a tenant must ever remap its SS58 or hold several (§5.2).

---

## 1. How S3 tenants get credentials

### 1.1 The two models, and why we keep remote assertion

| | **(A) Remote assertion — keep api.hippius.com** *(recommended, D1)* | **(B) Own credential store** |
|---|---|---|
| Who mints `hip_` keys | api.hippius.com / console (unchanged) | us |
| Where the secret lives | never stored; NaCl-wrapped, handed back per auth, cached 60 s, decrypted in-process | our Postgres/KMS, at rest, forever |
| SS58 binding | **authoritative from upstream** (`account_address`) | we must still source SS58 from upstream or chain |
| `token_type` (master/sub) | authoritative from upstream | we own it |
| Revocation | upstream `status != "active"`, ≤ 60 s lag | instant, ours |
| Availability coupling | hard dep on api.hippius.com (503 on cold-cache miss) | none |
| Latency | round trip on cache miss, then 60 s free | local |
| Blast radius of *our* secret | just the 32-byte NaCl decrypt key (`HIPPIUS_AUTH_ENCRYPTION_KEY`) — a leak needs the *ciphertext* too | the whole secret table |

**Decision (D1):** model (A). The SS58 is the billing key and the product already mints keys against it
upstream; forking that authority buys nothing in v1 and adds a permanent secret-at-rest liability. We
mitigate (A)'s availability coupling with D2 (durable local tenant row + last-known-good), exactly as
the plan-cache/credit-cache already survive upstream outages ([`05`](./05-auth-authz-billing.md) §5d).

Industry corroboration: Ceph RGW and MinIO both resolve a SigV4 access key to a *policy-bearing user*
via a single credential surface, and both are moving *away* from long-lived static secrets toward
STS/short-lived creds — remote assertion with a short cache is the same shape, one step further along.

### 1.2 The verification back-end to reproduce (from Python)

All key-based methods resolve the secret the same way ([`05`](./05-auth-authz-billing.md) §1):

1. **`cached_auth(access_key, redis)`** — Redis `hippius_auth:{key}`, TTL **60 s**
   (`hippius_s3/gateway/services/auth_cache.py:13,17`). Miss → `HippiusApiClient.auth(key)` POSTs
   `{"accessKeyId": key}` to `POST /objectstore/tokens/auth/`
   (`hippius_s3/services/hippius_api_service.py:401`), caches `TokenAuthResponse`.
2. **`TokenAuthResponse`** (`hippius_s3/services/hippius_api_service.py:70`):
   `valid, status, account_address, token_type, encrypted_secret, nonce` — every field but `valid` is
   optional so the `{valid:false, detail}` error shape parses; callers gate on `valid` first.
3. **Validity gates** (`hippius_s3/gateway/middlewares/access_key_auth.py:85-103`): `valid==True`,
   `status=="active"`, `account_address` matches `SS58_PATTERN` (`models/sub_token.py:19`),
   `token_type ∈ ALLOWED_TOKEN_TYPES={master,sub}` (`access_key_auth.py:46`), secret+nonce present.
4. **Secret decryption** (`hippius_s3/gateway/services/auth_service.py:14`): NaCl `SecretBox` over
   `base64(encrypted_secret)` with the 32-byte-hex `HIPPIUS_AUTH_ENCRYPTION_KEY`
   (`config.py:214`, `hippius_secret_decryption_material`). The `nonce` arg is accepted for API
   compatibility but unused (nonce is embedded in the box).

Access-key shape gate: `ACCESS_KEY_PATTERN = ^hip_[a-zA-Z0-9_-]{1,240}$` (`models/sub_token.py:18`).

### 1.3 How an access key maps to tenant, SS58, and credit

```
hip_ key ──cached_auth──► account_address (SS58)   ← the tenant id AND the hcfs attribution key AND the credit key
         └─────────────► token_type {master,sub}   ← selects the authZ path (§2.4)

credit:  SS58 ──► hippius_main_account_credits:{SS58} (redis-accounts, has_credits default TRUE on cold cache)
                 (hippius_s3/gateway/services/account_service.py:42-52)
```

- **One SS58, many keys.** A tenant may hold many `hip_` keys (master + several subs), all resolving to
  one `account_address`. The tenant is the SS58, not the key.
- **The durable local row (D2).** On each *successful* auth, upsert `accounts(account_id=SS58)` (see §5).
  This makes the SS58 a first-class, own-DB fact — needed for suspension state, an exempt flag mirror,
  `created_at`, and to attribute/bill even while api.hippius.com is down (the auth cache already gives us
  60 s of coverage; the row gives us the identity forever).
- **No secret in our DB.** The decrypted secret lives only in the request's memory + the 60 s Redis
  `hippius_auth:` cache. We hold only the decrypt key. (D1's whole point.)

---

## 2. SigV4 / presigned verification in the Rust service

### 2.1 Where the secret comes from (the one real divergence from the harvest)

Harvest the hcfs `service`-branch verifier (`hcfs-server/src/s3/auth/signing.rs`,
[`05`](./05-auth-authz-billing.md) §7a): `verify_sigv4` (header), `verify_sigv4_query_string`
(presigned), `verify_sigv2_query_string` (legacy), plus `derive_signing_key`,
`build_canonical_request`, `constant_time_eq`. It maps ~1:1 onto §1a/§1b of doc 05.

**But** the branch stores the raw secret locally in Sled and derives the signing key from it. We do **not**
store the secret — replace `get_credentials` with the §1.2 flow ([`05`](./05-auth-authz-billing.md) §7b):

```
verify_sigv4(request):
    key       = parse Credential=  (must match ACCESS_KEY_PATTERN, start "hip_")
    resp      = cached_auth(key)                    # Redis 60s → api.hippius.com
    gate(resp.valid && resp.status=="active" && SS58_PATTERN(resp.account_address) && resp.token_type ∈ {master,sub})
    secret    = nacl_secretbox_open(b64(resp.encrypted_secret), HIPPIUS_AUTH_ENCRYPTION_KEY)   # per-verify
    k_signing = HMAC-chain("AWS4"+secret, date, region, "s3", "aws4_request")
    assert constant_time_eq(hmac(k_signing, string_to_sign), provided_signature)
    identity  = { access_key: key, ss58: resp.account_address, token_type: resp.token_type }
```

### 2.2 Caching

- **Auth cache (Redis `hippius_auth:`, 60 s):** the only cache required. It amortizes the api.hippius.com
  round trip and carries `encrypted_secret` so each verify decrypts locally. Revocation lags ≤ 60 s
  (accepted; matches Python). Provide an admin cache-bust (§3) for immediate revocation.
- **Do not persist the derived signing key.** It is cheap (four HMACs) and per-`(secret, date, region)`;
  the 60 s secret cache already bounds the work. (An optional in-process LRU keyed
  `(access_key, yyyymmdd)` is a micro-opt — flag, not v1.)
- Reuse a long-lived `reqwest` client for api.hippius.com (the "shared warm pool" note,
  `auth_cache.py:26`).

### 2.3 Presigned + the CVE-2026-54330 lesson

Presigned rules to keep ([`05`](./05-auth-authz-billing.md) §1b): `X-Amz-Expires ∈ [1, 604800]`,
`SignedHeaders` **must include `host`**, canonical query excludes `X-Amz-Signature`, payload defaults to
`UNSIGNED-PAYLOAD`.

> **⚠ SECURITY.** The branch verifier already **rejects an unsigned `x-amz-content-sha256`** on header
> SigV4 and does **±15 min clock-skew** — two guards the Python lacks
> ([`05`](./05-auth-authz-billing.md) §7a). This is exactly the class of bug behind **CVE-2026-54330**
> (Ceph RGW, 2026): a presigned-URL holder adding unsigned `x-amz-*` headers to gain capability beyond
> the signer's intent. Keep the branch's stricter posture; ensure every `x-amz-*` header that affects
> behavior is in the signed set or is rejected. This is a reason to harvest the Rust verifier rather than
> re-port the looser Python.

### 2.4 Master vs sub-token scopes — do we need subs in v1?

Both master and sub **authenticate identically** (same secret verify). The difference is **authorization
scope**, and it is not optional to *decide*:

- **Master** (`token_type=="master"`): full authority over its own account. In hcfs terms this is the
  "master-token bucket-owner bypass" ([`05`](./05-auth-authz-billing.md) §2.7). In our service it means:
  once the SS58 owns the bucket, skip the ACL grant scan.
- **Sub** (`token_type=="sub"`): the *same* SS58, but constrained by a stored scope. The R2 4-tier matrix
  (`hippius_s3/gateway/services/sub_token_scope.py`): `admin_read_write` / `admin_read` /
  `object_read_write` / `object_read`, each × a bucket list; `create_bucket` requires
  `bucket_scope==all` (`sub_token_scope.py:182`); `required_op` maps method+query→op
  (`sub_token_scope.py:121`); `evaluate` is fail-closed on a missing scope (`:172`). Cross-account subs
  fall through to the bucket ACL grant scan (the contractor pattern,
  [`05`](./05-auth-authz-billing.md) §2.6).

**⚑ D4.** Because a sub resolves to the same SS58 as its master, **accepting a sub without enforcing its
scope grants full-account access.** Two acceptable v1 postures:

1. **Enforce (recommended).** Port `sub_token_scope.py` (pure logic, a frozen contract per
   [`00-index.md`](./00-index.md)) + store scope rows in `api_credentials`
   ([`12`](./12-schema-design.md) §2.2 already models exactly this table:
   `access_key_id, account_id, permission, bucket_scope, bucket_ids`). Load with a **fail-closed** 60 s
   cache (Python's `hippius_subscope:` behavior, [`05`](./05-auth-authz-billing.md) §2.6).
2. **Fail-closed reject.** If scope enforcement can't ship in v1, **reject `token_type=="sub"` at auth**
   (403) rather than silently widening it. This breaks existing sub-key holders — a product call.

Never ship option (1)-without-enforcement. `api_credentials` scope is **our own DB** (like Python's
`sub_token_scopes`); the secret/SS58/`token_type` still come from api.hippius.com. That is the clean split:
**upstream owns the credential; we own its scope.**

---

## 3. Provisioning / lifecycle

- **Tenant provisioning is lazy.** There is no tenant-create API in the S3 service — a tenant exists the
  first time a valid `hip_` key resolves to an SS58, at which point we upsert `accounts(account_id=SS58)`
  (D2). Account and key *creation* stay with api.hippius.com / the console.
- **Bucket creation.** `CreateBucket` writes `buckets(account_id = SS58)` — the owner
  ([`12`](./12-schema-design.md) §2.4). Requires a master token, or a sub with `bucket_scope==all`
  (`sub_token_scope.py:182`). Ceph-style, each tenant SS58 gets its own bucket namespace; bucket names
  are globally unique over live rows (`uq_buckets_name_live`, [`12`](./12-schema-design.md) §2.4) — decide
  ⚑ whether to keep S3-global names or namespace by SS58 (open question, §6).
- **Credential rotation.** Owned upstream: rotating a `hip_` key at api.hippius.com propagates within the
  60 s cache TTL; because one SS58 holds many keys, rotation is add-new-then-revoke-old with zero downtime.
  The hcfs service credential rotates via a comma-separated set (D6 / [`11`](./11-hcfs-prerequisites.md)
  §3.1); `HIPPIUS_AUTH_ENCRYPTION_KEY` rotation is a coordinated change with api.hippius.com.
- **Revocation.** Upstream `status != "active"` denies within ≤ 60 s. For **immediate** revocation expose
  an admin endpoint that deletes `hippius_auth:{key}` (and the sub-scope cache key) from Redis — cache-bust,
  no data change. Sub-scope revocation = delete the `api_credentials` row (fail-closed cache expires ≤ 60 s).
- **Suspension** (billing control, not auth). Port the Python model: an `account_suspensions` row
  (`present = suspended`, `mode ∈ {full, read_only}`), 30 s write-through Redis cache, **fail-open**,
  driven by **admin-HMAC** endpoints (`POST /admin/accounts/{ss58}/suspend|reactivate`,
  [`05`](./05-auth-authz-billing.md) §3b, §4a). The suspended subject is enforced both as caller and as
  **bucket owner** (so cross-account and public reads of a suspended owner's buckets are blocked).

---

## 4. admin-bearer blast-radius mitigation

### 4.1 The exposure (grounded in hcfs code)

The chosen zero-change integration ([`13`](./13-hcfs-as-is-integration.md)) has the S3 service hold
`HCFS_ADMIN_BEARER_TOKEN` (`hcfs-server/src/auth/gates.rs:82`; digest+`ct_eq` compare `is_admin_token`
`gates.rs:146`). The admin token is verified but **short-circuits the `ss58 == owner` check on every
hcfs endpoint**:

- `validate_and_authorize` treats admin as authorized before the substrate-address compare
  (`gates.rs:~290`).
- `authorize_drive_access` returns `caller_ss58 = None` for admin and skips the owner/membership check
  (`gates.rs:~684`; `AuthorizedWriter.caller_ss58` is `Option`, `None` only for admin, `gates.rs:652,658`).
- `authenticate_caller` returns `None` for admin (`gates.rs:~851`).

So a leaked admin bearer can **read, write, and delete any hcfs account's data — Drive files included, not
just S3 blobs** ([`13`](./13-hcfs-as-is-integration.md) open question 1). It is one high-value secret with
a cluster-wide blast radius.

### 4.2 Mitigations (defense in depth)

1. **⚑ D5 — the app-level control that actually bounds the power: the S3 service never lets a client steer
   the SS58 it sends to hcfs.** The `account_ss58` passed to hcfs is *always* derived from our own auth
   result (the **bucket owner** SS58, resolved server-side), never from a request header/body/query. hcfs
   trusts the caller-named account under the admin bearer
   ([`13`](./13-hcfs-as-is-integration.md) §4 shows `authorize_claimed_ss58` accepts any claimed SS58 when
   `caller_ss58 is None`); therefore *our* auth boundary is the real enforcement point. This confines the
   admin bearer to "act as the tenant we already authenticated," turning a cross-tenant primitive into a
   same-tenant one **in normal operation**. (It does not protect against a *leak* of the token itself —
   that's 2–4.)
2. **Network isolation.** hcfs blob/gateway endpoints reachable **only** from the S3 service pods
   (k8s `NetworkPolicy`, deny-all ingress otherwise; ideally hcfs is cluster-internal, never
   internet-exposed). The admin bearer never traverses a public hop. (Note the standing k8s config-drift
   caveat: verify the live NetworkPolicy, don't trust the manifest — per repo memory.)
3. **Secret handling.** Inject from the secret manager (Rancher/Vault/k8s Secret), never baked into an
   image or a committed manifest. `parse_bearer_secret` fail-closes on the unrotated placeholder
   (`gates.rs:74`), so a misconfigured deploy fails rather than runs on a default. Rotate on a schedule.
4. **⚑ D6 — replace admin with a *scoped* service token (fast-follow).** [`11`](./11-hcfs-prerequisites.md)
   §3 designs `HCFS_S3_SERVICE_TOKEN` + a per-request `X-HCFS-Account: {ss58}` header and an
   `authorize_blob_service` gate that authenticates the *service* then attributes to a validated SS58 —
   scoped to the blob surface, with the `REPAIR_BEARER_TOKEN` precedent (`gates.rs:114`) showing hcfs
   already supports separately-scoped tokens. A leak is then contained to blob store/get/delete, not all
   admin ops (suspend/purge/any-Drive). This is the **one hcfs change worth pushing for** despite the
   zero-change decision, because it is the only structural fix to §4.1.
5. **⚑ D7 — own storage prefix.** Set `HCFS_STORAGE_S3_PREFIX` so S3 blobs and Drive files never collide
   on a content-addressed key (else a Drive `delete_file`'s unconditional reclaim can drop a refcounted S3
   blob, [`11`](./11-hcfs-prerequisites.md) §1.5).
6. **Audit + reconcile.** hcfs already `warn!`s "Admin token used for …" on every admin-token request
   (`gates.rs:~289,~686`). The S3 service logs every hcfs call with `(tenant_ss58, ray_id, op)` so admin-token
   use is reconcilable against our own authenticated requests — any hcfs admin call with no matching S3
   request is an alarm.
7. **mTLS** between the S3 service and hcfs at ingress — orthogonal transport-layer identity on top of the
   app token ([`11`](./11-hcfs-prerequisites.md) §3.3).

---

## 5. The tenant↔ss58 mapping table and billing attribution

### 5.1 The mapping is (deliberately) near-degenerate

Because identity is upstream-asserted as an SS58, **the tenant *is* the SS58**; there is no separate
tenant key to map to it. [`12`](./12-schema-design.md) §2.1 already encodes this:

```sql
CREATE TABLE accounts (                    -- one row per tenant SS58 (our durable record, D2)
    account_id  text PRIMARY KEY,          -- SS58 main account address == tenant id
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT accounts_id_not_sentinel CHECK (
        btrim(account_id) <> '' AND lower(account_id) NOT IN ('anonymous','none','null','undefined'))
);
```

Recommended additive columns for lifecycle/billing (extend §2.1, all own-DB, **no secret**):

```sql
ALTER TABLE accounts
    ADD COLUMN first_seen_at   timestamptz NOT NULL DEFAULT now(),  -- lazy provisioning (§3)
    ADD COLUMN last_auth_at    timestamptz,                          -- observability / stale-tenant reaping
    ADD COLUMN is_exempt       boolean NOT NULL DEFAULT false,       -- mirror of billing exempt status (see caveat)
    ADD COLUMN suspended_mode  text CHECK (suspended_mode IN ('full','read_only'));  -- or a separate account_suspensions table (§3)
```

- `buckets.account_id → accounts.account_id` FK already binds every bucket (hence every object/blob) to
  the owner SS58 ([`12`](./12-schema-design.md) §2.4). That owner SS58 is the billed + attributed subject.
- **`api_credentials.account_id → accounts.account_id`** ([`12`](./12-schema-design.md) §2.2) records the
  `hip_` → SS58 edge for **sub-token scope** rows. This is the only place a key string is stored, and it
  stores the *scope*, never the secret.

### 5.2 ⚑ D8 — SS58 as the id, no surrogate, in v1

Keep `account_id == SS58`. Introduce a surrogate `tenant_id uuid` **only** if a future need appears:
a tenant that must rotate/replace its SS58, or hold several SS58s under one billing entity. Both are
speculative; the indirection would touch every `buckets.account_id`/`api_credentials.account_id` join and
every hcfs attribution call for no v1 payoff. Flagged so the choice is conscious.

### 5.3 How the mapping feeds billing attribution (the load-bearing part)

The whole point of D5's "bill the owner" + non-exempt tenants ([`13`](./13-hcfs-as-is-integration.md) §4,
[`11`](./11-hcfs-prerequisites.md) §6):

```
object write lands ciphertext blobs in hcfs
    │
    ├─ S3 service resolves BILLED subject = bucket.account_id (owner SS58)      ← D5, not the caller
    │      (matches Python _billed_account, 05 §5b; branch billing_user_id(user, owner_id), 05 §7a)
    │
    ├─ credit gate (own or hcfs /can_upload) keyed on that SS58                  ← non-exempt ⇒ gated (13 §5)
    │
    └─ hcfs store called with account_ss58 = owner SS58
           → hcfs writes user_summaries BARE row {ss58} delta (upload.rs:608-609)
           → hcfs-chain-reporter classifies bare {ss58} (identify.rs:16-24,47)
           → s3_bytes = max(0, bare − Σfolders − shares)  → pallet_marketplace + pallet_arion, per tenant
```

Attribution is **per tenant iff** each blob op carries the tenant's real SS58; a single shared service
SS58 collapses all tenants into one on-chain row ([`13`](./13-hcfs-as-is-integration.md) §4 warning). So:

- **⚑ Tenant SS58s must be non-exempt** to be reported on chain — a listed
  `HCFS_EXEMPT_ACCOUNTS` SS58 is written `bill:false` and skipped by the reporter
  ([`13`](./13-hcfs-as-is-integration.md) §4; helpers.rs:453). Non-exempt implies hcfs quota-gated — the
  coupling ("reported ⇔ gated") is inherent to the existing surface; decoupling requires owning billing
  ourselves ([`13`](./13-hcfs-as-is-integration.md) §5).
  - **Caveat on `accounts.is_exempt`:** it must be a *read-only mirror* of the hcfs-side
    `HCFS_EXEMPT_ACCOUNTS` config, or attribution intent and reality diverge. Reserve exemption for the
    S3 service's *own* infra account, not tenants ([`11`](./11-hcfs-prerequisites.md) §3.2).
- **Cross-account writes** (a contractor sub writing to another owner's bucket) attribute to the **bucket
  owner**, not the caller — the whole reason D5 resolves the owner. This matches hcfs's own
  `billing_user_id(user, owner_id)` ([`05`](./05-auth-authz-billing.md) §7a).
- **Shared bare-row caveat:** if a tenant SS58 *also* uses native hcfs Drive, its bare `{ss58}` row mixes
  Drive + S3 and the credit gate prices the combined total. Keep S3 tenant SS58s disjoint from Drive users
  ([`13`](./13-hcfs-as-is-integration.md) open question 4).

---

## 6. Open questions

1. **✅ RESOLVED (D4).** Port `sub_token_scope.py` and **enforce** the R2 4-tier scopes in v1,
   **fail-closed** (register B2). Rejecting-all-subs is not the path.
2. **✅ RESOLVED (D6 / B3).** Ship v1 on the admin bearer (always-derive-ss58 from our auth +
   network-locked egress + mTLS); land the scoped `HCFS_S3_SERVICE_TOKEN` as a fast-follow **before
   production cutover**, not a Phase-0 blocker.
3. **Bucket-name namespacing.** S3-global unique names (`uq_buckets_name_live`,
   [`12`](./12-schema-design.md) §2.4), or per-tenant-SS58 namespaces (Ceph RGW multitenancy style)? The
   latter removes cross-tenant name contention and the name-reuse cache-poisoning class
   ([`05`](./05-auth-authz-billing.md) §2.2) but diverges from vanilla-S3 client expectations.
4. **Immediate revocation surface.** Is a Redis `hippius_auth:` cache-bust admin endpoint (§3) enough, or
   do we need a shorter auth-cache TTL for the S3 product (trading api.hippius.com load for tighter
   revocation)?
5. **✅ RESOLVED — `has_credits` fail-open (register B5).** Keep fail-open on a cold credit cache; it's
   **safer here** than in Python because HCFS's own per-tenant 402 gate is a second backstop at store
   time. (No tighten-to-fail-closed.)
6. **`HIPPIUS_AUTH_ENCRYPTION_KEY` custody & rotation.** Same 32-byte key as Python (`config.py:214`),
   or a distinct key for the Rust product? Rotation is a coordinated change with api.hippius.com — who owns
   the schedule?
7. **Anonymous + presigned parity.** Confirm we reproduce anonymous GET/HEAD routing
   ([`05`](./05-auth-authz-billing.md) §1e) and the presigned `UNSIGNED-PAYLOAD` footgun posture (§2.3) vs.
   requiring a signed payload hash for the S3 product.
8. **Surrogate `tenant_id` (D8).** Confirm no near-term need for SS58 remap / multi-SS58 tenants before
   committing to `account_id == SS58` (changing later touches every attribution join).

---

*Cross-repo: `hcfs` = `/Users/camden/Source/hcfs` (auth in `hcfs-server/src/auth/gates.rs`,
S3 SigV4 harvest in `hcfs-server/src/s3/auth/signing.rs`); `hippius-s3` = this repo (live auth in
`hippius_s3/gateway/services/` + `gateway/middlewares/`). Sources for the external SigV4/multi-tenancy
notes: Ceph RGW authentication & multitenancy docs, and CVE-2026-54330 (RGW SigV4 unsigned-header flaw).*
