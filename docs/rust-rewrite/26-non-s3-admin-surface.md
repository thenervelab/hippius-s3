# 26 — The non-S3 API surface: who serves it after cutover

> **Scope of this doc.** The Rust rewrite plan (docs [05](05-auth-authz-billing.md),
> [15](15-account-credential-model.md), [IMPLEMENTATION-PLAN](IMPLEMENTATION-PLAN.md) §10–11)
> nails down the **S3 API + billing/plan gate + suspension enforcement**. It does **not**
> enumerate the *rest* of what `hippius-s3` serves. At cutover the Python `api` Deployment is
> **scaled to 0** ([09](09-ops-deploy-observability.md) §… table row 2: "api | Deployment |
> **scaled to 0 at cutover**"). So every non-S3 route the console, the billing backend, staff
> tooling, ATS, and k8s currently hit must have a new owner, or it silently 404s the moment
> Python goes away.
>
> This doc inventories every non-S3 route registered in `hippius_s3/main.py`, categorizes each
> (IN-SCOPE v1 / DEFER / SEPARATE SERVICE / STAYS ON PYTHON / OBSOLETE), and states the cutover
> implication. It deliberately does **not** re-cover the S3 API itself (docs 06/22) nor
> re-derive the auth mechanisms (doc 05 §3–4 already do).
>
> **Load-bearing distinction used throughout:** *auth mechanism* (does Rust implement admin-HMAC
> / frontend-HMAC?) vs *route ownership* (does Rust serve the endpoints those secrets protect?).
> Doc 05 answers the first for suspend/scope; this doc answers the second for the whole surface.

---

## 0. Route census (everything registered in `main.py`)

Registration order, with the S3 surface elided:

| # | Mount | Source | Auth layer | Non-S3? |
|---|-------|--------|-----------|---------|
| — | `GET /robots.txt` | `main.py:582` (inline) | none | ✅ non-S3 |
| — | `GET /health` | `main.py:617` (inline) | none (middleware-exempt) | ✅ non-S3 |
| — | `/docs`, `/redoc`, `/openapi.json` | `main.py:476-478` (cond. `enable_api_docs`) | none | ✅ non-S3 |
| — | `/static/*` mount | `main.py:638-639` (`StaticFiles`) | none | ✅ non-S3 |
| 1 | `/user/*` | `main.py:622` → `api/user.py` | frontend-HMAC | ✅ non-S3 |
| 2 | `/user/sub-tokens/*` | `main.py:623` → `api/sub_token_scopes.py` | frontend-HMAC | ✅ non-S3 |
| 3 | `/admin/*` | `main.py:626` → `api/admin.py` (`include_in_schema=False`) | admin-HMAC | ✅ non-S3 |
| 4 | `/public/{bucket}/{key}` | `main.py:627` → `api/s3/public_router.py` | anonymous + ACL | S3-adjacent |
| 5 | `/internal/parts/…` | `main.py:634-635` (cond. `peer_serve_enabled` + secret) → `api/internal_parts.py` | peer-auth | ✅ non-S3 |
| 6 | `/{bucket}/{key:path}` etc. | `main.py:636` → `api/s3/router.py` | SigV4 / bearer / presigned | S3 (out of scope here) |

Two things that look like a surface but are **not HTTP routes**:

- **The ATS auth-probe** — `auth_probe_middleware` (`gateway/middlewares/auth_probe.py:34`)
  short-circuits any request carrying a valid `X-Hippius-Auth-Probe` to an empty `200`. It is a
  middleware, not a route, but it is a real *integration contract* with the ATS `authproxy`
  plugin. Covered in §7.
- **Service-account management** — there is **no** service-account CRUD endpoint. The allowlist is
  the `HIPPIUS_SERVICE_ACCOUNT_IDS` env var (`services/service_accounts.py:100`), consumed by
  `is_service_account` (`:15`) and the destructive-op guards (`:72`, `:103`). Managed
  declaratively via k8s secret + redeploy. Covered in §6.
- **Metrics** — there is **no** `/metrics` route. Telemetry is **pushed** over OTLP to the
  collector; the collector owns the scrape surface (`monitoring.py`; [09](09-ops-deploy-observability.md)
  §3, §6.3: "Do NOT add a Prometheus `/metrics` endpoint"). Covered in §5.

---

## 1. Admin API — `/admin/*` (admin-HMAC)

**Source:** `hippius_s3/api/admin.py`; gate `gateway/middlewares/admin_hmac.py:19`; docs
`docs/admin-api.md`, `docs/example-admin.md`.

### Inventory

| Verb | Path | Does | Handler | Who calls |
|------|------|------|---------|-----------|
| POST | `/admin/accounts/{id}/suspend` | Upsert `account_suspensions` row (`full`/`read_only`), write-through Redis | `admin.py:178` | Billing backend / staff cockpit |
| POST | `/admin/accounts/{id}/reactivate` | Delete suspension row (409 if purge active) | `admin.py:212` | Billing backend / staff cockpit |
| GET | `/admin/accounts/{id}/status` | Suspension state + live bucket count + logical bytes | `admin.py:246` | Staff cockpit |
| DELETE | `/admin/accounts/{id}/data` | Enqueue `purge_jobs` row + force `full` suspension → `202 {job_id}` | `admin.py:289` | Billing backend (account deletion) |
| GET | `/admin/purge-jobs/{job_id}` | Poll purge progress | `admin.py:328` | Billing backend (polls its own job) |

**Auth:** admin-HMAC — `X-HMAC-Signature = hex(HMAC-SHA256(HIPPIUS_ADMIN_HMAC_SECRET, METHOD +
PATH[+?QUERY]))`, dedicated secret, **fail-closed** (empty secret → `403 "Admin API is not
enabled"`, `admin_hmac.py:41`). Target SS58 in the signed path, never the body.

**Protective invariant to carry over:** `_refuse_service_account` (`admin.py:115`) 403s any
suspend/purge aimed at an SS58 in `HIPPIUS_SERVICE_ACCOUNT_IDS`, *before* any write — mirrors
`services/service_accounts.py:refuse_destructive_operation`. The Rust admin surface must
reproduce this guard or a mistyped SS58 can take our own ingest offline.

### Category: **IN-SCOPE v1** (partly already committed)

Doc 05 §3b and doc 15 §3 already commit the Rust product to **admin-HMAC suspend/reactivate**
and the `account_suspensions` model, because suspension is enforced *inside* the S3 request
path (`suspension_middleware`, doc 05 §4a) and that enforcement is worthless without an endpoint
to set the row. **This doc extends that:** `status`, `DELETE .../data` (purge), and
`GET /purge-jobs/{id}` are *also* in-scope, because:

- **Purge is the account-deletion path.** When a user deletes their Hippius account, the billing
  backend calls `DELETE /admin/accounts/{id}/data`. If Rust does not serve it, account deletion
  silently stops reclaiming S3 storage the moment Python scales to 0 — a compliance/GDPR and a
  cost problem. The purge *worker* is already in-scope ([17](17-workers-and-background-tasks.md));
  the endpoint that feeds it must be too.
- **`status` is the cockpit's read model.** Staff tooling reads it to show account state + usage.
  Load-bearing for the internal cockpit product.

**Cutover implication:** all five endpoints must exist in the Rust app before Python scales to 0.
The `purge_jobs` / `account_suspensions` tables move into the Rust DB (doc 12 schema). The
billing backend's HMAC signing is unchanged (same scheme, same secret env var), so **no backend
change is required** if Rust keeps the path shapes byte-identical. **Flag:** keep the paths and
the HMAC message construction (`METHOD + PATH[+?QUERY]`) exactly — the backend signs the literal
request line (`docs/example-admin.md`), so any path rename breaks every signature.

**Open sub-question:** the purge endpoint's "logical bytes" and `status`'s bucket/byte aggregate
depend on the storage schema; recompute against the new dedup/refcount model (doc 12), don't
port the SQL verbatim.

---

## 2. Sub-token scope API — `/user/sub-tokens/*` (frontend-HMAC)

**Source:** `hippius_s3/api/sub_token_scopes.py`; gate `gateway/middlewares/frontend_hmac.py:27`.

### Inventory

| Verb | Path | Does | Handler | Who calls |
|------|------|------|---------|-----------|
| GET | `/user/sub-tokens/{access_key_id}/scope` | Read the stored R2-style scope record | `sub_token_scopes.py:197` | Console/frontend |
| PUT | `/user/sub-tokens/{access_key_id}/scope` | Upsert scope (perm tier × bucket list), validate ownership via `HippiusApiClient.auth`, invalidate 60 s cache | `sub_token_scopes.py:254` | Console/frontend |
| DELETE | `/user/sub-tokens/{access_key_id}/scope` | Delete scope row (idempotent), invalidate cache | `sub_token_scopes.py:327` | Console/frontend |

**Auth:** frontend-HMAC (`X-HMAC-Signature` with `FRONTEND_HMAC_SECRET`). Callers are the
console, never end users. Writes go to the `sub_token_scopes` table and invalidate
`hippius_subscope:` Redis keys so the ACL middleware sees changes ≤ 60 s.

### Category: **IN-SCOPE v1** (write side is the missing half of doc 15 D4)

Doc 15 §2.4 / D4 already commits Rust to **enforcing** sub-token scopes fail-closed (or rejecting
sub-tokens outright), and says the scope rows are "our own DB" (`api_credentials`, doc 15:214).
But enforcement reads a row that **something must write**. The console is that something, via
these three endpoints. If Rust enforces scopes but does not serve the write API:

- the console can no longer create/edit/revoke sub-token scopes,
- **and** because enforcement is fail-closed, every sub-token whose scope can't be (re)installed
  **default-denies** — an outage of the sub-token product, not a graceful degrade.

So the write endpoints are **strictly coupled** to the D4 enforcement decision. If Rust ships D4
option 1 (enforce), it **must** ship these three. If Rust ships D4 option 2 (reject all
sub-tokens in v1), these can **DEFER** with the enforcement — but then the console's sub-token
management UI must be disabled in lockstep.

**Load-bearing for the console:** yes, iff sub-tokens are offered as a product feature in v1.

**Cutover implication:** endpoints + `sub_token_scopes` table + cache-invalidation move to Rust.
`put_scope` currently calls out to `api.hippius.com` (`HippiusApiClient.auth`,
`sub_token_scopes.py:159`) to verify the target key is an active sub of the account — Rust keeps
the same upstream dependency (doc 15 §1 "upstream owns the credential; we own its scope"). Keep
the frontend-HMAC message construction identical so the console's signing is unchanged.

**Note the `api_credentials` vs `sub_token_scopes` naming:** doc 15:214 calls the target table
`api_credentials`; the Python table is `sub_token_scopes`. Reconcile the name in doc 12 — the
*endpoint contract* (request/response shapes in `sub_token_scopes.py`) is what the console
depends on, not the table name.

---

## 3. User / frontend read API — `/user/*` (frontend-HMAC)

**Source:** `hippius_s3/api/user.py`; gate `gateway/middlewares/frontend_hmac.py:27`.

### Inventory

| Verb | Path | Does | Handler | Who calls |
|------|------|------|---------|-----------|
| GET | `/user/list_buckets` | JSON: all buckets for `main_account_id` + per-bucket object/byte totals | `user.py:32` | Console dashboard |
| GET | `/user/get_bucket_location` | JSON: one bucket's metadata + `location:"decentralized"` | `user.py:82` | Console |
| GET | `/user/list_objects` | JSON: paginated objects in a bucket (prefix/limit/offset) incl. `ipfs_cid`, `arion_hash`, `body_blake3` | `user.py:127` | Console file browser |
| GET | `/user/recent_uploads` | JSON: recent uploads for an account (5 s Redis cache) | `user.py:211` | Console dashboard |
| POST | `/user/unban` | Reset banhammer state (block key + infringement counters) for an IP | `user.py:256` | Ops / support tooling |

**Auth:** frontend-HMAC. These are the console's **read model** — JSON-shaped views of the same
data the S3 `ListBuckets`/`ListObjectsV2` return as XML, plus fields the console specifically
wants (`ipfs_cid`, `arion_hash`, aggregate totals, recent-uploads feed).

### Category: **IN-SCOPE v1** (the JSON read endpoints) — **console-load-bearing**

These are the endpoints most at risk of being forgotten, because they duplicate S3 semantics the
Rust plan *does* cover — but the **console renders from these JSON shapes, not from S3 XML**. If
they vanish at cutover, the Hippius console dashboard and file browser go blank even though the
S3 API works perfectly. They are load-bearing for the product front-end.

Recommended posture per endpoint:

- `list_buckets`, `list_objects`, `get_bucket_location`, `recent_uploads` → **IN-SCOPE v1.** Thin
  JSON projections over the Rust DB. Low cost (they're SELECTs), high blast radius if missing.
  Fields like `ipfs_cid`/`arion_hash` map onto the new HCFS/dedup model (doc 12/13) — confirm the
  console still needs the CID surfaced, and what it maps to when a version is multi-chunk (Python
  returns `arion_hash` NULL for multi-chunk — `user.py:172`).
- `POST /user/unban` → **DEFER or SEPARATE.** It is a Redis banhammer control, coupled to the
  Python rate-limiter/banhammer that lives in the gateway middleware stack, **not** to S3 data.
  Whether Rust reimplements the banhammer at all is a separate decision (doc 05 rate-limiting).
  If Rust keeps the same Redis banhammer keys, `unban` is a trivial `DEL` endpoint to port; if
  Rust drops or replaces the banhammer, `unban` is **OBSOLETE**. Confirm against the rate-limit
  design before porting.

**Cutover implication:** the four read endpoints must ship with the Rust app (console depends on
them directly). The frontend-HMAC gate (mechanism) is already in doc 05 §3a; this doc says the
**routes behind it** are in-scope too. Keep path + query-param + JSON field names identical so the
console needs no change.

**⚠ Open question for the console team:** enumerate exactly which `/user/*` JSON fields the
console reads today. The safe default is "all of them", but confirming lets us drop dead fields
(e.g. the `is_public` column is already dead server-side — `public_router.py:37` — but the
`/user` handlers still derive `is_public` from the ACL, `user.py:52`).

---

## 4. Public anonymous read — `/public/{bucket}/{key}` (anonymous + ACL)

**Source:** `hippius_s3/api/s3/public_router.py:46` (GET), `:80` (HEAD).

### Inventory

| Verb | Path | Does | Who calls |
|------|------|------|-----------|
| GET | `/public/{bucket}/{object_key}` | Anonymous object fetch for public-ACL buckets | Anyone (public links) |
| HEAD | `/public/{bucket}/{object_key}` | Anonymous HEAD | Anyone |

The only api entry point the gateway does **not** authorize; publicness is established in-handler
via `bucket_has_public_read_acl` (`public_router.py:24`), not the dead `buckets.is_public` column.

### Category: **IN-SCOPE v1 — but it belongs to the S3 conformance work (docs 06/22), not here**

This is really part of the S3 read surface (anonymous public-read), so it is covered by the S3
plan's anonymous/public-ACL handling. Flagged here only so it is not lost in the "non-S3" split:
it is a distinct *route prefix* (`/public/`) the S3 router does not own, and the Rust router must
register it explicitly and reproduce the "private bucket is indistinguishable from absent"
(`NoSuchKey`, never `AccessDenied`) behavior (`public_router.py:63-70`). **Load-bearing:** yes —
public share links break without it.

---

## 5. Health & metrics

**Source:** `main.py:617` (`/health`); `monitoring.py` (OTLP push, no route).

### Inventory

| Surface | Path | Does | Who calls |
|---------|------|------|-----------|
| Health | `GET /health` → `{"status":"healthy"}` | k8s startup/live/ready probe; Cachet checker | kubelet, `cachet_health_check.py` |
| Metrics | *(none — pushed)* | OTLP/gRPC to collector `:4317`, scraped at collector `:8889` | otel-collector |

### Category

- **`/health` → IN-SCOPE v1.** Doc 09 §3.4 / §6.4 already require the Rust API to serve
  `GET /health → {"status":"healthy"}` for the existing k8s `httpGet /health :8000` probes and the
  Cachet checker (`gateway:8080/health`). Must be bypassed by all middleware and suppressed from
  access logs (Python precedent). **No new work beyond what doc 09 already states** — recorded
  here for completeness of the non-S3 census.
- **Metrics → OBSOLETE as an HTTP surface / already covered.** There is no `/metrics` route to
  reimplement. The Rust app pushes OTLP (doc 09 §6.3, "Do NOT add a Prometheus `/metrics`
  endpoint"; metric registry in `monitoring.py`). Nothing to serve at cutover.

**Cutover implication:** none beyond doc 09. Health probes and the collector scrape are unchanged.

---

## 6. Service-account management (config, not an API)

**Source:** `services/service_accounts.py` (`is_service_account:15`,
`refuse_destructive_operation:72`, `require_service_account_env:103`,
`SERVICE_ACCOUNT_ENV_VAR:100`).

There is **no** HTTP surface here. Service accounts are the billing-exempt internal SS58s listed
in `HIPPIUS_SERVICE_ACCOUNT_IDS`. The allowlist is:
- read by the billing gate to bypass credit/plan checks (already in doc 05 billing scope),
- read by the destructive-op guards to 403 admin purge/suspend of our own accounts (§1).

### Category: **IN-SCOPE v1 as config + guard logic** (no endpoint to serve)

Rust must carry the env var and the two consumers (billing bypass + destructive-op refusal). Both
are already implied by the billing-gate and admin scope. **Cutover implication:** ensure the same
`HIPPIUS_SERVICE_ACCOUNT_IDS` secret is wired to the Rust pods, and that the admin suspend/purge
guard (§1) reads it. Managed by k8s-secret + redeploy exactly as today — the "escape hatch is
declarative" contract (`service_accounts.py:88`) must survive; do not add a `force` parameter.

---

## 7. Peer chunk serving — `/internal/parts/…` (peer-auth)

**Source:** `hippius_s3/api/internal_parts.py:52`; auth `peer_auth.py:68/90`; mounted only when
`peer_serve_enabled` **and** a secret (`main.py:634`).

### Inventory

| Verb | Path | Does | Who calls |
|------|------|------|-----------|
| GET | `/internal/parts/{object_id}/{version}/{part}/chunks/{chunk_index}` | Serve one **ciphertext** chunk from this node's local NVMe tier, or 404 | A sibling `api-local` pod's `PeerChunkFetcher` |

Part of the Python multi-tier read cache (node-local NVMe → **peer** → CephFS pool → backend,
doc 03 §3.3/§4). Bounds: local tier only, ciphertext only, shed over an in-flight cap.

### Category: **OBSOLETE under the Rust design** — *confirmed*

The task hypothesis is correct. The Rust architecture drops the read cache entirely:

> "HCFS is the sole backend, **UNCHANGED** (no Ceph, **no read cache**, no janitor)"
> — [`00-index.md`](00-index.md):54.

The Rust write path uses per-node SSD **staging for writes** (`staged_blobs`, doc 10 / doc 03
bridge) — a durability buffer before the HCFS forward, **not** a cross-node read tier. There is no
CephFS pool, no peer NVMe read tier, and therefore no peer-fetch protocol. The
`chunk_reads_by_tier_total{tier=peer}` metric and the `PeerChunkFetcher`/`PeerRegistry`
machinery (`main.py:199-254`) all go away with it.

**Cutover implication:** **do not reimplement.** The endpoint, `peer_auth.py`,
`is_authorized_peer_fetch` bypass (doc 05 §4c), `HIPPIUS_INTERNAL_PEER_SECRET`,
`HIPPIUS_PEER_SERVE_ENABLED`, and the peer-fetch bypass in the middleware chain are all dead in
the Rust design. **Action:** doc 05 §4c (internal-peer-fetch bypass) should be annotated as
"obsolete in Rust — no peer tier"; nothing calls `/internal/parts` once the Python DaemonSet's
read cache is gone.

**One caveat to verify:** doc 03's SSD-staging write path keeps *node-sticky routing for pending
reads* (00-index:54: "node-sticky routing for pending reads") — i.e. a read of a
staged-but-not-yet-forwarded blob must land on the node that holds it. Confirm that is done by
**routing** (the S3 GET is steered to the right node) and **not** by resurrecting an
`/internal/parts`-style cross-node fetch. If a cross-node "fetch the staged blob from the sibling"
path is needed, it would be a *new* internal contract, scoped by doc 03 — not a port of this one.

---

## 8. Docs / static / robots

| Surface | Path | Category | Note |
|---------|------|----------|------|
| Swagger/ReDoc/OpenAPI | `/docs`, `/redoc`, `/openapi.json` (`main.py:476`) | **DEFER / optional** | Gated on `enable_api_docs`; off in prod-sensitive configs. Nice-to-have dev surface; not load-bearing. Rust can generate its own or skip. |
| Favicon / swagger assets | `/static/*` (`main.py:638`) | **DEFER** | Only exists to serve `favicon.ico` to Swagger. Ships with `/docs` or not at all. |
| Crawler control | `GET /robots.txt` (`main.py:582`) | **IN-SCOPE (trivial)** | 3-line static response disallowing all crawlers. Cheap; keep it so the public S3/host isn't indexed. |

---

## 9. ATS auth-probe integration (middleware, not a route)

**Source:** `gateway/middlewares/auth_probe.py:34`; also consulted by `auth_router` + `acl`
(`is_valid_auth_probe:19`).

When ATS's `authproxy` plugin asks "is this request authorized?", the subrequest carries
`X-Hippius-Auth-Probe: <secret>`; after auth/ACL have validated, the middleware returns an empty
`200` so ATS proceeds to cache/origin. Fail-closed on unset secret.

### Category: **STAYS coupled to the ATS/CDN topology — decide with the edge design**

This is not an S3 or console surface; it is a contract with the Apache Traffic Server edge cache
in front of the product. Whether Rust needs it depends entirely on whether the **ATS edge tier is
kept** in front of the Rust service:

- **If ATS is kept** (same edge topology): Rust must reproduce `is_valid_auth_probe` +
  the short-circuit, because ATS re-authorizes every cache hit through this probe. Load-bearing
  for cached-object authorization (`docs/admin-api.md` op-note: "every ATS cache hit
  re-authorizes through the gateway"). **IN-SCOPE, conditionally.**
- **If the edge tier is redesigned/dropped**: **OBSOLETE**, along with the `ats_purge` and
  `cache_control` middleware family.

**Dependency — RECORDED as H9 (register):** the ATS edge is **deferred to the ops/CDN track** (no v1
functional dependency). Regardless of the keep/drop call, v1 **must** still emit correct
`Cache-Control` + the `X-Hippius-Visibility` sentinel on responses, and keep the edge/LB idle timeout
**≥75s**. The auth-probe + `ats_purge` + `cache_control` warm-path trio lives or dies with the
ATS-retention decision ([09](09-ops-deploy-observability.md)). Same fork governs
`ats_purge_middleware` and `cache_control_middleware`.

---

## 10. Recommendation table

| Surface | Route(s) | Auth | Caller | Category | Serves it after cutover |
|---------|----------|------|--------|----------|-------------------------|
| Admin suspend/reactivate | `POST /admin/accounts/{id}/suspend\|reactivate` | admin-HMAC | Billing backend, cockpit | **IN-SCOPE v1** (already committed, doc 05/15) | Rust app |
| Admin status | `GET /admin/accounts/{id}/status` | admin-HMAC | Cockpit | **IN-SCOPE v1** | Rust app |
| Admin purge + poll | `DELETE /admin/accounts/{id}/data`, `GET /admin/purge-jobs/{id}` | admin-HMAC | Billing backend (account deletion) | **IN-SCOPE v1** | Rust app + purge worker (doc 17) |
| Sub-token scope R/W/D | `GET\|PUT\|DELETE /user/sub-tokens/{key}/scope` | frontend-HMAC | Console | **IN-SCOPE v1** (D4 resolved = enforce fail-closed, register B2) — required *because* we enforce | Rust app |
| Console read model | `GET /user/list_buckets\|list_objects\|get_bucket_location\|recent_uploads` | frontend-HMAC | Console dashboard/browser | **IN-SCOPE v1** (console-load-bearing) | Rust app |
| IP unban | `POST /user/unban` | frontend-HMAC | Ops tooling | **DEFER / OBSOLETE** (coupled to banhammer decision) | Rust app *iff* banhammer ported, else drop |
| Public anon read | `GET\|HEAD /public/{bucket}/{key}` | anon + ACL | Public links | **IN-SCOPE v1** (owned by S3 plan, docs 06/22) | Rust S3 router |
| Health | `GET /health` | none | k8s, Cachet | **IN-SCOPE v1** (doc 09) | Rust app |
| Metrics | *(none — OTLP push)* | — | collector | **OBSOLETE as HTTP** (already push, doc 09) | collector (unchanged) |
| Service accounts | *(env var, no route)* | — | billing gate, admin guard | **IN-SCOPE as config** | k8s secret + Rust guard logic |
| Peer chunk serving | `GET /internal/parts/…` | peer-auth | sibling api-local pod | **OBSOLETE** (no read cache — 00-index:54) | nobody — deleted |
| Auth-probe | *(middleware short-circuit)* | probe secret | ATS authproxy | **CONDITIONAL** on ATS retention | Rust app *iff* ATS kept, else obsolete |
| Docs/Swagger | `/docs`, `/redoc`, `/openapi.json`, `/static` | none | devs | **DEFER / optional** | Rust app (optional) |
| robots.txt | `GET /robots.txt` | none | crawlers | **IN-SCOPE (trivial)** | Rust app |

### The short list of "someone must serve these or the product breaks at cutover"

1. **`/admin/*` (all 5)** — account suspension *and account deletion*. Billing backend + cockpit.
2. **`/user/*` read model (4 JSON endpoints)** — the console dashboard/browser renders from these.
3. **`/user/sub-tokens/*` (3)** — iff sub-tokens are a v1 product feature (doc 15 D4=enforce).
4. **`/public/{bucket}/{key}`** — public share links (owned by S3 plan).
5. **`/health`, `/robots.txt`** — trivial, but probes and crawler hygiene depend on them.

Everything else is either already covered (metrics, service-account config), obsolete (peer
serving), or a conditional/deferrable (unban, auth-probe, docs).

---

## 11. Open questions

1. **D4 gate for `/user/sub-tokens/*`.** Doc 15 leaves "enforce vs reject-all-subs" open. This
   doc adds: the *write endpoints* live or die with that choice, and the console's sub-token UI
   must move in lockstep. **Who decides D4, and does the console offer sub-tokens in v1?**
2. **Console field contract for `/user/*`.** Exactly which JSON fields does the console consume
   (esp. `ipfs_cid` / `arion_hash` — how do these map onto the new HCFS/dedup model when a version
   is multi-chunk)? Needed to avoid porting dead fields or dropping live ones.
3. **Banhammer / rate-limit decision.** Does the Rust design keep the Redis banhammer? If not,
   `POST /user/unban` is obsolete; if yes, it's a trivial port. Depends on the doc 05
   rate-limiting design, which is not yet settled for Rust.
4. **ATS edge retention (§9).** Keep the ATS cache tier in front of the Rust service? Governs
   whether auth-probe + `ats_purge` + `cache_control` are in-scope or obsolete. **Ops/CDN call.**
5. **Node-sticky pending reads (§7).** Confirm the SSD-staging write path serves
   staged-but-unforwarded blobs by *routing*, not by a resurrected cross-node `/internal/parts`
   fetch. If a cross-node staged-read fetch is needed, it's a *new* contract to scope in doc 03,
   not a port of the (obsolete) peer endpoint.
6. **Table naming reconciliation.** Doc 15 calls the scope table `api_credentials`; Python uses
   `sub_token_scopes`; purge/suspension tables (`purge_jobs`, `account_suspensions`) need homes in
   the Rust schema (doc 12). The *endpoint contracts* are the console/backend dependency, not the
   table names — reconcile in doc 12.
7. **Path/HMAC byte-fidelity.** The billing backend and console sign the literal request line
   (`METHOD + PATH[+?QUERY]`). Any path rename in Rust breaks every existing signature. Decision:
   commit to byte-identical admin/frontend paths, or coordinate a signing change with both callers.

---

### Source references

- Route registration: `hippius_s3/main.py:476-639`
- Admin API: `hippius_s3/api/admin.py`; `gateway/middlewares/admin_hmac.py:19`; `docs/admin-api.md`; `docs/example-admin.md`
- Sub-token scopes: `hippius_s3/api/sub_token_scopes.py`; `gateway/middlewares/frontend_hmac.py:27`
- User/frontend read: `hippius_s3/api/user.py`
- Public anon read: `hippius_s3/api/s3/public_router.py`
- Peer serving: `hippius_s3/api/internal_parts.py`; `hippius_s3/peer_auth.py`
- Auth-probe: `gateway/middlewares/auth_probe.py`
- Service accounts: `hippius_s3/services/service_accounts.py`
- Metrics/health: `hippius_s3/monitoring.py`; `main.py:617`
- Context: [`05-auth-authz-billing.md`](05-auth-authz-billing.md) §3–4, [`15-account-credential-model.md`](15-account-credential-model.md) §2.4/§3/D4, [`09-ops-deploy-observability.md`](09-ops-deploy-observability.md) §3/§6, [`00-index.md`](00-index.md):54 (no read cache), [`03-data-plane-cache-streaming.md`](03-data-plane-cache-streaming.md) §3–4 (Python peer tier being dropped)
