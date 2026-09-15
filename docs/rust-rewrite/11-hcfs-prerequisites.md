# 11 — HCFS prerequisites for the Rust S3 backend

**Status:** implementable requirements spec for the `hcfs-server` team. Companion to
[10b — Hand off byte storage to `hcfs-server`](10b-option-hcfs-handoff.md), which does the
fitness/gap analysis; this doc turns the four "must-add" gaps from 10b §3 into concrete designs
(DDL, endpoint signatures, auth flow) that fit hcfs's existing patterns.
**Date:** 2026-09-15.

> **⚠️ SUPERSEDED — NOT the chosen path (2026-09-15).** The team elected to make **zero hcfs changes** and own everything in the S3 service. [`13-hcfs-as-is-integration.md`](13-hcfs-as-is-integration.md) verified that store / ranged-GET / delete / per-tenant credit-gating / usage→chain billing all work through hcfs's **existing** surface unchanged, with the S3 service owning dedup + refcount. This doc is retained only as the reference design **if** hcfs is ever changed to expose a first-class blob API. Read `13` for the actual integration.

> **Scope.** The new Rust S3 product terminates SigV4/SigV2, owns buckets/objects/versions/
> parts/ACL/multipart in its **own** Postgres, and does its **own** AES-256-GCM envelope
> encryption. It then hands **already-encrypted ciphertext blobs, keyed by content hash**, to
> `hcfs-server`, and relies on hcfs for Arion/S3 dual-write, retry, and usage→chain reporting.
> hcfs only ever sees ciphertext — consistent with its "server never sees plaintext" principle.
> This spec is **write-path agnostic**: both candidate S3 write designs (SSD-staging drain, or
> inline PUT-through) consume the *same* four hcfs capabilities below, so none of this depends on
> that pending decision.
>
> All `backend.rs` / `files.rs` / `gates.rs` / `upload.rs` / etc. paths are under
> `hcfs/hcfs-server/src/` at `/Users/camden/Source/hcfs`; worker paths under
> `hcfs/hcfs-retry-worker/src/` and `hcfs/hcfs-chain-reporter/src/`.

---

## 0. Summary & the one thing to build first

| # | Capability | 10b verdict today | Effort | Blocker? |
|---|---|---|---|---|
| 1 | **Refcounted content-hash blob store** | No refcount; delete is unconditional + orphan-tolerant (`cleanup.rs:1-13`) | **L** | **TRUE BLOCKER** — correctness. Dedup/copy/versioning will drop live bytes without it. |
| 2 | **Raw-blob, Range-capable API** decoupled from FileRecord/Manifest/session | Only hash-addressed read is `/public/download/{hash}` — no auth, no Range (`public.rs:23-46`) | **L** | **TRUE BLOCKER** — the seam the S3 service actually calls. |
| 3 | **Attributed service credential** | Only cross-account cred is one global *unattributed* admin bearer (`gates.rs:82`, writes `caller_ss58 = None` `gates.rs:651-659`) | **M** | **BLOCKER for multi-tenant billing**; a single-tenant PoC can defer. |
| 4 | **Streamed large PUT** to S3/`both` | S3/`both` buffer the whole object before PUT (`backend.rs:1154-1156`) | **M** | Not a blocker for ≤16 MiB chunks; blocker only if the S3 service sends objects larger than one chunk in a single request. |

**Do capability 1 and 2 together** — they are one feature (a blob table plus the API that
increments/decrements it) and are the critical path. Capability 3 gates correct per-tenant
billing but not correctness of the bytes. Capability 4 is only needed if the S3 service pushes
objects larger than one hcfs chunk in a single request (see §4 and the sequencing note in §5).

A cross-cutting sub-item from 10b §3 item 4a — **parameterize the hard caps** (16 MiB chunk
`session.rs:92`, 16 MiB S3-gateway object `upload.rs:49` / `S3_FILE_FIELD_CAP`, 100 000
chunks/session `session.rs:193`) into env config — is **S** and folds into capabilities 2 and 4.

---

## 1. Refcounted content-hash blob store  (Effort: **L**)

### 1.1 Requirement

hcfs today ties a blob's lifetime 1:1 to a single `file_records` row: on delete it returns the
row's blob hashes and **unconditionally** fire-and-forget deletes every named blob
(`delete_file` `files.rs:490-528` → `cleanup::delete_record_blobs` `cleanup.rs:129-136`), with the
cleanup module explicitly documenting that "an orphaned blob is already tolerated"
(`cleanup.rs:1-13`). The S3 service will dedup (identical ciphertext → identical hash), fast-path
CopyObject, and keep versions — so **two or more logical objects will reference one physical
blob**. Under hcfs's current delete, removing either reference drops the shared bytes out from
under the others. This is a correctness bug, not a performance one.

**The blob store must:** track a reference count per content hash; increment atomically when a new
reference is created (store or copy); decrement atomically on delete; and perform the actual
backend (Arion + S3) delete **only** when the count reaches zero — and even then, tolerate the
existing orphan model as the failure mode (a crashed physical delete leaks, never drops).

### 1.2 Proposed design — schema

New additive migration `migrations/2026XXXX000000_content_blobs.{up,down}.sql`, same style as
`20260629000000_folder_entries.up.sql` (plain additive `CREATE TABLE`, timestamp-prefixed,
sqlx-cli managed):

```sql
-- Content-addressed, refcounted ciphertext blob registry. One row per unique
-- BLAKE3 content hash the raw-blob API (see §2) has stored. Independent of
-- file_records: file_records stays the Drive/native model; content_blobs is
-- the S3 product's dedup-safe reference ledger. The physical bytes live in
-- Arion and/or S3 under file_s3_key(prefix, content_hash) exactly as today.
CREATE TABLE content_blobs (
    content_hash TEXT        NOT NULL PRIMARY KEY,   -- 64-char lowercase BLAKE3 hex
    arion_hash   TEXT        NOT NULL DEFAULT '',    -- '' = not (yet) on Arion, mirrors FileRecord.arion_hash
    s3_hash      TEXT,                               -- NULL = not on S3
    size_bytes   BIGINT      NOT NULL,               -- CIPHERTEXT byte length (what was stored)
    refcount     BIGINT      NOT NULL,               -- invariant: > 0 for every live row
    created_at   BIGINT      NOT NULL,               -- unix seconds, matches existing convention
    updated_at   BIGINT      NOT NULL,
    CONSTRAINT content_blobs_refcount_nonneg CHECK (refcount >= 0)
);
```

Notes on the columns, matched to existing conventions:

- `arion_hash TEXT NOT NULL DEFAULT ''` / `s3_hash TEXT` mirror `FileRecord.arion_hash`
  (`types.rs:25`, empty string = "not in Arion") and `FileRecord.s3_hash` (`types.rs:33`,
  `Option` = "not in S3") exactly, so the same `StorageBackend::download(arion_hash, s3_hash, …)`
  and `StorageBackend::delete(arion_hash, s3_hash)` calls work unchanged.
- `size_bytes` here is **ciphertext** (the bytes actually stored), unlike `FileRecord.size_bytes`
  which is plaintext (workspace size-semantics rule). The S3 service owns the plaintext-size
  bookkeeping in its own DB; hcfs only knows the ciphertext it was handed. Document this
  divergence in the migration comment so it does not look like a bug against the size-semantics
  rule.
- No `user_id` column. A content blob is shared across accounts by definition of dedup; billing
  attribution is the S3 service's job (see capability 3 / §5) and is *not* modeled here.

### 1.3 Proposed design — atomic, race-safe refcount semantics

hcfs already does exactly the atomic-single-statement pattern this needs. `upsert_file_checked`
(`files.rs:107-185`) runs its optimistic-concurrency check as one
`INSERT … ON CONFLICT … DO UPDATE … WHERE … RETURNING` and reads the outcome from whether a row
came back. `delete_file` (`files.rs:490-528`) uses `DELETE … RETURNING`. The refcount store uses
the same primitives, each as a single atomic SQL statement (no read-modify-write in Rust, no
row-lock held across a round trip):

**Increment (on store or copy)** — `INSERT … ON CONFLICT DO UPDATE` bumps an existing row or
creates a fresh one at `refcount = 1`. `RETURNING (xmax = 0)` distinguishes insert from update so
the caller knows whether the physical bytes still need to be written:

```rust
// store/blobs.rs — new module beside files.rs, methods on HcfsStore
pub async fn incref_blob(&self, hash: &str, arion_hash: &str, s3_hash: Option<&str>,
                         size_bytes: i64) -> Result<IncrefOutcome, DatabaseError> {
    let now = /* unix seconds */;
    // Single statement; the ON CONFLICT arm serializes concurrent increfs of
    // the same hash on that row's lock exactly like the file upsert does.
    let row = sqlx::query(
        "INSERT INTO content_blobs
            (content_hash, arion_hash, s3_hash, size_bytes, refcount, created_at, updated_at)
         VALUES ($1, $2, $3, $4, 1, $5, $5)
         ON CONFLICT (content_hash) DO UPDATE SET
            refcount   = content_blobs.refcount + 1,
            -- Fill in a backend hash that was previously unknown; never clobber
            -- a known one with '' / NULL (same no-clobber discipline as the
            -- COALESCE on relative_path in upsert_file, files.rs:57).
            arion_hash = CASE WHEN content_blobs.arion_hash = '' THEN EXCLUDED.arion_hash
                              ELSE content_blobs.arion_hash END,
            s3_hash    = COALESCE(content_blobs.s3_hash, EXCLUDED.s3_hash),
            updated_at = EXCLUDED.updated_at
         RETURNING (xmax = 0) AS inserted, refcount",
    ).bind(hash).bind(arion_hash).bind(s3_hash).bind(size_bytes).bind(now)
     .fetch_one(&self.pool).await.map_err(DatabaseError::Sqlx)?;
    // inserted == true  → caller must ensure the physical bytes are stored.
    // inserted == false → dedup hit; bytes already durable, nothing to upload.
}
```

`(xmax = 0)` is the standard Postgres idiom for "this `ON CONFLICT` took the INSERT arm, not the
UPDATE arm". It lets the raw-blob PUT (§2) **skip the backend write entirely on a dedup hit** — a
free win the current path cannot express.

**Decrement (on delete)** — one statement decrements and, in the same statement, deletes the row
iff it reached zero, returning the blob hashes to reclaim only in that case:

```rust
pub async fn decref_blob(&self, hash: &str)
    -> Result<Option<BlobRef>, DatabaseError> {
    // CTE: decrement; then delete-and-return the row iff it hit zero.
    let reclaim = sqlx::query_as::<_, BlobRef>(
        "WITH bumped AS (
            UPDATE content_blobs
               SET refcount = refcount - 1, updated_at = $2
             WHERE content_hash = $1
            RETURNING content_hash, arion_hash, s3_hash, refcount
         )
         DELETE FROM content_blobs
          USING bumped
          WHERE content_blobs.content_hash = bumped.content_hash
            AND bumped.refcount = 0
         RETURNING content_blobs.arion_hash, content_blobs.s3_hash,
                   NULL::text[] AS chunk_hashes",  -- reuse BlobRef (types.rs:60-65)
    ).bind(hash).bind(now).fetch_optional(&self.pool).await?;
    // Some(BlobRef) → refcount hit 0, row gone, caller reclaims backend bytes.
    // None          → still referenced (or unknown hash); reclaim NOTHING.
}
```

When `decref_blob` returns `Some(BlobRef)`, and only then, the handler hands it to the **existing**
background reclamation path — `cleanup::spawn_blob_cleanup(app_state, vec![blob_ref])`
(`cleanup.rs:104-121`) or `spawn_targets` — which already does bounded-concurrency, off-request,
graceful-shutdown-drained backend deletes via `StorageBackend::delete` (`backend.rs:774-788`). So
the *only* change to the delete path is inserting the `decref_blob` gate before the reclamation
call; the reclamation machinery itself is reused verbatim, and the orphan-tolerant model
(`cleanup.rs:1-13`) is preserved as the crash failure mode.

### 1.4 Fit with existing hcfs patterns

- **sqlx store:** new `store/blobs.rs` module beside `store/files.rs`, methods on `HcfsStore`,
  single-statement atomic writes exactly like `upsert_file_checked` (`files.rs:107`) and
  `delete_file` (`files.rs:490`). Reuse `BlobRef` (`types.rs:60-65`) as the reclaim type so the
  cleanup path is untouched. Reuse `DatabaseError`.
- **Cleanup:** reuse `cleanup::spawn_blob_cleanup` / `spawn_targets` (`cleanup.rs:80-121`) — no new
  reclamation code. Only the refcount gate is new.
- **Coexistence with FileRecord:** `content_blobs` is a *parallel, independent* table. `file_records`
  and its Drive/native/S3-gateway flows are unchanged; the raw-blob API (§2) writes `content_blobs`
  and never a `file_records` row. This is the clean split 10b §2(e) asks for: the S3 service's
  objects do **not** force a shadow FileRecord.

### 1.4.1 Load-bearing consequence: the retry worker reconciles against `file_records`, not blobs

This is a gap that *must* be closed alongside the table, because the `both` backend's durability tail
depends on it. When a `both` write's background Arion push is skipped (budget exhausted) or fails,
the handler records a row in `failed_uploads`, and `hcfs-retry-worker` later streams the object back
from S3 and re-POSTs it to Arion. The `failed_uploads` schema
(`migrations/20260323000000_failed_uploads.up.sql`) is keyed on the **file** identity, not the blob:

```sql
CREATE TABLE failed_uploads (
    id BIGSERIAL PRIMARY KEY, user_id TEXT NOT NULL, path_hash BYTEA NOT NULL,
    s3_hash TEXT NOT NULL, file_id TEXT NOT NULL, error_message TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0, created_at BIGINT NOT NULL, last_attempt BIGINT NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX idx_failed_uploads_user_path ON failed_uploads (user_id, path_hash);
```

The worker's write-back after a successful Arion re-push targets a `file_records` row:
`record_arion_hash` runs `UPDATE file_records SET arion_hash=… WHERE user_id=$3 AND path_hash=$4 AND
s3_hash=$5` (`hcfs-retry-worker/src/worker.rs:378-405`), and its supersede check `record_still_holds`
(`worker.rs:478-488`) reads `file_records.s3_hash`. A raw blob written via `PUT /blob/{hash}` has **no
`file_records` row**, so as written the retry worker would either churn a dead row forever or never
reconcile the blob's Arion copy. The delete cascade also assumes it: `delete_file` clears
`failed_uploads WHERE user_id=$1 AND path_hash=$2` in the same tx (`files.rs:516-521`).

**Required work (part of capability 1):** teach the durability tail about `content_blobs`. Cleanest
shape — a blob-scoped retry row (or reuse `failed_uploads` with a sentinel `user_id`/`path_hash`
carrying `content_hash` in `s3_hash`, matching how chunk-native rows already overload `s3_hash` with
the `"chunked"` sentinel, `worker.rs:69`) whose successful re-push writes the Arion hash back to
`content_blobs.arion_hash` (the incref UPDATE at §1.3 already has the no-clobber `CASE` for exactly
this) instead of `file_records`. And `delete_blob`/`decref` must clear any pending retry row for a
blob it reclaims, mirroring `delete_file`'s `failed_uploads` cleanup (`files.rs:516`). This is
modest but non-optional; scope it into the L.

### 1.5 Migration / back-fill

- The table is **additive** and starts empty. New raw-blob writes populate it. No stop-the-world
  scan is required to *start* using it, because the S3 product is greenfield (10b scope note: fresh
  DB, data re-encrypted/migrated later) — the S3 service's blobs are all born through the new API.
- **Do NOT back-fill `file_records` into `content_blobs`.** The two models stay separate: existing
  Drive/native/gateway files keep hcfs's 1:1 delete (orphan-tolerant, which is fine for them);
  only blobs written through the raw-blob API are refcounted. Mixing them would require the
  stop-the-world scan 10b's open questions worry about, and buys nothing — a Drive file and an S3
  object never share the same logical lifecycle.
- **Open edge:** if a Drive file and an S3 object ever hash-collide to the same ciphertext (same
  bytes, same BLAKE3), they store to the same `file_s3_key`. The refcounted delete only guards the
  `content_blobs` side; a Drive `delete_file` would still unconditionally reclaim that key. Two
  mitigations, pick one in review: (a) give the S3 product its own `HCFS_STORAGE_S3_PREFIX` so keys
  never collide across the two models (cheapest, recommended — the prefix is already configurable,
  `backend.rs:462`); (b) route Drive deletes through `decref_blob` too when a matching
  `content_blobs` row exists. Prefer (a).

---

## 2. Raw-blob, Range-capable API decoupled from FileRecord/Manifest/session  (Effort: **L**)

### 2.1 Requirement

Every normal hcfs write requires either a signed `Manifest` (native + session paths, Ed25519 ToS
signature verified `session.rs:475`) or the S3-gateway multipart dialect, and every write creates
a `file_records` row with hcfs's own model (`path_hash`, `salted_hash`, `revision_seq`/`revision_id`
OCC, `encrypted_path`, …, `types.rs:17-54`). The chunked session additionally re-hashes each chunk
server-side (`session.rs:1013`), so the caller cannot choose the key. The only hash-addressed read
today is `/public/download/{hash}` — **no auth, no Range** (passes `None`, `public.rs:45`).

The S3 service wants a **content-hash-addressed blob API** — caller supplies the hash, no manifest,
no FileRecord, no session — with Range reads. This is the seam that lets hcfs stop being "used
against its grain".

### 2.2 Proposed design — endpoints

Mount a new `blob_route` sub-router in `build_router` (`router.rs:55`), merged like the existing
`folder_entries_route` / `chunk_upload_route`. All four are service-authed (capability 3). Body cap
on PUT is configurable (§0 cap parameterization), defaulting high enough for one part.

| Method + path | Handler | Semantics |
|---|---|---|
| `PUT /blob/{hash}` | `put_blob` | Body = raw ciphertext. Server verifies `blake3(body) == {hash}` (integrity, not key-choice), stores to S3 **and** Arion via `StorageBackend`, and `incref_blob`s. Dedup hit (`incref` returned `inserted=false`) → skip the backend write, still increments. Returns `200` + `{content_hash, size_bytes, deduped: bool}`. |
| `GET /blob/{hash}` | `get_blob` | Streams the blob. Honors `Range:` (`bytes=start-` / `bytes=start-end`). `200`/`206`, `Content-Length`, `Content-Range`, `Accept-Ranges: bytes`. Streams, never buffers. |
| `HEAD /blob/{hash}` | `head_blob` | Existence + `Content-Length` (from `content_blobs.size_bytes`, no backend round trip) without a body. |
| `DELETE /blob/{hash}` | `delete_blob` | `decref_blob`; if it returned `Some`, hand to `spawn_blob_cleanup`. Always `200` (idempotent: unknown/still-referenced hash is a no-op reclaim). Optional `{refcount_now}` in the body. |
| `POST /blobs:batchDelete` | `batch_delete_blobs` | Body `{hashes:[…]}`, per-item `{hash, refcount_now}` results, 200-on-partial like `batch_delete_files` (`file.rs:1165`), in-handler batch cap + a route body cap mirroring `batch_delete_route` (`router.rs:143-145`). |

Request/response types live in `hcfs-shared/src/network.rs` (the workspace rule — see CLAUDE.md
"Adding a new endpoint"), and each route gets a `route_catalog.rs` row + a
`middleware.rs` `route_label` arm, per the server's route-catalog test guard.

### 2.3 Proposed design — how each reuses `StorageBackend`

The whole point: the plumbing already exists, these handlers are thin.

- **`put_blob`** reuses `StorageBackend::upload_prepared(buf, hash, file_id, metrics)`
  (`backend.rs:603-635`) — the one entry point that already accepts a **caller-supplied hash** and
  dual-writes (`store_buffered_to_both` `backend.rs:1206`: S3 sync + background Arion under the
  2 GiB `ARION_BG_BLOB_BUDGET` `backend.rs:49`). Pass `file_id = hash`. This is *exactly* what the
  S3-gateway `handle_s3_upload` already calls (`upload.rs:538-540`), minus the FileRecord. For the
  dedup-hit case, `incref_blob` returning `inserted=false` lets the handler skip `upload_prepared`
  entirely.
  - **Streaming caveat:** `upload_prepared` takes an owned `Vec<u8>`, i.e. the whole part is
    buffered. Fine at chunk size (≤16 MiB, matching today's `S3_FILE_FIELD_CAP`); for larger single
    PUTs see capability 4. The Arion-background hash equivalence already holds: on `both`,
    `store_buffered_to_both` sets `arion_hash = s3_hash` optimistically (`backend.rs:1263`) and the
    retry worker reconciles — the raw-blob path inherits that behavior unchanged.
- **`get_blob`** reuses `StorageBackend::download(arion_hash, Some(s3_hash), metrics, range)`
  (`backend.rs:705-769`): S3-first with Arion fallback and the shadow-read benchmark, and full
  native Range support (`download_s3_range` `backend.rs:1534`, Arion Range forward
  `backend.rs:1342`). The handler reads `(arion_hash, s3_hash)` from the `content_blobs` row (one PK
  lookup) and forwards `ByteRange::parse(header)` (`backend.rs:140`). This is strictly the
  hardened, Range-capable, service-authed variant of `public_download` (`public.rs:23`) that 10b's
  open questions ask for.
  - **Suffix-range gap (unchanged, and correct to leave):** `ByteRange::parse` rejects `bytes=-N`
    (`backend.rs:139-152`). The S3 service knows each object's size in its own DB, so it translates
    `bytes=-N` to `bytes=(size-N)-(size-1)` before calling hcfs. Document this as the contract; do
    not add suffix support to hcfs for this caller.
- **`head_blob`** answers from `content_blobs.size_bytes` — no backend HEAD needed.
- **`delete_blob` / batch** reuse `decref_blob` (§1.3) + `spawn_blob_cleanup` (`cleanup.rs:104`).

### 2.4 Fit with existing hcfs patterns

New `http/handlers/blob.rs`, routed in `router.rs` as its own sub-router with a `DefaultBodyLimit`
layer (the codebase's pattern for per-route caps: `chunk_upload_route` `router.rs:105-110`). Reuse
`StorageDownload` → `Body::from_stream` for the response exactly as `public_download`
(`public.rs:85-96`) and the native download do. Reuse `hcfs_shared::storage::file_s3_key` keying
(unchanged). No new storage code.

### 2.5 Migration / back-fill

None beyond capability 1's table. The API is new surface; nothing to back-fill. `/public/download`
stays as-is for its existing (Drive share) callers.

---

## 3. Attributed service credential  (Effort: **M**)

### 3.1 Requirement

Today the only cross-account credential is a single global admin bearer (`HCFS_ADMIN_BEARER_TOKEN`,
"S3 gateway access", `gates.rs:78-93`). It short-circuits authorization before the
`substrate_address == ss58_address` check (`validate_and_authorize` `gates.rs:290-296`), so it can
act for any account — but it writes **unattributed**: `AuthorizedWriter.caller_ss58 = None`
(`gates.rs:684-687`, doc'd at `types.rs:44-51`), so rows carry a NULL uploader and, worse for
billing, there is no per-tenant identity to attribute storage/usage to. It is also one
high-value secret with a huge blast radius.

The S3 service acts on behalf of **many** end-user accounts. hcfs needs a credential that
(a) authenticates the S3 service as a trusted service, and (b) **attributes** each blob/usage
operation to the correct paying account, so hcfs's `user_summaries` + chain-reporter attribute
correctly (see §5).

### 3.2 Proposed design — service token + per-request attributed subject

Model it as a **scoped service token that names the acting account per request**, rather than a new
identity provider. This fits hcfs's existing bearer machinery with the least surface:

1. **New credential:** `HCFS_S3_SERVICE_TOKEN` (or a small set, comma-separated, for rotation),
   loaded and validated exactly like `ADMIN_BEARER_TOKEN` / `REPAIR_BEARER_TOKEN` — through
   `parse_bearer_secret` (fail-closed on unset/empty/`CHANGE_ME_IN_RANCHER`, `gates.rs:74-76`), a
   `blake3` digest compared with `subtle::ConstantTimeEq` via `token_digest_matches`
   (`gates.rs:137-143`), min-32-byte warning (`gates.rs:84-90`). This mirrors the existing
   separately-scoped repair token (`REPAIR_BEARER_TOKEN` `gates.rs:114`) — same rationale: a leak
   is contained to the blob surface, not all admin operations.
2. **Per-request acting account:** the blob endpoints (§2) require a header naming the paying
   account, e.g. `X-HCFS-Account: {ss58}`. A new gate:

```rust
// gates.rs — a service gate for the blob routes. Distinct from validate_and_authorize:
// it authenticates the SERVICE, then TRUSTS the service to name the account.
pub fn authorize_blob_service(headers: &HeaderMap)
    -> Result<ServiceWriter, (StatusCode, ErrorResponse)> {
    let token = validate_bearer_token(headers).map_err(|e| (UNAUTHORIZED, e))?;
    if !is_s3_service_token(&token) {                 // digest compare, fail-closed
        return Err(forbidden());
    }
    let account = headers.get("x-hcfs-account").and_then(|v| v.to_str().ok())
        .filter(|s| crate::utils::is_valid_ss58(s))   // validate shape, never trust blindly
        .ok_or_else(|| bad_request("x-hcfs-account required"))?;
    Ok(ServiceWriter { account_ss58: account.to_string() })  // ATTRIBUTED, unlike admin's None
}
```

`ServiceWriter.account_ss58` is `Some` by construction — the deliberate contrast with the admin
bearer's `AuthorizedWriter { caller_ss58: None }` (`gates.rs:651-659`). Refcount ops and usage
deltas (§5) key on this value.

3. **ExemptAccounts interaction:** the S3 service's accounts are billed per tenant, so they are
   **not** listed in `HCFS_EXEMPT_ACCOUNTS` (`exempt.rs:26`). Exemption is the *opposite* of what
   we want here — a listed SS58 skips suspension/quota/usage reporting entirely (`exempt.rs:1-5`),
   which collapses attribution (10b §3 item 5). Keep the tenant accounts non-exempt so their
   `user_summaries` rows are real and the chain-reporter reports them. Reserve `ExemptAccounts` for
   the *service's own* infrastructure account if it ever needs one, not for tenants.

### 3.3 Fit with existing hcfs patterns

Entirely reuses the token machinery in `auth/gates.rs`: `parse_bearer_secret`,
`token_digest_matches`, `LazyLock` digest, and the "separately-scoped token" precedent set by
`REPAIR_BEARER_TOKEN`. The `X-HCFS-Account` header + `is_valid_ss58` validation is the standard
"validate the shape, never trust blindly" discipline already used for `folder_hash`
(`is_drive_folder_hash`, `gates.rs:759`) and the public hash (`is_valid_public_hash`
`public.rs:19`). No new crates, no mTLS (though mTLS at the ingress is a fine additional layer and
is orthogonal — the token stays the app-level identity).

### 3.4 Migration / back-fill

None. New credential + new header on new routes. Existing admin/repair tokens and the
`ExemptAccounts` allowlist are untouched.

---

## 4. Streamed large single-PUT  (Effort: **M**)

### 4.1 Requirement

On `s3` and `both` backends, a single-object write **buffers the entire object in memory** before
the S3 `put_object`: `upload()` → `buffer_field_with_blake3(field, None)` (`backend.rs:575`,
`1031-1063`), and `upload_prepared` takes an owned `Vec<u8>` (`backend.rs:603`). `handle_s3_upload`
caps this at 16 MiB (`S3_FILE_FIELD_CAP` `upload.rs:49`), so today it is bounded — but if the S3
service ever sends an object larger than one chunk in a single PUT (raising the cap for 5 GiB S3
parts, §0), an S3/`both` pod OOMs. The only constant-memory large path is
`upload_from_file_for_migration` (`backend.rs:644-701`), explicitly labeled temporary
("delete when migrations are complete", `backend.rs:643`).

### 4.2 Proposed design

Two acceptable shapes; pick per the write-path decision:

- **Preferred — cap parts at chunk size, no new streaming needed.** Have the S3 service map each S3
  *part* (or shard each part) to one `PUT /blob/{hash}` at ≤ the (configurable) chunk cap, and use
  the **chunked-session path** — which already streams in the aggregate (bounded 16-slot mpsc,
  ~2-chunk peak memory, `file.rs:303-367`, `599`) — for object assembly, or just store each part as
  its own content blob and let the S3 service track the part→blob mapping in its own DB (it must
  compute per-part MD5/ETag there anyway, 10b §5.5). This needs **only** the cap parameterization
  (S), and no change to the buffering path. **Recommended for phase 1.**
- **If a true multi-GB single PUT is required:** promote the proven
  `upload_from_file_for_migration` streaming path (`backend.rs:644-701`) — spool the request body to
  a `tempfile::NamedTempFile` via `tokio::task::spawn_blocking` (never block the runtime, per the
  workspace concurrency rules), then `upload_multipart_to_s3` / `stream_parts_to_s3`
  (`backend.rs:263-361`, 8 MiB parts, constant memory) — from "temporary migration helper" to a
  first-class `upload_streaming(body_stream, hash)` on `StorageBackend`. Compute BLAKE3 while
  spooling so the caller-supplied hash is still verified. This is the **M** sizing.

Either way, keep the existing `both`-backend Arion tail behavior: S3 is the ACK path, Arion is
pushed under the byte budget (`backend.rs:49`) with skip-to-retry-worker on exhaustion — the
streaming variant records a `failed_uploads` row for the Arion copy just like the migration path
does (`backend.rs:687-696`).

### 4.3 Fit / migration

Reuses the multipart streaming already written and tested in `backend.rs:263-361`; promotes it out
of the migration-only cfg. No schema change. The cap parameterization (env vars replacing the
`const` at `session.rs:92`, `upload.rs:49`, `session.rs:193`) is the only config migration and is
backward-compatible (defaults = today's constants).

---

## 5. Dependency / sequencing note

**True blockers (correctness, must ship before any S3 traffic dedups/copies/versions):**
capabilities **1 + 2**, built together. They are one feature — a refcounted blob table plus the API
that maintains it — and without them the S3 service either (a) forfeits dedup/copy entirely by
storing a full physical copy per logical object, or (b) corrupts data by double-deleting shared
blobs. This is the single hardest item because it is the one that changes hcfs's data model; do it
first.

**Blocker for correct billing, not for correct bytes:** capability **3**. A single-tenant PoC (or a
"store everything under one service account, attribute in the S3 service's own DB" stopgap) can run
on the existing admin bearer temporarily — but that collapses all tenants into one `user_summaries`
row (10b §3 item 5), so per-tenant usage→chain reporting is wrong until 3 lands. Because the
attribution model (per-end-user ss58 vs one service account) also decides how §1's `incref`/usage
deltas are keyed, **decide the identity model early even if you implement it second.**

**Not on the critical path:** capability **4** (and the cap parameterization). If phase 1 keeps
objects/parts at ≤ the current 16 MiB chunk size (the "map a part to a blob" design in §4.2), the
buffering path is already safe and 4 is deferrable. It becomes a blocker only when the S3 service
needs to push objects larger than one chunk through a single request.

**Recommended order:** (1+2) → 3 → 4. The cap parameterization (S) rides along with whichever of 2
or 4 lands first.

---

## 6. Compatibility note — the mapping is already half-live

This is not a from-scratch integration. The production hippius-s3 gateway **already** posts one
AES-GCM cipher chunk per `POST /upload` to hcfs today: default `HIPPIUS_CHUNK_SIZE_BYTES` = 4 MiB
plaintext + 28 B nonce/tag, well under the 16 MiB `S3_FILE_FIELD_CAP` (`upload.rs:49`;
`docs/public/api/s3-gateway.md`). Each such request runs `handle_s3_upload` (`upload.rs:424`):
authorize → `validate_billing(BillingSubject::s3_gateway(ss58), …)` (`upload.rs:487`) →
`buffer_field_with_blake3` → `upload_prepared` (`upload.rs:538-540`, the same dual-write entry the
raw-blob PUT will use) → `file_records` upsert → `record_summary_delta(ss58, "", …)` on the **bare**
row (`upload.rs:609`). The key is `blake3(filename)` as `path_hash` (`s3_path_hash` `upload.rs:1086`).

So a degenerate "one cipher chunk per hcfs file, encrypt in the S3 layer, hand ciphertext to hcfs"
flow is **in production now**. The four capabilities *formalize and generalize* it, they do not
reinvent it:

- **Formalize, don't fork, the write path.** The raw-blob `PUT /blob/{hash}` (§2) is
  `handle_s3_upload` with the FileRecord/manifest ceremony stripped and `incref_blob` added — same
  `upload_prepared` call underneath. Keep `POST /upload` working unchanged for the existing gateway
  during transition; the raw-blob API is additive.
- **Reuse the billing rail derivation, do not build a new one.** hcfs already picks the billing
  rail from *where the bytes land* (the `user_summaries` key shape), not the route:
  `BillingSubject` (`helpers.rs:137-178`), rail = bare SS58 → S3, `{ss58}_{folderhash}` → Drive
  (`docs/HCFS-BILLING.md` §4.0). The S3 gateway writes the **bare** row (`upload.rs:609`). The
  chain-reporter then scans dirty `user_summaries` (`hcfs-chain-reporter/src/loop_.rs:183-193`),
  classifies each row purely by key shape (`classify_user_id`, `identify.rs:47-74`: bare `{ss58}` vs
  `{ss58}_{16hex}` folder vs `{ss58}_hcfs_shares`), and derives the rails as **Drive = folder +
  shares** and **S3 = bare − Drive (saturating)** (`loop_.rs:402-433`), then submits
  `pallet_marketplace::update_users_file_usage` + `pallet_arion::update_multiple_user_file_sizes`
  (`loop_.rs:584-627`, ≤250 accounts/call). So if capability 3 attributes each blob op to the
  tenant's real ss58 **and** the raw-blob path records a bare-row `record_summary_delta` for that
  ss58 (mirroring `upload.rs:609`), those bytes land in the bare row, get computed as the S3 rail,
  and are reported per-tenant with **zero new accounting code**. If instead everything is stored
  under one service account, all tenants collapse into one bare row (the §5 warning). Exempt SS58s
  are filtered before submit (`loop_.rs:404-411`) — another reason (§3.2) tenants must **not** be in
  `HCFS_EXEMPT_ACCOUNTS`, or their usage is never reported to chain at all.
- **Inherit the durability tail as-is.** The `both` backend's S3-ACK + background-Arion +
  retry-worker reconciliation (`backend.rs:1206-1296`; `hcfs-retry-worker` scanning `failed_uploads`)
  already backs every gateway upload today; the raw-blob path inherits it unchanged. Note the
  standing window where the optimistic `arion_hash` points at a blob Arion does not hold yet
  (`backend.rs:44-48`) — an existing property, not a new risk, but the S3 SLA must tolerate it (see
  open questions).

---

## 7. Open questions for the hcfs team

1. **Chunk vs S3 part mapping (drives capability 4's shape).** Should one S3 *part* map to one
   `PUT /blob/{hash}` — requiring the 16 MiB caps raised toward 5 GiB (§0) and the streaming PUT of
   capability 4 — or should the S3 service shard each part into ≤16 MiB blobs and track the mapping
   itself (fits current caps, no capability-4 streaming needed)? This decision selects §4.2's
   preferred vs streaming design.
2. **Refcount coexistence with `file_records` blobs.** Confirm the "separate prefix" mitigation
   (§1.5, use a distinct `HCFS_STORAGE_S3_PREFIX` for the S3 product) is acceptable, so a Drive
   file's unconditional reclaim (`cleanup.rs`) can never delete a refcounted S3 blob's physical key.
   The alternative (route Drive deletes through `decref` too) is a bigger change — is it wanted?
3. **Attribution model (drives capability 3 and all of §5/§6 billing).** Per-end-user ss58 per
   tenant (clean `user_summaries` + chain attribution, needs the S3 service to send each tenant's
   ss58 in `X-HCFS-Account` and hcfs to record bare-row deltas for it) vs one service account
   (simple, collapses all tenants into one usage row)? This is the single decision that most shapes
   the billing outcome.
4. **`incref` at store time vs at commit time.** Should `PUT /blob` incref eagerly (before the S3
   service has durably committed the referencing object in its own DB) or should there be a separate
   `POST /blob/{hash}:ref` / `:unref` so the S3 service controls refcount transitions exactly at its
   own object-commit boundaries? The latter decouples "bytes durable" from "referenced" and is
   safer for multipart-abort/CopyObject-rollback, at the cost of an extra round trip. Recommendation:
   support both — `PUT` increfs on first store, and an explicit `:ref`/`:unref` pair for
   copy/version/abort — but confirm the hcfs team's appetite for the extra endpoints.
5. **Arion durability window for an S3 SLA.** Does the `both` backend's in-band Arion push (2 GiB
   budget, skip-to-retry-worker, `backend.rs:49`) plus the retry-worker tail leave too long a window
   where Arion lacks a blob the S3 service has already ACKed to a client (`backend.rs:44-48`)? If the
   S3 SLA requires Arion durability before ACK, that is a behavior change to `store_buffered_to_both`
   (await the Arion push), which reintroduces the OOM/latency the budget was added to prevent — needs
   an explicit decision.
6. **Range-aware cold fetch.** A 1-byte Range on a cold chunk still transfers the whole chunk object
   from Arion (`download_chunk` fetches a whole object, `backend.rs:808-863`); S3 Range GET is
   native (`download_s3_range` `backend.rs:1534`) so on `both`/`s3` this only bites on Arion
   fallback. Is a range-aware Arion fetch in scope, or accepted as a known inefficiency (it is a
   standing open question on the hippius-s3 side too)?
7. **Suffix ranges.** Confirm the S3 service owns `bytes=-N` translation (§2.3) and hcfs will not add
   suffix-range support to `ByteRange::parse` (`backend.rs:139-152`) for this caller.
8. **Cap parameterization scope.** Turning `S3_FILE_FIELD_CAP` (`upload.rs:49`), `CHUNK_BODY_CAP`
   (`session.rs:92`), and the 100 000-chunk cap (`session.rs:193`) into env config affects the
   Drive/native clients that also hit those paths — is a per-route (blob-API-only) cap preferred
   over a global one so Drive limits are unaffected?

---

*Cross-repo references: `hcfs` = `/Users/camden/Source/hcfs`; `hippius-s3` = this repo. Companion
analysis: [10b-option-hcfs-handoff.md](10b-option-hcfs-handoff.md). Billing map:
`hcfs/docs/HCFS-BILLING.md`. Gateway wire contract: `hcfs/docs/public/api/s3-gateway.md`.*
