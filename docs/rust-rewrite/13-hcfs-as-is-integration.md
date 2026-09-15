# HCFS as-is as a ciphertext-blob backend (ZERO hcfs changes)

> **✅ RE-VERIFIED against `main` HEAD `7157b69` (2026-09-15 session).** All seven load-bearing claims
> confirmed against current source (line numbers had drifted; corrected here). **Native absolute Range
> GET holds** — the framed encrypted-read path's core dependency: single-blob download parses `Range`
> (`http/handlers/file.rs:993`), issues a true S3 `GetObjectRange` (`storage/backend.rs:1534`
> `download_s3_range`, streamed, no whole-object buffering), and returns 206 + `Content-Range`
> (`file.rs:759-791`); **1-byte inclusive ranges work** (routed around rust-s3's `assert!(start<end)`,
> `backend.rs:1474`); `start>=total` → 416. No suffix range / single-range-only unchanged
> (`backend.rs:140-154`). 16 MiB cap intact (`upload.rs:49`, `router.rs:110`). Content-addressed by
> `blake3(ciphertext)` (`hcfs-shared/src/storage.rs:30 file_s3_key`). Delete unconditional/no-refcount,
> missing=success (`backend.rs:774`). Per-tenant 402 gate + ss58 usage→chain reporter intact
> (`billing/credits.rs:428`, `hcfs-chain-reporter`). Admin bearer still bypasses per-account auth on
> every gate and there is **no** `X-HCFS-Account` header → B3 (scoped token) remains a small, real,
> unimplemented change. **⚠️ One design nuance:** the native sub-span Range GET applies to a
> **single-blob** stored object; HCFS *chunk-native* files emulate ranges at whole-chunk granularity.
> So the S3 rewrite must store each ciphertext blob as **one HCFS object** (not a chunk-native upload)
> — which is already the design (doc 16: 1 blob = 1 named HCFS object).

**Question.** Can a separate new Rust S3 service use the *current* hcfs-server
`main` (at `/Users/camden/Source/hcfs`) purely as durable byte storage —
storing ~4 MiB ciphertext blobs, retrieving them by range, deleting them at
refcount-zero, and ideally getting per-tenant usage→chain billing for free —
**without changing a single line of hcfs**?

**Short answer.** Yes for store / get-with-range / delete, and yes for
per-tenant billing attribution + on-chain reporting **if** the S3 service holds
an admin/service bearer and passes each tenant's SS58 as `account_ss58`. The
only thing that is genuinely impossible without an hcfs change is *storage-layer
blob refcounting* — hcfs deletes a content-hash-keyed blob unconditionally — but
that is exactly what the prompt already assigns to the S3 service, so it is a
non-issue as long as the S3 service gives each unique blob a unique hcfs object
name (see §Verdict and §Caps).

Everything below is traced against the real source; every claim carries a
`file:line`.

---

## The one endpoint that matters: the S3 gateway

hcfs already ships an "S3-compatible gateway" that is *precisely* a
bring-your-own-ciphertext blob API. It shares the `/upload` URL with the native
E2E path and is dispatched on the **first multipart field name**:

- first field `manifest` → native HCFS handler
- first field `account_ss58` → S3 gateway handler
  (`hcfs-server/src/http/handlers/upload.rs:108`, `:143`).

Routes (`hcfs-server/src/http/router.rs`):

| Op | Method + path | Handler |
|---|---|---|
| store | `POST /upload` (first field `account_ss58`) | `upload.rs:57` → `handle_s3_upload` `upload.rs:424` |
| get | `GET /download/{ss58}/{file_id}` | `router.rs:70` → `download_no_folder` `file.rs:926` → `download` `file.rs:942` |
| delete | `DELETE /delete/{ss58}/{file_id}` | `router.rs:74` → `delete_file_no_folder` `file.rs:1040` → `delete_file` `file.rs:1055` |
| pre-check | `POST /can_upload` | `router.rs:152` → `can_upload` `upload.rs:642` |
| list | `GET /get_state/{ss58}/{folder}` | `router.rs:66` |

The gateway is documented in `docs/public/api/s3-gateway.md`. The billing map is
`docs/HCFS-BILLING.md` §6.2.

---

## The six questions, answered concretely

### 1. Store a ciphertext blob and get a stable identifier — **YES**

Endpoint: `POST /upload`, multipart, **first field `account_ss58`** (the routing
signal), then a `file` field carrying the raw ciphertext bytes with its own
`Content-Length` (`handle_s3_upload`, `upload.rs:424`; `extract_file_field`
requires `filename` + `Content-Length`, `upload.rs:1295-1322`).

What hcfs does with the bytes:

- Buffers ≤ `S3_FILE_FIELD_CAP` = **16 MiB** and computes `blake3` of the bytes
  (`buffer_field_with_blake3(field, Some(S3_FILE_FIELD_CAP))`, `upload.rs:506`;
  const `upload.rs:49`).
- Enforces declared vs actual length: `buf.len() != content_length` → 400
  `size_mismatch` (`upload.rs:525-536`).
- Stores via `StorageBackend::upload_prepared(buf, hash, file_name, …)`
  (`upload.rs:538`, backend `storage/backend.rs:603`). The storage key is
  content-addressed: `file_s3_key(prefix, blake3(bytes))` =
  `{prefix}/{h0..2}/{h2..4}/{hash}` (`backend.rs:988-989`,
  `hcfs-shared/src/storage.rs:30`). Under `HCFS_STORAGE_BACKEND=both` it
  `put_object`s to S3 as the ACK path and fire-and-forgets Arion, with the
  retry-worker backstop (`store_buffered_to_both`, `backend.rs:1206`).

**The identifier the S3 service persists** is the pair **`(account_ss58,
file_id)`**, where:

```
file_id = hex(path_hash),  path_hash = blake3(file_name)
```

`s3_path_hash` = `blake3(file_name.as_bytes())` (`upload.rs:1086-1088`); the
`FileRecord` is keyed `user_id = account_ss58`, `path_hash = blake3(file_name)`
(`build_s3_file_record`, `upload.rs:1119-1120`); the response returns
`file_id = hex(path_hash)` (`upload.rs:573`, `:629`). The response also returns
`arion_hash`/`upload_id` (the content hash) but the **retrieval key is
`(ss58, file_id)`**, i.e. the name you chose — *not* the content hash.

Consequence the S3 service controls: **you pick the name.** Give each unique
ciphertext blob a unique, stable `file_name` (e.g. the blob's own content hash
or a UUID) and you get a 1:1 `file_record ↔ blob` mapping — which is what makes
the delete story clean (see §6).

Auth: `Authorization: Bearer <token>`, verified **before** the body is read
(`upload.rs:84`, `authenticate_caller` `gates.rs:847`). Either the **admin
bearer** (`HCFS_ADMIN_BEARER_TOKEN`, `gates.rs:82`, `is_admin_token`
`gates.rs:146`) or a user bearer whose resolved SS58 equals `account_ss58`
(`authorize_claimed_ss58`, `upload.rs:1154`).

Size cap: file part **16 MiB** declared-or-actual (`s3_declared_admissible`
`upload.rs:454`, in-loop cap `upload.rs:507`). `account_ss58` part ≤ 128 B
(`ACCOUNT_SS58_FIELD_CAP`, `upload.rs:38`). See §Caps for the request-level
`DefaultBodyLimit(16 MiB)` interaction (`router.rs:110`).

### 2. Retrieve by that identifier, with HTTP Range — **YES**

Endpoint: `GET /download/{ss58}/{file_id}` (`download_no_folder` `file.rs:926`
delegates to `download` with an empty `folder_hash` `file.rs:936`).

- **Range: supported.** The handler parses the `Range` header via
  `storage::ByteRange::parse` (`file.rs:993-996`) and streams a `206` with
  `Content-Range`/`Accept-Ranges` (`build_download_response` `file.rs:752-791`).
  For a non-chunked S3-gateway blob it goes `stream_single_download`
  (`file.rs:625`) → `StorageBackend::download(arion_hash, s3_hash, range)`
  (`backend.rs:705`), which does a native S3 Range GET (`download_s3_range`
  `backend.rs:1534`, HEAD-then-ranged-GET; Arion Range via header
  `backend.rs:1342`). Only single `bytes=start-` / `bytes=start-end` forms
  parse; suffix ranges (`bytes=-500`) return `None` → full body
  (`ByteRange::parse` `backend.rs:140-154`, table `backend.rs:1771`).
- **Auth required: yes.** `authorize_drive_read` (`file.rs:957`) — owner bearer
  matching `{ss58}`, admin bearer, or recovery principal. Empty `folder_hash`
  keeps the drive-membership fallback inert and passes `ExemptRoute::Allowed`
  (`file.rs:963`, `exempt.rs:80`), so the gateway path is owner/admin-only by
  construction.
- Response headers mirror native: `X-Size-Bytes` (plaintext size the S3 service
  declared), `Content-Length` (ciphertext), `X-Revision-*`, etc.
  (`file.rs:765-791`).

Contrast with `public_download` (`public.rs:23`, `router.rs:179`): **no auth**,
looked up **directly by content hash** (`storage.download(&hash, Some(&hash),
…, None)` `public.rs:45`) — and **no range** (it hard-codes `None` and never
reads a `Range` header). So `public_download` is not a substitute for the
authenticated ranged read; it is only useful as an unauthenticated
whole-object-by-content-hash fetch.

### 3. Delete by identifier; idempotent? — **YES to delete; single-delete is NOT idempotent, batch is**

Endpoint: `DELETE /delete/{ss58}/{file_id}` (`delete_file_no_folder`
`file.rs:1040` → `delete_file` `file.rs:1055`).

- Deletes the `file_records` row (`db.delete_file` `file.rs:1097`) then
  fire-and-forgets blob reclamation (`cleanup::delete_record_blobs`
  `file.rs:1127`, `storage/cleanup.rs`).
- **Idempotency: a missing file returns `404 file_not_found`**
  (`file.rs:1102-1111`) — so the *single* delete is not idempotent; a second
  DELETE of the same id 404s. If the S3 service wants idempotent
  delete-at-zero, either (a) treat 404 as success on its side, or (b) use the
  **batch** endpoint `POST /delete_files`, which *is* idempotent per item
  (`already_deleted` for a missing row, HTTP 200, `batch_delete_files`
  `file.rs:1165`, `:1254`). Caveat: `/delete_files` is `ExemptRoute::Denied`
  and drive-scoped (`authorize_drive_access` `file.rs:1185`), so a listed/exempt
  tenant SS58 would 403 there — with the admin bearer and a non-exempt tenant
  it works.

Blob deletion is best-effort and **carries no storage-layer refcount** (see §6)
— `StorageBackend::delete` just fans a delete to Arion + S3 concurrently and
logs failures (`backend.rs:774-788`). This is exactly why the S3 service must
own refcounting and give each blob a unique name.

### 4. Per-tenant billing attribution WITHOUT changes — **YES (with an admin/service bearer)**

The gateway keys everything under the caller-supplied `account_ss58`, and the
admin bearer lets you claim **any** SS58.

Trace of how the SS58 used for `record_summary_delta` is resolved:

1. `account_ss58` is read from the first multipart field (`upload.rs:143-156`)
   and handed to `handle_s3_upload(account_ss58, …)` (`upload.rs:158`, `:424`).
2. `authorize_claimed_ss58(caller_ss58, claimed=account_ss58, folder_hash="",
   "s3", ExemptRoute::Allowed)` (`upload.rs:435`). **With the admin bearer,
   `caller_ss58` is `None`** (`authenticate_caller` returns `None` for the admin
   token, `gates.rs:851-853`), and `authorize_claimed_ss58` short-circuits to
   `Ok(AuthorizedWriter { caller_ss58: None })` **without comparing to
   `account_ss58`** (`upload.rs:1162-1165`). So an admin caller can name an
   arbitrary tenant. (A *non-admin* bearer must have its verified SS58 ==
   `account_ss58`, `upload.rs:1166-1170`.)
3. The `FileRecord` is built with `user_id = account_ss58`
   (`build_s3_file_record` `upload.rs:1119-1120`).
4. The summary delta is recorded under that SS58:
   `record_summary_delta(app_state, &account_ss58, "", bytes_delta,
   count_delta)` (`upload.rs:608-609`). With `folder_hash = ""`,
   `composite_key(ss58, "") == ss58`, so **only the bare `{ss58}` row** is
   written, billed unless exempt (`record_summary_delta` `helpers.rs:440-455`;
   `docs/HCFS-BILLING.md` §3, §6.2).

Does `hcfs-chain-reporter` then report that tenant's S3 usage on-chain
automatically? **Yes.** The reporter scans dirty `user_summaries` rows, and a
bare `{ss58}` row classifies as `Origin::Bare` = the account's *combined*
total; S3 usage is derived as `s3 = max(0, bare − Σfolders − shares)`
(`hcfs-chain-reporter/src/identify.rs:16-24`, `classify_user_id`
`identify.rs:47`; `docs/HCFS-BILLING.md` §8). A tenant that only ever receives
S3-gateway blobs has no folder/share rows, so its entire bare total is reported
as `s3_bytes`/`s3_count` to `pallet_marketplace` + `pallet_arion`.

**The single-service-account collapse warning:** attribution is *only* per
tenant if the S3 service passes each tenant's real SS58 as `account_ss58`. If it
instead used **one service SS58** for all tenants, every blob's summary delta
lands on that one bare row (`upload.rs:609`) and all tenants collapse into a
single on-chain account — no per-tenant split is possible, because
`record_summary_delta` only ever sees the `account_ss58` string it was handed.
There is no other tenant dimension on this path.

**Important exemption caveat.** If the tenant SS58 is listed in
`HCFS_EXEMPT_ACCOUNTS`, the bare row is written with `bill: false`
(`helpers.rs:453`) and the reporter *skips* it at submit time
(`docs/HCFS-BILLING.md` §8 step 4, §9). So for per-tenant chain reporting to
happen, tenant SS58s must **not** be exempt. But a non-exempt tenant is then
also **quota-gated** (see §5) — you can't have "reported but never gated" for
the same SS58 through this surface without an hcfs change.

### 5. Credit/quota gate on the upload path — **YES, it enforces the tenant's quota; and `/can_upload` exists to pre-check**

The gateway upload path **does** run the full write gate for the claimed tenant:

`validate_billing(app_state, headers, BillingSubject::s3_gateway(account_ss58),
billable_growth(existing?.size, content_length))` (`upload.rs:484-496`). That
gate (`helpers.rs:192`):

1. `account_guard::enforce(Write, ExemptRoute::Allowed)` — suspension/read-only,
   but exempt SS58s skip it on this Allowed route (`helpers.rs:204`,
   `account_guard.rs:92`, `exempt.rs:90`).
2. `should_bypass_billing` — exempt SS58, valid `X-Billing-Bypass`, or pending
   migration (`helpers.rs:216`).
3. Otherwise the S3 rail: mapped plan cap, else **credits** via
   `s3_credits_gate` (`helpers.rs:289`, `evaluate_quota` `helpers.rs:243-273`).
   A non-exempt tenant with no credits gets **402** (`docs/HCFS-BILLING.md`
   §4.2, §6.2).

So: a **non-exempt** tenant IS gated (this is the same coin as §4 — reporting
requires non-exempt, non-exempt implies gated). An **exempt** tenant skips the
gate *and* skips reporting.

Pre-check: `POST /can_upload` (`upload.rs:642`, `router.rs:152`) mirrors the
gate with `preview` (peek, no headroom consumed) and returns HTTP 200
`{result, error}` (`upload.rs:686-698`). It is `ExemptRoute::Allowed`. hippius-s3
already calls it before every PUT (`docs/HCFS-BILLING.md` §6.3). **Quirk:**
`/can_upload` charges the **full** `size_bytes` and ignores `X-Billing-Bypass`,
while `/upload` charges **growth** and honors bypass — the two can disagree
(`docs/HCFS-BILLING.md` §4.1, §13.1). `CanUploadRequest.folder_hash` defaults to
`""` → the S3/credits rail, which matches the gateway write.

**When must the S3 service run its own credit gate?** Only if it wants a billing
policy different from hcfs's (e.g. gate exempt/service-account tenants, or price
per-object growth differently, or avoid the "S3 credit gate also prices the
tenant's Drive bytes on the shared bare row" behavior — `docs/HCFS-BILLING.md`
§13.12). For plain per-tenant credit gating, `/can_upload` + `/upload`'s own
gate suffice.

### 6. Refcount ownership stays with the S3 service — **YES; nothing in hcfs prevents it (and hcfs itself has no blob refcount)**

hcfs storage delete is unconditional: `StorageBackend::delete` deletes the
content-hash-keyed object from Arion and S3 with no reference check
(`backend.rs:774-788`), triggered by row delete via `delete_record_blobs`
(`cleanup.rs`, "the DB row removal is what makes a file gone"). There is **no**
cross-`file_record` refcount anywhere in hcfs — the code comment even notes an
orphaned blob is tolerated.

This *is* the one hcfs behavior that would bite a naive integration: because the
storage key is `blake3(ciphertext)` (`backend.rs:988`), **two `file_records`
with identical ciphertext share one storage object**, and deleting either
`file_record` deletes the shared blob out from under the other. But that only
happens if the S3 service points two hcfs objects at byte-identical ciphertext.

The clean design (and the one the prompt already intends): the S3 service keeps
its own blob refcount in its own Postgres, gives each **unique** blob a unique
hcfs `file_name` (1:1 `file_record ↔ blob`), and calls hcfs `DELETE` only when
its refcount reaches zero. hcfs neither knows nor cares — it just deletes the
one row and its one blob. Nothing in hcfs blocks this.

---

## The zero-change integration design

**Auth.** The S3 service holds the hcfs **admin bearer**
(`HCFS_ADMIN_BEARER_TOKEN`) and sends `Authorization: Bearer <admin>` on every
call. This lets it act for any tenant SS58 (§4). (Alternatively a per-tenant
user bearer whose SS58 == the tenant, but a multi-tenant gateway will use
admin.)

**Blob naming rule (the linchpin).** For each unique ciphertext blob, choose a
stable, unique hcfs object name — e.g. `name = <blob_id>` where `blob_id` is the
S3 service's own id or the blob's content hash. Store `(tenant_ss58, blob_id)`
in the S3 service's own DB. Then `file_id = hex(blake3(name))` is deterministic
and 1:1 with the blob.

### Store

```
POST {HCFS}/upload
Authorization: Bearer <admin>
Content-Type: multipart/form-data; boundary=...

--...
Content-Disposition: form-data; name="account_ss58"

<tenant_ss58>
--...
Content-Disposition: form-data; name="file"; filename="<blob_id>"
Content-Length: <exact ciphertext byte length>      # REQUIRED, must equal body
Content-Type: application/octet-stream

<~4 MiB ciphertext bytes>
--...--
```

- First field MUST be `account_ss58` (routing signal, `upload.rs:143`).
- `file` part MUST carry `Content-Length` == the bytes that follow, else 400
  `size_mismatch` (`upload.rs:525`; `extract_file_field` `upload.rs:1308-1319`).
- Persist in the S3 DB: `file_id = hex(blake3("<blob_id>"))` (or read it back
  from the `Success.file_id` in the response, `upload.rs:629`).

Response `200 {"Success":{file_id, upload_id, timestamp}}` (`upload.rs:626-634`,
`s3-gateway.md`).

### Get (with range)

```
GET {HCFS}/download/{tenant_ss58}/{file_id}
Authorization: Bearer <admin>
Range: bytes=<start>-<end>          # optional; single closed/open range only
```

`200`/`206`; body is the raw ciphertext (`file.rs:942`). The S3 service does its
own envelope decryption after fetch.

### Delete (at refcount zero)

```
DELETE {HCFS}/delete/{tenant_ss58}/{file_id}
Authorization: Bearer <admin>
```

Call only when the S3 service's own refcount for that blob hits zero. Treat
`404` as success/idempotent on the S3 side (`file.rs:1102`), or batch via
`POST /delete_files` for native idempotency (§3 caveat: Denied route + non-exempt
tenant).

### Optional pre-check

`POST /can_upload {ss58_address: tenant_ss58, size_bytes, folder_hash: ""}`
before store, if the S3 service wants hcfs's quota answer (`upload.rs:642`).

---

## What the S3 service must own

### Always (regardless of §4/§5)

- **Blob refcounting.** hcfs has no storage-layer refcount; delete is
  unconditional (`backend.rs:774`, `cleanup.rs`). The S3 service holds the
  refcount and only issues hcfs `DELETE` at zero, with the unique-name rule so
  one hcfs object == one blob (§6).
- **All S3 metadata** — buckets, objects, versions, multipart parts, the
  object→blob(s) chunk map. hcfs stores flat named blobs keyed by
  `(ss58, blake3(name))`; it has no concept of buckets, versions, or parts.
- **Multipart / large objects.** Each hcfs object is ≤16 MiB (§Caps). The S3
  service splits large objects into ~4 MiB ciphertext chunks itself (which it
  already does) and stores each chunk as its own hcfs object; reassembly and
  range math across chunks live in the S3 service.
- **Its own envelope encryption + keys.** hcfs stores plaintext-from-its-view
  bytes; the gateway path is explicitly *not* E2E (`s3-gateway.md` top).

### Conditionally (only if the existing surface doesn't fit)

- **Its own billing/credit gate** — needed only if it wants a policy different
  from hcfs's S3 rail: gating exempt/service tenants, per-object-growth pricing,
  or avoiding "the S3 credit gate prices the tenant's shared bare-row Drive
  bytes too" (`docs/HCFS-BILLING.md` §13.12). Otherwise `/can_upload` + the
  gateway gate suffice (§5).
- **Its own usage→chain reporting** — needed only if tenants must be exempt from
  the hcfs gate *yet still* reported, or if the S3 service wants a reporting
  cadence/dimension hcfs doesn't provide. Otherwise, non-exempt tenant SS58s are
  reported automatically as `s3_*` by `hcfs-chain-reporter` (§4,
  `identify.rs`). Note the coupling: through the existing surface a tenant is
  either (gated **and** reported) [non-exempt] or (ungated **and** unreported)
  [exempt] — you cannot get "reported but not gated" for one SS58 without owning
  billing yourself.

---

## Verdict

**A truly zero-hcfs-change integration is achievable for the core storage
contract** (store / ranged-get / delete) and **for per-tenant usage→chain
billing**, provided:

1. the S3 service holds the **admin bearer** and passes each tenant's real
   **SS58** as `account_ss58` (§4);
2. it gives each unique blob a **unique hcfs object name** and owns the
   **refcount**, calling hcfs delete only at zero (§6);
3. tenant SS58s are **not** in `HCFS_EXEMPT_ACCOUNTS` if on-chain reporting is
   wanted (§4) — which also means those tenants are quota-gated by hcfs (§5).

**What is genuinely impossible without an hcfs change** — and the S3-side
workaround for each:

| Impossible as-is | Why (code) | S3-side workaround |
|---|---|---|
| Storage-layer blob **refcount / dedup safety** | `StorageBackend::delete` is unconditional; storage key is `blake3(ciphertext)`, shared across identical-content rows; no cross-row refcount (`backend.rs:774`, `:988`) | Own the refcount; **unique name per unique blob** so 1 hcfs object = 1 blob (already the plan) |
| **Idempotent single delete** | single `DELETE` 404s on a missing row (`file.rs:1102`) | Treat 404 as success, or use `POST /delete_files` (§3 caveat) |
| **Authenticated ranged read without a per-tenant bearer** other than admin | `download` requires owner/admin/recovery (`file.rs:957`) | Use the admin bearer (as designed); `public_download` is no-auth but **no range** and content-hash-only (`public.rs:45`) |
| **"Reported but ungated" (or "gated but unreported") for the same SS58** | exempt ⇒ `bill:false` + reporter skip (`helpers.rs:453`, billing §8/§9); non-exempt ⇒ gated + reported | Own billing + reporting entirely if you need to decouple the two |
| **Buckets / versions / multipart-parts semantics, >16 MiB objects** | gateway is flat named blobs, 16 MiB cap (`upload.rs:49`) | Own all S3 metadata + chunking (already the plan) |

None of the "impossible" items block the intended design, because every one of
them is already something the prompt assigns to the S3 service. **Net: ship it
with zero hcfs changes.**

---

## Caps, limits, and quirks to respect

- **16 MiB file-part cap** (`S3_FILE_FIELD_CAP`, `upload.rs:49`; admission check
  `upload.rs:454`; in-loop cap `upload.rs:507`). Live hippius-s3 posts ≤ 4 MiB +
  28 B AEAD, well under (`s3-gateway.md`). Keep ~4 MiB chunks.
- **Request-level `DefaultBodyLimit(16 MiB)`** on the `/upload` route group
  (`router.rs:110`). The *whole* multipart request (file part + `account_ss58` +
  boundaries) must fit 16 MiB, so the effective usable file size is a few KiB
  under 16 MiB, not exactly 16 MiB. At ~4 MiB chunks this never binds.
- **`Content-Length` on the `file` part is mandatory and must equal the bytes**
  (`upload.rs:525`, `:1308`). `curl -F` doesn't always set it; the S3 client
  must (`s3-gateway.md` note).
- **`account_ss58` part ≤ 128 B** (`ACCOUNT_SS58_FIELD_CAP`, `upload.rs:38`).
- **Range parsing is narrow**: only `bytes=start-` and `bytes=start-end`;
  **suffix ranges `bytes=-N` are unsupported** and silently fall back to a full
  200 (`ByteRange::parse` `backend.rs:140-154`, cases `backend.rs:1771`). The S3
  service must translate S3 suffix-range semantics itself.
- **`path_hash` = `blake3(file_name)` keying / collisions**
  (`upload.rs:1086`). Within one `(ss58, folder_hash="")` namespace, two objects
  with the same name collide (overwrite, revision bumped). Cross-tenant is safe
  (different `user_id`). **Drive vs S3 collision:** a native no-folder upload
  also writes the bare `{ss58}` namespace (`composite_key(ss58,"")==ss58`,
  billing §3), so a native no-folder `path_hash` could in principle collide with
  a gateway `blake3(name)` in the same bare namespace. In practice the tenant
  SS58s owned by the S3 service won't be used for native no-folder uploads;
  don't share a tenant SS58 between the S3 service and a native Drive client.
- **Content-hash storage dedup** (`backend.rs:988`): identical ciphertext →
  identical storage key → shared object. With the unique-name rule this only
  matters if the S3 service deliberately points two names at identical bytes;
  don't, or accept shared-blob delete semantics.
- **`public_download` is no-auth, no-range, content-hash-addressed**
  (`public.rs:23`, `:45`, `router.rs:179`). Useful only as an anonymous
  whole-object fetch by `blake3(ciphertext)`; **do not** rely on it for the
  authenticated ranged read.
- **Delete is best-effort and fire-and-forget** (`cleanup.rs`,
  `backend.rs:774`): a `200`/success from `DELETE` means the *row* is gone; the
  Arion/S3 blob delete may lag or fail silently (orphan tolerated). The S3
  service should not assume immediate byte reclamation.
- **`X-Size-Bytes` = plaintext, `Content-Length` = ciphertext** on downloads
  (`file.rs:765-777`; CLAUDE.md size semantics). The gateway stores the uploaded
  ciphertext length as `size_bytes` (billing §2), so for the S3 service both are
  effectively the ciphertext length unless it declares otherwise — it declares
  nothing here (the gateway uses the part length).
- **`/can_upload` vs `/upload` disagree**: full-size + no-bypass vs growth +
  bypass (`docs/HCFS-BILLING.md` §4.1, §13.1). Don't treat a `/can_upload`
  `true` as a guarantee, nor a `false` as a hard stop for a replacement.
- **Exempt-account coupling** (`exempt.rs`, billing §9): listing a tenant SS58
  in `HCFS_EXEMPT_ACCOUNTS` waives gate + suspension + Django usage + chain
  submit on Allowed routes, but 403s it on Denied routes (e.g. `/delete_files`,
  `upload.rs`/`file.rs` Denied paths). Choose per-tenant exempt status
  deliberately.
- **Storage backend must be `s3` or `both`** for S3 range/HEAD behavior as
  described; `arion`-only stores via the Arion gateway and ranges via Arion
  (`from_env` `backend.rs:420`; download fallback `backend.rs:705`). This is an
  hcfs *deployment* config, not a code change.

---

## Open questions

1. **Admin bearer blast radius.** The admin bearer bypasses SS58 matching on
   *every* endpoint, not just the gateway (`gates.rs:290-296`, `:684-687`,
   `:851`). Is the S3 service comfortable holding a credential that can read/write
   any account, or should hcfs operators mint a *scoped* service bearer? (Would
   be an hcfs change; today only admin gives cross-tenant claim.)
2. **Exempt vs reported tradeoff.** Do tenants need hcfs-side quota gating, or
   will the S3 service gate credits itself? If the latter, tenants should be
   exempt — but then hcfs won't report them on-chain and the S3 service must own
   reporting. Which side owns the money path?
3. **Chain reporter tenant fan-out.** The reporter submits ≤ 250 updates/tick
   and there can be only one signer (`docs/HCFS-BILLING.md` §8). Does the
   expected tenant/blob churn from the S3 service fit that budget, or will it
   starve native Drive reporting?
4. **Shared bare-row semantics.** For a tenant that also uses native HCFS Drive,
   the bare `{ss58}` row mixes Drive + S3 (`identify.rs:16-24`), and the S3
   credit gate prices the combined total (`docs/HCFS-BILLING.md` §13.12). Are S3
   tenant SS58s guaranteed disjoint from Drive users?
5. **Delete durability expectations.** Given best-effort fire-and-forget blob
   delete, does the S3 service need a confirmation that bytes are actually gone
   (e.g. for compliance/erasure)? hcfs offers none today.
6. **Retry-worker interaction.** Under `HCFS_STORAGE_BACKEND=both`, a failed
   Arion push is retried from S3 by `hcfs-retry-worker` keyed on
   `(user_id, path_hash)` (`helpers.rs:682`, retry-worker). Confirm the S3
   service's tenant SS58 + name scheme doesn't collide with rows the retry
   worker keys on.
7. **`get_state` for listing.** The S3 service keeps its own object index, so it
   likely won't use `GET /get_state`; confirm it never needs hcfs to enumerate
   (the gateway lists gateway blobs alongside native ones, `s3-gateway.md`
   Notes).
