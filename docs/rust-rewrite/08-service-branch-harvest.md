# 08 — Service-Branch S3 Module: Harvest Plan

**Source:** `hcfs` `service` branch, `hcfs-server/src/s3/` (32 files, 14,141 LOC Rust)
**Worktree used for all `file:line` refs below:**
`…/scratchpad/service-wt/hcfs-server/src/s3/`
**Target contract to match:** `hippius-s3/hippius_s3/api/s3/` (currently Python/FastAPI; the Rust rewrite must emit the same wire behavior).

> **✅ RE-VERIFIED against service branch `778ea38` (2026-09-15).** The file-by-file audit below still
> matches the current branch: all 32 files present (~14k LOC); the coupling is exactly as described —
> `auth/signing.rs`'s SigV4/SigV2 *math* is clean standard-crate code (hmac/sha2/sha1/md5/hex/base64/
> chrono/percent_encoding), its only sled touch is 3 credential-lookup fns taking `&sled::Db` (the
> `CredentialStore` seam); `error.rs` has one `From<sled::Error>` to drop; `chunked.rs`/`utils.rs`/
> `auth/mod.rs` are clean; `policy.rs` splits at ~L589 into a (full IAM) evaluator + sled storage.
> **Does it compile as-is? No** (unchanged — undeclared module, deps like quick-xml/hmac absent, sled
> pervasive) — proving "the harvest compiles" *is* the Phase-0 extraction (lift signing.rs + error.rs
> + auth types + uri-encode into `s3-protocol`, stub `CredentialStore`, add the standard crates, run
> SigV4 vectors). The coupling analysis says that extraction is bounded and low-risk — the ~35% is not
> optimistic. **One scope correction:** `policy.rs`'s IAM evaluator is a *fast-follow*, not v1 — see
> its row in §2 (register C1/B2 = public-read subset only in v1).

## TL;DR

The module is dead and uncompilable, written for an old **sled + single-Arion** hcfs. But its **S3 protocol layer is genuinely good and mostly decoupled**: SigV4/SigV2 signing (`auth/signing.rs`), the ~57-variant `S3Error` catalog (`error.rs`), IAM-style policy evaluation (`policy.rs`), the AWS-chunked decoder (`chunked.rs`), per-feature XML parse/serialize (cors, lifecycle, tagging, encryption, locking), conditional-header (RFC 7232) evaluation (`objects/headers.rs`), and the query-param sub-resource dispatch table (`server.rs`).

The coupling is concentrated and shallow: **157 `sled` refs are almost all `db: &sled::Db` parameters + `db.open_tree(...)` calls in per-feature CRUD functions and HTTP handlers**, sitting *below* pure logic in the same file. `AppState` appears only as `app_state.db`, `app_state.next_arion_url()`, `app_state.arion_api_key`, `app_state.http_client` (arion.rs) — a tiny surface. The harvest strategy is therefore: **lift the pure protocol logic into an `s3-protocol` crate behind two storage traits (`MetadataStore`, `ObjectBackend`), and rewrite the storage halves against hippius-s3's Postgres + pipeline.**

Rough reuse: **~500 LOC harvestable as-is, ~4,500 LOC harvestable with edits, ~9,000 LOC must be rewritten, ~160 LOC discarded.**

---

## 1. File-by-file disposition table

Coupling legend: **sled** = direct `sled::Db`/tree use, **AS** = `AppState`, **Arion** = hard-coded Arion HTTP, **mw** = `crate::middleware`, **shared** = `hcfs-shared` type drift.

| File | LOC | Purpose | Coupling to old internals | Disposition | Notes |
|---|---:|---|---|---|---|
| `mod.rs` | 236 | Module root; `try_response!` macro, URI-encode, `format_iso8601`, `parse_copy_source`, XML response helpers, `make_s3_key/prefix`, `push_billing` | sled key-shape (`make_s3_key` for sled composite keys); AS+billing (`push_billing` L162); mw (`generate_request_id` L219) | **HARVEST w/ edits** | Pure helpers (`s3_uri_encode`, `format_iso8601`, `sanitize_content_type`, `parse_copy_source`, `validate_xml`, `to_xml_response`) move as-is (~140 LOC). `make_s3_key`/`make_s3_prefix` are sled-composite-key builders — DISCARD (Postgres uses columns). `push_billing` REWRITE. `try_response!` keep. |
| `error.rs` | 221 | `S3Error` enum (57 variants) → status/code/message; `IntoResponse` renders `<Error>` XML | `From<sled::Error>` L59, `From<bincode::Error>` L65; mw `generate_request_id` L181 | **HARVEST w/ edits** | Crown jewel. Drop the two `From` impls; inject a `request_id: &str` (or a `RequestIdFn`) instead of calling `crate::middleware`. XML shape already matches hippius-s3 (no `xmlns` on `<Error>`). See §5 for catalog-alignment deltas. |
| `db.rs` | 38 | `sled_get/put/delete` bincode helpers | 100% sled + bincode | **DISCARD** | Pure sled adapter; replaced by `MetadataStore` trait impls over Postgres. |
| `utils.rs` | 81 | HTTP date fmt/parse, `get_header`, `parse_param(_clamped)`, `strip_head_body` mw, `is_aws_chunked`, `strip_xml_declaration` | none | **HARVEST as-is** | Fully pure (`strip_head_body` is a generic axum middleware). Zero edits. |
| `chunked.rs` | 353 | `AwsChunkedDecoder` streaming SigV4 chunk decoder + per-chunk signature chain verify | none except `crate::s3::auth::hex_hmac_sha256` L342 (moves with signing) | **HARVEST as-is** | Self-contained `Stream` impl, `MAX_CHUNK_SIZE`/`MAX_BUFFER_SIZE` guards, constant-time compare. Only intra-crate ref. Excellent, keep verbatim. |
| `auth/signing.rs` | 786 | SigV4 header + SigV4/SigV2 presigned verification; canonical request/query/headers; `derive_signing_key`; constant-time compare | sled: `verify_*(db: &sled::Db …)` + `get_credentials(db,…)` calls (5 refs) | **HARVEST w/ edits** | The signing math is pure. Replace `db: &sled::Db` params with a `CredentialStore` trait (or pass the looked-up `Credentials` in). `.expect("HMAC key length")` L51/L59 trips hippius workspace `expect_used`—make infallible or return error. |
| `auth/mod.rs` | 240 | `parse_auth_header`, DashMap rate limiter, `authenticate_full`/`try_authenticate`/`require_auth`, `billing_user_id` | AS: `&Arc<AppState>` + `app_state.db` (3 refs) | **HARVEST w/ edits** | `parse_auth_header` + rate limiter are pure (harvest). `authenticate_full`/`try_authenticate` take `&AppState` only to reach `.db` → change to `&impl CredentialStore`. STS rejection & anonymous fallthrough are protocol logic, keep. |
| `auth/credentials.rs` | 90 | `S3Credentials` struct + sled CRUD (create/get/delete/list by user, secondary index) | 100% sled + bincode; `make_s3_key` index | **REWRITE** (struct HARVEST) | `S3Credentials` struct (~20 LOC) harvests as the `CredentialStore` value type. All CRUD is sled and must be reimplemented over Postgres (hippius already has access-key auth in `gateway/middlewares/access_key_auth.py`). |
| `policy.rs` | 832 | IAM-style bucket policy: types, `S3Action` (100+ actions→ARN strings), `evaluate_policy`, glob/ARN matching, condition eval, `validate_policy`; + sled storage + HTTP handlers | sled: `BUCKET_POLICIES_TREE` store/get/delete (L596–626); handlers L741–832 | **HARVEST w/ edits — but MIND V1 SCOPE** | ~430 LOC of pure evaluation (`evaluate_policy`, `principal_matches`, `action_matches`, `arn_glob_match`, `conditions_match`, `validate_policy`, `S3Action`) is well-built. **BUT decisions C1/B2 scope *general* bucket policy OUT of v1 — v1 is the public-read subset only** (an `Allow * s3:GetObject .../*`, matching Python's `_validate_public_policy`), with general policy a *fast-follow*. So for **v1, harvest only** the document types (`BucketPolicy`/`PolicyStatement` deserialize) + a public-read validator; keep `evaluate_policy`/principals/conditions **for the fast-follow**, not wired into the v1 enforcement path. Sled storage fns are **misnamed** `store_lifecycle_policy_raw`/`get_lifecycle_policy` (they store *bucket policies*, L596/L603) — rewrite over `MetadataStore`. Handlers rewrite. *(Verified 2026-09-15 against service branch `778ea38`: it IS a full Allow/Deny/Principal/Condition IAM evaluator — hence the scope note.)* |
| `cors.rs` | 497 | CORS XML parse/serialize, rule matching, preflight, `cors_middleware`, `apply_cors_headers` + sled store/handlers | sled store L448/L487; AS in `cors_middleware` L362 | **HARVEST w/ edits** | `parse_cors_xml`/`cors_to_xml`/`match_cors_rule`/`match_preflight`/`preflight_response`/`apply_cors_headers` pure (~380). `cors_middleware` needs config lookup via trait. Sled CRUD rewrite. |
| `encryption.rs` | 322 | SSE metadata model, encryption-config XML parse/serialize, SSE header parse/emit, default-encryption apply | sled: `BUCKET_ENCRYPTION_TREE` L53–68, `apply_bucket_default_encryption` L267 | **HARVEST w/ edits** | `SseMetadata`, `parse_encryption_config_xml`, `encryption_config_to_xml`, `parse_sse_headers`, `add_sse_headers`, `insert_sse_headers` pure (~220). Storage + `apply_bucket_default` rewrite. NOTE: hippius-s3 encrypts client-side w/ KEK/DEK envelopes — this SSE model is header-shaped only and likely **superseded**; see §5. |
| `tagging.rs` | 306 | Tag validation, tagging header/XML parse, XML serialize + sled store/handlers | sled: `BUCKET_TAGS_TREE` L187–211; object tags via `store_object` in handlers | **HARVEST w/ edits** | `validate_tags`, `parse_tagging_header`, `parse_tagging_xml`, `tagging_to_xml` pure (~180). CRUD + handlers rewrite. Watch §5: hippius emits `NoSuchTagSet`; this returns empty tagset. |
| `lifecycle.rs` | 1000 | Lifecycle XML parse/serialize, filter matching, sled store, expiration background loop, handlers | sled L508; AS in `lifecycle_expiration_loop` L623 (+ Arion delete) | **HARVEST w/ edits** | `parse_lifecycle_xml`, `serialize_lifecycle_xml`, `object_matches_filter`, all config types pure (~470). The expiration loop (scans sled, deletes via Arion) is REWRITE against hippius pipeline. |
| `versioning.rs` | 758 | Bucket versioning state, version-key sled encoding, version CRUD, `list_object_versions`, XML result, handlers | sled-heavy (16 refs): `VERSIONING_TREE`, `VERSIONS_TREE`, key builders, scans | **REWRITE** (XML+idgen HARVEST) | `generate_version_id`, `BucketVersioningState`, `ListVersionsResult` XML serialization harvest (~120). Everything else is sled version-store mechanics that Postgres does natively (hippius has `list_object_versions_endpoint.py` + version tables). |
| `public_access.rs` | 119 | Public Access Block config get/put/delete + XML | sled: `PUBLIC_ACCESS_BLOCKS_TREE` (9 refs) | **HARVEST w/ edits** | `PublicAccessBlockConfiguration` + XML (~60) harvest; storage rewrite. Small. |
| `server.rs` | 1026 | S3 router; `start_s3_server` (axum+TLS); `root_handler`/`bucket_handler`/`object_handler`; query-param sub-resource dispatch macros | AS everywhere; TLS via `crate::tls`; calls every handler | **REWRITE** (keep as dispatch **reference**) | The **dispatch table is the most valuable design artifact** — the exhaustive mapping of `?subresource`+method→action+handler (L429–692, L749–1020) encodes S3's REST routing. Port the *structure*; the wiring (AppState, sled, hcfs TLS, hcfs `middleware::request_logger`, `lifecycle`/`multipart` background spawns) is all hcfs-specific. hippius uses FastAPI routers instead (see §5). |
| `arion.rs` | 328 | Arion refcount (sled), stream-from-Arion, upload-to-Arion, blob cleanup | sled refcount tree (L19); Arion HTTP (53 refs); AS (5); `hcfs_shared::ArionUploadResponse` | **REWRITE** | Pure old-internals. Replaced wholesale by the `ObjectBackend` trait + hippius's writer/reader pipeline. The refcount-by-file-id GC pattern does not map (hippius dedups differently). DISCARD logic, keep only the response-building shape in `stream_from_arion` L239–278 as a header-emission reference. |
| `buckets/mod.rs` | 556 | Bucket model + owner index, `check_bucket_access` (policy+ACL+public), `bucket_auth_check`, create/head/delete/list/location handlers | sled (7): `BUCKETS_TREE`, `BUCKET_INDEX_TREE`; AS (8) | **REWRITE** (types + access-decision logic HARVEST) | XML types (`S3ListAllMyBucketsResult`, `S3BucketOwner`, etc. ~80) harvest. **`check_bucket_access` L307 is important protocol logic** (policy eval → ACL fallback → public-access precedence) — harvest the *decision flow* into the crate behind `MetadataStore`, rewrite its sled lookups. |
| `buckets/access_control.rs` | 458 | ACL model (`S3BucketAcl`, canned ACLs), `from_header`, public-read/write predicates, source-read check, ACL XML handlers | sled: `BUCKET_ACLS_TREE` (8); AS (3) | **HARVEST w/ edits** | `S3AclOperation`, `S3BucketAcl` + `from_header`/`allows_public_read`/`allows_public_write`/`check_bucket_source_read_access` pure (~120). Storage + handlers rewrite. §5: hippius emits `MalformedACLError`; canned-ACL XML differs (see `acl_endpoints.py`). |
| `buckets/config.rs` | 273 | Generic sled config CRUD (`handle_get/put/delete_bucket_config`, named-config variants), `MissingConfigBehavior` | 100% sled generic tree store | **DISCARD** (keep `MissingConfigBehavior` idea) | This is a sled generic-KV shim for the "store raw XML blob per bucket sub-resource" pattern. Postgres models these as columns/tables. `MissingConfigBehavior::{Error,DefaultXml}` (the default-XML-when-absent semantics) is worth preserving as a small enum. |
| `objects/mod.rs` | 114 | `S3Object` metadata model + `new_delete_marker` | sled tree name; `SseMetadata`, lock types | **REWRITE** (as the `MetadataStore` row type) | This is the object-metadata schema. It is the natural `MetadataStore` associated type but must be reconciled with hippius's object/version columns (it lacks `body_blake3`, storage-version, KEK envelope fields hippius needs; see §5). Keep as a modeling reference. |
| `objects/hashing.rs` | 60 | `is_hex_sha256`, `HashingStream` (SHA-256 pass-through) | none | **HARVEST as-is** | Pure. (S3_TODO wants to push hashing to Arion; if so this becomes optional, but it's free to keep.) |
| `objects/headers.rs` | 312 | Standard-header parse, `x-amz-meta-*`, checksum header, storage-class reject, **conditional-header eval (RFC 7232)**, response-header builders | depends on `S3Object`, `SseMetadata`, lock types only | **HARVEST w/ edits** | `check_conditional_headers` (L147) + `check_copy_source_conditions` (L89) are pure protocol logic — high value. The response-builder fns read `S3Object` fields; generalize over a small `ObjectMeta` trait or the crate's own object struct. §5: read-side conditionals match; **write-side `If-None-Match` differs** from hippius. |
| `objects/list.rs` | 454 | ListObjectsV1/V2 (prefix/delimiter/common-prefixes, pagination), XML result | sled scan; AS (3) | **REWRITE** (XML result HARVEST) | `ListObjectsResult` + XML serialization (~120) harvest. The list algorithm (delimiter rollup, continuation tokens) is protocol logic worth porting, but its sled prefix-scan implementation is replaced by SQL (hippius `list_objects_endpoint.py`). |
| `objects/upload.rs` | 846 | `put_object`, `handle_post_object` (POST form upload), `store_object(_with_versioning)` | Arion (35); sled `store_object` L36; AS (4) | **REWRITE** | Upload orchestration is entirely storage-path. `handle_post_object` (browser POST policy form) protocol parsing (~150) is portable as a reference. Rest rewrite against hippius writer. |
| `objects/download.rs` | 517 | `fetch_object(_or_error)`, `get_object`, `head_object`, `get_object_attributes` | Arion (19); AS (5); sled fetch | **REWRITE** | Response assembly / header emission is a good reference (matches AWS shapes) but bound to Arion streaming + sled. Rewrite against hippius reader. |
| `objects/delete.rs` | 696 | `delete_object`, `delete_objects` (batch), `remove_object` | Arion (7); sled+refcount (db 4); AS (3) | **REWRITE** | Batch-delete XML request parse + `DeleteResult` XML (~150) portable as reference; deletion mechanics (refcount GC) rewrite. |
| `objects/copy.rs` | 319 | `handle_copy_object` (CopyObject) | Arion (2); AS (2) | **REWRITE** | Copy-source parsing + conditional checks are already in `mod.rs`/`headers.rs` (harvested). Copy execution rewrite against hippius copy pipeline (`copy_helpers.py`). |
| `objects/locking.rs` | 537 | Object Lock model, config XML, retention/legal-hold, `check_object_lock_for_delete`, `apply_default_retention` | sled: `OBJECT_LOCK_CONFIGS_TREE` (9) | **HARVEST w/ edits** | Types + `RetentionMode` + XML parse/serialize + `check_object_lock_for_delete` (the delete-guard logic) pure (~300). Storage rewrite. §5: hippius has a rich object-lock impl (`object_lock_enforcement.py`, `object-lock.md`) — reconcile GOVERNANCE/COMPLIANCE + bypass semantics. |
| `objects/rename.rs` | 125 | `handle_rename_object` (non-standard `?rename` extension) | Arion (3); sled (3) | **DISCARD** | Not an AWS S3 operation. hippius's non-standard extension is **append** (`extensions/append.py`), not rename. Drop. |
| `objects/restore.rs` | 23 | `handle_restore_object` stub (objects always "hot") | sled (1) | **REWRITE** (trivial) | 23-line stub returning success; reimplement as a one-liner if RestoreObject is even routed. |
| `multipart.rs` | 1619 | Full MPU: create/upload-part/upload-part-copy/complete/abort/list-parts/list-uploads; models; stale-upload cleanup loop | Arion (58); sled (10); AS (9); shared | **REWRITE** (XML result types HARVEST) | Largest file. All the XML result structs (`InitiateResult`, `CompleteResult`, `ListPartsResult`, `ListMultipartUploadsResult`, `CopyPartResult` + serialization, ~250) harvest. Part-number/order validation is protocol logic (port). Everything else is Arion+sled MPU state machine → rewrite against hippius `multipart.py` semantics. |

---

## 2. Proposed `s3-protocol` crate

A storage-agnostic crate that owns everything on the wire and nothing about persistence. It compiles with **no `sled`, no `AppState`, no `reqwest`-to-Arion**. It depends only on `axum`/`http`, `quick-xml`, `serde`, the crypto crates, and defines traits the host implements.

### 2.1 Public module layout

```
s3-protocol/
├── error.rs        // S3Error (57 variants), IntoResponse, code/status/message
├── xml.rs          // to_xml_response, xml_ok, no_content, validate_xml, strip_xml_declaration
├── uri.rs          // s3_uri_encode, canonical_uri_encode, parse_copy_source(+version), sanitize_content_type
├── time.rs         // format_iso8601, format_http_date, parse_http_date
├── signing/        // parse_auth_header, verify_sigv4 / _query_string, verify_sigv2_query_string,
│                   //   derive_signing_key, canonical_request/query, constant_time_eq, SigningContext
├── chunked.rs      // AwsChunkedDecoder (verbatim)
├── ratelimit.rs    // DashMap failed-attempt limiter (check_rate_limit / record_failed_attempt)
├── policy.rs       // BucketPolicy, PolicyStatement, S3Action, evaluate_policy, validate_policy,
│                   //   arn_glob_match, bucket_arn/object_arn, PolicyDecision, PolicyRequestContext
├── acl.rs          // S3AclOperation, S3BucketAcl, canned-ACL parsing, public-read/write predicates
├── headers.rs      // parse_standard_headers, parse_metadata_headers, parse_checksum_header,
│                   //   check_conditional_headers, check_copy_source_conditions, response builders
├── model/          // BucketPolicy/CORS/Lifecycle/Encryption/Tagging/ObjectLock/PublicAccessBlock
│                   //   XML parse + serialize + validate (the pure halves of each feature file)
├── listing.rs      // ListObjects{V1,V2}Result, ListVersionsResult, Delete*Result XML shapes + delimiter rollup
├── multipart_xml.rs// Initiate/Complete/ListParts/ListUploads/CopyPart result XML shapes + part-order validation
├── dispatch.rs     // the query-param → (S3Action, method) sub-resource routing table (from server.rs), as data
└── store.rs        // ↓ THE SEAM: MetadataStore + ObjectBackend + CredentialStore traits
```

### 2.2 The SEAM — storage/backend traits

The protocol layer talks to persistence and blob storage **only** through these traits. Both sled (old) and Postgres+pipeline (hippius) can implement them; the crate itself is unaware of either. All are `async` where they touch I/O (use `async_trait` or return `impl Future` under edition-2024 AFIT).

```rust
// ---- Identity / credentials (replaces auth/credentials.rs sled CRUD) ----
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,   // needed by derive_signing_key
    pub user_id: String,
    pub expires_at: Option<i64>,
}

#[async_trait]
pub trait CredentialStore: Send + Sync {
    async fn get_credentials(&self, access_key_id: &str) -> Result<Option<Credentials>, S3Error>;
    // create/delete/list are host-admin concerns, not needed by the request path.
}

// ---- Object + bucket metadata (replaces s3_objects / s3_buckets / all config trees) ----
pub struct ObjectMeta { /* etag, size, content_type, updated_at, metadata, tags,
                           checksum_*, sse, retention, legal_hold, version_id,
                           is_delete_marker, is_latest, parts_count, backend_locator */ }

pub struct BucketMeta { pub name: String, pub owner_id: String, pub created_at: i64 }

#[async_trait]
pub trait MetadataStore: Send + Sync {
    // buckets
    async fn get_bucket(&self, bucket: &str) -> Result<Option<BucketMeta>, S3Error>;
    async fn create_bucket(&self, bucket: &str, owner: &str) -> Result<(), S3Error>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), S3Error>;      // caller checks emptiness
    async fn list_buckets(&self, owner: &str) -> Result<Vec<BucketMeta>, S3Error>;

    // objects
    async fn head_object(&self, bucket: &str, key: &str, version: Option<&str>)
        -> Result<Option<ObjectMeta>, S3Error>;
    async fn put_object_meta(&self, meta: &ObjectMeta) -> Result<(), S3Error>;
    async fn delete_object_meta(&self, bucket: &str, key: &str, version: Option<&str>)
        -> Result<(), S3Error>;
    async fn list_objects(&self, bucket: &str, q: &ListQuery)
        -> Result<ListPage, S3Error>;                                       // delimiter/prefix/cursor
    async fn list_object_versions(&self, bucket: &str, q: &ListQuery)
        -> Result<VersionPage, S3Error>;

    // per-bucket sub-resource config (policy, cors, lifecycle, encryption, tagging,
    // versioning-state, object-lock-config, public-access-block, acl).
    // Modeled as typed getters/setters, NOT a generic raw-blob KV.
    async fn get_bucket_policy_raw(&self, bucket: &str) -> Result<Option<Vec<u8>>, S3Error>;
    async fn put_bucket_policy_raw(&self, bucket: &str, json: &[u8]) -> Result<(), S3Error>;
    async fn get_cors(&self, bucket: &str) -> Result<Option<CorsConfiguration>, S3Error>;
    async fn put_cors(&self, bucket: &str, cfg: &CorsConfiguration) -> Result<(), S3Error>;
    // … lifecycle / encryption / tagging / versioning / object-lock / public-access …
}

// ---- Blob storage (replaces arion.rs) ----
pub struct BackendLocator(pub Vec<String>);  // opaque: hippius chunk ids / cids, was arion_file_ids

#[async_trait]
pub trait ObjectBackend: Send + Sync {
    async fn put(&self, body: BodyStream, ctx: &PutCtx)
        -> Result<StoredObject, S3Error>;                 // returns locator + etag + size
    async fn get(&self, locator: &BackendLocator, range: Option<&str>)
        -> Result<GetStream, S3Error>;                    // streamed body + content-length/range
    async fn delete(&self, locator: &BackendLocator) -> Result<(), S3Error>;
    // multipart: create/put_part/complete/abort or a compose() over part locators
}
```

**Handler signatures then become generic:** e.g. `pub async fn put_object<M: MetadataStore, B: ObjectBackend>(store: &M, backend: &B, user: &AuthedUser, …)` instead of `&Arc<AppState>`. The dispatcher (`server.rs` port) is parameterized over `<M, B, C>` and holds them in one `S3Ctx` struct.

**Request-id injection:** `error.rs` and `xml.rs` currently call `crate::middleware::generate_request_id()`. Replace with a `RequestId(String)` threaded from a request-scoped extension so the crate has no host dependency.

---

## 3. Dependency delta

S3_TODO lists 12 crates. Assessed against what the harvested files actually `use`, and against hippius-s3's existing Rust workspace (`Cargo.toml`, which already has `sha2`, `futures`, `reqwest`, `tokio`, `thiserror`, `serde`, `tokio-util`):

| Crate | S3_TODO | Actually used by harvest? | Where | Verdict |
|---|:--:|:--:|---|---|
| `quick-xml` (serialize) | ✓ | **Yes** | `mod.rs` L45, cors/lifecycle/tagging/encryption/policy XML, `to_xml_response` | **Required** |
| `hmac` | ✓ | **Yes** | `signing.rs` L8 (`HmacSha1`, `HmacSha256`) | **Required** |
| `sha1` | ✓ | **Yes** | `signing.rs` L11 (SigV2 HMAC-SHA1) | **Required** (only if SigV2 presigned is kept) |
| `md-5` | ✓ | **Yes** | `signing.rs` L9 `use md5::Digest`, Content-MD5 / BadDigest path | **Required** |
| `percent-encoding` | ✓ | **Yes** | `mod.rs` L88/L97, `signing.rs` canonicalization | **Required** |
| `base64` | ✓ | **Yes** | `signing.rs` L600 (SigV2 sig), checksum headers | **Required** (already in hcfs workspace as `base64 = {workspace}`) |
| `subtle` | ✓ | **Yes** | `signing.rs` L730, `chunked.rs` L344 (constant-time compare) | **Required** |
| `dashmap` | ✓ | **Yes** | `auth/mod.rs` L8 (rate limiter) | **Required** (or swap for `std` sharded mutex) |
| `uuid` | ✓ | **Weak** | Not seen in harvested protocol files; `generate_version_id` uses its own scheme; MPU upload ids | **Optional** — needed only if version/upload-id generation uses UUIDs; hippius may supply ids. Confirm. |
| `async-stream` | ✓ | **No (in harvest)** | Used in storage-path streaming (upload/download/multipart), all REWRITE | **Deferred** — belongs to the storage rewrite, not the protocol crate. |
| `crc32fast` | ✓ | **No (yet)** | `headers.rs` recognizes `x-amz-checksum-crc32` but no file *computes* CRC32 | **Conditional** — needed only when checksum *validation* is implemented (currently headers are stored/echoed, not verified). |
| `crc32c` | ✓ | **No (yet)** | same — `x-amz-checksum-crc32c` recognized, not computed | **Conditional** — same as above. |

Also implicitly required by harvested code but **absent from S3_TODO**: **`hex`** (`signing.rs` L65/L714, `hashing.rs` L54), **`chrono`** (dates throughout), **`sha2`** (already in workspace), **`bytes`**, **`futures`** (already), **`serde_json`** (policy JSON). Add `hex` and `chrono` explicitly.

Net: **10 of 12 are genuinely required for the protocol crate** (`quick-xml, hmac, sha1, md-5, percent-encoding, base64, subtle, dashmap` + implied `hex`, `chrono`, `serde_json`). `uuid` is probably needed for id generation. `async-stream`, `crc32fast`, `crc32c` are storage-path / not-yet-implemented and should not be added to the protocol crate now. All must be pinned to exact versions per hippius workspace policy (`wildcards = deny`).

---

## 4. Coupling-removal checklist

Concrete edits to sever coupling from the harvest-able files:

1. **`error.rs`** — delete `impl From<sled::Error>` (L59–63) and `impl From<bincode::Error>` (L65–69). In `IntoResponse` (L181) and `mod.rs::to_xml_response` (L219), replace `crate::middleware::generate_request_id()` with an injected `request_id: &str`.

2. **`auth/signing.rs`** — change `verify_sigv4` (L129), `verify_sigv4_query_string` (L291), `verify_sigv2_query_string` (L541) first param from `db: &sled::Db` to `creds: &impl CredentialStore` (or resolve creds before calling and pass `&Credentials`). The internal `get_credentials(db, …)` calls (L222, L377, L583) become `creds.get_credentials(...)`. Make `hmac_sha1`/`hmac_sha256` `.expect("HMAC key length")` (L51, L59) infallible via `Hmac::new_from_slice`'s `Result` → return `S3Error::InternalError` (hippius denies `expect_used`).

3. **`auth/mod.rs`** — `authenticate_full` (L160) and `try_authenticate` (L195) take `app_state: &Arc<AppState>` only for `app_state.db`; change to `creds: &impl CredentialStore`. `billing_user_id` (L40) references `AuthenticatedUser` (fine, moves to crate). Rate limiter (L107–156) is self-contained — move verbatim.

4. **`auth/credentials.rs`** — extract `S3Credentials` struct → crate `Credentials`. Delete all sled functions (L26–90); reimplement in the host as a `CredentialStore` impl over Postgres.

5. **`policy.rs`** — keep L1–589 (evaluation) + L633–734 (validation, ARNs) verbatim. Delete sled storage (`store_lifecycle_policy_raw` L596, `get_lifecycle_policy(_raw)` L603/L615, `delete_lifecycle_policy` L624) and the HTTP handlers (L741–832) — reimplement as thin host handlers calling `MetadataStore::{get,put}_bucket_policy_raw`. **Rename** the misnamed `*_lifecycle_policy_*` fns → `*_bucket_policy_*`.

6. **`mod.rs`** — keep `s3_uri_encode`, `format_iso8601`, `sanitize_content_type`, `parse_copy_source(_version_id)`, `validate_xml`, `xml_ok`/`no_content`/`to_xml_response`, `require_body`, `try_response!`. **Delete** `make_s3_key`/`make_s3_prefix` (L64/L80 — sled composite keys) and `push_billing` (L162 — AppState+billing). `s3_region()` (L50) keeps but reads a crate-level config, not env directly (or leave env-read).

7. **cors / encryption / tagging / lifecycle / locking / public_access** — in each, keep the `parse_*_xml` / `*_to_xml` / validation / matching functions; delete the `const *_TREE` + the `db.open_tree`-based store/get/delete fns + the `handle_*` HTTP wrappers, replacing storage with `MetadataStore` typed getters/setters.

8. **`objects/headers.rs`** — the response-builder fns (`add_standard_headers_with_overrides` L232, `add_checksum_headers` L271, `add_object_lock_headers` L284) read a concrete `S3Object`; change to read the crate's `ObjectMeta` (or a small trait). `check_conditional_headers` (L147) / `check_copy_source_conditions` (L89) are already pure — move verbatim.

9. **`chunked.rs`** — only edit: `crate::s3::auth::hex_hmac_sha256` (L342) becomes `crate::signing::hex_hmac_sha256` (intra-crate). Otherwise verbatim.

10. **`server.rs`** — not edited in place; its dispatch table (L429–692 bucket, L749–1020 object) is transcribed into `dispatch.rs` as the source-of-truth routing map, then re-expressed either as an axum router (hippius chose FastAPI routers — see §5) or a match. Drop `start_s3_server` (hcfs TLS + background-loop spawns), `crate::middleware::request_logger`, `crate::tls`.

11. **`arion.rs`, `db.rs`, `buckets/config.rs`** — no harvest edits; deleted and replaced by `ObjectBackend` / `MetadataStore` impls.

---

## 5. Compatibility check vs hippius-s3

Where the service-branch protocol behavior **differs** from hippius-s3's shipped contract and must be reconciled **toward hippius** (the rewrite matches hippius, not the service branch):

1. **Auth lives in a different layer.** In hippius-s3, SigV4/SigV2 is a **gateway** concern (`hippius_s3/gateway/middlewares/sigv4.py`, `access_key_auth.py`, `auth_orchestrator.py`); the `api/s3/` layer trusts an already-authenticated identity. The service branch does auth *inside* the S3 handlers (`server.rs` → `try_authenticate`). **Decision needed:** does the Rust `s3-protocol` crate own signing (harvest `signing.rs`) or does a separate gateway? The signing code is worth harvesting regardless, but its call site moves to the gateway boundary.

2. **Write-side conditional requests differ.** hippius-s3 supports **only `If-None-Match: *`** on writes (create-if-absent → **412 `PreconditionFailed`**) and returns **501 `NotImplemented`** for any other `If-None-Match` value on a write (`errors.py:256` `conditional_write_not_implemented_response`, `put_object_endpoint.py:96–103`, `multipart.py:1142`). The service branch has **no write-side conditional handling at all** (`headers.rs::check_conditional_headers` is read-side only). **The rewrite must add** hippius's write-side `If-None-Match: *` semantics; the harvested read-side conditional logic matches and stays.

3. **Error catalog mismatches.** hippius emits several codes the service branch does not, and vice-versa:
   - Policy: hippius `InvalidPolicyDocument` / `PolicyAlreadyExists` vs service `MalformedPolicy` (L116). **Align to hippius.**
   - Object lock: hippius `NoSuchObjectLockConfiguration` vs service `ObjectLockConfigurationNotFoundError` (L40). **Align.**
   - Tagging: hippius `NoSuchTagSet` vs service returns empty tagset. **Align.**
   - ACL: hippius `MalformedACLError` (not in service catalog). **Add.**
   - Versioning: hippius `IllegalVersioningConfigurationException` (not in service). **Add.**
   The service branch's catalog is *larger* (57 variants incl. website/replication/metrics/analytics/inventory/intelligent-tiering) than hippius's ~40 — those extra features may be out of hippius's scope; keep the variants but don't route features hippius doesn't implement.

4. **Error XML shape mostly matches, headers differ.** Both omit `xmlns` on `<Error>` (boto3 compat — good, `errors.py:69–71` vs service L197–206). But hippius adds convenience headers **`x-amz-error-code`** and **`x-amz-error-message`** (latin-1-safe) on every error (`errors.py:104–110`) — the service branch does not. hippius uses static **`HostId="hippius-s3"`**; service uses the request-id as HostId (L195). **Align to hippius** (add the headers, use the static HostId or a hippius-shaped one).

5. **Response-override params are gated in hippius.** hippius applies `response-content-type` etc. **only for signed (non-anonymous) requests** and validates CRLF/oversize (`common/headers.py:parse_response_overrides`, `MAX_OVERRIDE_VALUE_LEN=4096`). The service branch applies them unconditionally (`headers.rs:add_standard_headers_with_overrides` L232, `arion.rs` L235). **Align to hippius** (anonymous → no overrides; validate).

6. **SSE model likely superseded.** The service branch's `encryption.rs` models SSE as header echo + a default-encryption config. hippius encrypts **client/server-side with KEK/DEK envelopes** and exposes `X-Hippius-Body-Blake3(-Scope)` semantics (`common/headers.py:body_blake3_headers`) and a KEK service with its own error taxonomy (`errors.py:read_path_crypto_error_response`). The harvested SSE header parsing has value for *API-surface compatibility*, but the actual encryption path is hippius-specific — **do not port the service SSE semantics as behavior.**

7. **Body digest / checksum semantics differ.** hippius distinguishes digest **scope** (`full` / `first-chunk` / `prefix`) because multipart and appended objects don't hash the whole body (`common/headers.py`). The service branch's `HashingStream` assumes whole-body SHA-256. The rewrite must adopt hippius's scoped-digest model, especially for the **append** extension (below).

8. **Non-standard extensions differ.** Service branch ships **`?rename`** (`objects/rename.rs`); hippius ships **append** (`extensions/append.py`). **Discard rename; the append extension is hippius-native and has no service-branch counterpart to harvest.**

9. **Routing style differs (cosmetic).** Service uses one axum `any()` catch-all with query-param dispatch macros (`server.rs`); hippius uses **FastAPI routers** split by bucket/object (`buckets/router.py`, `objects/router.py`, `public_router.py`). The *sub-resource→action mapping* is the portable asset; the router mechanism is a rewrite decision (a Rust rewrite would likely keep the axum single-dispatch model, which is fine — just reproduce hippius's action set).

10. **Pool/capacity error mapping is hippius-specific.** hippius maps DB pool saturation and listing timeouts to retryable **503 `SlowDown`** with `Retry-After` (`errors.py:pool_saturation_response`, `listing_timeout_response`). The service branch has `SlowDown` (L38/L131) but no equivalent trigger. The rewrite should wire `SlowDown` to Postgres pool/timeout conditions to match hippius.

---

## 6. Honest reuse estimate

Of 14,141 LOC across 32 files:

| Disposition | ~LOC | % | What |
|---|---:|---:|---|
| **HARVEST as-is** | ~500 | 3.5% | `chunked.rs` (353), `objects/hashing.rs` (60), `utils.rs` (81). Zero/near-zero edits. |
| **HARVEST with edits** | ~4,500 | 32% | Whole files: `error.rs` (221), `auth/signing.rs` (786), `auth/mod.rs` (240), `objects/headers.rs` (312). Plus the *pure halves* of split files: `policy.rs` (~430), `lifecycle.rs` (~470), `cors.rs` (~380), `objects/locking.rs` (~300), `encryption.rs` (~220), `tagging.rs` (~180), `mod.rs` (~140), `buckets/access_control.rs` (~120), `public_access.rs` (~60), + XML result shapes carved from `multipart.rs` (~250), `versioning.rs` (~120), `objects/list.rs` (~120), `objects/delete.rs` (~150), `buckets/mod.rs` (~80). Edits are mechanical: swap `db: &sled::Db` for a trait, drop `From` impls, inject request-id. |
| **REWRITE** | ~9,000 | 63% | Storage/backend halves + orchestration: `arion.rs` (328), `multipart.rs` storage (~1,370), `objects/upload.rs` (846), `objects/download.rs` (517), `objects/delete.rs` storage (~546), `objects/copy.rs` (319), `objects/list.rs` storage (~334), `versioning.rs` storage (~638), `buckets/mod.rs` storage (~476), `buckets/config.rs` (273), `objects/mod.rs` (114), `server.rs` (1,026, ported as reference), `auth/credentials.rs` storage (~70), the sled/handler tails of every split feature file, `objects/restore.rs` (23). |
| **DISCARD** | ~160 | 1% | `db.rs` (38, sled adapter), `objects/rename.rs` (125, non-standard). |

**Bottom line:** roughly **one-third of the module is directly reusable** as the `s3-protocol` crate (signing, error catalog, policy engine, chunked decoder, conditional-header logic, per-feature XML codecs, the dispatch table as a design reference). The other **two-thirds is storage-path code** that assumed sled + a single hard-coded Arion and must be rewritten against hippius-s3's Postgres metadata layer and encryption/writer/reader pipeline. The high-value, hard-to-get-right, standards-conformance code (SigV4/SigV2 canonicalization, AWS-chunked signature chaining, IAM policy/ARN/condition evaluation, RFC 7232 conditionals) is squarely in the reusable third.

---

## Open questions

1. **Auth boundary:** does the Rust rewrite keep a separate gateway (like Python hippius) that owns SigV4, or does `s3-protocol` own signing end-to-end? This changes whether `signing.rs`/`chunked.rs`/`ratelimit.rs` land in the protocol crate or a gateway crate.
2. **`ObjectMeta` reconciliation:** hippius's object/version schema (storage-version, KEK envelope, `body_blake3` + scope, append_version) is richer than the service `S3Object`. Is `s3-protocol` allowed to define its own `ObjectMeta` that the host maps to/from Postgres, or must it use hippius's existing row types directly? Affects whether `objects/headers.rs` response builders generalize cleanly.
3. **Checksum validation scope:** are `crc32fast`/`crc32c` needed at launch (compute+verify `x-amz-checksum-*`) or is echo-only acceptable initially? Determines whether those two deps enter the crate now.
4. **Feature scope:** the service branch routes website/replication/metrics/analytics/inventory/intelligent-tiering/accelerate/logging/notification/ownership/request-payment sub-resources (mostly generic raw-XML store). Which of these does hippius-s3 actually intend to support? Unsupported ones can be `NotImplemented` stubs and their config-storage code dropped entirely.
5. **SigV2 support:** is legacy SigV2 presigned-URL support (`signing.rs` L541, `sha1`/`md-5` deps) in scope, or SigV4-only? Dropping SigV2 removes `sha1` and simplifies.
6. **`uuid` usage:** are version-ids / multipart upload-ids generated by the protocol layer or supplied by the host/Postgres? Determines whether `uuid` is a protocol-crate dep.
7. **Object Lock reconciliation:** hippius has a substantial object-lock implementation (`object_lock_enforcement.py`, `object_lock_guard.py`, `object-lock.md`); how much of the service branch's `objects/locking.rs` decision logic (`check_object_lock_for_delete`, GOVERNANCE bypass) should be harvested vs. re-derived from hippius's spec?
