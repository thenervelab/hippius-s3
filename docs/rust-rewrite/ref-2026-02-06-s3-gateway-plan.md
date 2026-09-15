> **⚠️ ARCHIVAL REFERENCE — pre-greenfield, reference-only (imported 2026-09-15 from the hcfs `service` branch, `docs/plans/2026-02-06-s3-gateway-evaluated-plan.md`).**
>
> Kept here so its **protocol-handler blueprint** — the per-file breakdowns of SigV4 verification, S3 error codes, XML serialization, router/dispatch, the ETag strategy, range, presigned URLs, ACL/policy, and multipart — is on hand while writing the `s3-protocol` crate and the object/multipart handlers in Phases 0–2. It is the design *behind* the code we are harvesting.
>
> **Its architecture is OBSOLETE — do NOT follow it.** It assumes sled metadata, a single zero-copy Arion backend, and on-port credential registration. Our design is greenfield: own Postgres + optimized schema, HCFS as the sole backend, a committing AEAD, per-blob dedup, and remote credential assertion. **`IMPLEMENTATION-PLAN.md` in this directory is authoritative.** Use the material below only as a code-organization reference for the protocol layer, never for storage, metadata, auth-trust, or deployment.

---

# S3-Compatible Gateway for hcfs-server — Implementation Plan

---

## Architecture: Zero-Copy Protocol Translator

The hcfs-server S3 gateway is a **pure protocol translator**. It mutates request/response envelopes (auth, headers, format) while the actual byte stream passes through **untouched** from S3 client to Arion and back.

**Encryption is handled at the Arion/validator layer**, transparent to both the S3 client and hcfs-server.

```
Upload:
  S3 Client ──PUT body──▶ hcfs-server ──POST /upload (same bytes)──▶ Arion ──▶ Validator (encrypts + RS) ──▶ Miners

Download:
  Miners ──▶ Validator (RS decode + decrypts) ──▶ Arion ──GET response──▶ hcfs-server ──S3 response (same bytes)──▶ S3 Client
```

**What the hcfs-server does:**
- Translates S3 protocol (SigV4 auth, XML responses, path-style URLs) to Arion protocol (API key auth, JSON responses, REST)
- Stores S3 metadata in Sled (buckets, objects, credentials)
- Passes the data stream through zero-copy — never buffers, encrypts, or transforms the bytes

---

## Table of Contents

1. [Codebase Research Findings](#1-codebase-research-findings)
2. [Plan Overview](#2-plan-overview)
3. [Dependencies](#3-dependencies)
4. [Module Structure](#4-module-structure)
5. [Phase 1: Foundation — Server, Auth, Error, XML, Router](#5-phase-1-foundation)
6. [Phase 2: Database Layer](#6-phase-2-database-layer)
7. [Phase 3: Bucket + Object Operations (Zero-Copy)](#7-phase-3-bucket--object-operations)
8. [Phase 4: ListObjectsV2](#8-phase-4-listobjectsv2)
9. [Phase 5: Multipart Upload](#9-phase-5-multipart-upload)
10. [Phase 6: Credential Registration](#10-phase-6-credential-registration)
11. [Phase 7A: Range Requests](#11-phase-7a-range-requests)
12. [Phase 7B: Presigned URLs](#12-phase-7b-presigned-urls)
13. [Phase 7C: ACLs + Bucket Policies](#13-phase-7c-acls--bucket-policies)
14. [Open Questions & Risks](#14-open-questions--risks)

---

## 1. Codebase Research Findings

### 1.1 Existing Directory Structure

```
hcfs-server/src/
├── main.rs              # Server startup, router, handlers (~1454 lines)
├── database.rs          # Sled metadata storage (~700 lines)
├── middleware.rs         # API key validation + request logging (~125 lines)
├── tls.rs               # TLS certificate configuration (~135 lines)
├── billing.rs           # User balance & tier limits (~429 lines)
└── backup/              # S3 disaster recovery
    ├── mod.rs
    ├── config.rs
    ├── manager.rs
    ├── s3.rs
    └── verification.rs
```

### 1.2 AppState (main.rs:34-82)

```rust
pub struct AppState {
    pub db: sled::Db,
    pub http_client: reqwest::Client,  // 600s read timeout, 30s connect, pool of 10
    pub arion_url: String,
    pub arion_api_key: String,
}
```

The S3 gateway shares this `AppState` via `Arc<AppState>`. No modifications needed — S3-specific config (region) uses `LazyLock<String>` statics (matching billing.rs pattern).

### 1.3 HTTP Client Configuration (main.rs:60-71)

```rust
let http_client = reqwest::Client::builder()
    .read_timeout(Duration::from_secs(600))      // 10 min for large files
    .connect_timeout(Duration::from_secs(30))
    .tcp_keepalive(Duration::from_secs(30))
    .pool_idle_timeout(Duration::from_secs(600))
    .pool_max_idle_per_host(10)
    .danger_accept_invalid_certs(accept_invalid_certs)  // For Arion self-signed certs
    .build()?;
```

S3 uploads/downloads reuse this client for Arion. The 10-minute timeout is appropriate for large file transfers. Under heavy S3 load, `pool_max_idle_per_host(10)` may need monitoring.

### 1.4 Upload to Arion Pattern (main.rs:423-481)

```rust
async fn stream_to_arion(
    app_state: &Arc<AppState>,
    ciphertext_field: multer::Field<'static>,
    file_id: &str,
) -> Result<String, Response> {
    let stream = ciphertext_field.map_err(|e| std::io::Error::other(e.to_string()));
    let reqwest_body = reqwest::Body::wrap_stream(stream);
    let part = Part::stream(reqwest_body).file_name(file_id.to_string());
    let form = Form::new().part(file_id.to_string(), part);

    let response = app_state.http_client
        .post(&format!("{}/upload", app_state.arion_url))
        .header("X-API-Key", &app_state.arion_api_key)
        .multipart(form)
        .send().await?;

    let parsed: ArionUploadResponse = serde_json::from_str(&response.text().await?)?;
    Ok(parsed.hash)  // Returns IPFS CID
}
```

This takes `multer::Field<'static>`. For S3 PutObject, we have `axum::body::Body`. We need a variant:
```rust
async fn stream_body_to_arion(
    app_state: &Arc<AppState>,
    body: Body,
    object_key: &str,
) -> Result<String, S3Error>
```
The conversion is: `Body::into_data_stream()` → map errors → `reqwest::Body::wrap_stream()` → same multipart form pattern. **Data bytes are never touched**, only wrapped in a different HTTP envelope.

### 1.5 Download from Arion Pattern (main.rs:1063-1160)

```rust
let arion_download_url = format!("{}/download/{}", app_state.arion_url, file_record.arion_cid);
let arion_response = app_state.http_client
    .get(&arion_download_url)
    .header("X-API-Key", &app_state.arion_api_key)
    .send().await?;

let stream = arion_response.bytes_stream()
    .map_err(|e| std::io::Error::other(e.to_string()));
let body = Body::from_stream(stream);
```

For S3 GetObject: identical pattern. Arion returns the data (validator decrypts transparently). We wrap the stream in an S3 response with proper headers. **Data bytes pass through untouched.**

**Content-Length:** Since encryption is at the validator layer, Arion returns the original data. The `Content-Length` from Arion (if provided) should match `S3Object.size` stored in Sled. If Arion doesn't provide it, we use our stored size.

### 1.6 Arion Delete Pattern (main.rs:84-106)

```rust
let delete_url = format!("{}/blobs/{}", app_state.arion_url, arion_cid);
app_state.http_client
    .delete(&delete_url)
    .header("Authorization", format!("Bearer {}", app_state.arion_api_key))
    .send().await;
```

Delete uses `Authorization: Bearer` header (NOT `X-API-Key` like upload/download). Best-effort — log warnings, don't fail the S3 response.

### 1.7 Database Patterns (database.rs)

**Key construction:**
```rust
const KEY_DELIMITER: u8 = b':';

fn make_key(user_id: &str, path_hash: &[u8]) -> Vec<u8> {
    let mut key = user_id.as_bytes().to_vec();
    key.push(KEY_DELIMITER);
    key.extend_from_slice(hex::encode(path_hash).as_bytes());
    key
}
```

**Patterns to follow in S3 database modules:**
- bincode serialization
- Composite keys with `:` delimiter
- Named trees per entity type (e.g., `"s3_credentials"`, `"s3_buckets"`)
- `validate_user_id()` reuse
- Explicit `flush()` after writes
- Prefix scanning via `db.scan_prefix()` — returns byte-sorted order (matches S3 lexicographic requirement)

### 1.8 Middleware (middleware.rs)

**S3 router must:**
- Reuse `request_logger` (8-char hex request ID, entry/exit timing)
- NOT use `validate_api_key` (S3 uses SigV4 auth, not API keys)

### 1.9 TLS (tls.rs)

Reuse same TLS cert for S3 port — both are the same server, just different ports. No separate TLS config needed.

### 1.10 Server Startup (main.rs:190-334)

Current shutdown flow uses `axum_server::Handle`. For S3, need a second handle shut down in the same ctrl-c handler:
```rust
let s3_handle = axum_server::Handle::new();
tokio::spawn(s3::server::start_s3_server(app_state.clone(), s3_handle.clone()));

// ctrl-c handler:
backup_cancel_token.cancel();
s3_handle.shutdown();     // S3 server
handle_clone.shutdown();  // HCFS server
```

### 1.11 Billing Integration

Billing validation is currently disabled in the upload handler but usage tracking runs via `push_usage_to_billing()`. S3 operations must also update `UserStorageSummary` on PutObject/DeleteObject and call `push_usage_to_billing()` for billing consistency.

---

## 2. Plan Overview

**Goal:** Add S3-compatible endpoints on port 8333 so boto3, awscli, minio client can store/retrieve objects.

```
S3 Client ◀──S3 protocol──▶ hcfs-server (port 8333) ◀──Arion protocol──▶ Arion Gateway ◀──▶ Validators ◀──▶ Miners
                              │                                                              (encrypt/decrypt here)
                              ├── SigV4 → API Key auth translation
                              ├── XML responses ← JSON responses
                              ├── Sled metadata (buckets, objects, credentials)
                              └── Data stream passes through untouched
```

**What the server translates per operation:**

| S3 Operation | S3 Protocol (client side) | Arion Protocol (backend side) |
|---|---|---|
| PutObject | `PUT /bucket/key` + raw body + SigV4 | `POST /upload` + multipart form + X-API-Key |
| GetObject | `GET /bucket/key` + SigV4 → response with S3 headers | `GET /download/{cid}` + X-API-Key → raw stream |
| GetObject (Range) | `GET /bucket/key` + `Range: bytes=X-Y` | `GET /download/{cid}` + Range passthrough or slice_stream fallback |
| DeleteObject | `DELETE /bucket/key` + SigV4 | `DELETE /blobs/{cid}` + Bearer auth |
| ListObjectsV2 | `GET /bucket?list-type=2&prefix=...` → XML | Sled scan only (no Arion call) |
| CreateBucket | `PUT /bucket` → 200 | Sled insert only |
| HeadObject | `HEAD /bucket/key` → headers only | Sled lookup only |
| Presigned GET/PUT/DELETE | Query string auth (`X-Amz-Algorithm=...`) | Same as non-presigned (after SigV4 QS verification) |
| GetBucketAcl / PutBucketAcl | `GET/PUT /bucket?acl` | Sled metadata only |
| GetBucketPolicy / PutBucketPolicy | `GET/PUT /bucket?policy` | Sled metadata only |
| GetObjectAcl / PutObjectAcl | `GET/PUT /bucket/key?acl` | Sled metadata only |

---

## 3. Dependencies

### Additions to hcfs-server/Cargo.toml

| Dependency | Version | Purpose | Notes |
|---|---|---|---|
| `hmac` | 0.12 | SigV4 HMAC-SHA256 | `sha2` already in workspace. Lightweight, standard for HMAC. |
| `quick-xml` | 0.37 + `serialize` | S3 XML responses | S3 requires XML. `quick-xml` with serde is the standard choice. |
| `uuid` | 1 + `v4` | Multipart upload IDs | Needed for Phase 5. Could defer or use `rand` hex for MVP. |
| `percent-encoding` | 2.3 | URL decode S3 keys | Evaluate: axum's `{*key}` extractor may already URL-decode. Test first. |
| `base64` | 0.22 | Continuation tokens | ListObjectsV2 continuation tokens are base64-encoded. |

**Not adding:**
- `aws-sigv4` — client-side signing library, not suitable for server-side verification
- `s3s` — full S3 server framework, conflicts with our axum architecture
- `md-5` — see ETag strategy below

### ETag Strategy

**Decision: Arion CID as ETag for MVP.**
- ETag = `"\"{arion_cid}\""` (quoted per S3 spec)
- Clients compare ETags for equality — CID serves this purpose
- No `md-5` dependency needed
- If any client breaks on non-MD5 ETags, upgrade to MD5 by teeing the stream through a hasher (bytes still flow to Arion untouched, just adds computation)

---

## 4. Module Structure

```
src/s3/
  mod.rs               — module declarations
  server.rs            — server setup, TLS, port binding
  auth.rs              — SigV4 verification (~150 lines)
  router.rs            — S3 operation dispatch (method + path + query → handler)
  xml.rs               — S3 XML response/request serialization
  error.rs             — S3 error codes → XML error responses
  types.rs             — S3Credentials, S3Bucket, S3Object, MultipartUpload, S3Part
  handlers/
    mod.rs
    bucket.rs          — CreateBucket, DeleteBucket, HeadBucket, ListBuckets
    object.rs          — PutObject, GetObject, DeleteObject, HeadObject
    list.rs            — ListObjectsV2 with prefix/delimiter/continuation-token
    multipart.rs       — CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts
  database/
    mod.rs
    credentials.rs     — Sled tree: s3_credentials
    buckets.rs         — Sled tree: s3_buckets
    objects.rs         — Sled tree: s3_objects, prefix scanning for ListObjectsV2
    multipart.rs       — Sled tree: s3_multipart
```

**15 files total.** Each handler file maps to a database file. Follows the backup/ nesting pattern from the existing codebase.

**MVP option:** Defer `handlers/multipart.rs` + `database/multipart.rs` + `uuid` dependency = 13 files.

---

## 5. Phase 1: Foundation

### 5.1 types.rs — Core Data Types

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Credentials {
    access_key_id: String,         // = user_id (SS58 address)
    secret_access_key: String,     // Random 40-char hex
    user_id: String,               // Same as access_key_id
    created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Bucket {
    name: String,
    user_id: String,
    created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Object {
    bucket: String,
    key: String,
    arion_cid: String,
    size: u64,                     // Byte size of the object
    etag: String,                  // Quoted: "\"QmXxx...\""
    content_type: String,          // Default: "application/octet-stream"
    user_id: String,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultipartUpload {
    upload_id: String,             // UUID v4
    bucket: String,
    key: String,
    user_id: String,
    parts: Vec<S3Part>,
    created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3Part {
    part_number: u32,
    arion_cid: String,
    size: u64,
    etag: String,
    uploaded_at: i64,
}
```

**Notes:**
- `access_key_id` = `user_id`: SS58 addresses are public on-chain, so the access key isn't secret. Authentication relies entirely on `secret_access_key`.
- `S3Object.etag` stores the complete quoted string (S3 spec requires quotes in ETag header values).
- `S3Object.content_type` defaults to `application/octet-stream` if not provided by client.

### 5.2 error.rs — S3 Error Codes

| Code | HTTP | When |
|------|------|------|
| `AccessDenied` | 403 | SigV4 verification failed |
| `SignatureDoesNotMatch` | 403 | Signature mismatch |
| `InvalidAccessKeyId` | 403 | Unknown access key |
| `NoSuchBucket` | 404 | Bucket not found |
| `NoSuchKey` | 404 | Object not found |
| `NoSuchUpload` | 404 | Multipart upload not found |
| `BucketAlreadyExists` | 409 | Duplicate bucket |
| `BucketNotEmpty` | 409 | Delete non-empty bucket |
| `InvalidArgument` | 400 | Bad query param |
| `InvalidBucketName` | 400 | Bucket name validation failed |
| `MalformedXML` | 400 | Bad XML in request body |
| `EntityTooSmall` | 400 | Multipart part < 5MB |
| `InternalError` | 500 | Sled/Arion failures |

Implement `IntoResponse` returning:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <RequestId>a1b2c3d4</RequestId>
</Error>
```

`RequestId` should match the request logger's 8-char hex ID for correlation.

### 5.3 auth.rs — SigV4 Verification

**This is the most complex and highest-risk module.** ~150 lines, manual implementation using `hmac` + `sha2`.

**Verification pipeline:**
1. Parse `Authorization: AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request, SignedHeaders=..., Signature=...`
2. Build canonical request (method, URI, query, headers, payload hash)
3. Build string-to-sign (algorithm, timestamp, scope, canonical request hash)
4. Derive signing key (4-step HMAC chain: `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), "s3"), "aws4_request")`)
5. Compute expected signature, constant-time compare

**Critical S3 compatibility details:**
- URI encoding: each path segment URI-encoded, `/` preserved
- Query param sorting: by name then value
- Header canonicalization: lowercase, trim, collapse spaces
- Accept `x-amz-content-sha256: UNSIGNED-PAYLOAD` (standard for S3 over HTTPS)
- `x-amz-date` format: `20260206T120000Z`
- Clock skew tolerance: 15 minutes
- Constant-time signature comparison (use `hmac::Mac::verify` or `subtle::ConstantTimeEq`)

**Known client-specific gotchas:**
- boto3 sends `UNSIGNED-PAYLOAD` for PUTs
- awscli may double-encode URIs
- `Expect: 100-continue` header
- Empty query values: `?uploads` (no `=`)

**Auth is called per-handler (not middleware)** because SigV4 needs the raw path/query before axum normalization.

**Risk level: HIGH.** Must unit test against [AWS SigV4 test vectors](https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html).

### 5.4 xml.rs — XML Serialization

Use `quick-xml` with serde. Define structs:

| Struct | Used by |
|--------|---------|
| `ListAllMyBucketsResult` | ListBuckets response |
| `ListBucketResult` | ListObjectsV2 response (Contents, CommonPrefixes, KeyCount) |
| `InitiateMultipartUploadResult` | CreateMultipartUpload response |
| `CompleteMultipartUploadResult` | CompleteMultipartUpload response |
| `CompleteMultipartUploadRequest` | Parse incoming XML |
| `ListPartsResult` | ListParts response |

**S3 XML requirements:**
- Root element needs `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` attribute
- `<?xml version="1.0" encoding="UTF-8"?>` declaration required
- Dates: ISO 8601 format (`2026-02-06T12:00:00.000Z`)
- `quick-xml` serde: attributes via `#[serde(rename = "@xmlns")]`

Helper: `fn to_xml_response<T: Serialize>(status: StatusCode, body: &T) -> Response`

### 5.5 router.rs — S3 Operation Dispatch

```rust
fn s3_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", any(root_handler))               // GET / → ListBuckets
        .route("/{bucket}", any(bucket_handler))      // PUT/HEAD/DELETE/GET → bucket ops
        .route("/{bucket}/{*key}", any(object_handler))  // object + multipart ops
        .layer(DefaultBodyLimit::disable())
        .layer(from_fn(request_logger))               // Reuse existing logger
        // NO validate_api_key — S3 uses SigV4
        .with_state(state)
}
```

**Dispatch inside handlers:**

`bucket_handler`:
- `PUT` → CreateBucket
- `HEAD` → HeadBucket
- `DELETE` → DeleteBucket
- `GET` → ListObjectsV2

`object_handler`:
- `PUT` + `partNumber` query → UploadPart
- `PUT` → PutObject
- `GET` + `uploadId` query → ListParts
- `GET` → GetObject
- `HEAD` → HeadObject
- `DELETE` + `uploadId` query → AbortMultipartUpload
- `DELETE` → DeleteObject
- `POST` + `uploads` query → CreateMultipartUpload
- `POST` + `uploadId` query → CompleteMultipartUpload

**Path-style only.** No virtual-hosted-style (`mybucket.host`). All three target clients support path-style via `endpoint_url`.

### 5.6 server.rs — Server Setup

- Port from `HCFS_S3_PORT` (default 8333)
- Reuse same TLS cert as HCFS server (`tls::TlsConfig::new("hcc-validator")`)
- Share `Arc<AppState>` with HCFS server
- Own `axum_server::Handle` for graceful shutdown

### 5.7 main.rs Modifications

```rust
mod s3;  // NEW

// After backup init, before HCFS router:
let s3_handle = axum_server::Handle::new();
if s3_enabled {
    tokio::spawn(s3::server::start_s3_server(app_state.clone(), s3_handle.clone()));
}

// In ctrl-c handler:
backup_cancel_token.cancel();
s3_handle.shutdown();      // NEW
handle_clone.shutdown();
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `HCFS_S3_ENABLED` | `true` | Set `false` to disable S3 gateway entirely |
| `HCFS_S3_PORT` | `8333` | S3 server listen port |
| `HCFS_S3_REGION` | `us-east-1` | Cosmetic — used in SigV4 scope string |

### Phase 1 Verification

- Unit test SigV4 signing key derivation against AWS test vectors
- Unit test canonical request construction
- Unit test XML serialization round-trips
- Start S3 server, request without auth → verify 403 XML error

---

## 6. Phase 2: Database Layer

### 6.1 credentials.rs

**Tree:** `"s3_credentials"` — Key: `{access_key_id}` bytes

Functions: `create_credentials`, `get_credentials`, `delete_credentials`

One access key per user (access_key = user_id). Simple lookup during auth — `get_credentials(access_key_id)` returns the user's secret for SigV4 verification.

### 6.2 buckets.rs

**Tree:** `"s3_buckets"` — Key: `{user_id}:{bucket_name}`

Functions: `create_bucket`, `get_bucket`, `delete_bucket`, `list_buckets`, `bucket_is_empty`

**Bucket name validation:** 3-63 chars, lowercase alphanumeric + hyphens, no leading/trailing hyphen.

**Buckets are per-user** (not globally unique). Two users can have `mybucket`. Simpler for private deployment.

> **Phase 7C note:** ACLs and anonymous access (Phase 7C) require **globally unique** bucket names and a `s3_bucket_index` tree for `bucket_name → owner_user_id` lookup. Consider populating `s3_bucket_index` in Phase 2 and enforcing global uniqueness from the start to avoid a breaking migration later.

### 6.3 objects.rs

**Tree:** `"s3_objects"` — Key: `{user_id}:{bucket}:{key}`

Functions: `put_object`, `get_object`, `delete_object`, `head_object`

**ListObjectsV2 scanning:**
```rust
fn list_objects(db, user_id, bucket, prefix, delimiter, continuation_token, max_keys)
    -> ListObjectsResult
```
- `scan_prefix("{user_id}:{bucket}:{prefix}")` — Sled returns byte-sorted (matches S3 lexicographic requirement)
- Delimiter grouping: keys with delimiter after prefix → collapse into CommonPrefixes
- Continuation token: base64 of last key (stateless pagination)
- Default max_keys: 1000

**Key collision note:** S3 object keys can contain `:`. Key `my:file.txt` → DB key `user1:mybucket:my:file.txt`. Safe because we only construct keys from known components and only extract keys via prefix scan where we know the prefix length. Could use `\0` as delimiter for extra safety.

### 6.4 multipart.rs

**Tree:** `"s3_multipart"` — Key: `{upload_id}` (UUID v4)

Functions: `create_multipart`, `get_multipart`, `add_part`, `delete_multipart`

`add_part` uses `fetch_and_update` for atomic concurrent part uploads.

### Phase 2 Verification

- Unit tests per database module (tempfile-based Sled, matching existing `database.rs` test patterns)
- Test prefix scanning with 20+ objects, various prefixes, delimiter `/`
- Test pagination with continuation tokens
- Test multipart lifecycle: create → add parts → get → abort

---

## 7. Phase 3: Bucket + Object Operations

**Goal:** End-to-end zero-copy operations working with boto3.

### 7.1 Bucket Operations

- **ListBuckets** (`GET /`): Scan s3_buckets for authenticated user → XML
- **CreateBucket** (`PUT /bucket`): Validate name, check duplicates, insert → 200 + `Location` header
- **HeadBucket** (`HEAD /bucket`): Exists check → 200 or 404
- **DeleteBucket** (`DELETE /bucket`): Check empty (scan s3_objects), delete → 204

### 7.2 PutObject — The Core Upload Translation

```
S3 Client ──PUT /bucket/key──▶ hcfs-server ──POST /upload (multipart form)──▶ Arion
           raw body stream     wrap in form,    same bytes inside form part
           Content-Length       add X-API-Key
```

Steps:
1. Verify SigV4 auth → get user_id
2. Verify bucket exists in Sled
3. **Zero-copy stream body to Arion:**
   ```rust
   let stream = body.into_data_stream()
       .map_err(|e| std::io::Error::other(e.to_string()));
   let reqwest_body = reqwest::Body::wrap_stream(stream);
   let part = Part::stream(reqwest_body).file_name(key.clone());
   let form = Form::new().part(key.clone(), part);
   // POST to Arion /upload with X-API-Key
   ```
   Data bytes flow from S3 client to Arion without buffering or transformation.
4. Parse Arion response → get CID
5. Store S3Object metadata in Sled (key, bucket, CID, size from Content-Length, ETag = quoted CID)
6. Update UserStorageSummary + push_usage_to_billing
7. Return 200 with `ETag` header

### 7.3 GetObject — The Core Download Translation

```
Arion ──GET /download/{cid}──▶ hcfs-server ──S3 response──▶ S3 Client
       raw stream               add S3 headers    same bytes
       X-API-Key                Content-Type,
                                Content-Length,
                                ETag, Last-Modified
```

Steps:
1. Verify SigV4 auth → get user_id
2. Look up S3Object in Sled → get arion_cid, size, etag, content_type
3. **Zero-copy stream from Arion:**
   ```rust
   let arion_response = http_client
       .get(&format!("{}/download/{}", arion_url, arion_cid))
       .header("X-API-Key", &arion_api_key)
       .send().await?;
   let stream = arion_response.bytes_stream()
       .map_err(|e| std::io::Error::other(e.to_string()));
   let body = Body::from_stream(stream);
   ```
4. Build S3 response with headers:
   - `Content-Type`: from S3Object
   - `Content-Length`: from S3Object.size (or Arion's Content-Length)
   - `ETag`: from S3Object
   - `Last-Modified`: from S3Object.updated_at (formatted as HTTP date)
   - `Accept-Ranges: bytes`

### 7.4 HeadObject

Same as GetObject but return headers only, no body. Just Sled lookup.

### 7.5 DeleteObject

1. Verify SigV4 auth
2. Remove from Sled (get arion_cid)
3. Best-effort delete from Arion (`DELETE /blobs/{cid}` with Bearer auth)
4. Update UserStorageSummary (decrement) + push_usage_to_billing
5. Return 204

### Phase 3 Verification

- Integration test with mock Arion: put + get + head + delete round-trip
- **boto3:** `create_bucket()`, `put_object()`, `get_object()`, `head_object()`, `delete_object()`, `delete_bucket()`
- **awscli:** `aws s3 cp file.txt s3://mybucket/ --endpoint-url https://localhost:8333`
- Error paths: put to nonexistent bucket (404), delete non-empty bucket (409), get nonexistent key (404)

---

## 8. Phase 4: ListObjectsV2

**Goal:** Make `aws s3 ls` work.

### 8.1 handlers/list.rs

Handles query params: `list-type=2`, `prefix`, `delimiter`, `max-keys`, `continuation-token`, `start-after`, `encoding-type`.

**Algorithm:**
1. Scan `s3_objects` tree with prefix `{user_id}:{bucket}:{prefix}`
2. For each key:
   - Strip the DB prefix to get the S3 object key
   - If delimiter is set and key contains delimiter after prefix: collapse into CommonPrefixes
   - Otherwise: add to Contents
3. Deduplicate and sort CommonPrefixes
4. Apply max_keys limit (counts both Contents + CommonPrefixes entries)
5. If more items remain: set `IsTruncated=true`, `NextContinuationToken` = base64(last key)
6. Return `ListBucketResult` XML

### Phase 4 Verification

- Test flat listing, prefix filtering, delimiter grouping (`a/b/c.txt` with `/`)
- Test pagination: max_keys=5 with 20 objects
- **awscli:** `aws s3 ls s3://mybucket/ --recursive`
- Empty bucket: returns empty list

---

## 9. Phase 5: Multipart Upload

**Goal:** Support large file uploads (awscli uses multipart for >8MB by default).

### 9.1 CreateMultipartUpload (`POST /bucket/key?uploads`)

Generate UUID v4 as upload_id, store in Sled → return XML with `<UploadId>`.

### 9.2 UploadPart (`PUT /bucket/key?partNumber=N&uploadId=ID`)

1. Stream part body to Arion (zero-copy, same as PutObject)
2. Get CID from Arion response
3. Store S3Part atomically in MultipartUpload record
4. Return `ETag` header (CID-based)

### 9.3 CompleteMultipartUpload (`POST /bucket/key?uploadId=ID`)

Three strategies, to be evaluated:

**Option A — Multi-CID object:** Store all part CIDs in S3Object. On GetObject, stream parts sequentially from Arion.
- Pro: No re-upload, instant completion, hcfs-server stays a pure metadata manager
- Con: GetObject becomes sequential multi-request. Content-Length requires summing all part sizes.

**Option B — Download + re-upload as single blob:** Download all parts from Arion, concatenate, upload as single object.
- Pro: Clean single-CID object, simple GetObject
- Con: 2x I/O through hcfs-server

**Option C — Arion-level concatenation:** If Arion supports a "concatenate CIDs" API, use it.
- Pro: No data movement through hcfs-server
- Con: Depends on Arion API capability (needs verification)

**Recommendation:** Start with Option A (multi-CID). Investigate Option C as optimization.

### 9.4 AbortMultipartUpload (`DELETE /bucket/key?uploadId=ID`)

Delete all part CIDs from Arion (best-effort). Remove MultipartUpload from Sled. Return 204.

### 9.5 ListParts (`GET /bucket/key?uploadId=ID`)

Return XML listing uploaded parts with ETag, size, last-modified.

### Phase 5 Verification

- Full lifecycle: create → upload 3 parts → complete → verify GetObject streams all parts
- Abort: create → upload → abort → verify cleanup
- **boto3:** `create_multipart_upload()` + `upload_part()` + `complete_multipart_upload()`
- **awscli:** `aws s3 cp largefile.bin s3://mybucket/` (auto-multipart)

---

## 10. Phase 6: Credential Registration

### Endpoint on HCFS port (9999)

**`POST /s3_register`**
- Behind existing `validate_api_key` middleware (only authorized services can create credentials)
- Accepts JSON: `{ "user_id": "<SS58 address>" }`
- Generates random `secret_access_key` (40 hex chars = 20 random bytes)
- Stores in `s3_credentials` Sled tree
- Returns `{ "access_key_id": "...", "secret_access_key": "..." }` (shown only once)

**`GET /s3_credentials/{user_id}`**
- Lists access_key_ids for a user (not secrets)

### Phase 6 Verification

- Register → use credentials with boto3 → verify operations work
- Duplicate registration handling
- Invalid user_id handling

---

## 11. Phase 7A: Range Requests

**Goal:** Support `Range: bytes=X-Y` on GetObject for download resume and partial reads.

### 11.1 New File: `src/s3/range.rs` (~120 lines)

```rust
/// Parsed byte range from Range header
pub(crate) enum ByteRange {
    /// bytes=0-100 (inclusive both ends)
    Bounded { start: u64, end: u64 },
    /// bytes=100- (from offset to end)
    FromOffset { start: u64 },
    /// bytes=-100 (last N bytes)
    Suffix { length: u64 },
}

/// Resolved range against known object size
pub(crate) struct ResolvedRange {
    pub start: u64,
    pub end_inclusive: u64,
    pub total_size: u64,
}

impl ByteRange {
    /// Parse "bytes=X-Y" from Range header value.
    /// Returns None for multi-range or invalid format.
    pub fn parse(header_value: &str) -> Option<Self>;
}

impl ResolvedRange {
    /// Resolve a ByteRange against a known object size.
    /// Returns None if range is unsatisfiable (start >= size).
    pub fn resolve(range: &ByteRange, total_size: u64) -> Option<Self>;

    /// Content-Range header value: "bytes 0-100/1000"
    pub fn content_range_header(&self) -> String;

    /// Content-Length for this range
    pub fn content_length(&self) -> u64;
}
```

**`slice_stream` helper** (~30 lines): Wraps a `Stream<Item=Result<Bytes>>`, skips first `start` bytes, takes next `length` bytes. Used as fallback when Arion doesn't support Range.

### 11.2 Modify: `src/s3/handlers/object.rs` — GetObject

Add Range handling after Sled lookup, before Arion request:

```rust
let range = headers.get("range")
    .and_then(|v| v.to_str().ok())
    .and_then(ByteRange::parse);

if let Some(ref byte_range) = range {
    let resolved = ResolvedRange::resolve(byte_range, s3_object.size)
        .ok_or(S3Error::InvalidRange)?;

    // Strategy 1 (preferred): Pass Range to Arion
    let arion_response = app_state.http_client
        .get(&format!("{}/download/{}", app_state.arion_url, s3_object.arion_cid))
        .header("X-API-Key", &app_state.arion_api_key)
        .header("Range", format!("bytes={}-{}", resolved.start, resolved.end_inclusive))
        .send().await?;

    if arion_response.status() == StatusCode::PARTIAL_CONTENT {
        // Arion supports Range — pass through
        let stream = arion_response.bytes_stream().map_err(...);
        return build_partial_response(stream, &resolved, &s3_object);
    }

    // Strategy 2 (fallback): Stream full, skip/take on our side
    let stream = arion_response.bytes_stream().map_err(...);
    let sliced = slice_stream(stream, resolved.start, resolved.content_length());
    return build_partial_response(sliced, &resolved, &s3_object);
}
```

### 11.3 Response Formats

**206 Partial Content:**
```
HTTP/1.1 206 Partial Content
Content-Range: bytes 0-999/5000
Content-Length: 1000
Content-Type: application/octet-stream
ETag: "QmXxx..."
Accept-Ranges: bytes
```

**416 Range Not Satisfiable:**
```
HTTP/1.1 416 Range Not Satisfiable
Content-Range: bytes */5000
```

### 11.4 New Error Code

| Code | HTTP | When |
|------|------|------|
| `InvalidRange` | 416 | Range start >= object size, or malformed range |

### 11.5 Other Changes

- Add `Accept-Ranges: bytes` header to HeadObject and GetObject responses (even without Range header) to advertise range support.

### Phase 7A Verification

- Unit test `ByteRange::parse`: `"bytes=0-100"`, `"bytes=100-"`, `"bytes=-100"`, invalid formats, multi-range (rejected)
- Unit test `ResolvedRange::resolve`: normal, suffix > size, start >= size (416)
- Unit test `slice_stream`: skip/take with multiple `Bytes` chunks of varying sizes
- Integration: `boto3.get_object(Range='bytes=0-99')` → verify 206 + correct bytes
- Integration: `awscli aws s3 cp` with `--range` flag
- Edge cases: Range on empty object, `Range: bytes=0-0` (single byte)

---

## 12. Phase 7B: Presigned URLs

**Goal:** Generate/verify time-limited URLs for GET, PUT, DELETE without requiring the caller to have credentials.

### 12.1 New File: `src/s3/presigned.rs` (~150 lines)

SigV4 presigned URLs use **query string authentication** instead of the `Authorization` header.

**Query parameters:**
- `X-Amz-Algorithm=AWS4-HMAC-SHA256`
- `X-Amz-Credential={access_key}/{date}/{region}/s3/aws4_request`
- `X-Amz-Date={ISO8601 timestamp}`
- `X-Amz-Expires={seconds}` (1 to 604800 = 7 days)
- `X-Amz-SignedHeaders={semicolon-delimited signed headers}`
- `X-Amz-Signature={hex signature}`

```rust
pub(crate) struct PresignedParams {
    pub algorithm: String,
    pub credential: String,
    pub date: String,
    pub expires: u64,
    pub signed_headers: String,
    pub signature: String,
}

impl PresignedParams {
    /// Extract from query string. Returns None if not a presigned URL.
    pub fn from_query(query: &str) -> Option<Result<Self, S3Error>>;

    /// Validate: 1 <= expires <= 604800, and current time < sign_time + expires
    pub fn validate_expiry(&self) -> Result<(), S3Error>;

    /// Parse credential into (access_key_id, date, region, service)
    pub fn parse_credential(&self) -> Result<(&str, &str, &str, &str), S3Error>;
}
```

### 12.2 Modify: `src/s3/auth.rs` — Unified Authentication

Refactor to handle three auth methods:

```rust
pub(crate) enum AuthMethod {
    Header,       // Authorization header present
    QueryString,  // X-Amz-Algorithm query param present (presigned URL)
    Anonymous,    // No auth
}

pub(crate) struct AuthenticatedUser {
    pub user_id: String,
    pub access_key_id: String,
}

/// Returns Ok(Some(user)) for authenticated, Ok(None) for anonymous, Err for bad auth.
pub(crate) async fn authenticate_request(
    db: &sled::Db,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    query_string: &str,
) -> Result<Option<AuthenticatedUser>, S3Error> {
    match detect_auth_method(headers, query_string) {
        AuthMethod::Header => {
            let user = verify_sigv4_header(db, method, uri, headers).await?;
            Ok(Some(user))
        }
        AuthMethod::QueryString => {
            let params = PresignedParams::from_query(query_string)?;
            params.validate_expiry()?;
            let user = verify_sigv4_query_string(db, method, uri, headers, &params).await?;
            Ok(Some(user))
        }
        AuthMethod::Anonymous => Ok(None),
    }
}
```

**SigV4 query string verification differs from header:**
1. Signature covers canonical request **without** the `X-Amz-Signature` param
2. Payload hash is always `UNSIGNED-PAYLOAD`
3. String-to-sign uses `X-Amz-Date` param instead of `x-amz-date` header
4. All `X-Amz-*` query params included in canonical query string (sorted)

Signing key derivation is identical — reuse existing `derive_signing_key()`.

### 12.3 Handler Changes

Every handler changes from direct SigV4 to unified auth:

```rust
// Before (Phases 1-6):
let user = verify_sigv4(db, method, uri, headers)?;

// After (Phase 7B):
let auth_result = authenticate_request(db, method, uri, headers, query_string).await?;
// For now: require authentication (anonymous = 403 until Phase 7C)
let user = auth_result.ok_or(S3Error::AccessDenied)?;
```

### 12.4 New Error Codes

| Code | HTTP | When |
|------|------|------|
| `AuthorizationQueryParametersError` | 400 | Presigned URL missing required params |
| `ExpiredToken` | 403 | `X-Amz-Date` + `X-Amz-Expires` < now |

### 12.5 Dependency

Evaluate `url = "2"` for query string parsing. May parse manually with `form_urlencoded` instead.

### Phase 7B Verification

- Unit test: Parse presigned query string with all params
- Unit test: Expiry validation — valid, expired, exceeds 7-day max
- Unit test: SigV4 query string canonical request construction
- Integration: `boto3.generate_presigned_url('get_object', ...)` → fetch with curl (no credentials) → 200
- Integration: `boto3.generate_presigned_url('put_object', ...)` → `curl -T file.txt <url>` → upload succeeds
- Integration: Expired presigned URL → 403
- Edge case: Presigned URL with existing query params in path

---

## 13. Phase 7C: ACLs + Bucket Policies

**Goal:** Per-bucket and per-object access control with canned ACLs, JSON bucket policies, and anonymous access.

### 13.1 New File: `src/s3/acl.rs` (~100 lines)

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum CannedAcl {
    #[default]
    Private,            // Owner-only
    PublicRead,         // Anyone can read
    PublicReadWrite,    // Anyone can read/write
    AuthenticatedRead,  // Any authenticated user can read
}

impl CannedAcl {
    /// Parse from x-amz-acl header value
    pub fn from_header(value: &str) -> Result<Self, S3Error>;

    /// Check if this ACL grants the requested permission
    pub fn allows(
        &self,
        action: S3Action,
        caller: Option<&AuthenticatedUser>,
        owner_id: &str,
    ) -> bool;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum S3Action {
    GetObject,
    PutObject,
    DeleteObject,
    ListBucket,
    GetBucketAcl,
    PutBucketAcl,
    GetObjectAcl,
    PutObjectAcl,
    GetBucketPolicy,
    PutBucketPolicy,
    DeleteBucketPolicy,
    CreateBucket,
    DeleteBucket,
}
```

**ACL stored inline** — no separate Sled trees. Extend existing types:

```rust
// S3Bucket gets:
acl: CannedAcl,  // default Private

// S3Object gets:
acl: CannedAcl,  // default Private
```

### 13.2 New File: `src/s3/policy.rs` (~250 lines)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BucketPolicy {
    #[serde(rename = "Version")]
    pub version: String,  // "2012-10-17"
    #[serde(rename = "Statement")]
    pub statements: Vec<PolicyStatement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PolicyStatement {
    #[serde(rename = "Sid", default)]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: PolicyEffect,
    #[serde(rename = "Principal")]
    pub principal: PolicyPrincipal,
    #[serde(rename = "Action")]
    pub action: OneOrMany<String>,
    #[serde(rename = "Resource")]
    pub resource: OneOrMany<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum PolicyEffect { Allow, Deny }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum PolicyPrincipal {
    Wildcard(String),  // "*"
    Aws { #[serde(rename = "AWS")] aws: OneOrMany<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum OneOrMany<T> { One(T), Many(Vec<T>) }

#[derive(Debug, PartialEq)]
pub(crate) enum PolicyDecision { ExplicitDeny, ExplicitAllow, NoMatch }

impl BucketPolicy {
    pub fn validate(&self) -> Result<(), S3Error>;
    pub fn evaluate(&self, action: &S3Action, resource: &str, caller: Option<&str>) -> PolicyDecision;
}
```

**Principals:** SS58 addresses or `"*"` for anonymous.

**Resource matching:** Simple glob on ARN — `arn:aws:s3:::bucket/*` matches all objects, `arn:aws:s3:::bucket/prefix*` matches prefix.

**Action matching:** Specific (`s3:GetObject`) or wildcard (`s3:*`).

### 13.3 New File: `src/s3/authorization.rs` (~120 lines)

Unified authorization check called after authentication:

```rust
pub(crate) async fn authorize_request(
    db: &sled::Db,
    caller: Option<&AuthenticatedUser>,
    owner_id: &str,
    bucket: &str,
    key: Option<&str>,
    action: S3Action,
    bucket_acl: &CannedAcl,
    object_acl: Option<&CannedAcl>,
) -> AuthzResult
```

**Evaluation order (matches AWS):**
1. **Owner check** → always allowed
2. **Bucket policy** → Explicit Deny > Explicit Allow > No Match
3. **ACL check** → fallback if policy has no match
4. **Default** → Deny

### 13.4 New Database Files

**`src/s3/database/policies.rs`** (~80 lines)
- Sled tree: `s3_bucket_policies` — Key: `{user_id}:{bucket_name}`, Value: bincode BucketPolicy
- Functions: `put_policy`, `get_policy`, `delete_policy`

**`src/s3/database/bucket_index.rs`** (~60 lines)
- Sled tree: `s3_bucket_index` — Key: `{bucket_name}`, Value: user_id bytes
- Functions: `index_bucket`, `lookup_bucket_owner`, `remove_bucket_index`

**Why `s3_bucket_index`:** Anonymous access needs to resolve `bucket_name → owner_user_id` without knowing who owns the bucket. Current bucket key is `{user_id}:{bucket_name}` — can't look up by name alone.

**Breaking change:** Buckets become **globally unique** (not per-user). Required for anonymous access. Update Phase 2 `buckets.rs` to check `s3_bucket_index` before creating.

### 13.5 New Handler Endpoints

**On bucket_handler:**

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| GetBucketAcl | GET | `/{bucket}?acl` | Return XML with canned ACL |
| PutBucketAcl | PUT | `/{bucket}?acl` | Set ACL from `x-amz-acl` header |
| GetBucketPolicy | GET | `/{bucket}?policy` | Return JSON policy document |
| PutBucketPolicy | PUT | `/{bucket}?policy` | Store JSON policy (validated) |
| DeleteBucketPolicy | DELETE | `/{bucket}?policy` | Remove policy |

**On object_handler:**

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| GetObjectAcl | GET | `/{bucket}/{*key}?acl` | Return XML with canned ACL |
| PutObjectAcl | PUT | `/{bucket}/{*key}?acl` | Set ACL from `x-amz-acl` header |

### 13.6 Modify All Existing Handlers

Every handler changes to:

```rust
let caller = authenticate_request(db, method, uri, headers, query)?;
let owner_id = match &caller {
    Some(user) => user.user_id.clone(),
    None => lookup_bucket_owner(db, bucket_name)?.ok_or(S3Error::NoSuchBucket)?,
};
let bucket = get_bucket(db, &owner_id, bucket_name)?.ok_or(S3Error::NoSuchBucket)?;
let authz = authorize_request(db, caller.as_ref(), &owner_id, bucket_name, ...);
if let AuthzResult::Denied(err) = authz { return Err(err); }
```

### 13.7 New Error Codes

| Code | HTTP | When |
|------|------|------|
| `MalformedPolicy` | 400 | Invalid JSON policy document |
| `NoSuchBucketPolicy` | 404 | GET/DELETE policy on bucket without one |
| `BucketAlreadyOwnedByYou` | 409 | Bucket name globally taken |

### 13.8 XML Types for ACL Responses

Add `AccessControlPolicy`, `Owner`, `AccessControlList`, `Grant` XML structs to `xml.rs`.

### Phase 7C Verification

- Unit test `CannedAcl::allows`: all 4 ACLs x {owner, authenticated, anonymous} x {read, write}
- Unit test `BucketPolicy::evaluate`: Allow/Deny/NoMatch, wildcard principal, prefix resource match
- Unit test `authorize_request`: owner bypass, policy deny overrides ACL, anonymous public-read
- Integration: `public-read` bucket ACL → anonymous GET works, anonymous PUT fails
- Integration: Policy allowing specific user → that user reads, others denied
- Integration: Explicit Deny in policy → overrides everything
- Integration: `boto3.put_bucket_policy()` → `boto3.get_bucket_policy()` round-trip

---

## Updated Module Structure (with Phase 7)

```
src/s3/
  mod.rs
  server.rs
  auth.rs              ← MODIFIED: unified authenticate_request()
  presigned.rs         ← NEW (7B)
  range.rs             ← NEW (7A)
  acl.rs               ← NEW (7C)
  policy.rs            ← NEW (7C)
  authorization.rs     ← NEW (7C)
  router.rs            ← MODIFIED: ?acl, ?policy routes
  xml.rs               ← MODIFIED: ACL XML types
  error.rs             ← MODIFIED: new error codes
  types.rs             ← MODIFIED: acl field on S3Bucket, S3Object
  handlers/
    mod.rs
    bucket.rs          ← MODIFIED: ACL/policy endpoints + authz
    object.rs          ← MODIFIED: Range + ACL endpoints + authz
    list.rs            ← MODIFIED: authz check
    multipart.rs       ← MODIFIED: authz check
  database/
    mod.rs             ← MODIFIED: new module declarations
    credentials.rs
    buckets.rs         ← MODIFIED: global uniqueness via bucket_index
    objects.rs
    multipart.rs
    policies.rs        ← NEW (7C)
    bucket_index.rs    ← NEW (7C)
```

**7 new files, 12 modified files.**

## Phase 7 Implementation Order

7A is independent — can start immediately.
7B must land before 7C (anonymous auth flow is the foundation).
7C depends on 7B.

---

## 14. Open Questions & Risks

### High Priority

1. **SigV4 correctness** — The #1 risk. If verification doesn't match what clients send, nothing works. Must test against AWS test vectors and all three target clients (boto3, awscli, minio).

2. **Arion upload format from raw Body** — Existing code uses `multer::Field` → reqwest multipart. S3 needs `axum::Body` → reqwest multipart. Must verify Arion accepts this and returns same CID format. The bytes are the same; only the wrapping differs.

3. **Content-Length on GetObject** — S3 clients expect it. Arion should return data with Content-Length since the validator decrypts transparently. Need to verify: does Arion always provide Content-Length? If not, we fall back to S3Object.size from Sled.

### Medium Priority

4. **Multipart CompleteUpload strategy** — Multi-CID vs re-upload vs Arion concatenation. Depends on GetObject complexity and Arion API capabilities. Decision deferred to Phase 5.

5. **S3 error code precision** — Clients parse specific codes. Returning wrong codes causes confusing errors. Test each error scenario with actual clients.

6. **ETag compatibility** — CID-as-ETag works for most operations but `Content-MD5` header validation and multipart ETag format (`md5s-count`) won't work. May need MD5 tee later for full compatibility.

### Low Priority

7. **ListObjectsV1** — Some older tools may use V1 (no `list-type=2`). boto3/awscli use V2 by default.

8. **Range requests** — Addressed in Phase 7A. `GET /bucket/key` with `Range: bytes=X-Y` for download resume and partial reads.

9. **Virtual-hosted-style** — Not needed for custom endpoints. Path-style sufficient.

10. **Expect: 100-continue** — Some clients send this. axum may handle it automatically. Test.

11. **SigV4 query string auth bugs (Phase 7B)** — Presigned URL verification differs from header-based SigV4. Must test against boto3-generated URLs.

12. **Bucket global uniqueness (Phase 7C)** — Breaking change from Phase 2's per-user buckets. Required for anonymous access. Must decide early; populate `s3_bucket_index` in Phase 2.

13. **Anonymous access security (Phase 7C)** — Unintended public exposure if ACLs/policies misconfigured. Default Private; require explicit opt-in.

### Risk Summary

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| SigV4 bugs | Blocks all usage | Medium | AWS test vectors, iterative client testing |
| Arion Body format | Upload failures | Low | Test early in Phase 3 |
| Content-Length mismatch | Download failures | Low | Verify Arion behavior, fall back to Sled |
| Multipart strategy | Complex GetObject | Medium | Start with multi-CID, optimize later |
| ETag compat | Client warnings | Low | CID-as-ETag for MVP, MD5 upgrade path |
| SigV4 query string auth bugs | Presigned URLs broken | Medium | Test against boto3-generated URLs |
| Policy evaluation bugs | Wrong access decisions | Medium | Exhaustive unit tests for all paths |
| Bucket global uniqueness | Breaking change from Phase 2 | High | Decide early; populate bucket_index in Phase 2 |
| Anonymous access security | Unintended public exposure | Medium | Default Private; require explicit opt-in |
| slice_stream correctness | Corrupted partial downloads | Low | Byte-level unit tests |

---

## Appendix A: Existing Code References

| Pattern | File | Lines | Usage in S3 Gateway |
|---------|------|-------|---------------------|
| AppState initialization | main.rs | 34-82 | Shared via Arc |
| Stream to Arion (multer) | main.rs | 423-481 | Adapted for Body input (same pattern, different source stream) |
| Stream from Arion | main.rs | 1063-1160 | Reused directly for GetObject |
| Arion delete | main.rs | 84-106 | Reused for DeleteObject |
| Sled key construction | database.rs | 136-181 | Pattern for S3 db modules |
| bincode serialization | database.rs | 188-195 | Pattern for S3 types |
| Prefix scanning | database.rs | 212-248 | Pattern for ListObjectsV2 |
| Request logger | middleware.rs | 51-116 | Reused for S3 routes |
| TLS config | tls.rs | 1-134 | Reused for S3 server |
| UserStorageSummary | database.rs | 330-400 | Called from PutObject/DeleteObject |
| push_usage_to_billing | billing.rs | 370-428 | Called from PutObject/DeleteObject |

## Appendix B: Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `HCFS_S3_ENABLED` | No | `true` | Enable/disable S3 gateway |
| `HCFS_S3_PORT` | No | `8333` | S3 server listen port |
| `HCFS_S3_REGION` | No | `us-east-1` | Region for SigV4 scope (cosmetic) |

## Appendix C: Sled Trees

| Tree Name | Key Format | Value | Phase |
|-----------|------------|-------|-------|
| `s3_credentials` | `{access_key_id}` | bincode S3Credentials | 2 |
| `s3_buckets` | `{user_id}:{bucket_name}` | bincode S3Bucket | 2 |
| `s3_objects` | `{user_id}:{bucket}:{key}` | bincode S3Object | 2 |
| `s3_multipart` | `{upload_id}` | bincode MultipartUpload | 2 |
| `s3_bucket_policies` | `{user_id}:{bucket_name}` | bincode BucketPolicy | 7C |
| `s3_bucket_index` | `{bucket_name}` | user_id bytes (UTF-8) | 7C |

## Appendix D: S3 API Coverage

| Operation | Method | Path | Phase | Priority |
|-----------|--------|------|-------|----------|
| ListBuckets | GET | / | 3 | Must |
| CreateBucket | PUT | /{bucket} | 3 | Must |
| HeadBucket | HEAD | /{bucket} | 3 | Must |
| DeleteBucket | DELETE | /{bucket} | 3 | Must |
| PutObject | PUT | /{bucket}/{key} | 3 | Must |
| GetObject | GET | /{bucket}/{key} | 3 | Must |
| HeadObject | HEAD | /{bucket}/{key} | 3 | Must |
| DeleteObject | DELETE | /{bucket}/{key} | 3 | Must |
| ListObjectsV2 | GET | /{bucket}?list-type=2 | 4 | Must |
| CreateMultipartUpload | POST | /{bucket}/{key}?uploads | 5 | Should |
| UploadPart | PUT | /{bucket}/{key}?partNumber&uploadId | 5 | Should |
| CompleteMultipartUpload | POST | /{bucket}/{key}?uploadId | 5 | Should |
| AbortMultipartUpload | DELETE | /{bucket}/{key}?uploadId | 5 | Should |
| ListParts | GET | /{bucket}/{key}?uploadId | 5 | Should |
| S3 Registration | POST | /s3_register (port 9999) | 6 | Must |
| List Credentials | GET | /s3_credentials/{user_id} (port 9999) | 6 | Nice |
| GetObject (Range) | GET | /{bucket}/{key} + Range header | 7A | Should |
| Presigned GET | GET | /{bucket}/{key}?X-Amz-Algorithm=... | 7B | Should |
| Presigned PUT | PUT | /{bucket}/{key}?X-Amz-Algorithm=... | 7B | Should |
| Presigned DELETE | DELETE | /{bucket}/{key}?X-Amz-Algorithm=... | 7B | Should |
| GetBucketAcl | GET | /{bucket}?acl | 7C | Should |
| PutBucketAcl | PUT | /{bucket}?acl | 7C | Should |
| GetObjectAcl | GET | /{bucket}/{key}?acl | 7C | Should |
| PutObjectAcl | PUT | /{bucket}/{key}?acl | 7C | Should |
| GetBucketPolicy | GET | /{bucket}?policy | 7C | Should |
| PutBucketPolicy | PUT | /{bucket}?policy | 7C | Should |
| DeleteBucketPolicy | DELETE | /{bucket}?policy | 7C | Should |

## Appendix E: Data Flow Diagrams

### PutObject (zero-copy)

```
boto3.put_object(Body=data)
    │
    ▼
PUT /mybucket/file.txt HTTP/1.1        ← S3 protocol
Host: localhost:8333
Authorization: AWS4-HMAC-SHA256 ...
Content-Length: 1048576
Content-Type: application/octet-stream
x-amz-content-sha256: UNSIGNED-PAYLOAD
    │
    │  [hcfs-server: verify SigV4, check bucket exists]
    │
    ▼
POST /upload HTTP/1.1                   ← Arion protocol
Host: arion:3000
X-API-Key: <arion_key>
Content-Type: multipart/form-data
    │
    │  [body bytes stream through untouched]
    │
    ▼
Arion → Validator (encrypts) → RS encode → distribute to Miners
    │
    ▼
{"hash": "QmXxx..."}                    ← Arion response
    │
    │  [hcfs-server: store metadata in Sled, return S3 response]
    │
    ▼
HTTP/1.1 200 OK                         ← S3 response
ETag: "QmXxx..."
```

### GetObject (zero-copy)

```
boto3.get_object(Bucket='mybucket', Key='file.txt')
    │
    ▼
GET /mybucket/file.txt HTTP/1.1         ← S3 protocol
Authorization: AWS4-HMAC-SHA256 ...
    │
    │  [hcfs-server: verify SigV4, lookup metadata in Sled]
    │
    ▼
GET /download/QmXxx... HTTP/1.1         ← Arion protocol
X-API-Key: <arion_key>
    │
    │  Arion → Validator (RS decode + decrypt) → stream
    │
    ▼
HTTP/1.1 200 OK                         ← S3 response
Content-Type: application/octet-stream
Content-Length: 1048576
ETag: "QmXxx..."
Last-Modified: Thu, 06 Feb 2026 12:00:00 GMT
    │
    │  [body bytes stream through untouched]
    │
    ▼
data = response['Body'].read()          ← boto3 receives data
```
