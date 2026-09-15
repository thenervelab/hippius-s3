# 06 — S3 Protocol Surface: Conformance Matrix

> **Scope.** This is the exhaustive, testable conformance contract for the S3 protocol
> surface the Rust reimplementation must reproduce byte-for-byte. It maps every action,
> every query-param sub-operation, every consumed header, every success/error XML shape,
> and every status code to the Python source it comes from.
>
> **Source of truth.** All references are to `hippius_s3/api/s3/` in the Python tree as of
> commit `ca74916f` (branch `main`/`staging`). Line numbers are `file.py:NNN`.
>
> **How to use as a checklist.** Every row and every bullet is written to be a test case.
> A Rust rewrite is conformant iff it reproduces the *Status*, *response shape*, *status
> code*, and *error code* columns exactly, including the deliberate divergences from AWS
> called out inline.

---

## 0. Routing model (read this first)

There is **no per-action routing**. FastAPI routes are keyed on `(verb, path-shape)` only;
the "action" is then resolved *inside the handler* by inspecting `request.query_params` and
certain headers. This query-param sub-operation dispatch is the single most important thing
to reproduce, and getting the **branch order** right is load-bearing (several comments in
the code document data-loss bugs caused by wrong ordering).

Routers (`hippius_s3/api/s3/router.py` includes all three):

| Router | File | Path shapes |
|--------|------|-------------|
| buckets | `buckets/router.py` | `/`, `/{bucket}` |
| objects | `objects/router.py` | `/{bucket}/{key:path}` |
| multipart | `multipart.py` | `POST /{bucket}/{key:path}` |
| public (anon) | `public_router.py` | `GET|HEAD /public/{bucket}/{key:path}` |

### 0.1 Bucket-router dispatch order (`buckets/router.py`)

- **`GET /`** → `ListBuckets` (`list_buckets` L40).
- **`GET /{bucket}`** (`get_bucket` L49) checks in this order: object-lock 501 guard →
  `object-lock` → `acl` → `location` → `tagging` → `lifecycle` → `uploads`
  (ListMultipartUploads) → `policy` → `versioning` → `versions` (ListObjectVersions) →
  **fallthrough** = ListObjects/ListObjectsV2.
- **`PUT /{bucket}`** (`create_or_modify_bucket` L109): object-lock 501 guard →
  `object-lock` → `acl` → `versioning` (**must precede create**, L123-127) → `retention`/
  `legal-hold` → **405 MethodNotAllowed** (L143-149) → invalid canned-ACL check →
  fallthrough = `handle_create_bucket` (which itself further dispatches `lifecycle` /
  `tagging` / `policy` / `cors` / create).
- **`DELETE /{bucket}`** (L157): `tagging` → any other query param → **501 NotImplemented**
  (L170-175, so `DeleteBucketPolicy`/`DeleteBucketCors` do NOT destroy the bucket) →
  fallthrough = `handle_delete_bucket`.
- **`POST /{bucket}`** (L180): `delete` → `DeleteObjects`; else **501 NotImplemented**.
- **`HEAD /{bucket}`** (L198) → `handle_head_bucket`.

### 0.2 Object-router dispatch order (`objects/router.py`)

- **`HEAD /{bucket}/{key}`** (L110): object-lock 501 guard → `handle_head_object`.
- **`GET /{bucket}/{key}`** (L123): object-lock 501 guard → `acl` → `retention`/`legal-hold`
  → `tagging` → `uploadId` (ListParts) → fallthrough = `handle_get_object`.
- **`PUT /{bucket}/{key}`** (L153-200): object-lock guard *with headers permitted* → `acl`
  → `retention`/`legal-hold` → invalid canned-ACL check → `uploadId`+`partNumber`
  (UploadPart / UploadPartCopy) → `tagging` → `x-amz-copy-source` (CopyObject) →
  fallthrough = `handle_put_object` (which detects S4 append internally). A trailing-slash
  variant is registered (`.../{key:path}/`, L153).
- **`DELETE /{bucket}/{key}`** (L203): object-lock 501 guard → `uploadId` (AbortMPU) →
  `tagging` → fallthrough = `handle_delete_object`.
- **`POST /{bucket}/{key}`** (`multipart.handle_post_object` L175): `uploads`
  (CreateMultipartUpload) → `uploadId` (CompleteMultipartUpload) → else 400 InvalidRequest.

### 0.3 `?versionId` combined with a subresource

`?versionId` on `?acl` or `?tagging` returns **501 NotImplemented** unconditionally
(`objects/router.py:_reject_version_id` L45-68) — tags and ACLs are stored on the current
version only, so honouring versionId would silently read/write the wrong version. Error
carries `Key` and `VersionId` elements. Reproduce this refusal exactly.

---

## 1. Action matrix

Status legend: **full** = AWS-equivalent behaviour; **partial** = works but diverges/limited;
**ack** = accepted and silently discarded (no persistence); **config-only** = persisted but
not fully enforced; **stub/501** = returns NotImplemented; **absent** = no handler (falls
through to something else or 404/501).

### 1.1 Service / bucket operations

| Action | HTTP verb + path/query | Status | Key headers consumed | Success shape (root / status) | Notable errors | Source |
|---|---|---|---|---|---|---|
| ListBuckets | `GET /` | full | — | `ListAllMyBucketsResult` / 200 | InternalError | `list_buckets_endpoint.py:18` |
| CreateBucket | `PUT /{bucket}` | full | `x-amz-acl`, `x-amz-bucket-object-lock-enabled`, `x-amz-grant-*` | empty / 200 | BucketAlreadyExists 409; AccessDenied 403 (anon / SS58-mismatch / service-acct write grant); InvalidArgument 400 (bad canned ACL) | `bucket_create_endpoint.py:26` |
| DeleteBucket | `DELETE /{bucket}` (no query) | full | — | empty / 204 | NoSuchBucket 404; BucketNotEmpty 409 (objects **or** in-progress MPU) | `bucket_delete_endpoint.py:16` |
| HeadBucket | `HEAD /{bucket}` | full | — | empty / 200 | 404 (empty body, no XML) | `bucket_head_endpoint.py:17` |
| ListObjects (v1) | `GET /{bucket}` | full | — | `ListBucketResult` / 200 | NoSuchBucket 404; InvalidArgument 400; SlowDown 503 (listing timeout) | `list_objects_endpoint.py:74` |
| ListObjectsV2 | `GET /{bucket}` (`list-type=2` ignored; same handler) | full | — | `ListBucketResult` / 200 | as ListObjects | `list_objects_endpoint.py:74` |
| ListObjectVersions | `GET /{bucket}?versions` | full | — | `ListVersionsResult` / 200 | NoSuchBucket 404; InvalidArgument 400 | `list_object_versions_endpoint.py:36` |
| ListMultipartUploads | `GET /{bucket}?uploads` | partial (no real paging) | `prefix` | `ListMultipartUploadsResult` / 200 | NoSuchBucket 404 | `multipart.py:1024` |
| DeleteObjects | `POST /{bucket}?delete` | full | — | `DeleteResult` / 200 | NoSuchBucket 404; MalformedXML 400; per-key `<Error>` | `delete_objects_endpoint.py:63` |
| GetBucketLocation | `GET /{bucket}?location` | partial (always us-east-1) | — | `LocationConstraint` / 200 | never errors (even for missing bucket) | `bucket_location_endpoint.py:6` |
| GetBucketVersioning | `GET /{bucket}?versioning` | full | `x-amz-expected-bucket-owner` | `VersioningConfiguration` / 200 | NoSuchBucket 404; AccessDenied 403 (owner mismatch) | `bucket_versioning_endpoint.py:38` |
| PutBucketVersioning | `PUT /{bucket}?versioning` | partial (Enabled only) | `x-amz-expected-bucket-owner` | empty / 200 | 501 (Suspended); 400 IllegalVersioningConfigurationException; MalformedXML 400; NoSuchBucket 404 | `bucket_versioning_endpoint.py:62` |
| GetObjectLockConfiguration | `GET /{bucket}?object-lock` | config-only | — | `ObjectLockConfiguration` / 200 | 404 ObjectLockConfigurationNotFoundError; NoSuchBucket 404 | `bucket_object_lock_endpoint.py:195` |
| PutObjectLockConfiguration | `PUT /{bucket}?object-lock` | config-only | — | empty / 200 | 409 InvalidBucketState (versioning off); MalformedXML 400; NoSuchBucket 404 | `bucket_object_lock_endpoint.py:218` |
| GetBucketPolicy | `GET /{bucket}?policy` | partial (public-read only) | — | JSON policy / 200 | NoSuchBucketPolicy 404 (private); NoSuchBucket 404 | `bucket_policy_endpoint.py:20` |
| PutBucketPolicy | `PUT /{bucket}?policy` | partial (public-read helper) | — | empty / **204** | MalformedPolicy 400; InvalidPolicyDocument 400; PolicyAlreadyExists 409; NoSuchBucket 404 | `bucket_policy_endpoint.py:74` |
| GetBucketTagging | `GET /{bucket}?tagging` | full | — | `Tagging` / 200 | NoSuchTagSet 404 (no tags); NoSuchBucket 404 | `bucket_tagging_endpoint.py:18` |
| PutBucketTagging | `PUT /{bucket}?tagging` | full | — | empty / 200 | MalformedXML 400; NoSuchBucket 404 | `bucket_create_endpoint.py:116` (dispatched from `handle_create_bucket`) |
| DeleteBucketTagging | `DELETE /{bucket}?tagging` | full | — | empty / 204 | NoSuchBucket 404 | `bucket_delete_endpoint.py:31` / `bucket_tagging_endpoint.py:94` |
| GetBucketLifecycle(Configuration) | `GET /{bucket}?lifecycle` | stub (never configured) | — | — | **404 NoSuchLifecycleConfiguration** always; NoSuchBucket 404 | `bucket_lifecycle_endpoint.py:17` |
| PutBucketLifecycle(Configuration) | `PUT /{bucket}?lifecycle` | ack (parsed, discarded) | — | empty / 200 | MalformedXML 400; NoSuchBucket 404 | `bucket_create_endpoint.py:50` |
| GetBucketAcl | `GET /{bucket}?acl` | full | — | `AccessControlPolicy` / 200 | NoSuchBucket 404 | `acl_endpoints.py:246` |
| PutBucketAcl | `PUT /{bucket}?acl` | full | `x-amz-acl`, `x-amz-grant-*` | empty / 200 | InvalidRequest 400 (canned+grant); InvalidArgument 400; MalformedACLError 400; AccessDenied 403; NoSuchBucket 404 | `acl_endpoints.py:293` |
| PutBucketCors | `PUT /{bucket}?cors` | ack (ignored) | — | empty / 200 | — | `bucket_create_endpoint.py:192` |
| PutBucketObjectLock retention/legal-hold on bucket path | `PUT /{bucket}?retention`/`?legal-hold` | 405 | — | — | **405 MethodNotAllowed** | `buckets/router.py:143` |

### 1.2 Object operations

| Action | HTTP verb + path/query | Status | Key headers consumed | Success shape (root / status) | Notable errors | Source |
|---|---|---|---|---|---|---|
| PutObject | `PUT /{bucket}/{key}` | full | `Content-MD5`, `If-None-Match`, `Content-Type`, `x-amz-meta-*`, `x-amz-acl`, `x-amz-object-lock-*` | empty / 200 (+ `ETag`, `x-amz-version-id`, `x-amz-meta-append-version:0`) | PreconditionFailed 412; NotImplemented 501 (If-None-Match ≠ `*`); InvalidDigest 400; BadDigest 400; NoSuchBucket 404; 499 (disconnect); SlowDown 503 | `put_object_endpoint.py:61` |
| GetObject | `GET /{bucket}/{key}` | full | `Range`, `If-None-Match`, `versionId`, `response-*` overrides | body / 200 or 206 | NoSuchKey 404; NoSuchBucket 404; NoSuchVersion 404; 304; 416; 405 (delete marker by version); 503 | `get_object_endpoint.py:42` |
| HeadObject | `HEAD /{bucket}/{key}` | full | `Range`(n/a), `If-None-Match`, `versionId`, `response-*` | empty / 200 (metadata headers) | 404; 304; 405 (marker by version); 400 InvalidArgument (bad versionId) | `head_object_endpoint.py:107` |
| DeleteObject | `DELETE /{bucket}/{key}` | full | `versionId`, `x-amz-bypass-governance-retention` | empty / 204 | NoSuchBucket 404; NoSuchVersion 404; 403 AccessDenied (object-lock); 501 (multi-alias versioned delete) | `delete_object_endpoint.py:213` |
| CopyObject | `PUT /{bucket}/{key}` + `x-amz-copy-source` | full | `x-amz-copy-source`(+`?versionId`), `x-amz-object-lock-*`, `If-None-Match` | `CopyObjectResult` / 200 (+ `ETag`, `x-amz-version-id`, `x-amz-copy-source-version-id`) | NoSuchBucket/Key/Version 404; 405 (source is marker by version); 501 (any If-None-Match); InvalidArgument 400 | `copy_object_endpoint.py:81` |
| GetObjectTagging | `GET /{bucket}/{key}?tagging` | full (current version only) | — | `Tagging` / 200 | NoSuchKey/Bucket 404; 501 (`?versionId`) | `tagging_endpoint.py:22` |
| PutObjectTagging | `PUT /{bucket}/{key}?tagging` | full (current version only) | — | empty / 200 | MalformedXML 400; NoSuchKey 404; 501 (`?versionId`) | `tagging_endpoint.py:78` |
| DeleteObjectTagging | `DELETE /{bucket}/{key}?tagging` | full | — | empty / 204 | NoSuchKey 404; 501 (`?versionId`) | `tagging_endpoint.py:148` |
| HeadObjectTagging (existence) | `HEAD /{bucket}/{key}?tagging` | partial | `versionId` | empty / 200 | 404/500 (bare headers) | `head_object_endpoint.py:134` |
| GetObjectAcl | `GET /{bucket}/{key}?acl` | full | — | `AccessControlPolicy` / 200 | NoSuchKey 404; 501 (`?versionId`) | `acl_endpoints.py:270` |
| PutObjectAcl | `PUT /{bucket}/{key}?acl` | full | `x-amz-acl`, `x-amz-grant-*` | empty / 200 | as PutBucketAcl; NoSuchKey 404; 501 (`?versionId`) | `acl_endpoints.py:440` |
| PutObject with canned ACL header | `PUT /{bucket}/{key}` + `x-amz-acl` | full | `x-amz-acl` | as PutObject | InvalidArgument 400 (unknown canned) | `objects/router.py:180-200` |
| GetObjectRetention | `GET /{bucket}/{key}?retention` | full (Tier 2) | `versionId` | `Retention` / 200 | NoSuchKey 404; NoSuchObjectLockConfiguration 404 | `object_lock_endpoints.py:243` |
| PutObjectRetention | `PUT /{bucket}/{key}?retention` | full (Tier 2) | `versionId`, `x-amz-bypass-governance-retention` | empty / 200 | InvalidRequest 400 (bucket lock off); MalformedXML 400; NoSuchKey 404; AccessDenied 403 (WORM transition); InvalidArgument 400 (> cap) | `object_lock_endpoints.py:264` |
| GetObjectLegalHold | `GET /{bucket}/{key}?legal-hold` | full (Tier 2) | `versionId` | `LegalHold` / 200 | NoSuchKey 404 | `object_lock_endpoints.py:308` |
| PutObjectLegalHold | `PUT /{bucket}/{key}?legal-hold` | full (Tier 2) | `versionId` | empty / 200 | InvalidRequest 400 (bucket lock off); MalformedXML 400; NoSuchKey 404 | `object_lock_endpoints.py:323` |
| GetObjectAttributes | `GET /{bucket}/{key}?attributes` | **absent** | — | (served as full GetObject body) | — | no handler; falls through `get_object_endpoint.py:42` |
| PostObject (browser form upload) | `POST /{bucket}` `multipart/form-data` | **absent** | — | — | 501 NotImplemented (falls into `POST /{bucket}` else-branch) | `buckets/router.py:191` |
| GetObjectTorrent / RestoreObject / SelectObjectContent / RenameObject | — | **absent** | — | — | — | — |
| Anonymous GetObject | `GET /public/{bucket}/{key}` | full | — | body / 200 (+ `x-hippius-access-mode: anon`) | SignatureDoesNotMatch 403 (any query param); NoSuchKey 404 (non-public) | `public_router.py:46` |
| Anonymous HeadObject | `HEAD /public/{bucket}/{key}` | full | — | empty / 200 | as above | `public_router.py:80` |

### 1.3 Multipart operations

| Action | HTTP verb + path/query | Status | Key headers consumed | Success shape (root / status) | Notable errors | Source |
|---|---|---|---|---|---|---|
| CreateMultipartUpload | `POST /{bucket}/{key}?uploads` | full | `Content-Type`, `x-amz-meta-*`, `x-amz-object-lock-*` | `InitiateMultipartUploadResult` / 200 | NoSuchBucket 404; EntityTooLarge 400; object-lock 400/400 | `multipart.py:366` |
| UploadPart | `PUT /{bucket}/{key}?uploadId&partNumber` | full | `Content-MD5` | empty / 200 (+ `ETag`) | NoSuchUpload 404; InvalidArgument 400 (part# range / zero-length); InvalidDigest/BadDigest 400; EntityTooLarge 400; 499 | `multipart.py:561` |
| UploadPartCopy | `PUT /{bucket}/{key}?uploadId&partNumber` + `x-amz-copy-source` | full | `x-amz-copy-source`(+`?versionId`), `x-amz-copy-source-range` | `CopyPartResult` / 200 | NoSuchUpload/Bucket/Key/Version 404; 405 (source marker); InvalidArgument 400; InvalidRange 416; EntityTooLarge 413 | `multipart.py:630` |
| CompleteMultipartUpload | `POST /{bucket}/{key}?uploadId` | full | `If-None-Match`, `Host` | `CompleteMultipartUploadResult` / 200 (+ `x-amz-version-id`) | NoSuchUpload 404; MalformedXML 400; InvalidPart 400; InvalidPartOrder 400; InvalidRequest 400 (no parts); PreconditionFailed 412; NotImplemented 501 (If-None-Match ≠ `*`) | `multipart.py:1134` |
| AbortMultipartUpload | `DELETE /{bucket}/{key}?uploadId` | full | — | empty / 204 | NoSuchUpload 404 | `multipart.py:898` |
| ListParts | `GET /{bucket}/{key}?uploadId` | full | `max-parts`, `part-number-marker` | `ListPartsResult` / 200 | NoSuchUpload 404; NoSuchBucket 404; InvalidRequest 400 (completed) | `multipart.py:241` |

### 1.4 Hippius extensions (non-AWS)

| Extension | Trigger | Behaviour | Source |
|---|---|---|---|
| S4 append | `PUT /{bucket}/{key}` + `x-amz-meta-append: true` | atomic O(delta) append with version CAS | `extensions/append.py:58`; §5 |
| `X-Hippius-Body-Blake3` + `-Scope` | GET/HEAD response | plaintext BLAKE3 with scope (`full`/`first-chunk`/`prefix`) | `common/headers.py:21` |
| `X-Hippius-Arion-File-Hash` | GET/HEAD response | Arion ciphertext hash of first chunk, or `pending` | `head_object_endpoint.py:257` |
| `x-hippius-source` | GET/HEAD response | `cache` or `pipeline` diagnostic | `common/headers.py:128` |
| `<ArionHash>` element | ListObjects/ListObjectVersions `<Contents>`/`<Version>` | per-object Arion hash when single-chunk | `list_objects_endpoint.py:194` |
| `x-hippius-access-mode: anon` | `/public/*` responses | anonymous access marker | `public_router.py:76` |

---

## 2. Conditional requests

### 2.1 On writes (PutObject, CompleteMultipartUpload, CopyObject) — PR #523 (`fix/put-if-none-match`)

`If-None-Match` on a write is parsed by `common/headers.py:parse_write_if_none_match` (L153):

| Header value | Behaviour | Status / error | Source |
|---|---|---|---|
| absent | normal write | — | L160 |
| `*` (create-only) | write iff key does not already exist; existence checked **inside the writer's reserve transaction** (not a pre-read) | on conflict → **412 PreconditionFailed** `<Error><Code>PreconditionFailed</Code>…<Condition>If-None-Match</Condition>` | `errors.py:246`; writer raises `PreconditionFailed` |
| any other value (incl. an ETag) | refused, never silently ignored | **501 NotImplemented** `<Header>If-None-Match</Header>` (raises `UnsupportedConditionalWrite`) | `errors.py:256`, `headers.py:164` |

Path-specific wiring:
- **PutObject** (`put_object_endpoint.py:95`): parsed header-only *before* body read;
  malformed → answered via `utils.respond_before_body` (body drained). `*` result threaded
  into `writer.put_simple_stream_full(if_none_match=…)`; a `PreconditionFailed` at reserve or
  finalize → 412, and the pre-existing key still serves its old content (L322-327).
- **CompleteMultipartUpload** (`multipart.py:1142`): `If-None-Match` evaluated **at
  completion, not at initiate**. `key_existed_at_initiate` was captured at initiate
  (`multipart.py:515`) because the upsert cleared any soft-delete; threaded into
  `mpu_complete(if_none_match=…, key_existed_at_initiate=…)`. Conflict → 412, upload stays
  open and abortable.
- **CopyObject** (`copy_object_endpoint.py:102`): **any** `If-None-Match` value, including
  `*`, returns **501 NotImplemented** — the copy paths (alias / v5-fast / streaming) do not
  run under the conditional reserve/finalize, so create-only is not implemented for copy.
- **S4 append**: `*` is judged inside append's own locked CAS transaction; see §5.

### 2.2 On reads (GetObject, HeadObject) — `If-None-Match`

`common/headers.py:if_none_match_matches` (L95): returns 304 iff the header indicates the
client already holds the current object.
- Accepts `"<md5>"`, `"<md5>-<parts>"`, weak `W/"…"` prefix stripped, comma-separated lists
  (RFC 7232), and `*` wildcard.
- GetObject: `get_object_endpoint.py:253` → `304` with only `ETag` header (no body, no
  blake3/override headers).
- HeadObject: `head_object_endpoint.py:218` → `304` with `ETag`. For multipart objects with
  a missing stored md5, the combined ETag is recomputed from parts first (L198-217).

### 2.3 Not implemented on reads or writes

- **`If-Match`** — not consulted anywhere. (docs/s3-compatibility.md §"Conditional reads":
  "`If-Match` and conditional writes (`If-None-Match` on PUT) are not yet supported" — the
  latter statement is now stale after PR #523; `If-Match` remains unimplemented.)
- **`If-Modified-Since` / `If-Unmodified-Since`** — not consulted on GET/HEAD/PUT/Copy.

> **Rust checklist:** reproduce 412 vs 501 split exactly; ensure the create-only existence
> check is transactional, not a TOCTOU pre-read; drain the request body before an early 412/
> 501 on PutObject/UploadPart/CompleteMPU (PR #523 commit `83f1d6e7`); CopyObject rejects
> every `If-None-Match`; `If-Match`/`If-*-Since` are accepted-and-ignored today (document as
> a gap, do not start honouring silently).

---

## 3. Content-MD5 and checksums — PR #522 (`fix/content-md5-verification`)

### 3.1 Content-MD5 (RFC 1864)

Parsed by `common/headers.py:parse_content_md5` (L171): base64 of a 16-byte digest.

| Condition | Error | Status | When detected | Source |
|---|---|---|---|---|
| absent | none (skip) | — | — | L179 |
| present but not base64-of-16-bytes | **InvalidDigest** | 400 | before body read | `errors.py:266`, raises `InvalidContentMD5` |
| well-formed but ≠ received body | **BadDigest** | 400 | after writer hashes body | `errors.py:271`, writer raises `BadDigest` |

Wiring: PutObject (`put_object_endpoint.py:88`), UploadPart (`multipart.py:616`; **skipped
for UploadPartCopy** — no request body, L617), S4 append (`append.py:138`, digest covers the
appended delta only). Malformed digest answered via `utils.respond_before_body`. The expected
digest is threaded to the writer (`expected_md5=`), which does the byte comparison.

### 3.2 `x-amz-checksum-*` (CRC32/CRC32C/SHA1/SHA256/CRC64NVME) and `x-amz-checksum-algorithm`

**Not implemented / not verified.** No handler reads any `x-amz-checksum-*` header. The only
integrity mechanism is Content-MD5 (above). AWS `x-amz-checksum-type: FULL_OBJECT|COMPOSITE`
is *conceptually* mirrored by the Hippius extension `X-Hippius-Body-Blake3-Scope`
(`common/headers.py:38`) but that is a **response-only plaintext BLAKE3**, not a client-
supplied checksum and not part of any verification.

> **Rust checklist:** InvalidDigest (malformed, pre-body) vs BadDigest (mismatch, post-body)
> are distinct 400s; UploadPartCopy ignores Content-MD5; no `x-amz-checksum-*` support.

---

## 4. Range and partial content

### 4.1 Range parsing (`range_utils.py:parse_range_header` L6)

Accepts only `bytes=`-prefixed specs:
- `bytes=start-end` → `(start, min(end, size-1))`.
- `bytes=start-` → `(start, size-1)`.
- `bytes=-suffix` → last `suffix` bytes; `suffix<=0` raises.
- `start >= size` → raises `ValueError` → **416**.
- `end < start` (inverted) → **treated as no range = full object** `(0, size-1)` (AWS
  behaviour, L37-38). GetObject additionally tracks `range_was_invalid` so headers reflect
  full body (`get_object_endpoint.py:304-312`).

### 4.2 Response semantics (GetObject)

- Valid range → **206 Partial Content** with `Content-Range: bytes start-end/size`,
  `Accept-Ranges: bytes`, `Content-Length: end-start+1` (`common/headers.py:build_headers`
  L130-135).
- Unsatisfiable (`start>=size`, bad suffix) → **416** with `Content-Range: bytes */{size}`,
  `Accept-Ranges: bytes`, `Content-Length: 0` (`get_object_endpoint.py:327-334`).
- Effective size for range resolution falls back to sum of chunk sizes when
  `objects.size_bytes` is 0 (L297-301).
- **No `multipart/byteranges`** — only a single contiguous range is supported.

### 4.3 Range on multipart / encrypted objects

- The reader (`reader.types.RangeRequest`, `object_reader.read_response`) computes which
  parts/chunks a range spans (`range_utils.calculate_chunks_for_range` L45,
  `extract_range_from_chunks` L59) and decrypts only what is needed.
- Range works across S4 append boundaries (parts on one version) and across multipart part
  boundaries.
- `x-amz-version-id` is set on both 200 and 206; `X-Hippius-Body-Blake3(+Scope)` and
  `response-*` overrides applied on 200/206 only, omitted on 304 (`get_object_endpoint.py:415-424`).

### 4.4 UploadPartCopy range

`x-amz-copy-source-range: bytes=start-end` — strict `^bytes=(\d+)-(\d+)$` only
(`multipart.py:647`). Invalid format → InvalidArgument 400; `start<0`/`end<start`/
`end>=source_size` → InvalidRange 416; sliced length > `max_multipart_part_size` →
EntityTooLarge 413.

> **Rust checklist:** inverted range = full 200 (not 416); 416 body-less with `bytes */size`;
> single range only; version-id present on 206.

---

## 5. The S4 append extension (`extensions/append.py`, `docs/s4.md`)

**Trigger:** `PUT /{bucket}/{key}` with header `x-amz-meta-append: true` (case-insensitive,
`put_object_endpoint.py:101`). Detected header-only; forces a full bucket-row lookup
(`needs_bucket_row`, L107) and delegates to `handle_append` (L152).

### 5.1 Semantics

- **Atomic O(delta):** publishes only the new delta as a new `parts` row on the **same**
  `object_version` (does NOT mint a new version — `object_writer.append_stream`), updates
  size and the multipart-style composite ETag in one transaction under `SELECT … FOR UPDATE`
  on the object row.
- **Version CAS (required):** `x-amz-meta-append-if-version: <int>` (`append.py:88`).
  - Missing → **400 InvalidRequest** "Missing append-if-version" (L109).
  - Non-integer → **400 InvalidRequest** "append-if-version must be an integer" (L118).
  - Mismatch → **412 PreconditionFailed** "Version precondition failed", with response
    headers `x-amz-meta-append-version: <current>` and `Retry-After: 0.1` (L149-158).
- **Current version exposure:** HEAD returns `x-amz-meta-append-version: <int>`
  (`head_object_endpoint.py:281`); a successful append returns it too (L198-204), and a base
  PutObject returns `x-amz-meta-append-version: 0` (`put_object_endpoint.py:317`), so clients
  can chain appends without a HEAD.
- **Idempotency (optional):** `x-amz-meta-append-id: <id>`. Result cached in Redis under
  `append_id:{bucket_id}:{object_key}:{append_id}` for 3600s; a repeat returns the stored
  ETag with 200 (L92-106, L211-214). Best-effort; Redis failure falls through.
- **Success:** 200 with `ETag: "<composite>"` and `x-amz-meta-append-version: <new>`.
- **Control metadata is not persisted** as user metadata: `append`, `append-id`,
  `append-if-version` are stripped (`put_object_endpoint.py:179`).

### 5.2 `If-None-Match` and Content-MD5 on append

- `If-None-Match: *` → an append only ever targets an existing key, so it can never succeed:
  existing key → **412 PreconditionFailed**; absent key → **404 NoSuchKey** (`append.py:141`,
  `159`). Judged inside the same row lock as the CAS. Any other `If-None-Match` value → 501
  (parsed upstream in PutObject).
- **Content-MD5** covers the appended delta only → **400 BadDigest** on mismatch
  (`append.py:146`).
- Empty append body → **400 InvalidRequest** "Empty append not allowed" (`EmptyAppendError`,
  L166).

### 5.3 Error catalog (append)

| Condition | Code | Status |
|---|---|---|
| version mismatch | PreconditionFailed | 412 (+`x-amz-meta-append-version`) |
| missing/malformed append-if-version | InvalidRequest | 400 |
| `If-None-Match: *` on existing key | PreconditionFailed | 412 |
| absent key | NoSuchKey | 404 |
| Content-MD5 mismatch | BadDigest | 400 |
| empty delta | InvalidRequest | 400 |
| temporarily not appendable | ServiceUnavailable/SlowDown | 503 |

### 5.4 The WORM tension (see `object-lock.md` §5.2, gap G9)

Append **mutates a version in place** (same version id, more bytes, new composite MD5). If
that version is Object-Lock-protected, this is a silent WORM violation. The design says
`PutObject` with `x-amz-meta-append: true` **must be refused 403 AccessDenied when the
current version is protected** — but **this refusal is NOT implemented**: `handle_append`
and `object_writer.append_stream` take only a *DB row lock*, never consulting
`is_version_locked`. Reproduce the append feature, and **add the missing lock check** (or
document the gap) in the rewrite.

---

## 6. Object Lock / retention / legal-hold

Tiers (`object_lock_guard.py`, `specs/s3-object-lock.md`): **Tier 0** = 501 everywhere
(historical); **Tier 1** = bucket config persisted; **Tier 2** = per-version retention/
legal-hold set + read + enforced.

### 6.1 Modes and rules (`object_lock_endpoints.py`, `object_lock_enforcement.py`)

- Modes: `GOVERNANCE`, `COMPLIANCE` (`object_lock_enforcement.py:42-43`).
- **Locked predicate** (`is_version_locked` L48; canonical SQL string
  `LOCKED_VERSION_SQL_PREDICATE` L37): `legal_hold == ON OR (retain_until IS NOT NULL AND
  retain_until > now())`. Retention and legal hold are **independent** — either locks.
- **Weakening rules** (`validate_retention_transition` L148):
  - COMPLIANCE, active: no mode change, no shortening, no clearing — **extend only**; else
    403 AccessDenied.
  - GOVERNANCE, active: weakening (shorten/remove/downgrade) requires
    `may_bypass_governance` = bucket-owner **and** `x-amz-bypass-governance-retention: true`
    (`enforcement.py:74`); else 403.
  - Unrecognised live mode → 403.
- **Bypass permission is bucket-owner-only** (deliberate deviation from AWS's IAM
  `s3:BypassGovernanceRetention`; `enforcement.py:74-85`), and `request_is_bucket_owner`
  compares `account.main_account` vs `bucket_owner_id` (NOT `main_account_id`, L118-133).
- **Retention cap:** `object_lock_max_retention_days` (default 3650) rejects far-future
  `RetainUntilDate` with 400 InvalidArgument (`object_lock_endpoints.py:107`, `475`).

### 6.2 Where it is enforced (what actually blocks)

- **Per-object set/read** (Tier 2): `Put/GetObjectRetention`, `Put/GetObjectLegalHold` fully
  implemented (`object_lock_endpoints.py`). Bucket must have Object Lock enabled
  (`bucket_lock_enabled` L371) else 400 InvalidRequest "Bucket is missing Object Lock
  Configuration".
- **Write-path headers** applied to the created version on PutObject
  (`put_object_endpoint.py:236`), CopyObject destination (`copy_object_endpoint.py:_apply_lock_to_copy`
  L33), and CreateMultipartUpload's reserved version (`multipart.py:485`). Precedence:
  explicit `x-amz-object-lock-*` headers override bucket default retention
  (`lock_for_new_version` L392). Bucket default is a duration computed from version creation
  time (`_bucket_default_retention` L447).
- **Read-path echo:** GET/HEAD emit `x-amz-object-lock-mode`, `-retain-until-date`,
  `-legal-hold` (`lock_response_headers` L485; HEAD wires at `head_object_endpoint.py:252`).
- **Delete enforcement (now wired — contradicts the stale `enforcement.py` module
  docstring):**
  - `DELETE ?versionId` on a locked version → **403 AccessDenied** via
    `deletion_refusal_reason` inside the row lock (`delete_object_endpoint.py:107`). A
    GOVERNANCE bypass clears the retention after an authorised delete (L150-159).
  - Versioned delete of an object published under >1 name (alias) → **501 NotImplemented**
    (`delete_object_endpoint.py:131`).
  - Whole-object (unversioned) delete when any live version is locked → **403 AccessDenied**
    (`count_locked_versions`, `delete_object_endpoint.py:288`).
  - `DeleteObjects` maps a per-key 403 lock refusal to `<Error><Code>AccessDenied</Code>`,
    501 to NotImplemented, else InternalError (`delete_objects_endpoint.py:159`); and checks
    `count_locked_versions` on the whole-object branch (L205).
  - A **simple** (versionId-less) DELETE on a versioning-enabled bucket writes a delete
    marker and is **always allowed** — never refused (`enforcement.py` rule 2).
- **Bucket config prerequisite:** `PUT ?object-lock` requires `versioning_status == Enabled`
  else 409 InvalidBucketState (`bucket_object_lock_endpoint.py:242`). `CreateBucket` with
  `x-amz-bucket-object-lock-enabled: true` enables versioning implicitly
  (`bucket_create_endpoint.py:278`).

### 6.3 Config-only vs enforced

- Bucket `?object-lock` config round-trips and is stored in `buckets.object_lock` JSONB
  (`bucket_object_lock_endpoint.py`). The module docstring says "no enforcement" — that is
  **stale**: default retention IS materialised onto new versions and delete paths ARE gated.
- **Genuinely unenforced today:** S4 append vs a locked current version (§5.4); background
  workers (unpinner/reaper/hard-delete) enforce in SQL per `object-lock.md` §5.1 but that is
  outside the API surface.

### 6.4 Object-lock 501 guard (`object_lock_guard.py:63`)

The remaining 501 surface: `x-amz-object-lock-*` headers on read/delete/bucket routes
(anywhere the header names nothing a write can apply). `_QUERY_SUBRESOURCES` is now **empty**
(retention/legal-hold are real). PUT/Copy/CreateMPU pass `object_lock_headers_supported=True`
to opt out. `x-amz-bypass-governance-retention` and bucket `?object-lock` /
`x-amz-bucket-object-lock-enabled` are **not** triggers.

---

## 7. Error model (`errors.py`)

All errors serialise via `s3_error_response` (L44) as a **no-namespace**
`<Error><Code/><Message/><RequestId/><HostId>hippius-s3</HostId>…</Error>` (deliberately no
xmlns for boto3 compatibility, L69), UTF-8 with declaration, plus headers:
`Content-Type: application/xml; charset=utf-8`, `x-amz-request-id`, `Content-Length`,
`x-amz-error-code`, `x-amz-error-message` (latin-1-coerced, L110). `**kwargs` add extra
child elements (e.g. `BucketName`, `Key`, `VersionId`, `Condition`, `Header`).

### 7.1 Catalog (code → HTTP status → where)

| Code | HTTP | Meaning / trigger | Source |
|---|---|---|---|
| NoSuchBucket | 404 | bucket missing | pervasive |
| NoSuchKey | 404 | key missing / delete marker as current / non-public anon | `get_object_endpoint.py:202` |
| NoSuchVersion | 404 | version missing, or versionId on unversioned bucket | `get_object_endpoint.py:171`, `delete_object_endpoint.py:249` |
| NoSuchUpload | 404 | uploadId unknown/mismatched | `multipart.py:255` |
| NoSuchTagSet | 404 | bucket has no tags | `bucket_tagging_endpoint.py:36` |
| NoSuchBucketPolicy | 404 | private bucket (no public-read) | `bucket_policy_endpoint.py:35` |
| NoSuchLifecycleConfiguration | 404 | lifecycle never persisted (always) | `bucket_lifecycle_endpoint.py:46` |
| NoSuchObjectLockConfiguration | 404 | version has no retention | `object_lock_endpoints.py:251` |
| ObjectLockConfigurationNotFoundError | 404 | bucket lock not configured | `bucket_object_lock_endpoint.py:207` |
| BucketAlreadyExists | 409 | unique-violation on create | `bucket_create_endpoint.py:307` |
| BucketNotEmpty | 409 | non-empty / in-progress MPU | `bucket_delete_endpoint.py:88`,`98` |
| PolicyAlreadyExists | 409 | public policy already set | `bucket_policy_endpoint.py:94` |
| InvalidBucketState | 409 | `?object-lock` PUT, versioning off | `bucket_object_lock_endpoint.py:243` |
| PreconditionFailed | 412 | If-None-Match `*` conflict / append CAS | `errors.py:246` |
| BadDigest | 400 | Content-MD5 ≠ body | `errors.py:271` |
| InvalidDigest | 400 | malformed Content-MD5 | `errors.py:266` |
| MalformedXML | 400 | bad request XML | pervasive |
| MalformedPolicy | 400 | bad/empty policy JSON | `bucket_policy_endpoint.py:103` |
| InvalidPolicyDocument | 400 | policy not a valid public-read | `bucket_policy_endpoint.py:109` |
| MalformedACLError | 400 | bad ACL XML/body | `acl_endpoints.py:386` |
| IllegalVersioningConfigurationException | 400 | versioning body not Enabled/Suspended | `bucket_versioning_endpoint.py:113` |
| InvalidArgument | 400 | bad versionId / max-keys / canned ACL / grant / lock date / part# | pervasive |
| InvalidRequest | 400 | missing append-if-version; bucket lock off; unsupported multipart POST; canned+grant ACL | `append.py:110`, `object_lock_endpoints.py:385` |
| InvalidPart | 400 | complete: part missing or ETag mismatch | `multipart.py:1281` |
| InvalidPartOrder | 400 | complete: parts not ascending | `multipart.py:1223` |
| EntityTooLarge | 400/413 | object/part exceeds max | `multipart.py:424`,`730` |
| InvalidRange | 416 | UploadPartCopy bad range | `multipart.py:710` |
| AccessDenied | 403 | anon create / SS58 mismatch / expected-owner mismatch / service-acct write grant / object-lock refusal / put-acl unauthorized | pervasive |
| SignatureDoesNotMatch | 403 | any query param on `/public/*` | `public_router.py:58` |
| MethodNotAllowed | 405 | delete marker addressed by version; source marker; retention/legal-hold on bucket path | `get_object_endpoint.py:239`, `buckets/router.py:144` |
| NotImplemented | 501 | If-None-Match ≠ `*`; Suspended versioning; versionId+acl/tagging; unknown bucket DELETE subresource; unsupported POST; multi-alias versioned delete; unsupported storage/enc suite; per-object lock headers on read/delete | `errors.py:256`, pervasive |
| SlowDown | 503 | pool saturation / listing timeout / KMS brownout / download not ready | `errors.py:119`,`138`,`176` |
| InternalError | 500 | uncaught / undecryptable object / KMS misconfig | `errors.py:194` |
| — | 499 | client closed request (nginx code, nothing written) | `errors.py:16` (`CLIENT_CLOSED_REQUEST`) |

### 7.2 Read-path exception mapping (`errors.py:map_read_path_exception` L206)

First-match-wins chain used by both the global handler and inline GET catch-all
(`get_object_endpoint.py:484`): `DownloadNotReadyError`/`initial_stream_timeout` → 503
SlowDown; pool saturation → 503; KMS transient markers → 503; `InvalidTag`/unreadable KEK/
KMS-misconfig → 500 InternalError; `UnsupportedStorageVersionError` → 501 NotImplemented;
`unsupported_enc_suite_id` → 501. The transient/permanent split is intentional (retry vs no).

---

## 8. XML (de)serialization

### 8.1 Parser hardening — `hippius_s3/xml_helpers.py`

**All request bodies MUST be parsed with `parse_untrusted_xml` (L33):**
`XMLParser(resolve_entities=False, load_dtd=False, no_network=True, huge_tree=False)`.
Blocks billion-laughs/XXE; still decodes predefined + numeric entities so quote-escaping
round-trips. Raises `ValueError` (not `XMLSyntaxError`) on malformed input. Builders:
`create_element`, `add_subelement`, `to_xml_bytes` (escape values).

> **Divergence to preserve:** several handlers (`bucket_create_endpoint.py`,
> `bucket_tagging_endpoint.py`, `tagging_endpoint.py`, `list_buckets_endpoint.py`,
> `delete_objects_endpoint.py` response, `acl_endpoints.py` via stdlib `xml.etree`) still use
> raw `lxml.etree.fromstring`/`ET.fromstring`. The rewrite should use the hardened parser
> everywhere, but must keep the **acceptance behaviour** identical (see 8.3).

### 8.2 Namespace

Responses use `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` on the root — **except**
error responses (`<Error>` has no namespace, `errors.py:69`), `CopyObjectResult` /
`CopyPartResult` (no xmlns, `copy_helpers.py:232`, `multipart.py:867`), and
`AccessControlPolicy` (namespace set as attribute via stdlib ElementTree,
`acl_endpoints.py:54`).

### 8.3 `local-name()` / namespace-tolerant request parsing (critical)

S3 clients disagree on namespacing request bodies (botocore/aws-cli namespace; minio-go/mc
send bare). Real S3 accepts both. Reproduce **both**:

| Body | Matching strategy | Source |
|---|---|---|
| CompleteMultipartUpload `<Part>`/`<PartNumber>`/`<ETag>` | `local-name()` xpath, paired per `<Part>`, direct children only; empty/whitespace/entity ETag rejected | `multipart.py:parse_complete_multipart_upload` L63 |
| DeleteObjects `<Object>`/`<Key>`/`<VersionId>`/`<Quiet>` | `local-name()` xpath (a namespaced xpath silently deletes nothing) | `delete_objects_endpoint.py:parse_delete_request` L39 |
| PutBucketVersioning `<Status>` | `local-name()` xpath | `bucket_versioning_endpoint.py:98` |
| Retention/LegalHold | `_local()` tag-suffix walk (both namespaced and bare) | `object_lock_endpoints.py:47` |
| ObjectLockConfiguration | tries `{ns}Tag` then bare `find` | `bucket_object_lock_endpoint.py:106` |
| Bucket/object tagging, ACL | tries `s3:` namespace then bare fallback (`_find_element`) | `bucket_tagging_endpoint.py:74`, `acl_endpoints.py:45` |

### 8.4 Response element ordering (SDK-strict)

- **ListObjectsV2** (`list_objects_endpoint.py:159`): `Name`, `Prefix`,
  [`ContinuationToken`], [`StartAfter`], `MaxKeys`, [`Delimiter`], `IsTruncated`,
  [`EncodingType`], `KeyCount`, [`NextContinuationToken`], then `Contents*`, then
  `CommonPrefixes*`. `Contents` = `Key`, `LastModified`, `ETag`, `Size`, `StorageClass`
  (`STANDARD`), [`ArionHash`], `Owner{ID,DisplayName}`.
- **ListVersionsResult** (`list_object_versions_endpoint.py:102`): scalars, then interleaved
  `Version`/`DeleteMarker`, then `CommonPrefixes`. Markers omit `ETag`/`Size`/`StorageClass`
  (L153). `IsLatest` = `object_version == current_object_version`.
- **CompleteMultipartUploadResult** (`multipart.py:108`): `Location` (from `Host` header,
  escaped), `Bucket`, `Key`, `ETag` (quoted).
- Timestamps: `format_s3_timestamp` = `YYYY-MM-DDTHH:MM:SS.mmmZ` (`common/format.py:4`) in
  listings/parts; HTTP `Last-Modified` uses RFC-1123 `%a, %d %b %Y %H:%M:%S GMT`.
- ETag always double-quoted in headers and XML; multipart ETag = `md5(concat(part_md5_binary))-N`
  (`multipart.py:hash_all_etags` L1115).

### 8.5 Version IDs

Decimal integers, not opaque tokens (`object-versions.md` §5.1). Parsing
(`common/req.py:parse_version_id` L62): only bare ASCII digits, positive, `<= 2^63-1`; the
literal `"null"` and empty → "current version" (`None`). Rejects `int()`-isms
(underscores, signs, Arabic-Indic digits). Bad → 400 InvalidArgument (GET/DELETE XML; HEAD
bare headers).

---

## 9. Genuine gaps vs AWS (`docs/s3-compatibility.md`)

### 9.1 Of the ~108 AWS S3 actions, unsupported (no handler; ~80 actions)

Bucket config families entirely absent: **Accelerate, Analytics, Encryption
(SSE-KMS/SSE-S3 config), IntelligentTiering, Inventory, Logging, Metrics, Notification,
OwnershipControls, PublicAccessBlock, Replication, RequestPayment, Website, Cors (GET/DELETE;
PUT is ack-only), MetadataConfiguration/Table**. Object actions absent: **GetObjectAttributes,
GetObjectTorrent, RestoreObject, SelectObjectContent, RenameObject, WriteGetObjectResponse**.
Session/directory: **CreateSession, ListDirectoryBuckets**. `DeleteBucketPolicy`,
`DeleteBucketCors`, `DeleteBucketLifecycle`, `DeleteBucketTagging` (only tagging exists).

### 9.2 Partial / degraded (present but diverge)

| Action | Divergence |
|---|---|
| GetBucketLocation | always `us-east-1`, never 404 |
| PutBucketLifecycle / GetBucketLifecycle | PUT parsed-and-discarded (ack); GET always 404 NoSuchLifecycleConfiguration |
| PutBucketPolicy / GetBucketPolicy | only a canned **public-read** policy; no per-prefix, per-principal, or arbitrary statements — **per-prefix policy is entirely absent**; returns 204 on PUT |
| PutBucketVersioning | `Enabled` only; `Suspended` → 501 |
| Object Lock config | persisted; append-vs-lock (§5.4) unenforced |
| ListMultipartUploads | no real pagination (`KeyMarker`/`UploadIdMarker` always empty, `IsTruncated: false`, `MaxUploads: 1000`) |
| CopyObject | `If-None-Match` (any value) → 501; v5 fast path disabled (always streaming/alias) |
| Object tagging / ACL | current-version only; `?versionId` → 501 |
| x-amz-checksum-* | unsupported (Content-MD5 only) |
| If-Match / If-*-Since | accepted and ignored (no conditional-read/write beyond If-None-Match) |
| PutBucketCors | 200 ack, ignored |

### 9.3 Deliberate divergences (must reproduce)

- **Object keys may not contain `?` or `#`** → **400 InvalidURI** (gateway path parsing;
  `docs/s3-compatibility.md` "Known divergences"). CopyObject (header source) and
  DeleteObjects (body keys) bypass path parsing and can still reach such keys.
- **`GET /public/*` with any query param → 403 SignatureDoesNotMatch** (whitelist).
- **Non-public bucket on `/public/*` → 404 NoSuchKey** (never AccessDenied — avoids an
  existence oracle).
- **DeleteBucket with any unknown subresource → 501**, not a bucket delete (prevents
  `DeleteBucketPolicy` destroying the bucket).
- **`PUT /{bucket}?retention|legal-hold` → 405 MethodNotAllowed** (object-level subresources
  on a bucket path).
- **Bucket-owner-only governance bypass** (no IAM; §6.1).
- **Same-bucket CopyObject creates a name alias** on one `object_id` (no re-encrypt);
  affects delete semantics (alias/promoted/last, `object_names.py:14`).
- **Anonymous/signed reads attribute storage to the bucket owner** (`main_account_id`), not
  the caller.

---

## 10. Open questions

1. **`object_lock_enforcement.py` module docstring is stale.** It states delete enforcement
   is "NOT WIRED YET", but `delete_object_endpoint.py:107` calls `deletion_refusal_reason`
   and `delete_objects_endpoint.py:205` checks `count_locked_versions`. Confirm the delete
   paths are the intended enforced behaviour (they appear to be) and that the rewrite should
   treat delete-of-locked as 403, not 501.
2. **RESOLVED — S4 append vs Object Lock (§5.4 / G9).** The rewrite **MUST** add the 403-if-current-
   version-protected check **inside the append CAS transaction** (register C2/D-series; doc 18 §4,
   Rule S4-WORM). This is the live Python WORM hole, closed by construction in the rewrite. No
   "preserve unguarded" option.
3. **RESOLVED — `GetObjectAttributes`.** In scope: the rewrite adds the real `?attributes` XML shape
   (register C1; doc 22 §2.2 "resolves 06 Q3"), not the body fall-through.
4. **`?tagging` HEAD** returns bare `x-amz-error-*` headers on 404/500 rather than an XML
   body (HEAD has no body). Confirm this is acceptable for the rewrite's clients.
5. **RESOLVED — ListMultipartUploads pagination** is in scope (real pagination) for the rewrite
   (register C1; doc 22 §2.2 "resolves 06 Q5"). The Python stub (`MaxUploads` fixed 1000) is the gap.
6. **`PutBucketVersioning` authorization** is by ACL middleware grade (WRITE_ACP), and the
   handler's `get_by_name_and_owner` is a *resolution*, not an ownership check
   (`bucket_versioning_endpoint.py:62` comment). The rewrite must keep authz in the ACL layer,
   not in these handlers.
7. **`x-amz-copy-source-version-id`** is emitted by byte-copy paths but not the alias path
   (alias has no source version snapshot). Confirm parity expectations.
8. **`X-Hippius-Body-Blake3-Scope: prefix`** on appended objects means the digest is stale
   (covers only the pre-append prefix). Clients verifying the full body will mismatch — this
   is documented behaviour (`common/headers.py:33`), not a bug. Ensure the rewrite emits the
   scope so clients can distinguish.
