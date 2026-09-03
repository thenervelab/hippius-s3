## Hippius S3 Compatibility Matrix

This document tracks AWS S3 API compatibility for Hippius S3.

Notes:

- This file lists features that are currently supported.
- Endpoints are path-style and compatible with standard S3 clients (AWS CLI, MinIO, boto3).

### All AWS S3 actions

| Action                                          | Supported | Notes                                                       | Endpoint(s)                                   | Test                                                                 |
| ----------------------------------------------- | --------- | ----------------------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------- |
| AbortMultipartUpload                            | ✔         | Aborts upload and cleans up                                 | DELETE /{bucket}/{key}?uploadId=...           | test_AbortMultipartUpload.py                                         |
| CompleteMultipartUpload                         | ✔         | Computes combined ETag; 501 on x-amz-object-lock-\* (set at initiate) | POST /{bucket}/{key}?uploadId=...     | test_CompleteMultipartUpload.py, test_ObjectLockCopyAndMpu.py         |
| CopyObject                                      | ✔         | Via x-amz-copy-source; honours lock headers + dest bucket default | PUT /{bucket}/{key}                     | test_CopyObject.py, test_CopyObject_CrossBucket.py, test_ObjectLockCopyAndMpu.py |
| CreateBucket                                    | ✔         | Rejects x-amz-acl with InvalidBucketAclWithObjectOwnership; honours x-amz-bucket-object-lock-enabled | PUT /{bucket}                  | test_CreateBucket.py, test_BucketObjectLock.py                       |
| CreateBucketMetadataConfiguration               |           |                                                             |                                               |                                                                      |
| CreateBucketMetadataTableConfiguration          |           |                                                             |                                               |                                                                      |
| CreateMultipartUpload                           | ✔         | Honours x-amz-object-lock-\*; lock is fixed on the reserved version | POST /{bucket}/{key}?uploads          | test_CreateMultipartUpload.py, test_ObjectLockCopyAndMpu.py           |
| CreateSession                                   |           |                                                             |                                               |                                                                      |
| DeleteBucket                                    | ✔         | 404 if missing; 204 on success; 409 if non-empty            | DELETE /{bucket}                              | test_DeleteBucket.py                                                 |
| DeleteBucketAnalyticsConfiguration              |           |                                                             |                                               |                                                                      |
| DeleteBucketCors                                |           |                                                             |                                               |                                                                      |
| DeleteBucketEncryption                          |           |                                                             |                                               |                                                                      |
| DeleteBucketIntelligentTieringConfiguration     |           |                                                             |                                               |                                                                      |
| DeleteBucketInventoryConfiguration              |           |                                                             |                                               |                                                                      |
| DeleteBucketLifecycle                           |           |                                                             |                                               |                                                                      |
| DeleteBucketMetadataConfiguration               |           |                                                             |                                               |                                                                      |
| DeleteBucketMetadataTableConfiguration          |           |                                                             |                                               |                                                                      |
| DeleteBucketMetricsConfiguration                |           |                                                             |                                               |                                                                      |
| DeleteBucketOwnershipControls                   |           |                                                             |                                               |                                                                      |
| DeleteBucketPolicy                              |           |                                                             |                                               |                                                                      |
| DeleteBucketReplication                         |           |                                                             |                                               |                                                                      |
| DeleteBucketTagging                             | ✔         | Deletes all tags                                            | DELETE /{bucket}?tagging                      | test_BucketTagging.py                                                |
| DeleteBucketWebsite                             |           |                                                             |                                               |                                                                      |
| DeleteObject                                    | ✔         | Idempotent 204                                              | DELETE /{bucket}/{key}                        | test_DeleteObject.py                                                 |
| DeleteObjects                                   | ✔         | Batch delete via XML; idempotent; supports Quiet            | POST /{bucket}?delete                         | test_DeleteObjects.py                                                |
| DeleteObjectTagging                             | ✔         | Deletes all tags                                            | DELETE /{bucket}/{key}?tagging                | test_ObjectTagging.py                                                |
| DeletePublicAccessBlock                         |           |                                                             |                                               |                                                                      |
| GetBucketAccelerateConfiguration                |           |                                                             |                                               |                                                                      |
| GetBucketAcl                                    | ✔         | ACL XML response via gateway                                | GET /{bucket}?acl                             | test_acl_access_keys_minio.py                                        |
| GetBucketAnalyticsConfiguration                 |           |                                                             |                                               |                                                                      |
| GetBucketCors                                   |           |                                                             |                                               |                                                                      |
| GetBucketEncryption                             |           |                                                             |                                               |                                                                      |
| GetBucketIntelligentTieringConfiguration        |           |                                                             |                                               |                                                                      |
| GetBucketInventoryConfiguration                 |           |                                                             |                                               |                                                                      |
| GetBucketLifecycle                              | ✔         | 404 NoSuchLifecycleConfiguration when not configured        | GET /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| GetBucketLifecycleConfiguration                 | ✔         | 404 NoSuchLifecycleConfiguration when not configured        | GET /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| GetBucketLocation                               | ✔         | Always returns us-east-1 XML                                | GET /{bucket}?location                        | test_GetBucketLocation.py                                            |
| GetBucketLogging                                |           |                                                             |                                               |                                                                      |
| GetBucketMetadataConfiguration                  |           |                                                             |                                               |                                                                      |
| GetBucketMetadataTableConfiguration             |           |                                                             |                                               |                                                                      |
| GetBucketMetricsConfiguration                   |           |                                                             |                                               |                                                                      |
| GetBucketNotification                           |           |                                                             |                                               |                                                                      |
| GetBucketNotificationConfiguration              |           |                                                             |                                               |                                                                      |
| GetBucketOwnershipControls                      |           |                                                             |                                               |                                                                      |
| GetBucketPolicy                                 | ✔         | Only for public buckets                                     | GET /{bucket}?policy                          | test_BucketPolicy.py                                                 |
| GetBucketPolicyStatus                           |           |                                                             |                                               |                                                                      |
| GetBucketReplication                            |           |                                                             |                                               |                                                                      |
| GetBucketRequestPayment                         |           |                                                             |                                               |                                                                      |
| GetBucketTagging                                | ✔         | XML response                                                | GET /{bucket}?tagging                         | test_BucketTagging.py                                                |
| GetBucketVersioning                             | ✔         | Returns Status=Enabled; omits Status when never enabled     | GET /{bucket}?versioning                      | test_BucketVersioning.py                                             |
| GetBucketWebsite                                |           |                                                             |                                               |                                                                      |
| GetObject                                       | ✔         | Range; If-None-Match (304); does NOT echo x-amz-object-lock-\* (use HEAD) | GET /{bucket}/{key}             | test_GetObject.py, test_GetObject_Range.py, test_GetObject_Errors.py |
| GetObjectAcl                                    | ✔         | ACL XML response via gateway                                | GET /{bucket}/{key}?acl                       | test_acl_access_keys_minio.py                                        |
| GetObjectAttributes                             |           |                                                             |                                               |                                                                      |
| GetObjectLegalHold                              | ✔         | Per-version hold status; independent of retention           | GET /{bucket}/{key}?legal-hold                | test_ObjectLegalHold.py                                              |
| GetObjectLockConfiguration                      | ✔         | Returns persisted config; 404 ObjectLockConfigurationNotFoundError when unset | GET /{bucket}?object-lock      | test_BucketObjectLock.py                                             |
| GetObjectRetention                              | ✔         | Per-version mode + retain-until                             | GET /{bucket}/{key}?retention                 | test_ObjectRetention.py                                              |
| GetObjectTagging                                | ✔         | XML response                                                | GET /{bucket}/{key}?tagging                   | test_ObjectTagging.py                                                |
| GetObjectTorrent                                |           |                                                             |                                               |                                                                      |
| GetPublicAccessBlock                            |           |                                                             |                                               |                                                                      |
| HeadBucket                                      | ✔         | 200 if exists, 404 if not (empty body)                      | HEAD /{bucket}                                | test_CreateBucket.py                                                 |
| HeadObject                                      | ✔         | Metadata headers; If-None-Match (304); echoes x-amz-object-lock-\* | HEAD /{bucket}/{key}                     | test_HeadObject.py, test_HeadObject_Pending.py                       |
| ListBucketAnalyticsConfigurations               |           |                                                             |                                               |                                                                      |
| ListBucketIntelligentTieringConfigurations      |           |                                                             |                                               |                                                                      |
| ListBucketInventoryConfigurations               |           |                                                             |                                               |                                                                      |
| ListBucketMetricsConfigurations                 |           |                                                             |                                               |                                                                      |
| ListBuckets                                     | ✔         | Lists buckets owned by the authenticated account            | GET /                                         | test_CreateBucket.py                                                 |
| ListDirectoryBuckets                            |           |                                                             |                                               |                                                                      |
| ListMultipartUploads                            | ✔         | Lists ongoing multipart uploads                             | GET /{bucket}?uploads                         | test_ListMultipartUploads.py                                         |
| ListObjects                                     | ✔         | Optional prefix filtering                                   | GET /{bucket}                                 | test_ListObjects.py                                                  |
| ListObjectsV2                                   | ✔         | Prefix, Delimiter/CommonPrefixes, ContinuationToken/NextContinuationToken, StartAfter, and real IsTruncated | GET /{bucket}                                 | test_ListObjects.py                                                  |
| ListObjectVersions                              | ✔         | Versions + DeleteMarkers; prefix/delimiter/key-marker paging| GET /{bucket}?versions                        | test_BucketVersioning.py                                             |
| ListParts                                       | ✔         | Lists parts; supports pagination                            | GET /{bucket}/{key}?uploadId=...              | test_ListParts.py                                                    |
| PutBucketAccelerateConfiguration                |           |                                                             |                                               |                                                                      |
| PutBucketAcl                                    | ✔         | Supports canned ACLs, grant headers, or ACL XML             | PUT /{bucket}?acl                             | test_acl_access_keys_minio.py                                        |
| PutBucketAnalyticsConfiguration                 |           |                                                             |                                               |                                                                      |
| PutBucketCors                                   |           |                                                             |                                               |                                                                      |
| PutBucketEncryption                             |           |                                                             |                                               |                                                                      |
| PutBucketIntelligentTieringConfiguration        |           |                                                             |                                               |                                                                      |
| PutBucketInventoryConfiguration                 |           |                                                             |                                               |                                                                      |
| PutBucketLifecycle                              | ✔         | Accepts config; not persisted yet (ack only)                | PUT /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| PutBucketLifecycleConfiguration                 | ✔         | Accepts config; not persisted yet (ack only)                | PUT /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| PutBucketLogging                                |           |                                                             |                                               |                                                                      |
| PutBucketMetricsConfiguration                   |           |                                                             |                                               |                                                                      |
| PutBucketNotification                           |           |                                                             |                                               |                                                                      |
| PutBucketNotificationConfiguration              |           |                                                             |                                               |                                                                      |
| PutBucketOwnershipControls                      |           |                                                             |                                               |                                                                      |
| PutBucketPolicy                                 | ✔         | Public-read helper only                                     | PUT /{bucket}?policy                          | test_BucketPolicy.py                                                 |
| PutBucketReplication                            |           |                                                             |                                               |                                                                      |
| PutBucketRequestPayment                         |           |                                                             |                                               |                                                                      |
| PutBucketTagging                                | ✔         | XML request                                                 | PUT /{bucket}?tagging                         | test_BucketTagging.py                                                |
| PutBucketVersioning                             | ✔         | Status=Enabled only; Suspended returns 501                  | PUT /{bucket}?versioning                      | test_BucketVersioning.py                                             |
| PutBucketWebsite                                |           |                                                             |                                               |                                                                      |
| PutObject                                       | ✔         | MD5 as ETag; x-amz-meta-\*; honours x-amz-object-lock-\*     | PUT /{bucket}/{key}                           | test_PutObject.py, test_PutObject_Metadata.py, test_ObjectRetention.py |
| PutObjectAcl                                    | ✔         | Supports canned ACLs, grant headers, or ACL XML             | PUT /{bucket}/{key}?acl                       | test_acl_access_keys_minio.py                                        |
| PutObjectLegalHold                              | ✔         | ON/OFF; 400 InvalidRequest if the bucket has no lock config | PUT /{bucket}/{key}?legal-hold                | test_ObjectLegalHold.py                                              |
| PutObjectLockConfiguration                      | ✔         | Default retention (Days or Years); ENFORCED. 409 InvalidBucketState if unversioned | PUT /{bucket}?object-lock  | test_BucketObjectLock.py                                             |
| PutObjectRetention                              | ✔         | Extend allowed; shorten/clear refused 403 while live        | PUT /{bucket}/{key}?retention                 | test_ObjectRetention.py                                              |
| PutObjectTagging                                | ✔         | XML request                                                 | PUT /{bucket}/{key}?tagging                   | test_ObjectTagging.py                                                |
| PutPublicAccessBlock                            |           |                                                             |                                               |                                                                      |
| RenameObject                                    |           |                                                             |                                               |                                                                      |
| RestoreObject                                   |           |                                                             |                                               |                                                                      |
| SelectObjectContent                             |           |                                                             |                                               |                                                                      |
| UpdateBucketMetadataInventoryTableConfiguration |           |                                                             |                                               |                                                                      |
| UpdateBucketMetadataJournalTableConfiguration   |           |                                                             |                                               |                                                                      |
| UploadPart                                      | ✔         | Returns part ETag                                           | PUT /{bucket}/{key}?uploadId=...&partNumber=N | test_UploadPart.py                                                   |
| UploadPartCopy                                  | ✔         | Supports cross-bucket and encrypted sources                 | PUT /{bucket}/{key}?uploadId=...&partNumber=N | test_UploadPartCopy.py                                               |
| WriteGetObjectResponse                          |           |                                                             |                                               |                                                                      |

### Supported

- **Authentication**

  - HMAC SigV4 via custom credentials (seed phrase based)
  - HMAC SigV4 via Hippius access keys (`hip_*`) in `Authorization` header credentials
  - SigV4 presigned URLs for `hip_*` access keys (`X-Amz-*` query authentication)
  - `Authorization: Bearer hip_*` access key support (Hippius extension)
  - Anonymous `GET`/`HEAD` allowed for non-root paths (authorization enforced by ACLs)
  - Path-style addressing

- **Bucket operations**

  - `GET /` — List buckets (returns only buckets owned by the authenticated account)
  - `PUT /{bucket}` — Create bucket
  - `HEAD /{bucket}` — Head bucket (200 if exists, 404 if not)
  - `DELETE /{bucket}` — Delete bucket (requires ownership; returns 404 if missing)
  - `GET /{bucket}?location` — Get bucket location (returns `us-east-1` XML)
  - CreateBucket rejects `x-amz-acl` / `ACL=...` with `InvalidBucketAclWithObjectOwnership` (BucketOwnerEnforced semantics)
  - ACL APIs: `GET/PUT /{bucket}?acl` (XML)

- **Bucket tagging**

  - `GET /{bucket}?tagging` — Retrieve bucket tags (XML)
  - `PUT /{bucket}?tagging` — Set/replace bucket tags (XML)
  - `DELETE /{bucket}?tagging` — Delete all bucket tags

- **Bucket policy (public-read helper)**

  - `PUT /{bucket}?policy` — Accepts a standard public-read JSON policy; marks bucket public
  - `GET /{bucket}?policy` — Returns policy JSON for public buckets; 404 `NoSuchBucketPolicy` for private buckets

Notes:

- **Bucket lifecycle (minimal support)**

  - `GET /{bucket}?lifecycle` — Returns 404 NoSuchLifecycleConfiguration (lifecycle not persisted)
  - `PUT /{bucket}?lifecycle` — Accepts lifecycle XML (acknowledged; not persisted)

- **Object operations (simple uploads)**

  - `PUT /{bucket}/{key}` — Upload object (stores metadata, content type, MD5 as ETag)
  - `GET /{bucket}/{key}` — Download object (supports Range requests; returns S3-like headers)
  - `HEAD /{bucket}/{key}` — Object metadata (size, content type, ETag, Last-Modified)
  - `DELETE /{bucket}/{key}` — Delete object (idempotent 204)
  - `POST /{bucket}?delete` — Delete multiple objects (XML body). Idempotent; supports `Quiet`. Response includes `Deleted` and optional `Errors`.
  - User metadata: `x-amz-meta-*` stored and returned on HEAD/GET
  - ETag: MD5 of content for simple uploads (quoted in responses)
  - Object ACL APIs: `GET/PUT /{bucket}/{key}?acl` (XML)
  - Create/update object ACL during upload: `PUT /{bucket}/{key}` with `x-amz-acl: ...`

- **Object copying**

  - `PUT /{bucket}/{key}` with header `x-amz-copy-source=/{srcBucket}/{srcKey}` — Copy object
    - Fast path (same bucket and encryption context)
    - Re-encrypt path (cross-bucket or different public/private state)
    - Returns XML `CopyObjectResult` and sets `ETag` header

- **Object tagging**

  - `GET /{bucket}/{key}?tagging` — Retrieve object tags (XML)
  - `PUT /{bucket}/{key}?tagging` — Set/replace object tags (XML)
  - `DELETE /{bucket}/{key}?tagging` — Delete all object tags
  - `HEAD /{bucket}/{key}?tagging` — Existence check (200/404)

- **Range requests**

  - `GET /{bucket}/{key}` with `Range: bytes=...` — 206 Partial Content with `Content-Range`, `Accept-Ranges: bytes`
  - Validates and returns 416 with `Content-Range: bytes */{size}` for invalid ranges

- **Conditional reads**

  - `GET`/`HEAD` support `If-None-Match` — returns `304 Not Modified` when the client's ETag matches the current object (`get_object_endpoint.py:218`, `head_object_endpoint.py:205`, `common/headers.py:53`)
  - `If-Match` and conditional writes (`If-None-Match` on PUT) are not yet supported

### Hippius extensions

- **Append semantics (non-S3 extension)**

  - `PUT /{bucket}/{key}` with metadata controls:
    - `x-amz-meta-append: true` — enable append mode
    - `x-amz-meta-append-if-version: <N>` — CAS guard; current version obtained from `HEAD` response header `x-amz-meta-append-version`
    - `x-amz-meta-append-id: <id>` — optional idempotency key to deduplicate retries
  - Responses and errors:
    - Success appends return 200 and advance `x-amz-meta-append-version`
    - Stale version returns 412 `PreconditionFailed`
    - Missing key returns 404 `NoSuchKey`
  - Works with Range GETs across append boundaries
  - Tests: `test_AppendObject.py`

- **Object listing (within bucket)**

  - `GET /{bucket}` — List objects (optional `prefix` filtering)
  - Returns standard XML with `Contents` entries; includes custom summary headers
  - `ListObjectsV2` compatibility: supports `Prefix`, `Delimiter`/`CommonPrefixes`, v2 pagination (`ContinuationToken`/`NextContinuationToken`), `StartAfter`, and a real `IsTruncated` (`hippius_s3/api/s3/buckets/list_objects_endpoint.py`)

- **Multipart uploads**

  - `POST /{bucket}/{key}?uploads` — Initiate multipart upload (returns `UploadId`)
  - `PUT /{bucket}/{key}?uploadId=...&partNumber=N` — Upload part (returns part ETag)
  - `POST /{bucket}/{key}?uploadId=...` — Complete multipart upload (computes combined ETag, enqueues publish)
  - `DELETE /{bucket}/{key}?uploadId=...` — Abort multipart upload
  - `GET /{bucket}?uploads` — List ongoing multipart uploads
  - `GET /{bucket}/{key}?uploadId=...` — List parts (supports `max-parts` and `part-number-marker` pagination)

- **Error responses**

  - S3-like XML error payloads and status codes for common cases (NoSuchBucket, NoSuchKey, BucketAlreadyExists, etc.)
  - HEAD variants return appropriate status codes without XML bodies where required

- **Headers returned**
  - `Content-Type`, `Content-Length`, `ETag` (quoted), `Last-Modified`
  - `Accept-Ranges: bytes` on GETs
  - `x-amz-version-id`: object version number
  - `x-hippius-source`: `cache` or `pipeline` (diagnostic; indicates serving source)
  - `X-Hippius-Arion-File-Hash`: Arion backend file hash or `pending`
  - `x-amz-meta-append-version`: current append version (when applicable)

## Object Lock (WORM)

Fully enforced as of 2026-09-02. The behaviours below were verified against production, not
inferred from the code — the probe output is in the release notes for that train.

**Enforcement is below the API.** The predicate that decides "is this version locked" is embedded in
`get_chunk_backend_identifiers.sql` (the single chokepoint every backend deletion flows through),
`find_objects_ready_for_hard_delete.sql` and `find_versions_ready_for_reap.sql`. It therefore holds
with no API code running, including for the ops scripts (`nuke_user`, `purge_buckets`,
`delete_legacy_object_versions`). Gating in the handlers alone would have left those able to walk
straight past it. `hippius_s3/api/s3/object_lock_enforcement.py` is the Python mirror, and a test
asserts the two agree.

Deliberately NOT gated: the FS/Ceph cache janitor. Evicting a cached chunk removes a copy, not the
object, and pinning locked objects in NVMe would fill the cache for no durability gain.

### Semantics

| Rule | Behaviour |
|---|---|
| Versioning | Hard prerequisite. `CreateBucket --object-lock-enabled-for-bucket` enables it implicitly; `PUT ?object-lock` on an unversioned bucket is `409 InvalidBucketState` |
| Retention vs legal hold | Independent. Either alone locks the version; an expired retention with a live hold is still locked |
| `DELETE ?versionId` on a locked version | `403 AccessDenied` |
| `DELETE` with no versionId | Succeeds, writes a delete marker; the locked version survives beneath it |
| `DeleteObjects` | Per key. The locked key returns `AccessDenied` under `<Error>`; its neighbours delete normally |
| Overwrite of a locked key | Allowed — creates a new version |
| COMPLIANCE | Cannot be shortened, cleared, mode-changed or bypassed by anyone. Extension only |
| GOVERNANCE bypass | Bucket owner AND `x-amz-bypass-governance-retention: true`. Both halves required |
| Lock headers on a non-opted bucket | `400 InvalidRequest` — refused, never silently dropped |
| Retention cap | `HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS`, default 3650 |

### Divergences from AWS

- **Bypass permission is bucket-owner-only.** AWS gates it on the `s3:BypassGovernanceRetention` IAM
  action. There is no IAM here, and the nearest ACL verb (`WRITE_ACP`) is grantable to another
  account — accepting it would mean an owner who delegates ACL administration has also delegated
  "may destroy retained data", which is exactly the authority Object Lock exists to withhold.
  Owner-only is the conservative reading and can be widened later; the reverse cannot.
- **`x-amz-bucket-object-lock-token` is not required** to enable Object Lock on an existing bucket.
  Versioning must already be on. We are more permissive than AWS here, not less.
- **`GetObject` does not echo `x-amz-object-lock-*`.** `HeadObject` does. AWS returns them on both.
- **`CompleteMultipartUpload` returns 501 on lock headers.** The lock is fixed at
  `CreateMultipartUpload`, onto the version reserved there, which is the version completion
  finalises. Accepting the headers at completion would mean taking intent that cannot be applied.

### Not implemented

S3 Batch Operations (no retroactive holds), replication of lock state, `POST Object`, and
suspending versioning on a lock-enabled bucket (`501`, as in AWS).

## Known divergences from AWS S3

- **Object keys may not contain `?` or `#`.** AWS permits both. Requests naming such a key are
  refused with `400 InvalidURI` rather than accepted.

  This is a deliberate divergence, not an omission. The gateway forwards by interpolating the
  decoded request path into a URL string, which the HTTP client then re-parses — and both
  characters are delimiters there. A request for `report?v1.txt` was therefore already being
  truncated at the `?` before it reached the API: a GET returned the object named `report`, and a
  PUT wrote to it. Two keys differing only after the delimiter collapsed onto one object, with a
  200 on both and nothing in the logs.

  So the practical change is the error, not the access: these keys were never addressable over
  the S3 path. `CopyObject` (which takes the source from the `x-amz-copy-source` header) and
  batch `DeleteObjects` (which takes keys from the request body) do not go through path parsing
  and still reach such an object if one exists.

  `hippius_s3/scripts/report_delimiter_keys.py` reports any live keys containing either
  character.
