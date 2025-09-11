## Hippius S3 Compatibility Matrix

This document tracks AWS S3 API compatibility for Hippius S3.

Notes:

- This file lists features that are currently supported.
- Endpoints are path-style and compatible with standard S3 clients (AWS CLI, MinIO, boto3).

### All AWS S3 actions

| Action                                          | Supported | Notes                                                      | Endpoint(s)                                   | Test                                                                 |
| ----------------------------------------------- | --------- | ---------------------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------- |
| AbortMultipartUpload                            | ✔         | Aborts upload and cleans up                                | DELETE /{bucket}/{key}?uploadId=...           | test_AbortMultipartUpload.py                                         |
| CompleteMultipartUpload                         | ✔         | Computes combined ETag                                     | POST /{bucket}/{key}?uploadId=...             | test_CompleteMultipartUpload.py                                      |
| CopyObject                                      | ✔         | Via x-amz-copy-source header                               | PUT /{bucket}/{key}                           | test_CopyObject.py, test_CopyObject_CrossBucket.py                   |
| CreateBucket                                    | ✔         | Rejects x-amz-acl with InvalidBucketAclWithObjectOwnership | PUT /{bucket}                                 | test_CreateBucket.py                                                 |
| CreateBucketMetadataConfiguration               |           |                                                            |                                               |                                                                      |
| CreateBucketMetadataTableConfiguration          |           |                                                            |                                               |                                                                      |
| CreateMultipartUpload                           | ✔         | Initiate multipart upload                                  | POST /{bucket}/{key}?uploads                  | test_CreateMultipartUpload.py                                        |
| CreateSession                                   |           |                                                            |                                               |                                                                      |
| DeleteBucket                                    | ✔         | 404 if missing; 204 on success; 409 if non-empty           | DELETE /{bucket}                              | test_DeleteBucket.py                                                 |
| DeleteBucketAnalyticsConfiguration              |           |                                                            |                                               |                                                                      |
| DeleteBucketCors                                |           |                                                            |                                               |                                                                      |
| DeleteBucketEncryption                          |           |                                                            |                                               |                                                                      |
| DeleteBucketIntelligentTieringConfiguration     |           |                                                            |                                               |                                                                      |
| DeleteBucketInventoryConfiguration              |           |                                                            |                                               |                                                                      |
| DeleteBucketLifecycle                           |           |                                                            |                                               |                                                                      |
| DeleteBucketMetadataConfiguration               |           |                                                            |                                               |                                                                      |
| DeleteBucketMetadataTableConfiguration          |           |                                                            |                                               |                                                                      |
| DeleteBucketMetricsConfiguration                |           |                                                            |                                               |                                                                      |
| DeleteBucketOwnershipControls                   |           |                                                            |                                               |                                                                      |
| DeleteBucketPolicy                              |           |                                                            |                                               |                                                                      |
| DeleteBucketReplication                         |           |                                                            |                                               |                                                                      |
| DeleteBucketTagging                             | ✔         | Deletes all tags                                           | DELETE /{bucket}?tagging                      | test_BucketTagging.py                                                |
| DeleteBucketWebsite                             |           |                                                            |                                               |                                                                      |
| DeleteObject                                    | ✔         | Idempotent 204                                             | DELETE /{bucket}/{key}                        | test_DeleteObject.py                                                 |
| DeleteObjects                                   |           | Batch delete not supported                                 |                                               |                                                                      |
| DeleteObjectTagging                             | ✔         | Deletes all tags                                           | DELETE /{bucket}/{key}?tagging                | test_ObjectTagging.py                                                |
| DeletePublicAccessBlock                         |           |                                                            |                                               |                                                                      |
| GetBucketAccelerateConfiguration                |           |                                                            |                                               |                                                                      |
| GetBucketAcl                                    |           |                                                            |                                               |                                                                      |
| GetBucketAnalyticsConfiguration                 |           |                                                            |                                               |                                                                      |
| GetBucketCors                                   |           |                                                            |                                               |                                                                      |
| GetBucketEncryption                             |           |                                                            |                                               |                                                                      |
| GetBucketIntelligentTieringConfiguration        |           |                                                            |                                               |                                                                      |
| GetBucketInventoryConfiguration                 |           |                                                            |                                               |                                                                      |
| GetBucketLifecycle                              | ✔         | 404 NoSuchLifecycleConfiguration when not configured       | GET /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| GetBucketLifecycleConfiguration                 | ✔         | 404 NoSuchLifecycleConfiguration when not configured       | GET /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| GetBucketLocation                               | ✔         | Always returns us-east-1 XML                               | GET /{bucket}?location                        | test_GetBucketLocation.py                                            |
| GetBucketLogging                                |           |                                                            |                                               |                                                                      |
| GetBucketMetadataConfiguration                  |           |                                                            |                                               |                                                                      |
| GetBucketMetadataTableConfiguration             |           |                                                            |                                               |                                                                      |
| GetBucketMetricsConfiguration                   |           |                                                            |                                               |                                                                      |
| GetBucketNotification                           |           |                                                            |                                               |                                                                      |
| GetBucketNotificationConfiguration              |           |                                                            |                                               |                                                                      |
| GetBucketOwnershipControls                      |           |                                                            |                                               |                                                                      |
| GetBucketPolicy                                 | ✔         | Only for public buckets                                    | GET /{bucket}?policy                          | test_BucketPolicy.py                                                 |
| GetBucketPolicyStatus                           |           |                                                            |                                               |                                                                      |
| GetBucketReplication                            |           |                                                            |                                               |                                                                      |
| GetBucketRequestPayment                         |           |                                                            |                                               |                                                                      |
| GetBucketTagging                                | ✔         | XML response                                               | GET /{bucket}?tagging                         | test_BucketTagging.py                                                |
| GetBucketVersioning                             |           |                                                            |                                               |                                                                      |
| GetBucketWebsite                                |           |                                                            |                                               |                                                                      |
| GetObject                                       | ✔         | Supports Range; S3-like headers                            | GET /{bucket}/{key}                           | test_GetObject.py, test_GetObject_Range.py, test_GetObject_Errors.py |
| GetObjectAcl                                    |           |                                                            |                                               |                                                                      |
| GetObjectAttributes                             |           |                                                            |                                               |                                                                      |
| GetObjectLegalHold                              |           |                                                            |                                               |                                                                      |
| GetObjectLockConfiguration                      |           |                                                            |                                               |                                                                      |
| GetObjectRetention                              |           |                                                            |                                               |                                                                      |
| GetObjectTagging                                | ✔         | XML response                                               | GET /{bucket}/{key}?tagging                   | test_ObjectTagging.py                                                |
| GetObjectTorrent                                |           |                                                            |                                               |                                                                      |
| GetPublicAccessBlock                            |           |                                                            |                                               |                                                                      |
| HeadBucket                                      | ✔         | 200 if exists, 404 if not (empty body)                     | HEAD /{bucket}                                | test_CreateBucket.py                                                 |
| HeadObject                                      | ✔         | Returns metadata headers; pending status header            | HEAD /{bucket}/{key}                          | test_HeadObject.py, test_HeadObject_Pending.py                       |
| ListBucketAnalyticsConfigurations               |           |                                                            |                                               |                                                                      |
| ListBucketIntelligentTieringConfigurations      |           |                                                            |                                               |                                                                      |
| ListBucketInventoryConfigurations               |           |                                                            |                                               |                                                                      |
| ListBucketMetricsConfigurations                 |           |                                                            |                                               |                                                                      |
| ListBuckets                                     | ✔         | Lists buckets owned by the authenticated account           | GET /                                         | test_CreateBucket.py                                                 |
| ListDirectoryBuckets                            |           |                                                            |                                               |                                                                      |
| ListMultipartUploads                            | ✔         | Lists ongoing multipart uploads                            | GET /{bucket}?uploads                         | test_ListMultipartUploads.py                                         |
| ListObjects                                     | ✔         | Optional prefix filtering                                  | GET /{bucket}                                 | test_ListObjects.py                                                  |
| ListObjectsV2                                   |           |                                                            |                                               |                                                                      |
| ListObjectVersions                              |           |                                                            |                                               |                                                                      |
| ListParts                                       |           | TODO (skipped placeholder)                                 | GET /{bucket}/{key}?uploadId=...              | test_ListParts.py                                                    |
| PutBucketAccelerateConfiguration                |           |                                                            |                                               |                                                                      |
| PutBucketAcl                                    |           |                                                            |                                               |                                                                      |
| PutBucketAnalyticsConfiguration                 |           |                                                            |                                               |                                                                      |
| PutBucketCors                                   |           |                                                            |                                               |                                                                      |
| PutBucketEncryption                             |           |                                                            |                                               |                                                                      |
| PutBucketIntelligentTieringConfiguration        |           |                                                            |                                               |                                                                      |
| PutBucketInventoryConfiguration                 |           |                                                            |                                               |                                                                      |
| PutBucketLifecycle                              | ✔         | Accepts config; not persisted yet (ack only)               | PUT /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| PutBucketLifecycleConfiguration                 | ✔         | Accepts config; not persisted yet (ack only)               | PUT /{bucket}?lifecycle                       | test_BucketLifecycle.py                                              |
| PutBucketLogging                                |           |                                                            |                                               |                                                                      |
| PutBucketMetricsConfiguration                   |           |                                                            |                                               |                                                                      |
| PutBucketNotification                           |           |                                                            |                                               |                                                                      |
| PutBucketNotificationConfiguration              |           |                                                            |                                               |                                                                      |
| PutBucketOwnershipControls                      |           |                                                            |                                               |                                                                      |
| PutBucketPolicy                                 | ✔         | Public-read helper only                                    | PUT /{bucket}?policy                          | test_BucketPolicy.py                                                 |
| PutBucketReplication                            |           |                                                            |                                               |                                                                      |
| PutBucketRequestPayment                         |           |                                                            |                                               |                                                                      |
| PutBucketTagging                                | ✔         | XML request                                                | PUT /{bucket}?tagging                         | test_BucketTagging.py                                                |
| PutBucketVersioning                             |           |                                                            |                                               |                                                                      |
| PutBucketWebsite                                |           |                                                            |                                               |                                                                      |
| PutObject                                       | ✔         | MD5 as ETag; x-amz-meta-\*                                 | PUT /{bucket}/{key}                           | test_PutObject.py, test_PutObject_Metadata.py                        |
| PutObjectAcl                                    |           |                                                            |                                               |                                                                      |
| PutObjectLegalHold                              |           |                                                            |                                               |                                                                      |
| PutObjectLockConfiguration                      |           |                                                            |                                               |                                                                      |
| PutObjectRetention                              |           |                                                            |                                               |                                                                      |
| PutObjectTagging                                | ✔         | XML request                                                | PUT /{bucket}/{key}?tagging                   | test_ObjectTagging.py                                                |
| PutPublicAccessBlock                            |           |                                                            |                                               |                                                                      |
| RenameObject                                    |           |                                                            |                                               |                                                                      |
| RestoreObject                                   |           |                                                            |                                               |                                                                      |
| SelectObjectContent                             |           |                                                            |                                               |                                                                      |
| UpdateBucketMetadataInventoryTableConfiguration |           |                                                            |                                               |                                                                      |
| UpdateBucketMetadataJournalTableConfiguration   |           |                                                            |                                               |                                                                      |
| UploadPart                                      | ✔         | Returns part ETag                                          | PUT /{bucket}/{key}?uploadId=...&partNumber=N | test_UploadPart.py                                                   |
| UploadPartCopy                                  |           |                                                            |                                               |                                                                      |
| WriteGetObjectResponse                          |           |                                                            |                                               |                                                                      |

### Supported

- **Authentication**

  - HMAC SigV4 via custom credentials (seed phrase based)
  - Path-style addressing

- **Bucket operations**

  - `GET /` — List buckets (returns only buckets owned by the authenticated account)
  - `PUT /{bucket}` — Create bucket
  - `HEAD /{bucket}` — Head bucket (200 if exists, 404 if not)
  - `DELETE /{bucket}` — Delete bucket (requires ownership; returns 404 if missing)
  - `GET /{bucket}?location` — Get bucket location (returns `us-east-1` XML)
  - Bucket ACL (create-time): `x-amz-acl` supports `private`, `public-read`, `public-read-write` → maps to `is_public`

- **Bucket tagging**

  - `GET /{bucket}?tagging` — Retrieve bucket tags (XML)
  - `PUT /{bucket}?tagging` — Set/replace bucket tags (XML)
  - `DELETE /{bucket}?tagging` — Delete all bucket tags

- **Bucket policy (public-read helper)**

  - `PUT /{bucket}?policy` — Accepts a standard public-read JSON policy; marks bucket public
  - `GET /{bucket}?policy` — Returns policy JSON for public buckets; 404 `NoSuchBucketPolicy` for private buckets

Notes:

- Block Public Access (Bucket/Account PublicAccessBlock) is not currently supported. As a result, public policies are accepted by default. A future version will add `PutPublicAccessBlock`/`GetPublicAccessBlock` and enforcement; tests are prepared to re-enable assertions when implemented.

- **Bucket lifecycle (minimal support)**

  - `GET /{bucket}?lifecycle` — Returns a minimal default lifecycle XML (placeholder)
  - `PUT /{bucket}?lifecycle` — Accepts lifecycle XML (acknowledged; not persisted yet)

- **Object operations (simple uploads)**

  - `PUT /{bucket}/{key}` — Upload object (stores metadata, content type, MD5 as ETag)
  - `GET /{bucket}/{key}` — Download object (supports Range requests; returns S3-like headers)
  - `HEAD /{bucket}/{key}` — Object metadata (size, content type, ETag, Last-Modified)
  - `x-amz-object-status` on HEAD:
    - `pending` — object accepted, not yet published to IPFS
    - `pinning` — publishing/pinning in progress
    - `available` — content published and retrievable
    - `unknown` — status not determined (header omitted)
  - `DELETE /{bucket}/{key}` — Delete object (idempotent 204)
  - User metadata: `x-amz-meta-*` stored and returned on HEAD/GET
  - ETag: MD5 of content for simple uploads (quoted in responses)

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

- **Object listing (within bucket)**

  - `GET /{bucket}` — List objects (optional `prefix` filtering)
  - Returns standard XML with `Contents` entries; includes custom summary headers

- **Multipart uploads**

  - `POST /{bucket}/{key}?uploads` — Initiate multipart upload (returns `UploadId`)
  - `PUT /{bucket}/{key}?uploadId=...&partNumber=N` — Upload part (returns part ETag)
  - `POST /{bucket}/{key}?uploadId=...` — Complete multipart upload (computes combined ETag, enqueues publish)
  - `DELETE /{bucket}/{key}?uploadId=...` — Abort multipart upload
  - `GET /{bucket}?uploads` — List ongoing multipart uploads

- **Error responses**

  - S3-like XML error payloads and status codes for common cases (NoSuchBucket, NoSuchKey, BucketAlreadyExists, etc.)
  - HEAD variants return appropriate status codes without XML bodies where required

- **Headers returned**
  - `Content-Type`, `Content-Length`, `ETag` (quoted), `Last-Modified`
  - `x-amz-ipfs-cid` with CID or `pending`
  - `Accept-Ranges: bytes` on GETs
