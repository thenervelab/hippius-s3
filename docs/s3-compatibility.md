## Hippius S3 Compatibility Matrix

This document tracks AWS S3 API compatibility for Hippius S3.

Notes:

- This file lists features that are currently supported.
- Endpoints are path-style and compatible with standard S3 clients (AWS CLI, MinIO, boto3).

### All AWS S3 actions

| Action                                          | Supported | Notes                                                     | Endpoint(s)                                   | Test                 |
| ----------------------------------------------- | --------- | --------------------------------------------------------- | --------------------------------------------- | -------------------- |
| AbortMultipartUpload                            | ✔         | Aborts upload and cleans up                               | DELETE /{bucket}/{key}?uploadId=...           |
| CompleteMultipartUpload                         | ✔         | Computes combined ETag                                    | POST /{bucket}/{key}?uploadId=...             |
| CopyObject                                      | ✔         | Via x-amz-copy-source header                              | PUT /{bucket}/{key}                           |
| CreateBucket                                    | ✔         | Supports x-amz-acl: private/public-read/public-read-write | PUT /{bucket}                                 |
| CreateBucketMetadataConfiguration               |           |                                                           |                                               |
| CreateBucketMetadataTableConfiguration          |           |                                                           |                                               |
| CreateMultipartUpload                           | ✔         | Initiate multipart upload                                 | POST /{bucket}/{key}?uploads                  |
| CreateSession                                   |           |                                                           |                                               |
| DeleteBucket                                    | ✔         | 404 if missing; 204 on success                            | DELETE /{bucket}                              |
| DeleteBucketAnalyticsConfiguration              |           |                                                           |                                               |
| DeleteBucketCors                                |           |                                                           |                                               |
| DeleteBucketEncryption                          |           |                                                           |                                               |
| DeleteBucketIntelligentTieringConfiguration     |           |                                                           |                                               |
| DeleteBucketInventoryConfiguration              |           |                                                           |                                               |
| DeleteBucketLifecycle                           |           |                                                           |                                               |
| DeleteBucketMetadataConfiguration               |           |                                                           |                                               |
| DeleteBucketMetadataTableConfiguration          |           |                                                           |                                               |
| DeleteBucketMetricsConfiguration                |           |                                                           |                                               |
| DeleteBucketOwnershipControls                   |           |                                                           |                                               |
| DeleteBucketPolicy                              |           |                                                           |                                               |
| DeleteBucketReplication                         |           |                                                           |                                               |
| DeleteBucketTagging                             | ✔         | Deletes all tags                                          | DELETE /{bucket}?tagging                      |
| DeleteBucketWebsite                             |           |                                                           |                                               |
| DeleteObject                                    | ✔         | Idempotent 204                                            | DELETE /{bucket}/{key}                        | test_DeleteObject.py |
| DeleteObjects                                   |           | Batch delete not supported                                |                                               |
| DeleteObjectTagging                             | ✔         | Deletes all tags                                          | DELETE /{bucket}/{key}?tagging                |
| DeletePublicAccessBlock                         |           |                                                           |                                               |
| GetBucketAccelerateConfiguration                |           |                                                           |                                               |
| GetBucketAcl                                    |           |                                                           |                                               |
| GetBucketAnalyticsConfiguration                 |           |                                                           |                                               |
| GetBucketCors                                   |           |                                                           |                                               |
| GetBucketEncryption                             |           |                                                           |                                               |
| GetBucketIntelligentTieringConfiguration        |           |                                                           |                                               |
| GetBucketInventoryConfiguration                 |           |                                                           |                                               |
| GetBucketLifecycle                              | ✔         | Minimal default lifecycle XML                             | GET /{bucket}?lifecycle                       |                      |
| GetBucketLifecycleConfiguration                 | ✔         | Same as above (compat)                                    | GET /{bucket}?lifecycle                       |                      |
| GetBucketLocation                               | ✔         | Always returns us-east-1 XML                              | GET /{bucket}?location                        |                      |
| GetBucketLogging                                |           |                                                           |                                               |
| GetBucketMetadataConfiguration                  |           |                                                           |                                               |
| GetBucketMetadataTableConfiguration             |           |                                                           |                                               |
| GetBucketMetricsConfiguration                   |           |                                                           |                                               |
| GetBucketNotification                           |           |                                                           |                                               |
| GetBucketNotificationConfiguration              |           |                                                           |                                               |
| GetBucketOwnershipControls                      |           |                                                           |                                               |
| GetBucketPolicy                                 | ✔         | Only for public buckets                                   | GET /{bucket}?policy                          |
| GetBucketPolicyStatus                           |           |                                                           |                                               |
| GetBucketReplication                            |           |                                                           |                                               |
| GetBucketRequestPayment                         |           |                                                           |                                               |
| GetBucketTagging                                | ✔         | XML response                                              | GET /{bucket}?tagging                         |
| GetBucketVersioning                             |           |                                                           |                                               |
| GetBucketWebsite                                |           |                                                           |                                               |
| GetObject                                       | ✔         | Supports Range; S3-like headers                           | GET /{bucket}/{key}                           | test_GetObject.py    |
| GetObjectAcl                                    |           |                                                           |                                               |
| GetObjectAttributes                             |           |                                                           |                                               |
| GetObjectLegalHold                              |           |                                                           |                                               |
| GetObjectLockConfiguration                      |           |                                                           |                                               |
| GetObjectRetention                              |           |                                                           |                                               |
| GetObjectTagging                                | ✔         | XML response                                              | GET /{bucket}/{key}?tagging                   |
| GetObjectTorrent                                |           |                                                           |                                               |
| GetPublicAccessBlock                            |           |                                                           |                                               |
| HeadBucket                                      | ✔         | 200 if exists, 404 if not (empty body)                    | HEAD /{bucket}                                | test_CreateBucket.py |
| HeadObject                                      | ✔         | Returns metadata headers                                  | HEAD /{bucket}/{key}                          | test_HeadObject.py   |
| ListBucketAnalyticsConfigurations               |           |                                                           |                                               |
| ListBucketIntelligentTieringConfigurations      |           |                                                           |                                               |
| ListBucketInventoryConfigurations               |           |                                                           |                                               |
| ListBucketMetricsConfigurations                 |           |                                                           |                                               |
| ListBuckets                                     | ✔         | Lists buckets owned by the authenticated account          | GET /                                         | test_CreateBucket.py |
| ListDirectoryBuckets                            |           |                                                           |                                               |
| ListMultipartUploads                            | ✔         | Lists ongoing multipart uploads                           | GET /{bucket}?uploads                         |
| ListObjects                                     | ✔         | Optional prefix filtering                                 | GET /{bucket}                                 | test_ListObjects.py  |
| ListObjectsV2                                   |           |                                                           |                                               |
| ListObjectVersions                              |           |                                                           |                                               |
| ListParts                                       |           |                                                           |                                               |
| PutBucketAccelerateConfiguration                |           |                                                           |                                               |
| PutBucketAcl                                    |           |                                                           |                                               |
| PutBucketAnalyticsConfiguration                 |           |                                                           |                                               |
| PutBucketCors                                   |           |                                                           |                                               |
| PutBucketEncryption                             |           |                                                           |                                               |
| PutBucketIntelligentTieringConfiguration        |           |                                                           |                                               |
| PutBucketInventoryConfiguration                 |           |                                                           |                                               |
| PutBucketLifecycle                              | ✔         | Accepts XML, not persisted (ack only)                     | PUT /{bucket}?lifecycle                       |
| PutBucketLifecycleConfiguration                 | ✔         | Accepts XML, not persisted (ack only)                     | PUT /{bucket}?lifecycle                       |
| PutBucketLogging                                |           |                                                           |                                               |
| PutBucketMetricsConfiguration                   |           |                                                           |                                               |
| PutBucketNotification                           |           |                                                           |                                               |
| PutBucketNotificationConfiguration              |           |                                                           |                                               |
| PutBucketOwnershipControls                      |           |                                                           |                                               |
| PutBucketPolicy                                 | ✔         | Public-read helper only                                   | PUT /{bucket}?policy                          |
| PutBucketReplication                            |           |                                                           |                                               |
| PutBucketRequestPayment                         |           |                                                           |                                               |
| PutBucketTagging                                | ✔         | XML request                                               | PUT /{bucket}?tagging                         |
| PutBucketVersioning                             |           |                                                           |                                               |
| PutBucketWebsite                                |           |                                                           |                                               |
| PutObject                                       | ✔         | MD5 as ETag; x-amz-meta-\*                                | PUT /{bucket}/{key}                           | test_PutObject.py    |
| PutObjectAcl                                    |           |                                                           |                                               |
| PutObjectLegalHold                              |           |                                                           |                                               |
| PutObjectLockConfiguration                      |           |                                                           |                                               |
| PutObjectRetention                              |           |                                                           |                                               |
| PutObjectTagging                                | ✔         | XML request                                               | PUT /{bucket}/{key}?tagging                   |
| PutPublicAccessBlock                            |           |                                                           |                                               |
| RenameObject                                    |           |                                                           |                                               |
| RestoreObject                                   |           |                                                           |                                               |
| SelectObjectContent                             |           |                                                           |                                               |
| UpdateBucketMetadataInventoryTableConfiguration |           |                                                           |                                               |
| UpdateBucketMetadataJournalTableConfiguration   |           |                                                           |                                               |
| UploadPart                                      | ✔         | Returns part ETag                                         | PUT /{bucket}/{key}?uploadId=...&partNumber=N |
| UploadPartCopy                                  |           |                                                           |                                               |
| WriteGetObjectResponse                          |           |                                                           |                                               |

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

- **Bucket lifecycle (minimal support)**

  - `GET /{bucket}?lifecycle` — Returns a minimal default lifecycle XML (placeholder)
  - `PUT /{bucket}?lifecycle` — Accepts lifecycle XML (acknowledged; not persisted yet)

- **Object operations (simple uploads)**

  - `PUT /{bucket}/{key}` — Upload object (stores metadata, content type, MD5 as ETag)
  - `GET /{bucket}/{key}` — Download object (supports Range requests; returns S3-like headers)
  - `HEAD /{bucket}/{key}` — Object metadata (size, content type, ETag, Last-Modified)
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
