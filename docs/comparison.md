# S3 Feature Comparison: AWS S3 vs Cloudflare R2 vs Hippius S3

Feature-by-feature comparison of S3 API compatibility. AWS S3 is the reference implementation. Hippius S3 is a decentralized S3-compatible gateway with Arion storage backend and blockchain publishing. Cloudflare R2 is an S3-compatible store with zero egress fees.

✅ Supported | ❌ Not supported | ⚠️ Partial (see notes)

Sources: [AWS S3 API Reference (April 2026)](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html) | [Cloudflare R2 S3 API Compatibility (April 2026)](https://developers.cloudflare.com/r2/api/s3/api/) | [Hippius S3 Compatibility Matrix](s3-compatibility.md)

---

## Bucket Operations

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| CreateBucket | ✅ | ✅ | ✅ | |
| DeleteBucket | ✅ | ✅ | ✅ | |
| HeadBucket | ✅ | ✅ | ✅ | |
| ListBuckets | ✅ | ✅ | ✅ | |
| GetBucketLocation | ✅ | ✅ | ✅ | R2: `auto`; Hippius: `us-east-1` |
| ListDirectoryBuckets | ✅ | ❌ | ❌ | S3 Express One Zone only |

## Access Control

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| Bucket ACL (Get/Put) | ✅ | ⚠️ | ✅ | R2 accepts but ignores ACL headers [1] |
| Object ACL (Get/Put) | ✅ | ⚠️ | ✅ | R2 accepts but ignores ACL headers [1] |
| Bucket Policy (Get/Put/Delete) | ✅ | ❌ | ⚠️ | Hippius: public-read only [2] |
| Public Access Block | ✅ | ❌ | ❌ | |
| Ownership Controls | ✅ | ❌ | ❌ | |
| ABAC | ✅ | ❌ | ❌ | |

## Bucket Configuration

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| CORS (Get/Put/Delete) | ✅ | ✅ | ❌ | Hippius CORS at gateway, not S3 API |
| Bucket Tagging (Get/Put/Delete) | ✅ | ❌ | ✅ | |
| Versioning (Get/Put) | ✅ | ❌ | ❌ | Hippius has internal versioning, not via API [4] |
| Lifecycle (Get/Put/Delete) | ✅ | ✅ | ⚠️ | Hippius accepts XML, does not enforce [3] |
| Encryption Config (Get/Put/Delete) | ✅ | ✅ | ❌ | R2: AES256 only |
| Logging | ✅ | ❌ | ❌ | |
| Website Hosting | ✅ | ❌ | ❌ | |
| Notifications | ✅ | ⚠️ | ❌ | R2 uses Cloudflare Queues [5] |
| Request Payment | ✅ | ❌ | ❌ | |
| Transfer Acceleration | ✅ | ❌ | ❌ | |
| Replication | ✅ | ❌ | ❌ | |
| Analytics | ✅ | ❌ | ❌ | |
| Metrics Config | ✅ | ❌ | ❌ | |
| Inventory | ✅ | ❌ | ❌ | |
| Intelligent-Tiering | ✅ | ❌ | ❌ | |
| Metadata Config | ✅ | ❌ | ❌ | S3 Tables |

## Object Operations

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| PutObject | ✅ | ✅ | ✅ | |
| GetObject | ✅ | ✅ | ✅ | |
| HeadObject | ✅ | ✅ | ✅ | |
| DeleteObject | ✅ | ✅ | ✅ | |
| DeleteObjects (batch) | ✅ | ✅ | ✅ | |
| CopyObject | ✅ | ✅ | ✅ | |
| RenameObject | ⚠️ | ❌ | ❌ | S3 Express One Zone only [8] |
| GetObjectAttributes | ✅ | ❌ | ❌ | |
| RestoreObject | ✅ | ❌ | ❌ | Glacier retrieval |

## Object Features

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| User Metadata (`x-amz-meta-*`) | ✅ | ✅ | ✅ | |
| Object Tagging (Get/Put/Delete) | ✅ | ❌ | ✅ | |
| Range Requests | ✅ | ✅ | ✅ | |
| Conditional Reads (If-Match etc.) | ✅ | ✅ | ❌ | |
| Conditional Writes (If-None-Match) | ✅ | ✅ | ❌ | |
| Conditional Deletes (If-Match) | ✅ | ❌ | ❌ | |
| ETag (MD5) | ✅ | ✅ | ✅ | |
| Checksums (CRC32, SHA256 etc.) | ✅ | ⚠️ | ❌ | R2: composite only; CRC64NVME full |
| CRC64NVME | ✅ | ✅ | ❌ | |

## Multipart Uploads

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| CreateMultipartUpload | ✅ | ✅ | ✅ | |
| UploadPart | ✅ | ✅ | ✅ | |
| UploadPartCopy | ✅ | ✅ | ✅ | |
| CompleteMultipartUpload | ✅ | ✅ | ✅ | |
| AbortMultipartUpload | ✅ | ✅ | ✅ | |
| ListMultipartUploads | ✅ | ✅ | ✅ | |
| ListParts | ✅ | ✅ | ✅ | |

## Versioning

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| Bucket Versioning Config | ✅ | ❌ | ❌ | [4] |
| ListObjectVersions | ✅ | ❌ | ❌ | |
| Get/Delete Specific Versions | ✅ | ❌ | ❌ | |
| MFA Delete | ✅ | ❌ | ❌ | |

## Storage Classes

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| Standard | ✅ | ✅ | ✅ | Hippius: Arion decentralized backend |
| Standard-IA | ✅ | ❌ | ❌ | |
| One Zone-IA | ✅ | ❌ | ❌ | |
| Intelligent-Tiering | ✅ | ❌ | ❌ | |
| Glacier Instant Retrieval | ✅ | ❌ | ❌ | |
| Glacier Flexible Retrieval | ✅ | ❌ | ❌ | |
| Glacier Deep Archive | ✅ | ❌ | ❌ | |
| Express One Zone | ✅ | ❌ | ❌ | |
| R2 Infrequent Access | ❌ | ✅ | ❌ | R2-specific |

## Encryption

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| SSE-S3 (AES-256 default) | ✅ | ✅ | ❌ | |
| SSE-KMS | ✅ | ❌ | ❌ | |
| SSE-C (customer keys) | ✅ | ✅ | ❌ | |
| Always-on transparent encryption | ❌ | ✅ | ✅ | [7] |
| Envelope encryption (external KMS) | ✅ | ❌ | ✅ | Hippius: OVH KMS [7] |

## Object Lock & Compliance

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| Object Lock (WORM) | ✅ | ❌ | ❌ | |
| Object Retention | ✅ | ❌ | ❌ | |
| Legal Hold | ✅ | ❌ | ❌ | |
| Bucket Locks | ❌ | ✅ | ❌ | R2-specific |

## Data Processing

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| S3 Select | ✅ | ❌ | ❌ | |
| S3 Object Lambda | ✅ | ❌ | ❌ | |
| S3 Batch Operations | ✅ | ❌ | ❌ | |

## Data Management

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| Cross-Region Replication | ✅ | ❌ | ❌ | |
| Same-Region Replication | ✅ | ❌ | ❌ | |
| Transfer Acceleration | ✅ | ❌ | ❌ | |
| Event Notifications | ✅ | ⚠️ | ❌ | R2: Cloudflare Queues [5] |
| S3 Access Points | ✅ | ❌ | ❌ | |
| Multi-Region Access Points | ✅ | ❌ | ❌ | |

## Authentication

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| AWS SigV4 | ✅ | ✅ | ✅ | |
| Presigned URLs | ✅ | ⚠️ | ✅ | R2: no POST support [6] |
| Presigned POST | ✅ | ❌ | ❌ | |
| Access Keys | ✅ | ✅ | ✅ | Hippius: `hip_*` keys |
| Bearer Token Auth | ❌ | ❌ | ✅ | Hippius extension |
| Seed Phrase SigV4 | ❌ | ❌ | ✅ | Hippius extension |
| Anonymous Access | ✅ | ✅ | ✅ | |

## Monitoring & Logging

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| Server Access Logging | ✅ | ❌ | ❌ | |
| Audit Logging | ✅ | ✅ | ✅ | |
| Metrics | ✅ | ✅ | ✅ | |
| Request Tracing | ✅ | ✅ | ✅ | |

## Append Operations

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| S3 Express Append | ⚠️ | ❌ | ❌ | Express One Zone only [8] |
| S4 Atomic Append | ❌ | ❌ | ✅ | Hippius extension with CAS [9] |

## Sessions & Advanced

| Feature | AWS S3 | R2 | Hippius | Notes |
|---------|--------|-----|---------|-------|
| CreateSession | ✅ | ❌ | ❌ | S3 Express only |
| GetObjectTorrent | ✅ | ❌ | ❌ | Legacy |
| SelectObjectContent | ✅ | ❌ | ❌ | S3 Select |
| WriteGetObjectResponse | ✅ | ❌ | ❌ | Object Lambda |

---

## Notes

[1] R2 accepts `x-amz-acl` headers but does not enforce S3-style ACLs. Access is managed via API tokens.

[2] Hippius bucket policy supports `public-read` only. No arbitrary IAM-style policies.

[3] Hippius lifecycle endpoint accepts XML and returns 200 but does not persist or enforce rules.

[4] Hippius has internal object versioning for migrations and append tracking, not exposed via S3 versioning API.

[5] R2 event notifications use Cloudflare Queues, not SNS/SQS/Lambda. Up to 100 rules per bucket.

[6] R2 presigned URLs support GET/PUT/DELETE/HEAD only. No POST (form uploads).

[7] Hippius uses always-on AES-256-GCM encryption with per-object NaCl keys and OVH KMS envelope encryption in production. Users cannot control encryption via S3 headers.

[8] AWS RenameObject and append are available only on S3 Express One Zone directory buckets.

[9] Hippius S4 provides atomic O(delta) appends with compare-and-swap (`x-amz-meta-append-if-version`), idempotent retries (`x-amz-meta-append-id`), and no byte rewrites.

---

For more details about Hippius S3 and other Hippius services, see [docs.hippius.com](https://docs.hippius.com/).
