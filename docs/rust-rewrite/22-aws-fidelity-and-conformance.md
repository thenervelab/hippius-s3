# 22 — AWS S3 fidelity, the conformance oracle, and the parity/object-lock decisions

Status: **DRAFT SPEC** for the greenfield Rust reimplementation of hippius-s3. Testable. Normative where it says "MUST".

> **Purpose.** The product goal is a *very tight emulation of AWS S3* over an hcfs/arion storage
> backend, in Rust. Two documents already fix the internal contract:
> [`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md) (the exhaustive action/error/XML
> matrix, mapped to the Python source) and [`18-object-lock-and-s4.md`](./18-object-lock-and-s4.md)
> (the WORM enforcement contract). This document adds the **outward-facing AWS ground truth** those two
> are measured against, resolves the three open product decisions, and names the *measurable* parity bar:
>
> 1. **C1 — Parity scope**: the definitive in-scope vs explicitly-out action/feature list (§2), tied to
>    the Ceph **s3-tests** and MinIO **mint** suites as the acceptance oracle (§1).
> 2. **D1/D2/D3 — Object-lock defaults**: AWS's exact defaults (max retention, Days/Years, default-retention
>    retroactivity, COMPLIANCE vs GOVERNANCE strictness, lifecycle×lock), with a verbatim-adopt
>    recommendation and the GDPR posture (§4).
> 3. **The tight-emulation gotcha list** — the behaviors clones most often get wrong that we must nail (§5).
>
> Every external fact is cited inline to a URL; AWS-canonical facts are tagged **[AWS]**. Sources are
> collected in the final section. Where this doc and `06`/`18` state a *deliberate hippius divergence*,
> that divergence wins for our implementation and is flagged **⚑ divergence**.

---

## 0. TL;DR decisions

- **C1 (scope):** Ship *full vanilla-S3 parity* for the **core data plane + the config subresources that
  gate data-plane behavior**: object CRUD, copy, multipart, versioning + delete markers, object/bucket
  tagging, ACL, bucket policy, CORS, lifecycle, and **object lock** (§2.2). **Explicitly OUT of v1**:
  Select/torrent/website/replication/analytics/inventory/metrics/accelerate/requester-pays/notification/
  logging/intelligent-tiering/ownership-controls/public-access-block, plus directory buckets, S3-Control,
  and the annotation/metadata-table families (§2.3). This matches where every tight clone converges
  (Garage returns 501 for absent endpoints; R2/B2 drop ACL-as-AWS; none implement Select in the core
  data path). Out-of-scope endpoints return **`501 NotImplemented`**, not a fake success — with the two
  documented exceptions our code already makes (CORS PUT ack, lifecycle PUT ack; `06` §1.1).
- **C1 (bar):** Adopt **Ceph s3-tests** (protocol-level oracle) + **MinIO mint** (SDK-interop oracle) as
  the two required CI gates, run against a branch-local server exactly like the existing `e2e-local`
  lane. Parity = "the in-scope subset of s3-tests passes, and mint's `core` mode passes," with an
  explicit, reviewed skip-list keyed to §2.3 (§1.4).
- **D1/D2/D3 (object lock):** **Adopt AWS semantics verbatim** — max retention **100 years** / **36,500
  days**, a **year = 365 days (no leap years)**, default retention is **NOT retroactive**, explicit
  per-PUT headers override the bucket default, COMPLIANCE is absolute (no one incl. root; only account
  deletion lifts it), GOVERNANCE is bypassable only with `s3:BypassGovernanceRetention` + the header,
  legal hold is independent and never bypassed, retention **wins over lifecycle expiration**. Keep the
  hippius deviations already decided in `18` (bucket-owner-master-token bypass instead of IAM; a lower
  *configurable* retention cap default; `terminated_at` as the single escape hatch). Set
  `HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS` default to **2555 (~7 years)** and hard-cap it at AWS's 36,500
  (§4.2, resolves `18` ⚑ Q1). GDPR-vs-COMPLIANCE has no technical fix and is postured contractually (§4.6).

---

## 1. The conformance oracle — s3-tests + mint as the parity bar

The only trustworthy definition of "parity" is *a suite AWS-compatible software already measures itself
against, run against our endpoint*. Vendor compatibility tables are self-reported and, per our own
standing rule, "declarations aren't behavior." Two suites are the industry de-facto oracles.

### 1.1 Ceph s3-tests — the protocol-level oracle

- **What it is.** "A set of unofficial Amazon AWS S3 compatibility tests, that can be useful to people
  implementing software that exposes an S3-like API." Python, built on **boto2 + boto3**, pytest-driven.
  https://github.com/ceph/s3-tests
- **Why it's the de-facto bar.** It is authored by Ceph but explicitly aimed at *any* S3-like
  implementation, and it ships `fails_on_aws` / `fails_on_rgw` / `fails_on_s3` markers proving it is run
  against real AWS and multiple backends. https://github.com/ceph/s3-tests
- **Structure.** Two trees (`s3tests/` boto2 legacy, `s3tests_boto3/` current); tests carry pytest
  markers used for selection/exclusion. https://deepwiki.com/ceph/s3-tests/3-s3-api-testing-framework
- **Coverage, by marker** (from `pytest.ini`) — this *is* the practical map of the S3 surface and how to
  slice it: `bucket_policy`, `bucket_encryption`, `bucket_logging`, `conditional_write`, `checksum`,
  `copy`, `encryption`, `lifecycle`, `lifecycle_expiration`, `lifecycle_transition`, `list_objects_v2`,
  `object_lock`, `object_ownership`, `tagging`, `versioning`, `delete_marker`, `sse_s3`, `storage_class`,
  `appendobject`, `s3select`, `s3website`, `sns`, plus IAM/STS families (`iam_*`, `test_of_sts`,
  `webidentity_test`) and the `fails_on_*` exclusion family. Full list:
  https://raw.githubusercontent.com/ceph/s3-tests/master/pytest.ini
- **Run against a non-Ceph endpoint.** Config via `S3TEST_CONF` (template `s3tests.conf.SAMPLE`); needs
  an endpoint + **two credential sets** (main + alt, for ACL/cross-account tests), run through **tox**.
  https://github.com/ceph/s3-tests
  ```
  S3TEST_CONF=hippius.conf tox -- -m 'not fails_on_aws'          # AWS-parity subset
  S3TEST_CONF=hippius.conf tox -- -m 'not s3select and not s3website and not sns'   # drop out-of-scope
  S3TEST_CONF=hippius.conf tox -- s3tests_boto3/functional/test_s3.py::<one test>
  ```
- **What clones exclude.** Whole feature families they don't implement, via `-m 'not <marker>'`
  (`object_lock`, `s3select`, `s3website`, `sse_s3`, `iam_*`, `sns`, `cloud_transition`), plus per-test
  IDs. Third-party forks that repackage it as "compatibility tests for S3 clones" maintain their own skip
  lists (e.g. https://github.com/open-io/ceph-s3-tests). https://github.com/ceph/s3-tests

### 1.2 MinIO mint — the SDK-interop oracle

- **What it is.** "A testing framework … available as a podman image. It runs correctness, benchmarking
  and stress tests." https://github.com/minio/mint (Note: the repo was **archived read-only 2026-03-26**,
  so pin a commit; it still runs.)
- **What it bundles** — each is a separate suite run in-container against the endpoint: `awscli`,
  `aws-sdk-go-v2`, `aws-sdk-java-v2`, `aws-sdk-php`, `aws-sdk-ruby`, `minio-{go,java,js,py}`, `s3cmd`,
  `s3select`, `mc`, `healthcheck`, `versioning`. https://github.com/minio/mint
- **Why it complements s3-tests.** It validates that the *real, unmodified* AWS SDKs and CLI work against
  the endpoint — i.e. client-level interop, not just raw HTTP. That catches divergences the boto-only
  s3-tests can mask, and vice-versa. https://github.com/minio/mint
- **Run.** Docker/podman, env-var driven; any S3 endpoint (`SERVER_ENDPOINT`+creds):
  ```
  podman run -e SERVER_ENDPOINT=host:port -e ACCESS_KEY=… -e SECRET_KEY=… \
             -e ENABLE_HTTPS=1 -e MINT_MODE=core -e ENABLE_VIRTUAL_STYLE=1 minio/mint
  ```
  Modes: `core` / `full`. Output at `/mint/log/log.json` (per-test name, args, status PASS/FAIL/NA).
  `ENABLE_VIRTUAL_STYLE` toggles vhost addressing (relevant to §5-G6). https://github.com/minio/mint

### 1.3 SDK-free / handcrafted-request testing (context)

MinIO's older **s3verify** took the opposite stance from mint: it "creates its own handcrafted HTTP
requests" specifically to *avoid* "dependency on other SDKs that could mask implementation bugs," and it
checks both valid inputs (correct response required) and invalid inputs (failure required). It is now
"replaced by Mint." https://min.io/blog/s3verify-a-simple-tool-to-verify-aws-s3-api-compatibility
The takeaway for us: SDK-driven testing measures real-world interop; raw-request/boto testing measures
protocol conformance directly. We want **both**, which is why the recommendation is s3-tests + mint.

### 1.4 Recommended adoption (measurable "parity" bar)

1. Add an **`s3-tests` CI lane** modeled on the existing `e2e-local` job (real postgres + minio/arion +
   the branch's own server), running the AWS-parity subset with a **reviewed skip file** whose every
   entry maps to a §2.3 out-of-scope family or a documented `06`/`18` divergence. A skip needs a comment
   citing the doc section that justifies it. Make it a required merge check, same as `e2e-local`.
2. Add a **`mint` CI lane** in `MINT_MODE=core` (both path- and virtual-style via `ENABLE_VIRTUAL_STYLE`).
   `full` mode can be a nightly, non-blocking lane.
3. **Parity is defined as**: the in-scope s3-tests subset is green and mint `core` is green, with the
   skip file as the single source of truth for "what we deliberately don't do." Growth of the skip file
   is the metric to watch — every addition is a parity regression that must be justified in review.
4. Because both suites are self-hosted and the vendors' *own* tables are unreliable, **do not treat any
   vendor compatibility claim (ours or a competitor's) as evidence**; treat a green suite run as the
   evidence. (Per project memory: verify against behavior, not declarations.)

---

## 2. C1 — Parity scope

### 2.1 The canonical AWS action set (the denominator)

The full Amazon S3 (non-Control, non-Outposts) action list, from the API reference
(https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html),
is ~100 operations. Grouped:

| Group | Actions |
|---|---|
| **Object core** | PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject, GetObjectAttributes |
| **Bucket core** | CreateBucket, DeleteBucket, HeadBucket, ListBuckets, GetBucketLocation |
| **Listing** | ListObjects, ListObjectsV2, ListObjectVersions |
| **Multipart** | CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads |
| **Versioning** | GetBucketVersioning, PutBucketVersioning |
| **Tagging** | Get/Put/DeleteObjectTagging, Get/Put/DeleteBucketTagging |
| **ACL** | GetBucketAcl, PutBucketAcl, GetObjectAcl, PutObjectAcl |
| **Policy** | GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy, GetBucketPolicyStatus |
| **CORS** | GetBucketCors, PutBucketCors, DeleteBucketCors |
| **Lifecycle** | GetBucketLifecycle(Configuration), PutBucketLifecycle(Configuration), DeleteBucketLifecycle |
| **Object Lock** | Get/PutObjectLockConfiguration, Get/PutObjectRetention, Get/PutObjectLegalHold |
| **Encryption** | Get/Put/DeleteBucketEncryption, UpdateObjectEncryption |
| **Website** | Get/Put/DeleteBucketWebsite |
| **Replication** | Get/Put/DeleteBucketReplication |
| **Analytics/Inventory/Metrics** | Get/Put/Delete + List{Analytics,Inventory,Metrics}Configuration(s) |
| **Accelerate / RequestPayment / Logging** | Get/PutBucketAccelerateConfiguration, Get/PutBucketRequestPayment, Get/PutBucketLogging |
| **Notification** | GetBucketNotification(Configuration), PutBucketNotification(Configuration) |
| **Intelligent-Tiering** | Get/Put/Delete + ListBucketIntelligentTieringConfiguration(s) |
| **Ownership / Public-access** | Get/Put/DeleteBucketOwnershipControls, Get/Put/DeletePublicAccessBlock |
| **Advanced object** | SelectObjectContent, RestoreObject, GetObjectTorrent, RenameObject, WriteGetObjectResponse |
| **Directory buckets / session** | CreateSession, ListDirectoryBuckets |
| **Metadata tables / annotations** | CreateBucketMetadata(Table)Configuration, Update…MetadataTableConfiguration, {Get,Put,Delete,List}ObjectAnnotation(s), GetBucketAbac, PutBucketAbac |

### 2.2 In-scope — "full vanilla-S3 parity" (MUST pass s3-tests/mint for these)

These are the surfaces a normal S3 client and the standard SDKs exercise, and the ones every tight clone
that markets "S3 compatibility" is expected to have. Ship these to AWS fidelity:

| In-scope family | Rationale / bar |
|---|---|
| **Object CRUD + CopyObject + GetObjectAttributes** | Core data plane. `GetObjectAttributes` is currently **absent** in Python (falls through to a body GET — `06` §9.1, Q3); the rewrite MUST add the real `?attributes` XML shape (resolves `06` Q3). mint's SDK suites call it. |
| **Multipart (all 7 ops)** | Core; s3-tests `copy`/multipart + mint cover it. Real `ListMultipartUploads` pagination is in scope for the rewrite (resolves `06` Q5 — Python stubs it). |
| **Versioning + delete markers + ListObjectVersions** | s3-tests `versioning`, `delete_marker`. The rewrite MUST implement **Suspended** (Python returns 501 — `06` §9.2), or it fails the versioning suite. |
| **Tagging (object + bucket, all verbs)** | s3-tests `tagging`. Note `?versionId` tagging is a deliberate 501 (`06` §0.3) — mark that in the skip file, not as a bug. |
| **ACL (bucket + object, canned + grants)** | s3-tests ACL/`object_ownership`. This is where clones most diverge (R2/B2/Garage drop object ACLs). If we claim "tight," object ACLs are in scope; if cost forces a cut, model it on B2 (Get Object ACL returns bucket ACL) and record it as a scoped divergence. |
| **Bucket policy** | Python supports only a canned public-read policy (`06` §9.2). Full arbitrary-statement policy is a large IAM-shaped effort; **decision:** keep the public-read subset in v1, mark the rest of the `bucket_policy` suite skipped, and treat full policy as a fast-follow (it is the biggest gap vs "vanilla parity"). |
| **CORS** | s3-tests + browser clients. Python only **acks** PutBucketCors and lacks GET/DELETE (`06` §9.1). The rewrite MUST implement real Get/Put/DeleteBucketCors (Garage and R2 both do). |
| **Lifecycle (config round-trip)** | Config storage + read-back is in scope (Python discards it — `06` §9.2). The lifecycle **engine** (actual expiration/transition execution) is out of v1 (§2.3) but the config API must round-trip so SDKs/tools work, matching Garage's "stores config, limited enforcement." |
| **Object Lock (config + retention + legal-hold + enforcement)** | Full subject of `18`; s3-tests `object_lock`. In scope, AWS-verbatim semantics (§4). |
| **Error model, conditional requests, list semantics, ETag rules, addressing, presigned** | The cross-cutting fidelity surface (§3); these are what the "gotcha list" (§5) is about and where s3-tests/mint fail clones most. |

### 2.3 Explicitly OUT of scope for v1 (return `501 NotImplemented`)

Confirmed against what tight clones actually do — Garage returns "501 Not Implemented" for all missing
endpoints (https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/); R2 omits the
same advanced families (https://developers.cloudflare.com/r2/api/s3/api/); B2 lists Website/Tagging-parity
gaps (https://www.backblaze.com/docs/cloud-storage-s3-compatible-api). None ship SelectObjectContent in
the core data path.

| Out-of-scope family | What clones do / why out | s3-tests marker to skip |
|---|---|---|
| **SelectObjectContent** | Query engine; MinIO has it but Garage/R2/B2 don't; huge surface, not "vanilla." | `s3select` |
| **GetObjectTorrent** | Deprecated by AWS in real terms; nobody clones it. | — |
| **Website hosting + routing rules** | R2/Garage partial or absent; B2 lists as unsupported. | `s3website*` |
| **Replication** | Cross-site orchestration; RGW only "across zones". | — |
| **Analytics / Inventory / Metrics** | Reporting; no client depends on them for data. | — |
| **Accelerate / RequestPayment** | Edge/billing config; R2 says request-payer not implemented. | — |
| **Logging** | MinIO lists unsupported; ops concern, not data plane. | `bucket_logging*` |
| **Notification (SNS/SQS/Lambda/Kafka/AMQP)** | RGW substitutes its own transports; not vanilla. | `sns` |
| **Intelligent-Tiering / storage-class transitions** | Single effective class here; expose only `STANDARD`. | `storage_class`, `cloud_transition`, `lifecycle_transition` |
| **Ownership controls / Public-access-block** | IAM-adjacent governance; our authz model differs. | `object_ownership` (partial) |
| **Bucket encryption config (`*BucketEncryption`)** | ⚠️ **RECONCILED with doc 24 §S4 (register C7):** do NOT 501 these — implement a **coherent AES256-default stub** (Put accepts/echoes `AES256`, Get reads it back, Delete resets to default) so scanners see a consistent posture. The always-on `AES256` SSE echo depends on it. External KMS ARNs + SSE-C are rejected (clean 400). | `bucket_encryption`, `sse_s3` (see §3.1 / doc 24) |
| **Directory buckets, CreateSession, S3-Control** | Express One Zone / control-plane; entirely separate product. | `s3control` |
| **Metadata tables, annotations, ABAC, RenameObject, WriteGetObjectResponse, UpdateObjectEncryption** | New/niche AWS-only surfaces; no clone parity, no client dependency. | — |
| **IAM/STS** | We have no IAM; auth is ed25519/SS58 (`06`). | `iam_*`, `test_of_sts`, `webidentity_test` |

**Rule:** an out-of-scope endpoint returns a well-formed `501 NotImplemented` error XML (§3.5), *not* a
200 stub — except the two documented ack-only compatibility shims the Python already ships and the
rewrite preserves (CORS-PUT-ack is superseded by real CORS in §2.2; lifecycle-config round-trips per
§2.2). Silent 200s on unimplemented config are an anti-pattern that breaks tools which then assume the
feature is active.

### 2.4 The recurring cross-clone gap set (weight these in the oracle)

Across MinIO, Ceph RGW, Garage, R2, and B2, the features that most consistently diverge from AWS are:
**full ACL/grant support, object-level tagging, native versioning depth, object lock, website/redirect
config, request-payer, expected-bucket-owner, and FULL_OBJECT checksums**
(https://docs.min.io/aistor/developers/s3-api-compatibility/;
https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/;
https://developers.cloudflare.com/r2/api/s3/api/; https://www.backblaze.com/docs/cloud-storage-s3-compatible-api).
Of these, our in-scope commitments deliberately cover ACL, tagging, versioning, and object lock (the
hard ones) — that is what makes our target "tighter" than the median clone.

---

## 3. AWS exact-behavior reference (the byte-level ground truth)

This is the outward truth that `06`'s matrix is validated against. Reproduce these exactly.

### 3.1 ETag rules (single-part, multipart, and SSE)

From AWS's integrity guide
(https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html):

- **Single-part, plaintext or SSE-S3:** ETag **is** the MD5 of the object data. [AWS] — "If an object is
  created by the `PutObject`, `PostObject`, or `CopyObject` operation … and that object is also plaintext
  or encrypted by server-side encryption with Amazon S3 managed keys (SSE-S3), that object has an ETag
  that is an MD5 digest of its object data."
- **Single-part, SSE-C or SSE-KMS:** ETag is **NOT** an MD5 digest. [AWS] — "…encrypted by server-side
  encryption with customer-provided keys (SSE-C) or … AWS KMS keys (SSE-KMS), that object has an ETag
  that is not an MD5 digest of its object data."
- **Multipart or UploadPartCopy:** ETag is **NOT** an MD5 digest **regardless of encryption**. [AWS] —
  "If an object is created by either the multipart upload process or the `UploadPartCopy` operation, the
  object's ETag is not an MD5 digest, regardless of the method of encryption."
- **Multipart composite ETag format:** MD5 of the concatenated binary MD5s of each part, then a dash and
  the part count. [AWS] — "Amazon S3 concatenates the bytes for the MD5 digests together and then
  calculates the MD5 digest of these concatenated values. During the final ETag creation step, Amazon S3
  adds a dash with the total number of parts to the end." i.e. `md5(concat(part_md5_binary...))-N`,
  matching `06` §8.4 (`hash_all_etags`).
- **CopyObject changes the ETag of a multipart source:** [AWS] — "With a copy command, the checksum of the
  object is a direct checksum of the full object. If the object was originally uploaded using a multipart
  upload, the checksum value changes even though the data doesn't."
- **ETag is always double-quoted** in headers and XML (`06` §8.4).

**Hippius mapping.** Our backend encrypts (AES-256-GCM, CTX-over-frames committing AEAD — doc 25 suite
`hip-enc/aes256gcm-ctx-frames-v1`) but the *stored* ETag semantics must match what the client sees:
single-part PUT → MD5-of-plaintext ETag; multipart/append → `…-N` composite. The one hazard is claiming
an MD5 ETag on a path where AWS would not (see §5-G1). Per doc 24 §S4 we emit a coherent always-on
`AES256` SSE echo and a stubbed `*BucketEncryption` (see the reconciliation note in §2.3), but the ETag
is MD5(plaintext) regardless — the multipart non-MD5 rule still applies to us.

- **Additional checksums (`x-amz-checksum-*`: CRC32/CRC32C/CRC64NVME/SHA1/SHA256):** AWS supports
  FULL_OBJECT vs COMPOSITE checksum types, algorithm-specific headers, and trailing checksums via
  `aws-chunked` (`x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER`, `x-amz-trailer`). [AWS]
  (same page.) **Not implemented** in Python (Content-MD5 only — `06` §3.2). **Decision:** out of scope
  for v1 but skip-list the s3-tests `checksum` marker explicitly; SDKs increasingly *default* to
  CRC64NVME, so watch for clients that send `x-amz-checksum-*` and expect it echoed — at minimum the
  server must not 400 on an unrecognized `x-amz-checksum-*` header (accept-and-ignore, like R2's partial
  support: https://developers.cloudflare.com/r2/api/s3/api/).

### 3.2 Conditional requests (reads, 2024 conditional writes, conditional deletes)

AWS overview: conditional reads on GET/HEAD/COPY; conditional writes on PutObject/CompleteMultipartUpload/
CopyObject; conditional deletes on DeleteObject/DeleteObjects.
https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html

**Reads (GET/HEAD)** — RFC 7232 headers: `If-Match`, `If-None-Match`, `If-Modified-Since`,
`If-Unmodified-Since`. `If-None-Match` match → `304 Not Modified`; `If-Match` mismatch /
`If-Unmodified-Since` violated → `412 Precondition Failed`.
https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-reads.html

**Writes (2024 feature).** From https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
and the launch notes (https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes;
https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets/;
copy support 2025: https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-s3-conditional-write-functionality-copy-operations):

- `If-None-Match` on write is **create-only** and **expects the `*` value**. [AWS] — "The `If-None-Match`
  header expects the * (asterisk) value." Success `200`; existing current version → `412 Precondition
  Failed`. Works on **PutObject, CompleteMultipartUpload, CopyObject**.
- Versioning nuance: [AWS] — "For buckets with versioning enabled, if there's no current object version
  with the same name, or if the current object version is a delete marker, the write operation succeeds.
  Otherwise, it … [returns] a `412 Precondition Failed`." (`If-None-Match` "only applies to the current
  version.")
- `If-Match` on write **expects the ETag value** and writes only if it matches the current object's ETag;
  mismatch → `412`. Works on PutObject/CompleteMultipartUpload/CopyObject.
- Concurrency corner cases (must reproduce for tight parity): first writer wins, subsequent → `412`; a
  concurrent delete beating an `If-None-Match` write → `409 Conflict` (retry PutObject; re-init MPU); a
  concurrent delete beating an `If-Match` write → `404 Not Found`; an `If-Match` against a
  now-delete-marker/absent current version → `404`. In-progress MPU parts don't count as an existing
  object for conditional evaluation. [AWS] (conditional-writes page, "Conditional write behavior").
- Conditional writes require **SigV4**. [AWS] (same page.)

**Hippius current state (`06` §2) and the rewrite gap:**
- Python implements only `If-None-Match: *` create-only on PutObject/CompleteMPU (→ `412`), returns
  `501 NotImplemented` for any *other* `If-None-Match` value, rejects **all** `If-None-Match` on
  CopyObject with 501, and does **not** consult `If-Match` / `If-Modified-Since` / `If-Unmodified-Since`
  at all (accept-and-ignore). The create-only check is transactional (no TOCTOU), and the body is
  drained before an early 412/501.
- **Rewrite decision:** the *existing* `If-None-Match: *` behavior is correct and must be preserved
  byte-for-byte (412 vs 501 split, transactional check, body drain — `06` §2.1). To reach true AWS
  parity for the 2024 feature the rewrite SHOULD add: (a) `If-Match` conditional writes on
  PutObject/CompleteMPU/CopyObject (→ 412/409/404 per the matrix above); (b) conditional reads for
  `If-Match`/`If-Modified-Since`/`If-Unmodified-Since` on GET/HEAD (Python only does `If-None-Match`
  read-304). These are the single largest *functional* fidelity gap vs current AWS and are in the
  s3-tests `conditional_write` marker — do not leave them accept-and-ignore, which silently corrupts
  optimistic-concurrency clients. Where a value can't be honored, prefer `501 NotImplemented` over silent
  success (the Python `If-None-Match ≠ *` → 501 precedent), and document it in the skip file.

### 3.3 List semantics (pagination, delimiter/prefix/common-prefixes, encoding-type, max-keys)

From https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html and
https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html:

- **Default/limit `MaxKeys` = 1000**; "The response might contain fewer keys but will never contain
  more." A rolled-up `CommonPrefixes` entry "counts as only one return against the MaxKeys value."
- **ContinuationToken** (V2) "is obfuscated and is not a real key"; `NextContinuationToken` is returned
  when `IsTruncated` is true. V1 uses `Marker`/`NextMarker` (and `NextMarker` is only present when a
  `Delimiter` is set).
- **Delimiter** groups keys sharing the substring between `prefix` and the first delimiter into a single
  `CommonPrefixes` element; "these rolled-up keys are not returned elsewhere in the response."
- **encoding-type:** only `url` is valid. When set, S3 URL-encodes these response elements: **Delimiter,
  Prefix, Key, StartAfter** (V2) / and `Marker`/`NextMarker` (V1). The `EncodingType` element is echoed
  in the response and sits **after `Delimiter`, before `IsTruncated`** (element order is SDK-load-bearing
  — `06` §8.4).
- **StartAfter** (V2): "CommonPrefixes is filtered out … if it is not lexicographically greater than the
  StartAfter value"; if sent, StartAfter is echoed in the response.
- Keys are returned in **UTF-8 binary (lexicographic) order** — the sort every SDK paginator assumes.

**ListObjectVersions ordering** (https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html):
"a sequence of `Version` and `DeleteMarker` tags … returned in **(object key, version id) order** with
Versions and DeleteMarkers **interleaved**." `IsLatest` marks the current version; truncation uses
`KeyMarker`/`VersionIdMarker` → `NextKeyMarker`/`NextVersionIdMarker`. `DeleteMarker` entries omit
`ETag`/`Size`/`StorageClass` (`06` §8.4). Preserving the interleaved key+version order is a known SDK
pain point (https://github.com/aws/aws-sdk-go-v2/issues/3164).

### 3.4 Error model (code → status → XML)

Error XML structure (https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>...</Code><Message>...</Message>
  <Resource>/bucket/key</Resource>
  <RequestId>...</RequestId>
  <HostId>...</HostId>
</Error>
```
Real S3 includes **both** `RequestId` and `HostId` (the latter mirrors the `x-amz-id-2` header); boto3
reads them into `ResponseMetadata`
(https://docs.aws.amazon.com/AmazonS3/latest/API/get-request-ids.html). Our error XML is a **no-namespace**
`<Error>` with `<HostId>hippius-s3</HostId>` and `x-amz-request-id` — a deliberate boto3-compat choice
(`06` §7); keep it, and keep emitting `HostId` (some strict parsers expect it).

Canonical code → HTTP status catalog (https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html),
cross-checked against `06` §7.1:

| Code | HTTP | Code | HTTP |
|---|---|---|---|
| NoSuchKey / NoSuchBucket / NoSuchUpload / NoSuchVersion | 404 | AccessDenied / SignatureDoesNotMatch | 403 |
| BucketAlreadyExists / BucketAlreadyOwnedByYou / BucketNotEmpty | 409 | PreconditionFailed | 412 |
| InvalidArgument / InvalidRequest / MalformedXML / BadDigest / InvalidDigest / EntityTooLarge / EntityTooSmall / InvalidPart / InvalidPartOrder / KeyTooLongError / InvalidBucketName | 400 | MethodNotAllowed | 405 |
| MissingContentLength | 411 | InvalidRange | 416 |
| NotImplemented | 501 | SlowDown | 503 |
| InternalError | 500 | (client closed, nginx) | 499 |

Note the hippius-specific status uses `409 InvalidBucketState` for object-lock-config-without-versioning
(`06` §6.2), matching AWS's `InvalidBucketState` (§4.5).

### 3.5 Addressing (path vs virtual-hosted) and presigned URLs

- **Two addressing styles:** path-style `https://s3.<region>.amazonaws.com/<bucket>/<key>` vs
  virtual-hosted `https://<bucket>.s3.<region>.amazonaws.com/<key>`. AWS "delayed the deprecation of
  path-style URLs" and still supports path-style for buckets created on/before 2020-09-30
  (https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/). A tight clone
  MUST accept **both**; mint's `ENABLE_VIRTUAL_STYLE` toggles the test. The rewrite must resolve the
  bucket from either the `Host` header (vhost) or the first path segment (path-style) and canonicalize
  before dispatch. (This interacts with `06` §9.3: our gateway rejects `?`/`#` in keys with 400
  InvalidURI — a documented divergence.)
- **Presigned URLs** (https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html):
  - **Max validity = 7 days (604800s)** for SigV4 URLs. [AWS] — `X-Amz-Expires` "must be less than
    604800 seconds." (Console-generated: 1 min–12 h; SDK/CLI: up to 7 days.)
    https://github.com/aws/aws-cli/issues/5464
  - A presigned URL created with **temporary credentials expires when those credentials expire**, even if
    `X-Amz-Expires` is longer. [AWS].
  - The signature covers a specific method + canonical resource + **signed headers** (`X-Amz-SignedHeaders`)
    + `X-Amz-Date` + `X-Amz-Credential` + `X-Amz-Expires`; any signed header the client omits or alters at
    request time → `SignatureDoesNotMatch 403`.
  - Our `/public/*` anon path is stricter-by-design: **any query param → 403 SignatureDoesNotMatch**
    (`06` §9.3). That's a deliberate divergence; keep it, but the *signed* presigned path must implement
    real SigV4 presign validation for the in-scope verbs.

### 3.6 Size and multipart limits (constants to enforce)

From https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html and
https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html:

- **Single PUT max = 5 GB** (5 GiB).
- **Multipart part size = 5 MiB – 5 GiB**; **the last part has no minimum**.
- **Max parts per upload = 10,000**; part numbers **consecutive from 1** for composite checksums (else
  AWS returns `HTTP 500` — integrity page).
- **Max object size = 5 TiB** historically; AWS now documents up to ~**48.8 TiB** via multipart.
- A too-small non-final part → `EntityTooSmall`; oversize → `EntityTooLarge`. `06` §1.3 already maps
  these; keep the exact codes.

---

## 4. D1/D2/D3 — Object-lock defaults (adopt AWS verbatim)

Sources: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html,
https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html,
https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html.
This section is the AWS ground truth for the enforcement contract already specified in
[`18-object-lock-and-s4.md`](./18-object-lock-and-s4.md); where `18` states a hippius deviation, that wins.

### 4.1 Modes: COMPLIANCE vs GOVERNANCE strictness (D1)

- **COMPLIANCE is absolute.** [AWS] — "In compliance mode, a protected object version can't be
  overwritten or deleted by any user, including the root user in your AWS account. When an object is
  locked in compliance mode, its retention mode can't be changed, and its retention period can't be
  shortened." The only lift: [AWS] — "The only way to delete an object under the compliance mode before
  its retention date expires is to delete the associated AWS account." → In hippius this maps to
  `terminated_at` (the single escape hatch — `18` §0.1, §3.4).
- **GOVERNANCE is bypassable with both a permission and a header.** [AWS] — "To override or remove
  governance-mode retention settings, you must have the `s3:BypassGovernanceRetention` permission and must
  explicitly include `x-amz-bypass-governance-retention:true` as a request header." Bypass permits
  "deleting an object version, shortening the retention period, or removing the … retention period"
  (object-lock-managing.html). **⚑ divergence:** hippius has no IAM, so `s3:BypassGovernanceRetention` is
  modeled as **bucket-owner master token + the header** (`18` §0.2, `06` §6.1) — do not accept the header
  alone or from a delegated grantee/sub-token.

### 4.2 Retention duration: max, min, Days/Years (D2)

- **Maximum retention = 100 years.** [AWS] — "The maximum retention period is 100 years."
  (object-lock-managing.html). Corroborated numerically: "You can specify 1 to 36,500 days, or 1 to 100
  years" (object-lock-configure.html).
- **A "year" = 365 days, no leap years.** [AWS] — "Amazon S3 counts 1 year as 365 days and doesn't count
  leap years … Amazon S3 evaluates a duration of 1 year as 365 days" (object-lock-managing.html, the
  years→days condition-key context; it is the only place AWS pins the number, and 36,500 = 100×365
  confirms it). → **Resolves `18` ⚑ Q2: a Year = 365 days.**
- **Minimum = 1** (day or year), positive integer.
- **Bucket default retention is a duration in Days XOR Years.** [AWS] — "The DefaultRetention period can
  be either Days or Years but you must select one. You cannot specify Days and Years at the same time."
  (PutObjectLockConfiguration API). Per-object uses an absolute `RetainUntilDate` timestamp; the bucket
  default is materialized at write time as `creation_time + duration` [AWS] — matching `18` §2.4.
- **Recommended hippius default (resolves `18` ⚑ Q1):** set `HIPPIUS_OBJECT_LOCK_MAX_RETENTION_DAYS`
  default to **2555 (~7 years)** — it covers the regulatory cases (SEC 17a-4 is 6 years) without allowing
  a century-long unreclaimable pin, and it is load-bearing given hippius absorbs the storage cost of
  locked bytes on non-paying/suspended accounts (`18` §3.4). **Hard-cap the configurable value at AWS's
  36,500 days** so no operator can exceed AWS semantics. A `RetainUntilDate` beyond the cap → `400
  InvalidArgument` (`06` §6.1).

### 4.3 Default-retention behavior + retroactivity (D3)

- **NOT retroactive.** [AWS] — the default "will be applied by default to every **new** object placed in
  the specified bucket" (PutObjectLockConfiguration API); "placing a default retention setting on a
  bucket doesn't place any retention settings on objects that already exist"
  (https://aws.amazon.com/blogs/storage/applying-amazon-s3-object-lock-at-scale-for-petabytes-of-existing-data/;
  https://repost.aws/questions/QUGKrl8XRLTEeuIzUHq0Ikew/s3-object-lock-on-existing-s3-objects). To protect
  existing objects you must `PutObjectRetention` per version (or S3 Batch Ops). → **Resolves `18` ⚑ Q6:
  match AWS, not retroactive.**
- **Explicit per-PUT headers override the bucket default.** [AWS] — "the object version's individual
  Object Lock settings override any bucket property retention settings." Headers:
  `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, `x-amz-object-lock-legal-hold`
  (object-lock.html). Matches `18` §2.4 / `06` §6.2.

### 4.4 Legal hold, versioning prerequisite, deletes, lifecycle

- **Legal hold** — [AWS] — "has no expiration date … remains in place until you explicitly remove it …
  independent from retention periods." "Legal holds can be freely placed and removed by any user who has
  the `s3:PutObjectLegalHold` permission." **Governance bypass does NOT touch it** — [AWS] — "Bypassing
  governance mode doesn't affect an object version's legal hold status." (Matches `18` §2.3, W11.)
- **Versioning prerequisite (one-way door).** [AWS] — "Object Lock works only in buckets that have S3
  Versioning enabled"; and "After you enable Object Lock on a bucket, you can't disable Object Lock or
  suspend versioning for that bucket." → A `PutBucketVersioning Suspended` on a lock-enabled bucket must
  return `409 InvalidBucketState`, distinct from the generic Suspended path (`18` §2.5, C3). Enabling
  lock on an **existing** bucket has been allowed by AWS since 2023-11-20
  (https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-s3-enabling-object-lock-buckets); versioning
  must already be on, and a `x-amz-bucket-object-lock-token` may be required.
- **Deletes.** [AWS] — permanent `DELETE` with `versionId` of a protected version → **`403 Access Denied`
  (Forbidden)**; a simple `DELETE` (no versionId) → **`200 OK` + a delete marker**, always allowed and
  never destroying the protected version. **Delete markers are never WORM-protected** — [AWS] — "Delete
  markers are not WORM-protected, regardless of any retention period or legal hold." (Matches `18` §1.2,
  D1–D9.)
- **Lifecycle × lock — retention wins.** [AWS] — "Object lifecycle management configurations continue to
  function normally on protected objects, including placing delete markers. However, a locked version of
  an object cannot be deleted by a S3 Lifecycle expiration policy. Object Lock is maintained regardless of
  … storage class … and throughout S3 Lifecycle transitions." → **Resolves `18` ⚑ Q5:** if/when a
  lifecycle engine is added it runs as a system principal with **no bypass** and MUST honor the
  `protected()` predicate; a retention date beats an expiration rule. (Lifecycle engine itself is out of
  v1 — §2.3.)

### 4.5 Error codes (object lock)

| Trigger | AWS result | Source |
|---|---|---|
| Permanent `DELETE ?versionId` of a protected version | `403 Forbidden` / AccessDenied | object-lock.html |
| `RetainUntilDate` in the past on PUT/PutObjectRetention | `400 InvalidArgument` ("must be in the future") | API_PutObjectRetention; matches `06`/`18` W16 |
| `PutObjectLockConfiguration` when versioning off | `409 InvalidBucketState` | API_Error; matches `06` §6.2 |
| Retention beyond max cap | `400 InvalidArgument` | object-lock-managing.html (100y ceiling); hippius cap §4.2 |

### 4.6 GDPR / right-to-erasure vs COMPLIANCE (posture)

The load-bearing fact: a COMPLIANCE-locked version cannot be deleted by **anyone including account root**
before `RetainUntilDate`; the only lift is deleting the whole account (AWS) → `terminated_at` (hippius).
This directly collides with **GDPR Art. 17 (right to erasure)**: a per-subject deletion cannot be honored
while the object is compliance-locked. This is a property of WORM, not a bug — AWS positions COMPLIANCE
for regimes mandating non-erasability (the Cohasset SEC 17a-4/FINRA/CFTC assessment linked from
object-lock.html: https://d1.awsstatic.com/r2018/b/S3-Object-Lock/Amazon-S3-Compliance-Assessment.pdf).

**Recommended posture (resolves `18` ⚑ Q7):**
1. Describe our mode as **"compliance-mode semantics"** and do **not** claim SEC 17a-4/FINRA/CFTC
   certification (we have no Cohasset assessment — `18` §5.3.3).
2. Treat GDPR-erasure-vs-COMPLIANCE as **contractual, not technical**: a ToS/DPA warranty that the
   customer will not place personal data requiring erasure under a COMPLIANCE lock, plus an explicit
   warning at lock-enable time. Get legal sign-off before offering COMPLIANCE.
3. Note the **crypto-shredding limitation**: destroying the DEK/bucket-KEK makes locked data unreadable
   while leaving it undeletable — the worst outcome and **not** a GDPR escape (a regulator doesn't
   distinguish "deleted" from "cryptographically destroyed"). AWS agrees the encryption layer is
   orthogonal: object-lock-managing.html — "While Object Lock can help prevent … objects from being
   deleted or overwritten, it does not protect against losing access to the encryption keys." → **KEK
   destruction MUST be blocked for lock-enabled buckets** (`18` §3.5, ⚑ Q8).

### 4.7 Net recommendation

Adopt AWS's Object Lock semantics **verbatim** for modes, durations (100y max / 365-day year / Days-XOR-Years),
non-retroactive defaults, header override, legal-hold independence, versioning prerequisite, delete
behavior, and lifecycle precedence. Keep exactly three hippius deviations, all already decided in `18`:
(1) bypass = bucket-owner-master-token + header (no IAM); (2) a lower *configurable* max-retention default
(7 years) hard-capped at AWS's 100; (3) `terminated_at` as the single, no-`force`-parameter escape hatch.
And add the S4-append WORM gate `18` §4 requires (the one place the Python product violates WORM).

---

## 5. The tight-emulation gotcha list (prioritized)

Ordered by how often clones get them wrong × blast radius. Each is a concrete, testable requirement.

**P0 — data-corruption / silent-wrong-answer class**

- **G1 — ETag correctness under every path.** Emit MD5 ETag only for single-part plaintext/SSE-S3;
  emit `md5(concat(part_md5))-N` for multipart/append; **never** claim an MD5 ETag for a multipart object
  (§3.1). Getting this wrong makes SDK integrity checks and `If-Match` silently misfire. Test: upload
  same bytes single-part vs multipart → different ETags, composite has `-N`. (s3-tests `copy`/multipart.)
- **G2 — Conditional writes: 412 vs 501 vs 409/404 split.** `If-None-Match: *` create-only → 412 on
  conflict, evaluated **transactionally at reserve/finalize, not a TOCTOU pre-read**; `If-Match` write
  mismatch → 412, concurrent-delete races → 409 (PutObject) / 404 (If-Match); drain the body before an
  early 412/501 (§3.2, `06` §2.1). Clones routinely make this a non-atomic pre-check (a data-loss race).
- **G3 — Conditional requests must not be silently ignored.** `If-Match`/`If-Modified-Since`/
  `If-Unmodified-Since` currently accept-and-ignore (`06` §2.3) — that silently corrupts
  optimistic-concurrency clients. Either honor them (recommended, §3.2) or return `501`; never 200-as-if.
- **G4 — List pagination fidelity.** Keys in UTF-8 lexicographic order; `MaxKeys` cap with
  CommonPrefixes counting as one; opaque `NextContinuationToken`; `IsTruncated` correctness; V1
  `NextMarker` only-with-delimiter. A broken continuation token silently truncates a client's view of the
  bucket. (s3-tests `list_objects_v2`; the classic clone bug — https://github.com/seaweedfs/seaweedfs/issues/3166.)

**P1 — interop-breaking class**

- **G5 — `encoding-type=url` must actually encode.** When requested, URL-encode Delimiter/Prefix/Key/
  StartAfter (and Marker/NextMarker in V1) **and** echo the `EncodingType` element in the documented
  position (after Delimiter, before IsTruncated). The most common clone bug is ignoring the param and
  returning raw keys with no `EncodingType` element, which breaks clients with non-ASCII/control-char keys
  (§3.3; https://github.com/boto/boto3/issues/2917).
- **G6 — Virtual-hosted AND path-style addressing.** Resolve the bucket from either `Host` or first path
  segment; canonicalize before dispatch (§3.5). mint `ENABLE_VIRTUAL_STYLE` tests it; many clones only do
  path-style and break SDKs that default to vhost.
- **G7 — Error XML exactness.** Correct `<Code>`→HTTP-status pairing (§3.4), `<Error>` with
  `Code/Message/Resource/RequestId/HostId`, `x-amz-request-id` header, and the extra child elements AWS
  adds (e.g. `Condition`, `Key`, `VersionId`, `BucketName`). SDKs branch on the `Code` string; a wrong
  code or a namespaced `<Error>` (we deliberately omit xmlns for boto3 — `06` §7) breaks error handling.
- **G8 — ListObjectVersions interleaved ordering.** `Version` and `DeleteMarker` interleaved in
  (key, version-id) order; markers omit ETag/Size/StorageClass; `IsLatest` correct (§3.3). A known SDK
  pain point when clones sort wrong (https://github.com/aws/aws-sdk-go-v2/issues/3164).
- **G9 — Response element ORDER in list/complete XML.** Several SDKs are order-strict; reproduce the exact
  element sequence in ListObjectsV2 / ListVersionsResult / CompleteMultipartUploadResult (`06` §8.4).
- **G10 — Namespace-tolerant REQUEST parsing.** Accept both namespaced (aws-cli/botocore) and bare
  (minio-go/mc) request bodies for CompleteMultipartUpload/DeleteObjects/Versioning/Retention/Tagging via
  `local-name()` matching (`06` §8.3). A namespaced-only xpath silently deletes nothing on DeleteObjects.

**P2 — corner-case class**

- **G11 — Presigned SigV4 corners.** Enforce `X-Amz-Expires ≤ 604800`; validate exactly the
  `X-Amz-SignedHeaders`; honor the temp-credential-expiry rule; reject clock-skewed `X-Amz-Date` (§3.5).
- **G12 — Range semantics.** Inverted range (`end<start`) → **full 200**, not 416; unsatisfiable →
  **416 with `Content-Range: bytes */size`** and empty body; single contiguous range only; `x-amz-version-id`
  present on 206 (`06` §4). Clones often 416 on inverted ranges.
- **G13 — Multipart limits & part numbering.** Part 5 MiB–5 GiB (last part no min), ≤10,000 parts,
  consecutive from 1; correct `EntityTooSmall`/`EntityTooLarge`/`InvalidPart`/`InvalidPartOrder` (§3.6).
- **G14 — Copy changes multipart ETag.** A CopyObject of a multipart-origin object yields a **full-object**
  ETag (not the source's `-N` composite) — clients that cache ETags must see the change (§3.1).
- **G15 — `x-amz-checksum-*` accept-without-400.** Even though additional-checksum verification is out of
  scope (§3.1), don't 400 a request that carries a modern `x-amz-checksum-*`/CRC64NVME default header;
  accept-and-ignore like R2, or SDKs that default to CRC64NVME will fail against us.
- **G16 — Object Lock delete-shape split.** `DELETE ?versionId` of a protected version → 403 **before**
  any soft-delete/unpin enqueue; simple `DELETE` → 200 + marker always; delete markers never protected
  (§4.4, `18` §1.2). Assert on the *queue*, not just status (`18` §1.6).

---

## 6. Open decisions — status after this document

| ID | Decision |
|---|---|
| **C1 scope** | RESOLVED — §2.2 in / §2.3 out; 501 for out-of-scope. |
| **C1 bar** | RESOLVED — s3-tests + mint as required CI lanes with a reviewed skip file (§1.4). |
| **D1 (COMPLIANCE vs GOVERNANCE)** | RESOLVED — adopt AWS verbatim; hippius bypass = bucket-owner master token + header (§4.1). |
| **D2 (max retention / Years)** | RESOLVED — AWS max 100y/36,500d; Year=365d; hippius default 7y, hard-capped at 36,500 (§4.2; resolves `18` Q1, Q2). |
| **D3 (default retention retroactivity)** | RESOLVED — NOT retroactive; explicit PUT headers override (§4.3; resolves `18` Q6). |
| **GDPR × COMPLIANCE** | RESOLVED (posture) — contractual, "compliance-mode semantics", no SEC-17a-4 claim, KEK-destruction blocked (§4.6; `18` Q7/Q8). |
| Conditional writes (If-Match, conditional reads) | RECOMMENDED for the rewrite to reach 2024-AWS parity (§3.2) — the largest functional fidelity gap; otherwise 501, never silent-ignore. |
| GetObjectAttributes, real ListMultipartUploads pagination, Suspended versioning, real CORS GET/DELETE | IN SCOPE for the rewrite (§2.2) — each closes a current Python gap (`06` §9). |

---

## Sources

**AWS (canonical, tagged [AWS] inline):**
- Full action list: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
- ETag / integrity: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
- Conditional requests: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html ; reads: .../conditional-reads.html ; writes: .../conditional-writes.html ; deletes: .../conditional-deletes.html
- Conditional-writes launches: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes ; .../2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets/ ; copy 2025: .../2025/10/amazon-s3-conditional-write-functionality-copy-operations
- ListObjectsV2: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html ; ListObjects: .../API_ListObjects.html ; ListObjectVersions: .../API_ListObjectVersions.html
- Error responses: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html ; request IDs: .../get-request-ids.html
- Presigned URLs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html ; 7-day cap: https://github.com/aws/aws-cli/issues/5464
- Addressing / path-style deprecation: https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
- Multipart limits: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html ; https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- Object Lock: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html ; managing: .../object-lock-managing.html ; configure: .../object-lock-configure.html ; API: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html ; enable-on-existing: https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-s3-enabling-object-lock-buckets ; Cohasset: https://d1.awsstatic.com/r2018/b/S3-Object-Lock/Amazon-S3-Compliance-Assessment.pdf ; default-at-scale: https://aws.amazon.com/blogs/storage/applying-amazon-s3-object-lock-at-scale-for-petabytes-of-existing-data/

**Conformance suites & clone compat docs:**
- Ceph s3-tests: https://github.com/ceph/s3-tests ; markers: https://raw.githubusercontent.com/ceph/s3-tests/master/pytest.ini ; framework overview: https://deepwiki.com/ceph/s3-tests/3-s3-api-testing-framework
- MinIO mint: https://github.com/minio/mint ; s3verify context: https://min.io/blog/s3verify-a-simple-tool-to-verify-aws-s3-api-compatibility ; MinIO S3 compat: https://docs.min.io/aistor/developers/s3-api-compatibility/
- Ceph RGW S3 compat: https://docs.ceph.com/en/latest/radosgw/s3/
- Garage S3 compat: https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/
- Cloudflare R2 S3 API: https://developers.cloudflare.com/r2/api/s3/api/ ; extensions: https://developers.cloudflare.com/r2/api/s3/extensions/
- Backblaze B2 S3 API: https://www.backblaze.com/docs/cloud-storage-s3-compatible-api ; https://www.backblaze.com/apidocs/introduction-to-the-s3-compatible-api

**Internal:** [`06-s3-protocol-conformance.md`](./06-s3-protocol-conformance.md);
[`18-object-lock-and-s4.md`](./18-object-lock-and-s4.md).

> **Caveat (per project memory: declarations aren't behavior).** Every clone-compat statement above is
> self-reported vendor documentation. The authoritative parity signal is a **green s3-tests + mint run
> against our own endpoint** (§1.4), not any table — ours included.
