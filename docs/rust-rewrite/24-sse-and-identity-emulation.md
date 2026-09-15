# 24 — SSE Header Emulation & Non-IAM Identity / Credential Scoping

**Status:** research memo + design recommendation, now **RESOLVED** (2026-09-15). The SSE posture is
decided (register **C7**): SSE-S3/`AES256` always-on default, map `aws:kms`→our KEK, reject external
KMS ARNs + SSE-C, ETag = MD5(plaintext); and the sub-token model is decided (R2-style 4-tier ×
bucket-list, enforced fail-closed, no IAM engine). The ⚑ markers and §6 Q1/Q2 below are historical —
read them as the rationale behind those decisions, not as open sign-offs.

**Why this doc exists.** Two open questions from the greenfield design meet here:

1. **SSE posture (new decision).** hippius-s3 *always* envelope-encrypts every blob on the client-invisible
   server side (**per-blob** random DEK — not per-object-version — wrapped under the owner's bucket KEK;
   frozen construction in [`25-crypto-verification.md`](./25-crypto-verification.md)). AWS S3 clients, SDKs, and tools nonetheless send and inspect `x-amz-server-side-encryption*`
   headers. A tight emulation has to decide, header by header, what to **accept, echo, map, silently
   ignore, or reject** — and keep all of it consistent with our internal envelope and our ETag rules.
2. **Sub-token scope model (B1/B2 input).** [`15-account-credential-model.md`](./15-account-credential-model.md)
   §2.4 (⚑ D4) leaves open *how* a sub-token scopes access. This doc surveys how the non-IAM S3 clones
   model credentials/multi-tenancy and recommends a concrete scope model.

**Companions:** [`05-auth-authz-billing.md`](./05-auth-authz-billing.md) (live auth/ACL gate),
[`15-account-credential-model.md`](./15-account-credential-model.md) (identity spine, D1–D8),
[`25-crypto-verification.md`](./25-crypto-verification.md) (the FROZEN envelope our SSE posture must respect — per-blob DEK, CTX frames, opaque `blob_id` AAD).

---

## 0. TL;DR and decision list

### SSE posture (Part A)

Because we **always** envelope-encrypt, the honest AWS analogue is **SSE-S3**: server-managed keys,
transparent to the client, on by default. The whole SSE surface should be presented through that lens.

- **⚑ S1 — Advertise SSE-S3 (`AES256`) as the effective, always-on default.** Accept the `AES256`
  request header as a no-op confirmation of what we already do, and **echo `x-amz-server-side-encryption:
  AES256` on every mutating response and every GET/HEAD** — mirroring AWS's post-2023-01-05 behavior where
  every object is SSE-S3 by default and the header is returned on all objects.
  ([AWS: default encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html),
  [AWS: SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html))
- **⚑ S2 — Accept `aws:kms` and MAP it to our envelope/KEK; reject *external* KMS key material with a
  clear error.** Treat a bare `x-amz-server-side-encryption: aws:kms` (no external ARN, or an id that
  names one of *our* KEKs) as an alias for our KEK-wrapped envelope, and echo `aws:kms` +
  `x-amz-server-side-encryption-aws-kms-key-id: <our KEK id>`. A caller-supplied *external* AWS KMS ARN
  we cannot honor → **fail loudly** (400) rather than silently encrypt under our own key and lie in the
  echo. This matches how MinIO/Ceph expose real KMS while we expose our KEK. Do **not** stand up a real
  per-request KMS in v1.
- **⚑ S3 — Reject SSE-C in v1 with a clear, S3-shaped error; keep it as a documented future.** A
  customer-provided key (`x-amz-server-side-encryption-customer-*`) demands that the *client's* key be the
  actual data key — which collides with our always-on envelope (double encryption), breaks O(1)
  CopyObject / cross-object dedup (the reference DEK must be recoverable under the owner KEK,
  [`crypto-dedup-research.md`](./crypto-dedup-research.md) Q2/Q5), and changes ETag semantics. R2 and B2
  *do* offer SSE-C, so this is a deliberate scope cut, not an oversight — revisit as "wrap the reference
  DEK under the client key" only if a customer needs it.
- **⚑ S4 — Implement bucket default-encryption config as a coherent stub.** Support
  `PutBucketEncryption` / `GetBucketEncryption` / `DeleteBucketEncryption` returning at least the
  `AES256` default rule, so tooling and compliance scanners that *read back* the encryption config see a
  consistent answer. Accept `BucketKeyEnabled` and `x-amz-server-side-encryption-context` on the wire and
  echo/persist them, but treat them as no-ops against our single envelope.
- **⚑ S5 — ETag is computed over PLAINTEXT and is independent of our envelope.** Single-part ETag =
  MD5(plaintext); multipart ETag = the AWS composite `md5(concat(part-md5s))-N`. This is exactly AWS's
  **SSE-S3** rule (ETag stays content MD5) and is what keeps `aws s3 sync`, multipart, and integrity
  checks working. Our server-side envelope must never leak into the ETag. (If we ever accept SSE-C/KMS
  with real per-object keys we would have to switch those objects to non-MD5 ETags, per AWS — another
  reason to stay SSE-S3-shaped.)

### Credential scoping (Part B)

- **⚑ I1 — Port the R2-style 4-tier × bucket-list scope model as the v1 spine (resolves D4 toward
  "enforce").** It already exists as a frozen contract in Python (`sub_token_scope.py`,
  [`05`](./05-auth-authz-billing.md) §2.6) and it is the *median* of the clone field: R2, Garage, and
  the B2 default are all **bucket-grained** permission sets. Bucket + permission-tier "feels" AWS/clone-like
  and is enough for v1.
- **⚑ I2 — Do NOT build an IAM policy engine in v1; do reserve prefix-scoping as the one additive axis.**
  Only MinIO (full AWS-IAM JSON) and AWS itself express per-prefix/condition rules. B2 offers a single
  `namePrefix`. Keep the door open for a B2-style single-prefix restriction on a scope row, but ship
  bucket-grained first. A `Deny`/`Condition`/multi-statement policy language is explicitly out of scope
  (matches the "no real IAM policy engine" reality in [`05`](./05-auth-authz-billing.md) §2.5).

---

## Part A — Server-Side Encryption

## 1. The AWS SSE model (the spec we emulate)

AWS exposes four server-side modes. All encrypt at rest with AES-256; they differ in **who holds the
key** and **which headers travel**. ([AWS: protecting data with SSE](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html))

### 1.1 SSE-S3 — S3-managed keys (the default)

- **Request header:** `x-amz-server-side-encryption: AES256` (optional; it is now the default).
- **Since 2023-01-05, every new object is SSE-S3 by default**, at no cost, and the response header is
  returned on all objects. This is the crucial fact for us: "we always encrypt, keys are ours,
  transparent to the client" **is** SSE-S3.
  ([AWS: default encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html),
  [AWS: SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html))
- **Response header echoed:** `x-amz-server-side-encryption: AES256` on PUT, POST, CopyObject, and
  GET/HEAD.
- **ETag:** unchanged from plaintext — a single-part SSE-S3 object's ETag **is its content MD5**.
  ([storj.io/minio etag pkg](https://pkg.go.dev/storj.io/minio/pkg/etag))
- **⚠ Do not send SSE request headers on GET/HEAD for an SSE-S3 object** — AWS returns `400 Bad Request`.
  ([AWS: specifying SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-encryption.html))

### 1.2 SSE-KMS — KMS-managed keys (+ S3 Bucket Keys)

- **Request headers:** `x-amz-server-side-encryption: aws:kms` (or `aws:kms:dsse` for dual-layer),
  optional `x-amz-server-side-encryption-aws-kms-key-id` (KMS key ARN/id),
  `x-amz-server-side-encryption-context` (an encryption-context JSON), and
  `x-amz-server-side-encryption-bucket-key-enabled` (opt into S3 Bucket Keys, which cut KMS request cost
  by deriving a bucket-level data key instead of calling KMS per object).
  ([AWS: specifying SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html),
  [AWS: SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html))
- **Response headers echoed:** `x-amz-server-side-encryption: aws:kms`,
  `x-amz-server-side-encryption-aws-kms-key-id`, and `x-amz-server-side-encryption-bucket-key-enabled`.
- **Envelope model:** per-object random DEK wrapped by the KMS CMK; **CopyObject re-wraps the data key
  and does not re-encrypt object bytes** — this is the exact template our Option A envelope already
  follows ([`crypto-dedup-research.md`](./crypto-dedup-research.md) Q2).
- **ETag:** **not** the content MD5 for single-part; unpredictable to the client.
  ([storj.io/minio etag pkg](https://pkg.go.dev/storj.io/minio/pkg/etag),
  [AWS Object API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html))
- Note AWS **does not validate the KMS key id** in `PutBucketEncryption` at config time.
  ([AWS: PutBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html))

### 1.3 SSE-C — customer-provided keys

- **Request headers (every request that touches the object, incl. GET/HEAD/CopyObject):**
  `x-amz-server-side-encryption-customer-algorithm: AES256`,
  `x-amz-server-side-encryption-customer-key` (base64 of the 256-bit key),
  `x-amz-server-side-encryption-customer-key-MD5` (base64 MD5 of the key, an integrity check on the key
  transmission — **not** the object). Copy uses the `x-amz-copy-source-server-side-encryption-customer-*`
  trio for the source.
  ([AWS: SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html))
- **The server stores the key's MD5 (to verify future requests) but never the key itself.**
- **Response headers echoed:** `x-amz-server-side-encryption-customer-algorithm` and
  `-customer-key-MD5` (never the key).
- **ETag:** **not** the content MD5.
  ([AWS: SSE-C key MD5](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html))
- **SSE-C does not participate in bucket default-encryption** — it is per-request only (confirmed by MinIO,
  below).

### 1.4 Bucket default encryption

`PutBucketEncryption` / `GetBucketEncryption` / `DeleteBucketEncryption` set a
`ServerSideEncryptionConfiguration` of one `Rule` → `ApplyServerSideEncryptionByDefault{ SSEAlgorithm ∈
{AES256, aws:kms, aws:kms:dsse}, KMSMasterKeyID? }` + `BucketKeyEnabled?`. A PUT with no SSE header
inherits this default; a PUT *with* an SSE header overrides it (this is also MinIO's "auto-encryption only
affects requests without S3 encryption headers"). ([AWS: PutBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html),
[AWS: configuring default encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html))

### 1.5 Response-header echo matrix (what AWS returns)

| Mode | `x-amz-sse` | `…-aws-kms-key-id` | `…-bucket-key-enabled` | `…-customer-algorithm` | `…-customer-key-MD5` | ETag = content MD5? |
|------|-------------|--------------------|------------------------|------------------------|----------------------|---------------------|
| SSE-S3 | `AES256` | — | — | — | — | **Yes** (single-part) |
| SSE-KMS | `aws:kms` | ✔ | ✔ | — | — | No |
| SSE-C | — | — | — | `AES256` | ✔ | No |

### 1.6 The single most important consistency rule for us

**ETag is a function of the plaintext and the upload shape (single vs multipart), not of the encryption
layer, *for SSE-S3*.** Since our posture is SSE-S3-shaped (S1), our ETag must be MD5(plaintext) for
single-part and the `…-N` composite for multipart — identical to a plaintext bucket. Our internal
envelope (DEK/KEK/AAD/key-commitment) is invisible to the ETag. Any object we ever let a *real* per-object
external key touch (SSE-C/KMS) would have to move to a non-MD5 ETag to match AWS — a strong reason to keep
everything SSE-S3-shaped. ([storj.io/minio etag pkg](https://pkg.go.dev/storj.io/minio/pkg/etag),
[AWS Object API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html))

---

## 2. How tight clones that encrypt internally expose SSE

| Product | At-rest default | SSE-S3 (`AES256`) | SSE-KMS (`aws:kms`) | SSE-C | Notes |
|---|---|---|---|---|---|
| **AWS S3** | SSE-S3 since 2023-01-05 | ✔ default | ✔ (real KMS + Bucket Keys) | ✔ | The spec. ([AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html)) |
| **MinIO** | optional; **auto-encryption** encrypts all if KMS configured | ✔ (one deployment EK via KES) | ✔ (**recommended**, per-request key id, most granular) | ✔ but **no bucket-default**, client does all key mgmt | Auto-encryption "only affects requests without S3 encryption headers." KES fronts an external KMS. ([MinIO SSE](https://min.io/docs/minio/kubernetes/openshift/administration/server-side-encryption.html), [MinIO KMS README](https://github.com/minio/minio/blob/master/docs/kms/README.md)) |
| **Ceph RGW** | optional; `rgw_crypt_default_encryption` can force SSE-S3 | ✔ (server keys in KMS backend) | ✔ (Vault etc., per-request key id) | ✔ (client key per request, not stored) | Enforce via bucket policy. Was the CVE-2026-54330 SigV4 unsigned-header locus ([`15`](./15-account-credential-model.md) §2.3). ([Ceph SSE-S3](https://oneuptime.com/blog/post/2026-03-31-rook-set-up-sse-s3-ceph-rgw/view), [Ceph SSE-KMS](https://oneuptime.com/blog/post/2026-03-31-rook-rgw-sse-kms-backend/view)) |
| **Cloudflare R2** | **always AES-256 at rest, Cloudflare-managed keys** | *implicit* — R2 is always encrypted; SSE-S3 header not a differentiator | **Not supported** (no AWS KMS) | ✔ (S3 API + Workers), key MD5 "for identification only" | The closest analogue to us: "regardless of whether you use SSE-C, data is always encrypted at rest." ([R2 SSE-C](https://developers.cloudflare.com/r2/examples/ssec/), [R2 vs S3](https://runcloud.io/blog/cloudflare-r2-vs-aws-s3)) |
| **Backblaze B2** | **SSE-B2 (Backblaze keys) by default on all new uploads** | ✔ as "SSE-B2" (AES-256, per-file key wrapped by global key) | **Not supported** | ✔ (customer key wraps the per-file key) | Both S3-compatible + native APIs. ([B2 SSE](https://www.backblaze.com/docs/cloud-storage-server-side-encryption), [B2 default enc](https://www.backblaze.com/blog/backblaze-b2-to-encrypt-new-uploads-by-default/)) |

**Takeaways that anchor our posture.**

- **R2 and B2 — the two "we always encrypt internally" clones — are our closest precedent.** Neither
  offers SSE-KMS (no external KMS to honor). R2 treats internal encryption as *implicit* and only exposes
  SSE-C as an *additional* customer layer on top of the always-on baseline; B2 brands its always-on layer
  as "SSE-B2." Both prove that "always-encrypt + expose a subset of SSE headers" is a shipped, coherent
  posture — exactly S1.
- **MinIO/Ceph offer full SSE-KMS only because they front a real external KMS (KES/Vault).** We do not,
  so honoring an external KMS ARN would be a lie; mapping `aws:kms` to our own KEK (S2) is the honest
  middle path, and rejecting an *external* ARN mirrors R2/B2's "not supported."
- **SSE-C is universally per-request and never part of bucket defaults** — if we ever add it, it must be
  request-scoped and cannot flow through `PutBucketEncryption`.

---

## 3. Recommended SSE posture for hippius-s3

### 3.1 Request headers to accept, and how (⚑ S1–S3)

| Request header | Posture | Behavior |
|---|---|---|
| `x-amz-server-side-encryption: AES256` | **Accept (no-op confirm)** | We already envelope-encrypt; treat as "yes, SSE-S3." Echo `AES256`. |
| `x-amz-server-side-encryption: aws:kms` (no external key, or our KEK id) | **Accept → map to KEK** | Alias for our envelope. Echo `aws:kms` + our KEK id. |
| `x-amz-server-side-encryption-aws-kms-key-id: <external ARN>` | **Reject (400)** | We cannot honor a foreign KMS key. Clear `KMSKeyNotFound`-shaped error, do not silently substitute our KEK. |
| `x-amz-server-side-encryption-context` | **Accept, persist, ignore** | Store for echo/audit; no effect on our single envelope. |
| `x-amz-server-side-encryption-bucket-key-enabled` | **Accept, echo, no-op** | Our KEK is already a bucket-level key; the optimization is intrinsic. |
| `x-amz-server-side-encryption-customer-*` (SSE-C trio) | **Reject (400/501)** | Clear error (e.g. `NotImplemented`/`InvalidRequest`). See §3.4. |
| `aws:kms:dsse` (dual-layer) | **Reject or alias to `aws:kms`** | No second real layer; simplest is reject with a clear message. |

### 3.2 Response headers to echo

- **Always** echo `x-amz-server-side-encryption: AES256` (or `aws:kms` when the caller asked for and we
  mapped KMS) on PUT, POST (incl. CompleteMultipartUpload), CopyObject, GET, and HEAD — mirroring AWS's
  post-2023 always-on echo. Tools and compliance scanners assert on this header's *presence*.
- When we mapped `aws:kms`, also echo `x-amz-server-side-encryption-aws-kms-key-id: <our KEK id>` and
  `x-amz-server-side-encryption-bucket-key-enabled: true`.
- **Never** echo a customer key; never echo SSE-C headers (we reject SSE-C).

### 3.3 Bucket default-encryption config (⚑ S4)

- Implement `PutBucketEncryption` / `GetBucketEncryption` / `DeleteBucketEncryption`.
- Accept and persist a `Rule` with `SSEAlgorithm ∈ {AES256, aws:kms}` (map both to our envelope) +
  optional `KMSMasterKeyID` (must name our KEK or be absent; reject external ARNs) + `BucketKeyEnabled`.
- `GetBucketEncryption` on a bucket with no explicit config returns the **implicit `AES256` default**
  (AWS returns SSE-S3 as the baseline for every bucket) so scanners never see "no encryption."
- A PUT with an explicit SSE header overrides the bucket default (AWS/MinIO semantics); a PUT without one
  inherits it. Since every path lands in the same envelope, this is bookkeeping for the echo, not two
  code paths.

### 3.4 Why reject SSE-C in v1 (and the future path)

Accepting SSE-C would require the **client's** key to be the real data key. Against our design that means:

1. **Double encryption** (client key layer *and* our always-on envelope) — wasteful and confusing, or a
   special "envelope-off for SSE-C objects" path that fractures the storage model.
2. **Breaks O(1) CopyObject and cross-object dedup.** Option A relies on the reference/blob DEK being
   recoverable under the **owner KEK** so a copy just re-wraps the DEK
   ([`crypto-dedup-research.md`](./crypto-dedup-research.md) Q2/Q5). If the DEK is wrapped only under a
   client-held key, copy/refcount need that key present on every operation, and the blob can't be shared.
3. **Changes ETag semantics** to non-MD5 (§1.6), diverging from our SSE-S3-shaped ETag rule.

R2/B2 support SSE-C because it *layers on top of* their baseline; we could do the same later by **wrapping
the per-object reference DEK under the client-supplied key** (instead of, or in addition to, the owner
KEK), accepting that such objects lose cross-object dedup and take non-MD5 ETags. Ship the clear rejection
now; treat the DEK-wrap approach as scoped future work.

### 3.5 Must-expose vs may-stub

| Must expose (a tight emulation breaks without it) | May stub / reject cleanly |
|---|---|
| Accept + **echo `x-amz-server-side-encryption: AES256`** everywhere | External SSE-KMS ARNs (reject 400) |
| **Never 400** a GET/HEAD that omits SSE headers | SSE-C (reject 400/501) |
| Correct **ETag = plaintext MD5 / `…-N` composite** | `aws:kms:dsse` dual-layer (reject/alias) |
| `Put/Get/DeleteBucketEncryption` returning a coherent `AES256` default | `bucket-key-enabled` (accept + echo, no-op) |
| Map `aws:kms` (bare / our-KEK-id) → envelope + honest echo | `encryption-context` (accept, store, ignore) |

---

## Part B — Non-IAM Identity & Credential Scoping

## 4. How non-IAM S3 products scope credentials (prior art)

| Product | Credential | Scope grain | Permission model | Prefix scoping? | Policy language? |
|---|---|---|---|---|---|
| **MinIO** | access key / secret (or STS) | Full AWS ARN resources | **PBAC = AWS-IAM-compatible JSON**; deny-by-default, explicit `Deny` beats `Allow` | ✔ (via resource ARN) | ✔ (subset of IAM) ([MinIO PBAC](https://docs.min.io/aistor/administration/iam/access/)) |
| **Cloudflare R2** | S3 access key from an **API token** | **Bucket** (or account) | Tiers: **Admin R/W**, **Admin R-only** (account-wide, *not* bucket-scoped), **Object R/W**, **Object R-only** (bucket-scopable) | ✘ (bucket-level only) | ✘ ([R2 tokens](https://developers.cloudflare.com/r2/api/tokens/)) |
| **Backblaze B2** | application key | **Bucket** (or multi-bucket v4) **+ single `namePrefix`** | capabilities list (read/write/list/delete/…) | ✔ (one `namePrefix`) | ✘ ([B2 app keys](https://www.backblaze.com/docs/cloud-storage-application-keys), [b2_create_key](https://www.backblaze.com/apidocs/b2-create-key)) |
| **Garage** | access key / secret | **Bucket** (per key-per-bucket) | flags: **read / write / owner** | ✘ (bucket-level only) | ✘ (no ACLs/policies at all) ([Garage bucket ops](https://deepwiki.com/deuxfleurs-org/garage/8.3-bucket-and-key-operations)) |
| **hippius (today, Python)** | `hip_` sub-token → SS58 | **Bucket-list** | R2-style 4 tiers: `admin_read_write` / `admin_read` / `object_read_write` / `object_read` | ✘ | ✘ ([`05`](./05-auth-authz-billing.md) §2.6) |

**Observations.**

- **Bucket + permission-tier is the clone median.** R2, Garage, and B2's default are all bucket-grained.
  Only MinIO (full IAM JSON) and AWS express per-prefix/condition rules; B2 offers exactly **one**
  `namePrefix` as its single prefix knob.
- **hippius's existing Python sub-token model is already R2's model** — it copies R2's four tiers almost
  verbatim. We are not inventing anything by porting it; we are matching the field.
- **`Deny`, conditions, principals, multi-statement policies exist only in MinIO/AWS.** Every lightweight
  clone omits them, and hippius's "bucket public flag" reality ([`05`](./05-auth-authz-billing.md) §2.5)
  already reflects that.

## 5. Recommended sub-token scope model (⚑ I1–I2; resolves D4/B1/B2)

**Port the R2 4-tier × bucket-list model as the v1 spine, enforced (not stubbed).** This is D4 option (1)
in [`15`](./15-account-credential-model.md) §2.4, and this survey is the corroboration for choosing it:
it is a frozen, ~190-line pure-logic contract, it stores its rows in *our* DB (`api_credentials`,
[`12-schema-design.md`](./12-schema-design.md) §2.2: `access_key_id, account_id, permission, bucket_scope,
bucket_ids`), and it is the median clone posture, so it "feels" AWS/clone-like to SDK users.

Concrete scope shape for a sub-token row:

```
SubTokenScope {
    access_key_id: hip_…            // the sub key
    account_id:    SS58             // same tenant as its master (15 §2.4)
    permission:    admin_read_write | admin_read | object_read_write | object_read
    bucket_scope:  all | list       // 'all' required to create_bucket
    bucket_ids:    [uuid, …]        // when scope == list
    // ⚑ I2 future axis (do NOT build v1): name_prefix: Option<String>  (B2-style, one prefix)
}
```

- **Enforcement is fail-closed** (any scope-cache/DB miss → deny), matching the live Python
  `hippius_subscope:` behavior ([`05`](./05-auth-authz-billing.md) §2.6). Never "sub accepted, scope
  ignored" — that silently grants full-account access because a sub resolves to the same SS58 as its
  master ([`15`](./15-account-credential-model.md) ⚑ D4).
- **`required_op` maps method+query → op**, `create_bucket` requires `bucket_scope == all`, and
  cross-account subs fall through to the bucket-ACL grant scan (the "contractor" pattern) — all already
  specified in [`05`](./05-auth-authz-billing.md) §2.6. Keep it.
- **Prefix scoping (I2) is the single sanctioned future axis**, modeled on B2's one `namePrefix`. Ship
  bucket-grained first; add a single optional `name_prefix` per scope row only if a customer needs
  `photos/public/*`-style narrowing. Do **not** build a `Deny`/condition/multi-statement IAM engine —
  no lightweight clone has one and hippius already lacks a real policy engine
  ([`05`](./05-auth-authz-billing.md) §2.5).
- **The permission-tier vocabulary maps cleanly onto S3 ops** the way R2's does (Object-R/W vs
  Admin-R/W), so tooling that reasons about "read-only vs read-write keys" behaves as expected.

---

## 6. Open questions

1. **⚑ S2 boundary.** Do we accept `aws:kms` *at all*, or reject every `aws:kms` and only accept `AES256`?
   Accepting-and-mapping is more compatible (some tooling defaults to `aws:kms`); rejecting is simpler and
   more honest. Recommend accept-and-map for bare/our-KEK-id, reject external ARNs.
2. **⚑ S3 timing.** Confirm SSE-C stays rejected in v1. If a launch customer needs it, the DEK-wrap path
   (§3.4) is the design — but it forfeits dedup/O(1)-copy for those objects.
3. **DSSE (`aws:kms:dsse`).** Reject outright, or silently alias to `aws:kms` (our single envelope)?
   Aliasing risks over-claiming "dual-layer"; recommend reject.
4. **Bucket-default persistence.** Where does the `ServerSideEncryptionConfiguration` live — a
   `bucket_encryption` column/table in our Postgres ([`12`](./12-schema-design.md))? Needed for
   `GetBucketEncryption` round-trips even though it never changes our envelope.
5. **⚑ I2 prefix scope.** Commit to a single B2-style `name_prefix` axis as the *only* future scope
   extension, or leave prefix scoping entirely out until a full policy story is designed?
6. **Encryption-context echo fidelity.** If we accept and store `x-amz-server-side-encryption-context`,
   do we echo it back on GET/HEAD (AWS does not echo context on read)? Recommend store-for-audit, do not
   echo.

---

### Primary sources

**AWS SSE model**
- [Protecting data with server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html) ·
  [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html) ·
  [Specifying SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-encryption.html)
- [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html) ·
  [Specifying SSE-KMS + Bucket Keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html)
- [SSE-C (customer keys)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html)
- [Default bucket encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-bucket-encryption.html) ·
  [PutBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html)
- ETag semantics: [AWS Object API (ETag)](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html) ·
  [minio/storj etag package](https://pkg.go.dev/storj.io/minio/pkg/etag)

**Clones that encrypt internally**
- MinIO: [Server-side encryption](https://min.io/docs/minio/kubernetes/openshift/administration/server-side-encryption.html) ·
  [KMS/KES README](https://github.com/minio/minio/blob/master/docs/kms/README.md)
- Ceph RGW: [SSE-S3](https://oneuptime.com/blog/post/2026-03-31-rook-set-up-sse-s3-ceph-rgw/view) ·
  [SSE-KMS backend](https://oneuptime.com/blog/post/2026-03-31-rook-rgw-sse-kms-backend/view)
- Cloudflare R2: [SSE-C example](https://developers.cloudflare.com/r2/examples/ssec/) ·
  [R2 vs S3 (always-encrypted at rest)](https://runcloud.io/blog/cloudflare-r2-vs-aws-s3)
- Backblaze B2: [Server-side encryption](https://www.backblaze.com/docs/cloud-storage-server-side-encryption) ·
  [Default encryption for new uploads](https://www.backblaze.com/blog/backblaze-b2-to-encrypt-new-uploads-by-default/)

**Non-IAM credential scoping**
- MinIO PBAC: [Access control with policy management](https://docs.min.io/aistor/administration/iam/access/) ·
  [IAM overview](https://docs.min.io/aistor/administration/iam/)
- Cloudflare R2: [API tokens / permissions](https://developers.cloudflare.com/r2/api/tokens/)
- Backblaze B2: [Application keys](https://www.backblaze.com/docs/cloud-storage-application-keys) ·
  [b2_create_key](https://www.backblaze.com/apidocs/b2-create-key) ·
  [b2_authorize_account](https://www.backblaze.com/apidocs/b2-authorize-account)
- Garage: [Bucket and key operations](https://deepwiki.com/deuxfleurs-org/garage/8.3-bucket-and-key-operations)

*Cross-repo: `hippius-s3` = this repo; `hcfs` = `/Users/camden/Source/hcfs` (SigV4 harvest, KEK/envelope
groundwork).*
