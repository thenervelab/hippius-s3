# Object versions — AWS S3 parity

Status of S3 object versioning in `hippius-s3`: what we store, what we expose, what was broken, and
what this repo now implements.

Investigation date: **2026-08-22**. Findings were verified live against production (`s3.hippius.com`)
with the `aws` CLI, and with read-only queries against the `postgres-nvme-5` replica.

---

## 1. Summary

`hippius-s3` has always had a **fully working versioned storage engine**. It did not have a
versioning *API*, and its delete path did not respect version identity.

Every `PutObject`, `CompleteMultipartUpload`, and `CopyObject` onto an existing key allocates a new
`object_versions` row and bumps `objects.current_object_version`. Superseded versions keep their own
parts, chunks, and DEK envelope, and `GET`/`HEAD` with `?versionId=N` have always served them
correctly.

What was missing was everything that makes those versions *usable*: no way to turn versioning on, no
way to list what exists, no delete markers, and a `DELETE` that ignored the version you asked it to
delete.

---

## 2. What AWS gives you

Versioning is a **bucket-level** setting with three states — unversioned (default), `Enabled`, and
`Suspended`. Once enabled a bucket can never return to unversioned; it can only be suspended.

Enabling it opens up:

| Capability | What it does |
|---|---|
| `PutBucketVersioning` / `GetBucketVersioning` | The on/off switch. |
| Version IDs | Returned as `x-amz-version-id` on every write. Objects predating versioning have the literal ID `null`. |
| `ListObjectVersions` (`GET /bucket?versions`) | The discovery API — every version *and* every delete marker, with `IsLatest`. |
| `?versionId=` on `GET`/`HEAD` | Read any specific version. |
| Delete markers | A simple `DELETE` inserts a marker as the new current version. The object 404s but nothing is destroyed. |
| `DELETE ?versionId=` | Permanently destroys exactly one version. The only way to actually free bytes. |
| `DeleteObjects` with per-key `VersionId` | Bulk version deletion. |
| `CopyObject` from `source?versionId=` | The standard "restore an old version" move. |
| Lifecycle `NoncurrentVersionExpiration` | Cost control. Every version bills as a full object, not a diff. |
| MFA delete | Optional hard gate on version deletion. |

---

## 3. What we had before this change

### 3.1 Worked

`GET` and `HEAD` with `?versionId=N` resolved the exact version, returned `x-amz-version-id`, and
produced a correct `NoSuchVersion` for a missing one. Verified on prod:

```
PUT doc.txt ×3           ->  HEAD returns VersionId: "3"
GET --version-id 1       ->  "VERSION ONE"      ✅
GET --version-id 2       ->  "VERSION TWO xx"   ✅
GET --version-id 99      ->  NoSuchVersion      ✅
```

### 3.2 Did not work

| Operation | Behaviour on prod |
|---|---|
| `get-bucket-versioning` | Fell through to `ListObjects`; returned a `ListBucketResult` body. |
| `put-bucket-versioning` | Fell through to `handle_create_bucket` → **`BucketAlreadyExists`**. |
| `list-object-versions` | Same fallthrough. **Versions were undiscoverable** — reachable only by guessing integers. |
| `delete-objects` with `VersionId` | Per-key `NotImplemented: "Versioning not supported"`. |
| `copy-object` from `?versionId=N` | **Silently copied the current version** (`copy_helpers.py` split the query string off wholesale). |
| `put-object` | No `x-amz-version-id` in the response. |

### 3.3 Actively dangerous

**`DELETE ?versionId=N` ignored the version and destroyed the entire object.** Verified on prod:

```
DELETE doc.txt?versionId=1   ->  204
GET doc.txt                  ->  NoSuchKey      <- current version gone
GET doc.txt --version-id 2   ->  NoSuchVersion
GET doc.txt --version-id 1   ->  NoSuchVersion
```

A client following AWS semantics to prune one old version instead deleted its live object. Unlike
the bulk endpoint, which at least returned `NotImplemented`, the single-object path gave no signal.

**Deleted data came back.** Re-`PUT` to a deleted key revived the same row (`deleted_at = NULL`,
version bumped) and every prior version became readable again:

```
DELETE doc.txt                ->  204
PUT    doc.txt (new content)  ->  becomes VersionId 4
GET    doc.txt --version-id 1 ->  "VERSION ONE"    <- resurrected
GET    doc.txt --version-id 3 ->  "VERSION THREE"  <- resurrected, already enqueued for unpin
```

Combined with sequential integer version IDs, anyone with read access could walk a bucket's entire
overwrite history — including content the owner believed they had deleted.

### 3.4 The storage leak

`handle_delete_object` enqueued an `UnpinChainRequest` for `current_object_version` only, and
`get_chunk_backend_identifiers.sql` filters on that version. Nothing automatically unpinned
superseded versions — only three manual ops scripts ever touched them.

`hard_delete_object.sql`'s readiness gate requires **no live `chunk_backend` row across all
versions** (it joins `parts` on `object_id`, unfiltered by version). Superseded versions kept live
rows forever, so the gate never passed. The 24h aged relaxation does not help — it only applies when
there are *zero* `chunk_backend` rows.

Result: **any object overwritten at least once and then deleted became permanently
un-hard-deletable.** Its `parts` never cascaded, its FS bytes stayed pinned, and its backend data was
never reclaimed.

---

## 4. Production scale of the problem

Measured 2026-08-22 on the `postgres-nvme-5` replica using `TABLESAMPLE` and bounded keyset probes.
(Always the replica, never the primary — see the read-storm postmortem.)

**Scale**: 139.4M `objects` (141 GB), 144.4M `object_versions` (74 GB), 336M `chunk_backend` (64 GB),
39,407 buckets.

**Version accumulation** (0.5% sample, 693k objects):

| Metric | Value | Extrapolated |
|---|---|---|
| Objects with >1 version | 0.92% | ~1.28M |
| Average `current_object_version` | 1.024 | — |
| p99.99 versions | 38 | — |
| **Max versions on one key** | **1,443** | — |
| Superseded version rows | 5.3% | ~7.6M |

**Superseded storage: ~25 TB.** Stable at 24–27 GB per 0.1% sample across three seeds. The
*percentage* of total bytes swings 15–23% between seeds because the denominator is dominated by rare
very large objects; the superseded figure itself is stable.

**The stuck backlog.** Of the **200 oldest** soft-deleted multi-version objects:

- Deleted **2026-02-11 to 2026-02-16** — six months stale, against a 1-hour grace period.
- **200/200** still hold live `chunk_backend` rows.
- **103/200 (51.5%)** hold them on *superseded* versions — exactly what the unpin path never targeted.

~70k objects are both deleted and multi-version, at ~81 MB of superseded data each: roughly
**5.6 TB structurally unreclaimable**.

For context, the wider soft-delete backlog is larger than versioning alone explains — ~1.5M
soft-deleted objects, 98.9% older than 30 days. Versioning-stuck rows are a subset; the rest has
other causes.

---

## 5. What this change implements

Everything new is gated on a per-bucket `versioning_status`, which is `NULL` for all 39,407 existing
buckets. Their behaviour is unchanged. `is_delete_marker` defaults to `false` on all 144M existing
version rows, so every query change is a no-op for existing data.

Only two changes apply to **every** bucket, because both are bug fixes: the versioned-DELETE data
loss, and the unpin leak.

| Area | Delivered |
|---|---|
| `PutBucketVersioning` / `GetBucketVersioning` | `Status=Enabled` persists. `Suspended` returns 501 (see §6). |
| `ListObjectVersions` | `GET /bucket?versions` with `prefix`, `delimiter`, `key-marker`, `version-id-marker`, `max-keys`, `encoding-type`, and real `(key, version)` keyset pagination. |
| Delete markers | Simple `DELETE` on an `Enabled` bucket inserts a marker. `GET`/`HEAD` 404 with `x-amz-delete-marker: true`; an explicit `?versionId` on a marker returns 405. |
| Versioned `DELETE` | Deletes **only** the named version, rolling the current pointer back to the next-newest. Deleting a marker is an undelete. |
| `DeleteObjects` | Accepts per-key `VersionId`, and reports `VersionId` / `DeleteMarkerVersionId` the way AWS splits them. |
| `CopyObject` / `UploadPartCopy` | Honour `?versionId=` on `x-amz-copy-source` — version restore works, including for sources large enough to need a multipart copy. |
| `x-amz-version-id` | Returned on `PutObject`, `CopyObject`, and `CompleteMultipartUpload`; `x-amz-copy-source-version-id` on copies. |
| Unpin leak | Object delete enqueues one unpin covering **every** version. |
| Backlog repair | `scripts/backfill_superseded_version_unpins.py` (dry-run by default) + a k8s Job. |

### 5.1 Version ID format

Version IDs remain **decimal integers**, not opaque AWS-style tokens.

`object_version` is a `bigint` primary-key component referenced by `parts`, the FS cache path layout
(`v<version>/`), the crypto AAD (`hippius-dek:{bucket_id}:{object_id}:{version}`), unpin requests,
and the Rust drain agent. Migrating to opaque tokens would mean rewriting a 144M-row table and every
cache path on disk.

It is also not required: S3 clients treat `VersionId` as an opaque string, so integers are spec-
compatible as long as they are unique and stable. The literal `"null"` is additionally accepted as an
alias for the current version, for clients that send it against an unversioned bucket.

### 5.2 The subtle part: the "serveable" predicate

`size_bytes > 0 OR (md5_hash IS NOT NULL AND md5_hash != '')` is the repo-wide signal for "this
version is complete, not a reserved multipart placeholder".

A delete marker has size 0 and no md5, so it would be **skipped** by that predicate — and the query
would silently fall back to the previous content version, serving deleted data. Every query using it
had to become marker-aware: resolve the newest version matching `(serveable OR is_delete_marker)`,
then drop the key (listing) or 404 (read) when that version is a marker.

This is the highest-risk part of the change. It touches `list_objects.sql`,
`list_objects_delimited.sql`, `get_object_for_download_with_permissions.sql`, and
`get_object_by_path.sql` — the hottest read and list paths in the system.

### 5.3 Why a version DELETE does not delete rows synchronously

The unpinner resolves backend identifiers *at processing time*, by joining
`parts`/`part_chunks`/`chunk_backend`. Deleting those rows in the request handler would leave the
queued unpin with nothing to find — leaking the backend data, which is the exact bug being fixed.

So a version DELETE marks a new `object_versions.deleted_at`, enqueues the unpin, and repoints
`current_object_version`. A janitor sweep then reclaims the `parts` rows once every `chunk_backend`
row is confirmed deleted, on the same two-tier readiness gate the object-level hard delete uses:
no live backend copy, **and** either evidence the version was replicated at all or a 24-hour age.
That second tier matters — a version deleted while its upload is still queued has *zero*
`chunk_backend` rows, and reaping it there would strand the upload and orphan the bytes on Arion.

The `object_versions` row itself is deliberately kept as a tombstone. Version allocation is
`GREATEST(current_object_version, MAX(object_version)) + 1`, and a versioned DELETE moves
`current_object_version` *down*; removing the row would let `MAX` drop too and re-mint a version
number that already existed — colliding with stale FS cache under `v<version>/` and letting a
queued unpin target live data. The tombstone costs one narrow row and is removed when the object is
hard-deleted.

### 5.4 Why unpinning superseded versions cannot destroy live data

Deleting one version of a *live* object is the first path that unpins a version while siblings
remain readable, so it depends on backend identifiers never being shared between versions of the
same object.

They cannot be. Each `object_version` gets its own DEK, and chunk ciphertext is bound to
`(bucket_id, object_id, object_version)` through the AEAD AAD — so identical plaintext written
twice to the same key produces *different* ciphertext, hence a different content hash and a
different identifier. (This is the same binding that keeps the v5 copy fast path disabled.)
Confirmed empirically on prod: a two-version object has 5 `chunk_backend` rows and 5 distinct
identifiers.

Note this is narrower than "identifiers are globally unique" — they are not, and
`insert_chunk_backend.sql` says so: a content-addressed backend returns the same identifier for the
same bytes, so two *different objects* holding identical ciphertext can share one. That is
pre-existing and unchanged here; what this change relies on is only the per-version property above.

---

## 6. Deliberately not implemented

| Feature | Why not |
|---|---|
| `Suspended` versioning | AWS requires a PUT into a suspended bucket to *replace* the `null`-era version in place, which means unpinning and deleting the superseded version on the write hot path. Deferred rather than putting a destructive step there. `PutBucketVersioning` with `Status=Suspended` returns 501 `NotImplemented`. |
| MFA delete | Requires an MFA device binding we do not have. |
| Lifecycle `NoncurrentVersionExpiration` | `PutBucketLifecycle` is currently parsed and discarded repo-wide. Until lifecycle is stored and enforced, versioning has no automatic pruning — worth knowing before enabling it broadly, given ~25 TB of superseded data already exists without it. |
| Opaque version IDs | See §5.1. |
| `?versions` on unversioned buckets listing full history | Returns only the current version per key, matching AWS. Listing everything would newly expose ~25 TB of superseded content, including data users believe they deleted, as a side effect of a listing endpoint. |

---

## 7. Operational notes

- **Two migrations, deliberately.** `20260822120000` adds the three columns — all metadata-only on
  PostgreSQL 18.1 (nullable, or `NOT NULL DEFAULT false`), so no table rewrite. `20260822120001`
  builds the partial index `CONCURRENTLY` under `transaction:false`. They cannot be one file: a
  plain `CREATE INDEX` takes a lock that blocks writes for the whole build, and the build must scan
  the full ~49 GB heap to evaluate the partial predicate. Since migrations run on API pod startup,
  that would be a multi-minute data-plane outage. Same shape as
  `20260706000000_parts_upload_uploaded_at_index.sql`, including its invalid-index recovery caveat.
- **Object delete enqueues one unpin, not one per version.** The unpin request carries a NULL
  version meaning "every version", and the unpinner expands it under its own batching. Expanding it
  in the handler is not viable: prod holds an object with **646,993** versions (S4 append mints one
  per append), so a single DELETE would run a 647k-row join and push 647k queue entries before
  responding — a replay of the 1.29M-entry unpin overrun that made redis slow enough to break prod
  GETs.
- **Delete-marked keys stay in the active-objects index.** `idx_objects_bucket_prefix_active` is
  partial on `deleted_at IS NULL`, and a versioned DELETE deliberately leaves `objects.deleted_at`
  alone. So on a versioning-enabled bucket, deleted keys keep occupying the index that `ListObjects`
  scans, and are filtered out per row. A bucket that is mostly delete-marked pays proportionally
  more index rows per listing page, indefinitely. Combined with the absence of lifecycle expiration
  (§6), this is the main reason not to recommend versioning for a high-churn workload yet.
- Enabling versioning on a bucket is irreversible in AWS and here, and `Suspended` is not
  implemented — so today enabling it is a **one-way door** with no API-level way to stop accruing
  versions and no `NoncurrentVersionExpiration` to prune them. Treat it as opt-in per bucket,
  deliberately, until one of those lands.
- The existing backlog is not repaired by deploying this change. Run
  `backfill_superseded_version_unpins.py --dry-run` first; it reports object count and reclaimable
  bytes before anything is enqueued. Sanity-check the totals against §4 (~70k objects, ~5.6 TB)
  before applying — a wildly different figure means something else changed.
