# Decisions register — Rust S3 (tight AWS emulation over hcfs/arion)

**Date:** 2026-09-15 · **Purpose:** the single living list of every cross-cutting decision, with the research-backed answer and status. Legend: **✅ resolved** (research/prior-art gives a clear default we're adopting) · **★ needs human sign-off** (genuine judgment/authority call) · **confirm** (a default to rubber-stamp).

North star: **behave like real AWS S3**; where AWS doesn't dictate (our encrypted hcfs/arion backend), follow how the tight clones (MinIO, Ceph RGW, Garage, Cloudflare R2, Backblaze B2) solved it. The objective parity signal is a **green Ceph `s3-tests` + MinIO `mint`** run against our endpoint, not any table.

## Resolution log — 2026-09-15 review session (Camden + Claude)

A full read-through of the plan settled the remaining human items. Net state: **nothing blocks Phase 0 except a crypto sign-off** — and to be precise, that is a *review of the Phase-0 `s3-crypto` code once written*, **not a task actionable today**: this is greenfield, so no production committing wrapper exists yet. The construction itself is endorsed and verified (doc 25). The reference the implementer mirrors and the golden-vector oracle now live in-repo at [`ctx-poc/`](./ctx-poc/) (`cargo test` → 9/9). (The docs-12/16/21 reconciliation to doc 25 was completed this session — see below.)

- **Write path — DECIDED: SSD-staging** (owner call, this session; supersedes doc 10's stale "lean inline" recommendation). Consequences now accepted as design requirements, not open questions: the per-object durability window on fast-ack buckets, node-sticky routing for serve-pending reads, and the crash-recovery reconciler. **Risk to design around:** serve-pending + node-sticky reintroduces a *per-node* read dependency during the pre-drain window — the same shape as the prod node6-cache SPOF. Mitigate by keeping drain lag low (`S3rForwardBacklogAge` SLO) and offering sync-before-ack readily; WORM buckets already sidestep it. Add this note to docs 12/19.
- **A1/A5 crypto — ENDORSED**, with two gates before freeze: (1) reconcile docs 12/16/21 to doc 25 — **DONE this session**; (2) the human sign-off reviews the **Phase-0 `s3-crypto` code** (the ~120-line committing wrapper) against the [`ctx-poc/`](./ctx-poc/) reference + golden vectors — this is a future deliverable review, not a pending task (no production wrapper exists yet). **T3 locked:** the AAD `blob_id` is an **opaque, server-assigned, random, DEK-independent** id (e.g. 16-byte random), known at seal time — three distinct identities (`blob_id` / `content_hash=blake3(ct)` / private owner-scoped `blake3(pt)`), never conflated. Framing note: no user-chosen keys exist here, so committing's live value is cross-key/context binding + dedup-bug defense, not an active partitioning oracle — still worth its 0.023%.
- **B3 scoped HCFS token — RECOMMENDED, not a Phase-0 blocker.** Ship zero-change (admin bearer + always-derive-ss58-from-our-auth + network-locked egress + mTLS-at-ingress) through Phase 1; queue the hcfs-side scoped token early so it lands **before production cutover**. Small change (M, reuses existing token machinery), contains the god-mode bearer's all-endpoint blast radius to the blob surface, fixes attribution.
- **E1 — RESOLVED with prod data (this session).** Counted on the prod primary (`hippius` DB): storage_version distribution is **only v4 (212,498) and v5 (~176.94M)** — **no v1/v2/v3 exist at all** (doc 20's pre-versioned concern is moot). The read path hard-rejects `<5` (`storage_version.py:MIN_SUPPORTED_STORAGE_VERSION=5` → `require_supported_storage_version` raises), so **every v4 GET errors today** — including the 25,886 v4 rows that are the current version of a live object. Carrying v4 decrypt would only recover already-unreadable objects, so **skip-and-report; keep `s3-crypto::legacy` v5-only**. (Surfaced a Python-side finding — see python-side-findings.md #14: ~25.9k live objects stuck at v4, unreadable in prod.)
- **E2 — largely de-risked by a code probe (this session).** The v5 copy fast-path is **hard-disabled** in prod code — `should_use_v5_fast_path()` (copy_helpers.py:223) unconditionally returns `False, "v5_fast_path_disabled_object_id_binding"`, so `execute_v5_fast_path_copy` is never called; all copies stream/re-encrypt. So the AAD-bug undecryptable class is **empty for anything created under current code**. **"Was it ever enabled?" — resolved by git (this session):** the unconditional `return False` has sat *before* all eligibility logic since the very commit that introduced the function (`7c6fb2f0`, "#117"), so `eligible` was **always** False and `execute_v5_fast_path_copy` was never invoked in committed history; and the pre-#117 copy model aliased on the *same* `object_id` ("extra name on object_id" / "keep ciphertext on object_id" commits), which keeps the AAD valid. So **the copy-undecryptable class is empty** pending only a confirmatory prod probe (look for chunk CIDs shared across *different* `object_id`s — the telltale; expect none). Still keep the pilot decrypt probe and the added bit-rot policy: genuine bit-rot (GCM fail unrelated to the copy AAD) → quarantine + alert, **never silently dropped**.
- **E4 — accept the window cost.** Doubling is per-migrating-bucket + dedup-reduced, reclaimed by aged post-cut GC. Billing double-count mitigated by **option (a): don't report a bucket's usage from the new service until it cuts over** (cleaner than the exempt path, which also disables the quota gate). Bound concurrent in-flight buckets to HCFS headroom; **migrate WORM/COMPLIANCE buckets last**.
- **E3/E5 — confirmed** (freeze-and-abandon MPUs; per-bucket snapshot→catch-up→write-freeze). **Prerequisite now investigated (2026-09-15): the old Python side has NO per-bucket write-freeze** — only global `HIPPIUS_READ_ONLY_MODE` (too coarse) and per-account suspension `read_only` (whole account). So the per-bucket freeze needs one of: (1) a small old-side `buckets.write_frozen` flag; (2) **migrate at account granularity** using existing per-account `read_only` (zero old-side change — the default if per-account freeze windows are acceptable); or (3) the heavier old-side dual-write. Decide before Phase 4 (doc 20 §3.3). Also still open: the **soak/forward-commit point (OQ-8)**.
- **Prod-side findings re-quantified (2026-09-15, Python-product not rewrite):** ~**203k** NULL-envelope v5 rows, **141k of them the current version of a live object** (500 on GET today); plus **25.9k** v4-stuck live objects (finding #14) → **~167k live objects unreadable in prod**. The migration cannot carry these (no key / unsupported version) → skip-and-report/quarantine; the Python team must repair or consciously abandon them before cutover. See python-side-findings #2/#14.
- **H9 ATS — deferred to the ops/CDN track** (no v1 functional dependency). But v1 must, regardless: emit correct `Cache-Control` + the `X-Hippius-Visibility` sentinel, and keep the edge/LB idle timeout **≥75s**. The purge/warm/auth-probe trio lives or dies with the ATS-retention call.
- **Confirms — ratified:** B5 fail-open (safer here: HCFS's per-tenant 402 is a second backstop) · F1 sqlx owned by `s3-metadata` · F3 HPA on a custom in-flight/RPS metric (CPU secondary) · F4 alert rules owned in `thenervelab/hippius-otel` (contribute the `S3r*` rules), lean dedicated collector · F5 SSD sizing **MEASURED (see below): ~64 GB/node** (5-min burst buffer), ≪ Python's ~930 GB/node · F6 `hippius-s3r` placeholder, new branch here, all runtime resources separate · G1 seed-phrase stays removed · G3 no direct chain writes.
- **New top-tier risk flagged:** blob refcount/dedup GC **resurrection race** (decrement-to-zero → GC delete → concurrent copy resurrects a reference → dangling). Elevate alongside crypto/cutover; design a grace/tombstone window (never delete a blob whose refcount hit zero within the last N minutes).

**Doc reconciliation to doc 25 — DONE (this session).** Doc 12: added the opaque `blob_id` + per-blob envelope (`kek_id`/`wrapped_dek`) to the `blobs` table, added the private owner-scoped `blob_dedup` map, moved the DEK off `object_versions`/`multipart_uploads`, replaced the circular `content_hash`-as-AAD open-question with the frozen answer, dropped the stale `PRF(DEK,blob_id)` commitment, and added the refcount-GC grace window. Doc 16: rewrote Step 3 / D4 / D6 / OQ-1 / OQ-2 and the §0 constraint rows to the framed single-ranged-GET model. Doc 21: added a "superseded-for-byte-layout by doc 25" banner and fixed §6's "plaintext-derived `blob_id`" to the opaque-id + separate-plaintext-dedup-key split. Doc 10: marked its stale "lean inline" recommendation superseded by the ratified SSD-staging call. **Remaining crypto gate is now only the code review of the Rust committing wrapper + golden vectors.**

**SSD reservoir sizing (F5) / durability SLO — MEASURED from prod (same session).** Loki + Prometheus over the live 5-node ingest tier (2026-09-15 peak): per-node PUT ingest **~160–190 MB/s burst** (~72 MB/s sustained; burst ≈ 2.3× sustained), **~4.3–4.8 PUT/s/node**; HCFS single-4MiB-blob store latency **p50 630 ms / p95 1.37 s / p99 2.36 s / max ~6.3 s** (source: `arion-uploader` "Upload complete … duration=" for `chunks=1`; uploader-side wall-clock incl. semaphore wait + DB insert, so an upper bound on the pure HCFS POST — conservative). Derived + folded into doc 19 §5: **~64 GB/node reservoir** (rides a ~5-min HCFS stall at peak; ~96–128 GB for 10–15 min) — 7–14× smaller than Python's ~930 GB; **forwarder concurrency ~32–64/node** (need ~65 to drain peak at p95); **`S3rForwardBacklogAge` warn >30 s / page >120 s** (well above p99). Caveat: "PUT" conflates put_object + upload_part; bytes dominated by ~29 MB multipart parts, but bytes/s/node (what sizes the disk) is measured directly. **Closes remaining-verification item #4.**

**IMPLEMENTATION-PLAN extended (same session)** with 8 gap-fills: the refcount **resurrection-race guard** (§5/§8/§15 — was a real correctness omission), the **measured SSD/forwarder/SLO numbers** (§13: ~64 GB/node, forwarder 32–64/node, backlog warn>30s/page>120s), a **performance-targets** block (§13a — ~190 MB/s/node, PUT/GET/copy latency budgets), a consolidated **§11a Security posture** (rate-limiting parked, per-owner-only dedup oracle, cross-owner KEK isolation, input-validation/traversal, presigned footgun, COMPLIANCE legal sign-off), the **node-sticky pre-drain SPOF** (§2 mechanism + §15 risk), the **dedup write algorithm** + **DB scale/partitioning** (§5), the **soak/forward-commit open point** + **no-old-side-per-bucket-write-freeze** prereq (§12/§16), and the **~167k unreadable-prod-objects** the migration can't carry (§12).

**Full cross-doc consistency sweep — DONE (same session).** Audited all 33 docs (5 parallel auditors) against this register + IMPLEMENTATION-PLAN + doc 25, and fixed every contradiction: crypto docs 01/21/23/dedup got legacy/superseded banners + byte-level fixes (opaque `blob_id`, BLAKE3, STREAM nonce, no `PRF`); doc 16 fully converted to the framed single-ranged-GET model (worked examples, tables, floor reasoning); docs 02/03/04/05/07/09 got "current-Python-system, not rewrite target" banners (no Ceph/read-cache/peer/Redis/13-workers/per-version-DEK/inline-write-path leakage; four auth methods; billing inherited from HCFS); docs 06/15/17/24/26 had resolved items un-flagged (S4-WORM 403 MUST, GetObjectAttributes/ListMPU in scope, SSE posture C7, sub-token enforce, fail-open B5, gc grace-window naming); doc 22 cipher name fixed (AES-256-GCM) + `*BucketEncryption` reconciled with doc 24 (coherent stub, not 501); docs 10/12/14/20 + 00-index + IMPLEMENTATION-PLAN + the top-level assessment had the crypto gate reworded to "Phase-0 wrapper **code** review" and all resolved sign-offs/open-questions marked. Nav docs (00-index, assessment) got supersede banners pointing here + to IMPLEMENTATION-PLAN.

**HCFS zero-change re-verification — DONE (same session).** All 7 load-bearing claims re-checked against hcfs `main` HEAD `7157b69` (no drift — same date as doc 13). **Native absolute Range GET confirmed intact** (the framed encrypted-read dependency), incl. 1-byte ranges, 206/Content-Range, 416 on start≥total, streamed (no whole-object buffering); no-suffix-range / single-range-only, 16 MiB cap, content-addressing by `blake3(ct)`, unconditional/no-refcount delete, and per-tenant ss58 402-gate + chain reporter all confirmed. B3 confirmed still a small unimplemented change (admin bearer bypasses per-account auth on every gate; no `X-HCFS-Account` header exists). **One design nuance folded into doc 13/16:** store each ciphertext blob as **one HCFS object** (not a chunk-native upload) or the native sub-span range GET degrades to whole-chunk granularity — already the design. This closes remaining-verification item #1.

**`s3-protocol` harvest re-verification — ASSESSED (same session).** Doc 08's file-by-file audit re-checked against service branch `778ea38`: still accurate — the SigV4/SigV2 math in `auth/signing.rs` is clean standard-crate code whose only sled coupling is 3 credential-lookup fns (the `CredentialStore` seam); `error.rs` = one `From<sled::Error>` to drop; `chunked.rs`/`utils.rs`/`auth/mod.rs` clean. **Does not compile as-is** (unchanged — undeclared, deps absent, sled pervasive); a literal compile *is* the Phase-0 extraction, and the coupling says it's bounded/low-risk (~35% reuse not optimistic) — so item #2 is **assessed, not machine-proven** (deferred to Phase-0 start). **Scope correction folded into doc 08:** `policy.rs` is a full IAM Allow/Deny/Principal/Condition evaluator, but C1/B2 scope general bucket policy OUT of v1 (public-read subset only) — so v1 harvests only the policy-doc types + public-read validator; the evaluator is the C1 fast-follow, not wired into v1.

**Follow-on consistency pass (same session):** doc 12 — fixed the ER diagram (orphaned `kek_id` edge → `blobs`; added `blob_dedup`; refreshed the `object_versions`/`blobs` field lists), added `accounts.terminated_at` (the object-lock escape hatch, was missing), removed the stale DEK re-wrap step from the MPU promote-on-complete flow (§2.13), added the maintained `zero_refcount_at` to the §6 refcount trigger, and added §2.9b **the blob write algorithm** (dedup→seal→refcount, incl. the same-plaintext concurrency race resolution). Doc 18 (object-lock spec — already complete/normative, did **not** need drafting): closed its Q1/Q2 per register D2 (cap 2555d default / 36,500d max / year=365) and added a greenfield column-name mapping (`lock_mode`/`lock_retain_until`/`legal_hold` on `object_versions`, `accounts.terminated_at`) since its §2.2 SQL was the Python in-place-migration shape.

## A. Crypto — the one true Phase-0 gate

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| A1 | Ratify the crypto envelope as one package | **CTX-over-frames**: 256 KiB plaintext frames (per-bucket tunable to 64 KiB), each frame a full **CMT-4** committing unit; **per-blob DEK** wrapped under the owner's KEK; **AAD = blob_id‖frame_index‖suite** (blob_id opaque/pre-assigned per T3); **STREAM nonce** (random prefix per blob ‖ frame counter); **per-owner copy-dedup**; reject convergent | ✅ endorsed — gates: reconcile 12/16/21 + review Rust wrapper/vectors | 21,23,12,crypto-dedup-research |
| A2 | DEK granularity | **Per-blob** DEK on the `blobs` table (doc 12 §2.9); per-version `wrapped_dek` reconciled away | ✅ | 16,20 |
| A3 | Dedup scope | Per-owner, copy-oriented; **reject cross-tenant convergent/MLE** | ✅ | crypto-dedup-research |
| A4 | Sub-chunk AEAD framing (C4) | **Adopt NOW** — folded into A1. Free during the already-mandated re-encrypt; deferring forces a *second* corpus re-encrypt and leaves intra-object reads at ~42,000× amplification | ✅ (flipped from "defer") | 23 |
| A5 | Reviewer sub-choices | CMT-4; D-pragmatic (GCM-tag+CT32); **BLAKE3** (T1); base cipher AES-256; **256 KiB frame default**; STREAM nonce ratified `prefix(7)‖ctr(4 BE)‖final(1)` (T2) | ✅ endorsed — code review of wrapper remains | 21,23,25 |

**Everything else can proceed once A1/A5 are ratified.** The mandated re-encrypt migration is the single free window to lock this in.

## B. Account / identity / security

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| B1 | Credential authority | Keep **remote assertion** (api.hippius.com); SS58 = tenant = billing = credit; secret-less `accounts(SS58)` row for durability | ✅ | 15 |
| B2 | Sub-tokens | Port the **R2-style 4-tier × bucket-list scope** (= the frozen Python contract), **fail-closed**; no IAM engine; reserve one optional B2-style `name_prefix` for later | ✅ | 15,24 |
| B3 | Scoped `HCFS_S3_SERVICE_TOKEN` | **Recommended** — contain admin-bearer blast radius (leaked god-mode bearer touches any account). Zero-change through Phase 1; land before prod cutover | ✅ recommended (not a Phase-0 blocker) | 11,15,19 |
| B4 | tenant ↔ ss58 | Tenant **is** the SS58; no surrogate id | ✅ | 15 |
| B5 | Credit-cache miss | `has_credits` fail-open (match Python) | confirm | 15 |

## C. Protocol / layout / scope

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| C1 | Parity scope | **IN:** object CRUD+Copy+GetObjectAttributes, all 7 multipart ops (real ListMultipartUploads pagination), versioning+delete-markers+ListObjectVersions (incl. **Suspended**), tagging, ACL, bucket policy (public-read subset v1), real CORS, lifecycle round-trip, object lock. **OUT v1 → 501 (never fake 200):** Select, torrent, website, replication, analytics/inventory/metrics, accelerate, request-payment, logging, notification, intelligent-tiering, ownership/public-access-block, directory buckets, IAM/STS. Bar = `s3-tests` + `mint` CI + reviewed skip-file | ✅ | 22 |
| C2 | S4 append in v1 | **Yes** — in-place O(delta), 403-when-protected enforced in the CAS txn | ✅ | 18 |
| C3 | Chunk/blob size | **4 MiB** (matches Ceph RGW stripe) | ✅ | 16,23 |
| C5 | Small objects / limits | No inline path; 0-byte = no blob; S3 5 MiB–5 GiB/10k-part/5 TiB limits bind the client MPU layer | ✅ | 16,23 |
| C6 | **2024 conditional-writes** | Implement `If-Match` on PUT/DELETE + conditional reads (the single biggest fidelity gap vs current AWS); transactional, no TOCTOU | ✅ in-scope | 22 |
| C7 | SSE header emulation | **SSE-S3 (`AES256`) always-on default** (echo everywhere); accept `aws:kms` → map to our KEK, **reject external KMS ARNs** (clean 400); **reject SSE-C** in v1; stub `*BucketEncryption` coherently; **ETag = MD5(plaintext)** regardless | ✅ | 24 |

## D. Object lock (adopt AWS verbatim)

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| D1 | Modes / bypass / GDPR | COMPLIANCE absolute (only account termination lifts it); GOVERNANCE bypass = bucket-owner master token + `x-amz-bypass-governance-retention:true`; legal hold independent; GDPR postured **contractually** (no crypto escape — **block KEK-destruction**) | ✅ | 22,18 |
| D2 | Retention cap / units | Max **36,500 days**; default cap **2555 d (~7y)**; Year = 365 days | ✅ | 22 |
| D3 | Defaults / lifecycle | Default retention **not retroactive**; explicit PUT headers override; lifecycle can't delete a locked version; delete markers never protected; suspend versioning → 409 | ✅ | 22 |

## E. Migration / cutover

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| E1 | Legacy `storage_version < 5` | **Prod-counted:** only v4 (212,498) + v5 exist, no v1-3; read path rejects <5 so all v4 GETs error today → skip-and-report, `s3-crypto::legacy` v5-only | ✅ resolved | 20 |
| E2 | Copy-fast-path corrupt objects | Pilot probe; re-key→byte-preserve→quarantine; bit-rot → quarantine+alert, never silent drop | ✅ | 20,01 |
| E3 | In-progress MPUs | Freeze-and-abandon | ✅ confirmed | 20 |
| E4 | Window costs | Accept; per-bucket + dedup-reduced; billing via defer-report-until-cutover (opt a); WORM last | ✅ | 20 |
| E5 | Dual-run | Per-bucket snapshot → catch-up → short write-freeze | confirm | 20 |

## F. Ops / infra

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| F1 | Migrations tooling | sqlx, owned by `s3-metadata` (separate DB → no coexistence issue) | confirm | 12,19 |
| F2 | Forwarder placement | Sidecar co-located in the ingest DaemonSet pod | ✅ | 19 |
| F3 | HPA signal | Pick a metric (in-flight/CPU) for the API tier | confirm (minor) | 19 |
| F4 | OTel collector / alerts | Own vs shared collector; who owns external alert rules | ★ (minor) | 19 |
| F5 | SSD reservoir sizing | ingest-rate × drain-lag (write-staging only — far smaller than Python) | compute | 19 |
| F6 | Repo / namespace | New branch in hippius-s3; namespace `hippius-s3r` | confirm | 19 |

## G. Product confirmations

| # | Decision | Answer | Status | Src |
|---|----------|--------|--------|-----|
| G1 | Seed-phrase auth | Stays removed (non-vanilla) | confirm | 15 |
| G2 | Rate-limiting | Deferred | parked | 05 |
| G3 | Direct chain-write path | None — keep delegating pinning/reporting | confirm | 07 |

---

## H. Non-S3 surfaces (scope closed)

| # | Surface | Decision | Status |
|---|---------|----------|--------|
| H1 | Admin API `/admin/*` (suspend/reactivate/status/purge/purge-job) | **IN-SCOPE v1** — purge is the account-deletion/GDPR path | ✅ |
| H2 | Console read model `/user/*` (list-buckets/objects, location, recent-uploads) | **IN-SCOPE v1 — console-load-bearing** (JSON, not S3 XML) | ✅ |
| H3 | Sub-token scope API `/user/sub-tokens/*` | **IN-SCOPE v1** (we enforce fail-closed, so the write API is required) | ✅ |
| H4 | Anonymous public read `/public/{bucket}/{key}` | IN-SCOPE, owned by the S3 layer | ✅ |
| H5 | `/health` + `robots.txt` | IN-SCOPE | ✅ |
| H6 | Peer / internal-parts serving | **OBSOLETE** (no read cache / peer tier) | ✅ drop |
| H7 | HTTP `/metrics` route | **OBSOLETE** (OTLP push) | ✅ drop |
| H8 | `unban` / banhammer | DEFER/obsolete | ✅ |
| H9 | ATS edge/caching tier + its auth-probe/cache-control/purge middleware | Deferred to ops/CDN track (no v1 functional dep). v1 must still emit `Cache-Control` + `X-Hippius-Visibility` + edge idle-timeout ≥75s | ✅ deferred |
| H10 | Console/backend request signatures (path + HMAC byte-fidelity) | Frozen wire contracts — must keep verifying | confirm |

## What's actually left for humans

All cross-cutting decisions are now settled (see the Resolution log at the top). What remains before/within Phase 0:

- **A1/A4/A5 — crypto: ENDORSED + VERIFIED** (doc 25 — source-checked, Rust PoC 9/9, golden vectors). Suite frozen: `hip-enc/aes256gcm-ctx-frames-v1`. Tweaks folded in: **T3** AAD `blob_id` = **DEK-independent, pre-assigned & opaque** (NOT `blake3(ciphertext)` — circular — and NOT `blake3(plaintext)` — confirmation oracle); `blake3(ciphertext)` = HCFS address only. **T2** nonce = `prefix(7)‖frame_index(4 BE)‖final_flag(1)`. **T1** BLAKE3 commitment + length-prefixed preimage w/ domain-sep `"hip-enc/ctx/v1"`. **Two gates remain:** (1) reconcile docs 12/16/21 to doc 25 — see "still stale" in the Resolution log (mechanical doc edit, in progress); (2) a cryptographer reviews the **Rust committing wrapper + reproduces the golden vectors**, not just the spec.
- **B3 — scoped `HCFS_S3_SERVICE_TOKEN`: RECOMMENDED, not a Phase-0 blocker.** Zero-change through Phase 1; land the scoped token before production cutover.
- **E1/E2/E4 — migration scope: RESOLVED** (see Resolution log) — needed for Phase 4, not the start.
- **Confirms — ratified:** B5, E3/E5, F1/F3/F4/F5/F6, G1/G3.

Everything else is resolved by "match AWS + follow the clones," measured by a green `s3-tests`/`mint` run.
