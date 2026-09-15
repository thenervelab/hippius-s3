# Rust Rewrite of hippius-s3 — Research Index & Synthesis

**Date:** 2026-09-15 · **Companion:** [`../rust-rewrite-assessment.md`](../rust-rewrite-assessment.md) (the strategy/decision doc)

This directory holds nine implementation-grade, **code-anchored** subsystem specifications for a from-scratch Rust reimplementation of the hippius-s3 object-storage product. Each was produced by a focused research pass reading the current source (Python `hippius_s3/`, the Rust `crates/hippius-drain-*`, and the hcfs `service`-branch S3 module), and each documents the **exact contracts** a Rust build must honor plus its own "Open questions". Total: ~6,100 lines.

> **How to read this:** the assessment doc is the *why/what/when* (strategy, phases, sizing, decisions). These nine are the *how* (byte layouts, DDL, wire formats, algorithms, conformance rows). Start here, then the assessment, then the section relevant to the crate you're building.

## The nine sections

| # | Section | One-line scope | Single most important finding |
|---|---------|----------------|-------------------------------|
| [01](01-crypto-envelope.md) | Crypto / envelope / KMS | AES-256-GCM chunks, DEK/KEK envelope, OVH KMS | **Nonce is random & prepended, not derived** — read it back; the AAD layouts + wrap are the real contracts (C1–C15). |
| [02](02-storage-engine-schema.md) | Storage engine + Postgres schema | Version-native model after 86 dbmate migrations | **Two DBs, three ownership domains** (dbmate app schema + drain's sqlx `cephor_*` in the same main DB + lazily-created keystore). Exact "serveable version" predicate. |
| [03](03-data-plane-cache-streaming.md) | Data plane: cache + streaming | Write/read pipeline, multi-tier cache, drain bridge | The FS `meta.json` layout is a **hard contract with the drain**; `chunk_backend`/CIDs are written by the **uploader worker**, not the drain. |
| [04](04-queues-and-workers.md) | Redis queues + workers | 5 Redis instances, queue wire formats, 13 workers | `redis-queues` is the single irreplaceable state (`noeviction`); the drain is the **sole producer** of upload work; purger high-water + DLQ cap are mandatory guards. |
| [05](05-auth-authz-billing.md) | Auth / authz / ACL / billing gate | 4 auth methods, ACL, sub-token scopes, plan gate | **Only 4 methods (seed-phrase removed); identity is remotely asserted** (api.hippius.com token-auth + 60s Redis cache) — hippius never stores the secret. |
| [06](06-s3-protocol-conformance.md) | S3 protocol conformance matrix | ~60 actions, headers, errors, XML, S4, object-lock | Testable checklist. **S4 `append` bypasses object-lock (live WORM hole)**; object-lock *delete* now enforced; per-prefix policy absent; CopyObject rejects all If-None-Match (501). |
| [07](07-chain-and-accounting.md) | Chain / substrate + accounting | Credit read, plan cache, CID identity | **hippius-s3 submits NO extrinsics** — Arion owns pinning; the only direct Substrate link is a read-only credit query. subxt-write pattern lives only in `hcfs-chain-reporter`. |
| [08](08-service-branch-harvest.md) | Service-branch harvest plan | File-by-file disposition + crate seam | Reuse is **~35% (~5k LOC), mostly with edits; ~63% rewrite** — smaller than first estimated. Seam = `CredentialStore`/`MetadataStore`/`ObjectBackend` traits. |
| [09](09-ops-deploy-observability.md) | Ops / deploy / observability / cutover | Config, pod topology, OTel, cutover playbook | The **drain is the ready-made Rust service template** (one image, two binaries, tini, injectable config, OTLP-only). Node-first cutover; the drain idempotently consumes parts from either language's API. |

## Decisions & design docs (post-research, 2026-09-15)

The research above fed a set of decisions and forward design work. These build on the nine specs:

| Doc | What it is | Status |
|---|---|---|
| [`IMPLEMENTATION-PLAN.md`](IMPLEMENTATION-PLAN.md) | **★ START HERE — the unified, standalone implementation plan** (architecture, decisions, crates, data model, crypto, phases 0–5 with gates, risks, open sign-offs). Reading this one doc is enough to understand and build the whole program. | Living — authoritative |
| [`../rust-rewrite-assessment.md`](../rust-rewrite-assessment.md) | Strategy/decision doc (now reframed to **greenfield** — see its banner) | Living |
| [`../python-side-findings.md`](../python-side-findings.md) | Live-Python issues found during research, for the Python repo to triage (not rewrite tasks) | Done |
| [`10a-option-arion-direct.md`](10a-option-arion-direct.md) | Cost of owning the data plane (drain reuse, uploader, Ceph/allocator) | Done |
| [`10b-option-hcfs-handoff.md`](10b-option-hcfs-handoff.md) | Cost/fitness of HCFS as the backend | Done |
| [`10-write-path-decision.md`](10-write-path-decision.md) | Write path: SSD-staging vs. inline-to-HCFS + ack/consistency semantics | ✅ **Decided — SSD-staging** (per-bucket ack policy) |
| [`11-hcfs-prerequisites.md`](11-hcfs-prerequisites.md) | Reference design *if* hcfs were changed to expose a first-class blob API | **Superseded by 13 — not the chosen path** |
| [`12-schema-design.md`](12-schema-design.md) | First-draft optimized greenfield Postgres schema | Draft — needs review |
| [`13-hcfs-as-is-integration.md`](13-hcfs-as-is-integration.md) | **The chosen storage integration: ZERO hcfs changes** — store/ranged-GET/delete + per-tenant credit-gate + usage→chain billing via hcfs's existing surface; S3 service owns dedup+refcount | Done — verified vs hcfs `main` |
| [`14-build-plan.md`](14-build-plan.md) | **The build plan** — crate DAG + 6-phase build order (each with an exit gate), critical path, outstanding sign-offs | First cut — ready to refine |
| [`crypto-dedup-research.md`](crypto-dedup-research.md) | Cited online research on encrypted dedup security → recommends per-owner copy-dedup + key-committing AEAD, rejects convergent | Done — recommendation ready for crypto-reviewer ratification |
| [`15-account-credential-model.md`](15-account-credential-model.md) | Identity/creds: keep remote assertion, SS58 = tenant = billing = credit; SigV4 harvest; sub-token scope; admin-bearer containment | Done |
| [`16-object-chunk-layout.md`](16-object-chunk-layout.md) | 4 MiB part-local chunks; **framed reads → one HCFS ranged GET** (its "never Range to HCFS" model is superseded by 23/25); ETag vs content_hash vs opaque blob_id vs body_blake3 | Done (range model superseded by 23/25) |
| [`17-workers-and-background-tasks.md`](17-workers-and-background-tasks.md) | 8 Postgres-SKIP-LOCKED workers (S3-side refcount GC is the linchpin); Python-worker mapping; no Redis | Done |
| [`18-object-lock-and-s4.md`](18-object-lock-and-s4.md) | Object-lock enforcement matrix + S4 kept in v1 (in-place O(delta), 403-when-protected in the CAS txn); owner/master-only bypass; worker+SQL enforcement | Done |
| [`19-deployment-ops.md`](19-deployment-ops.md) | Separate namespace; API Deployment + ingest DaemonSet + workers + own CNPG Postgres; durability-window SLI; toxiproxy CI | Done |
| [`20-migration-backfill.md`](20-migration-backfill.md) | Re-encrypt via the live write path; per-bucket state machine; verify on plaintext BLAKE3; old-format decrypt from Phase 0 | Done |
| [`21-committing-aead-construction.md`](21-committing-aead-construction.md) | CTX over AES-256-GCM+SHA-256 (CMT-4); now applied **per 256 KiB frame** (see 23) | Done — for crypto-reviewer ratification |
| [`22-aws-fidelity-and-conformance.md`](22-aws-fidelity-and-conformance.md) | AWS-fidelity answers: parity scope (C1), object-lock defaults (D1/D2/D3), 16-item gotcha list; s3-tests + mint as the bar | Done |
| [`23-chunking-and-range-prior-art.md`](23-chunking-and-range-prior-art.md) | Confirms 4 MiB blob; **flips C4 → adopt sub-chunk AEAD framing now** (256 KiB frames) for cheap encrypted ranges | Done |
| [`24-sse-and-identity-emulation.md`](24-sse-and-identity-emulation.md) | SSE posture (SSE-S3 default, map KMS, reject SSE-C) + sub-token scoping (R2-style, fail-closed) | Done |
| [`25-crypto-verification.md`](25-crypto-verification.md) | **Verified** crypto spec — source-checked + Rust PoC (9/9) + golden vectors; suite `hip-enc/aes256gcm-ctx-frames-v1`; 3 byte-level fixes folded in | Done — ready for final security sign-off |
| [`decisions-register.md`](decisions-register.md) | **The living decisions register** — every open item with its research-backed answer + status | Living |
| [`ref-2026-02-06-s3-gateway-plan.md`](ref-2026-02-06-s3-gateway-plan.md) | **Archival, reference-only** — the old hcfs S3-gateway design plan (protocol-handler blueprint for Phase 0/2 coding). Architecture (sled/Arion) is obsolete — NOT authoritative | Archival |
| [`26-non-s3-admin-surface.md`](26-non-s3-admin-surface.md) | The non-S3 surface: admin API + console read model + sub-token scope API are **in v1 scope** (console-load-bearing); peer-serving/`/metrics`/banhammer obsolete; ATS edge = ops decision | Done |

**Architecture decided so far:** greenfield Rust build (new branch in this repo, separate DB + optimized schema + separate pods, eventual re-encrypt data migration); **HCFS is the sole backend, UNCHANGED** (no Ceph, no read cache, no janitor) — verified (doc 13) that store/ranged-GET/delete + per-tenant credit-gate + usage→chain billing all work through hcfs's existing surface with **zero hcfs changes**; the S3 service owns dedup + refcount + all S3 metadata + envelope crypto + chunking. Single axum app. No cross-repo hcfs work on the critical path. **Write path decided (doc 10):** SSD-staging — land ciphertext on per-node SSD, ack, then a modest forwarder POSTs to HCFS; per-bucket ack policy (fast-ack + serve-pending-from-SSD by default; sync-to-HCFS-before-ack for object-lock/WORM buckets). Commits us to a per-SSD-node ingest DaemonSet + a `staged_blobs` table + node-sticky routing for pending reads. **Crypto model FROZEN** (doc 25, suite `hip-enc/aes256gcm-ctx-frames-v1`): CTX-over-frames committing AEAD (CMT-4); per-blob DEK; **opaque, pre-assigned `blob_id`** in the AAD (three distinct identities — not content-derived); per-owner copy-oriented dedup (O(1) copy via DEK re-wrap); reject cross-tenant convergent/MLE. Remaining: a cryptographer's **code review of the Phase-0 wrapper** (`ctx-poc/` is the reference), not a spec review. **Conditions to hold zero-change billing:** tenants stored under real ss58 + non-exempt; admin bearer secured.

## Second research pass (docs 15–21) — converged findings

> **⚠️ The "reconcile before freeze" / "remaining sign-offs" lists below are RESOLVED (2026-09-15) — kept for provenance.** The three crypto-contract items were reconciled into docs 12/16/21 (per-blob DEK, opaque `blob_id`, framed reads); the crypto suite is frozen in doc 25; and the sign-offs (sub-token enforce, scoped token B3, retention caps, sub-chunk framing) are all decided. The **living authority is [`decisions-register.md`](decisions-register.md)** (resolution log) and [`IMPLEMENTATION-PLAN.md`](IMPLEMENTATION-PLAN.md) §16. Read the lists below as history, not open work.

Reconciled (all done): **DEK granularity** → per-blob (doc 12 §2.9); **chunk wire → 256 KiB CTX frames**, range → covering frames → one HCFS ranged GET (doc 23/25, superseding doc 16's "never Range"); **dedup key** → owner-scoped `plaintext→blob` map (doc 12 §2.9a).

Resolved sign-offs: (a) crypto — endorsed; only the Phase-0 **wrapper code review** remains; (b) sub-token — **enforce fail-closed**; (c) scoped `HCFS_S3_SERVICE_TOKEN` — **recommended, not a blocker (B3)**, land before prod cutover (note: no `X-HCFS-Account` header exists in hcfs today); (d) object-lock retention cap — **max 36,500 / default 2555 / year=365**; (e) sub-chunk AEAD framing — **adopted** (doc 23/25).

## Corrections to first-pass assumptions (verify-against-code payoff)

The initial architectural sweep (and the earliest draft of the assessment) got several things wrong; the deep dives corrected them against the code. Anyone building on the early notes should use these instead:

1. **Chunk nonce is random (`os.urandom(12)`, prepended), not deterministic-by-chunk-index.** A deterministic scheme existed and was *deliberately removed* (nonce-reuse vuln). Good news: nothing to derive. [01]
2. **`chunk_index` is per-part, not global** (committed `writer/CLAUDE.md` is stale). [01]
3. **No per-object chain publishing.** Pinning is delegated to Arion over HTTP; Substrate is read-only credit; plans are HTTP-scraped. [07]
4. **Four auth methods, not five** — seed-phrase SigV4 removed; identity is remotely asserted, not locally verified. [05]
5. **Service-branch reuse is ~35%, not ~60%** — the reusable third is the hard standards code, and needs error-code/XML reconciliation toward hippius's contract. [08]
6. **`chunk_backend`/CID rows are written by the uploader worker**, not the drain (the drain enqueues and confirms). [03]
7. **Object-lock delete enforcement is wired** (403), contradicting a stale "NOT WIRED YET" docstring — but S4 append still bypasses it. [06]

## Contracts the migration's legacy decrypt / SigV4 harvest must honor

> **Scope correction (greenfield):** the rewrite has its own DB + re-encrypt migration, so it is **not** bit-compatible with the live system. Only the **OVH KMS setup** must match, plus the SigV4/error/XML *wire* contracts (which are AWS-standard anyway). The ciphertext-framing, chunk-AAD, and **per-version** DEK-wrap rows below are the **OLD format**, honored **only by the `s3-crypto::legacy` decrypt module** for migration — the new write path uses doc 25 (per-blob DEK, opaque `blob_id` AAD, CTX frames). The FS `meta.json` / `UploadChainRequest` / CID / Ceph rows are **current-Python-system** contracts the rewrite does **not** reproduce (HCFS-only, Postgres-queued).

| Contract | Where defined | Spec |
|---|---|---|
| Chunk ciphertext framing `nonce(12)‖ct‖tag(16)` (AES-256-GCM) | `crypto_service.py` | [01] |
| Chunk AAD `bucket_id‖object_id‖part_number‖chunk_index` (per-part) | `crypto_service.py` V2 adapter | [01] |
| DEK-wrap AAD `hippius-dek:{bucket}:{object}:{version}` + local-KEK wrap-key derivation | `envelope_service.py` / local wrapper | [01] |
| OVH KMS datakey/decrypt protocol (mTLS) | `ovh_kms_client.py` | [01] |
| Postgres app schema (86 migrations) + serveable-version predicate | `sql/migrations/`, `sql/queries/` | [02] |
| FS `meta.json` `{chunk_size,num_chunks,size_bytes}` + on-disk part/chunk layout | `cache/`, `metadata/`, drain `localfs.rs` | [03] |
| `UploadChainRequest` / `UnpinChainRequest` JSON + queue-name formulas + retry-ZSET/backoff | `queue.py`, drain `enqueue.rs` | [04] |
| SigV4/SigV2 canonicalization + the token-auth request/response shape | `gateway/middlewares/sigv4`, `auth_orchestrator.py` | [05] |
| S3 error catalog (code→HTTP→XML) + XML request/response shapes | `api/s3/errors.py`, `xml_helpers.py` | [06] |
| CID identity (Arion content-addresses the ciphertext) + where CIDs are stored | `cids`/`part_chunks`/`chunk_backend` | [07] |
| OTel metric names + bounded labels (dashboards/alerts depend on them) | `otel_setup.py`, `monitoring.py` | [09] |

## Consolidated open questions (pulled from all nine dives)

> **⚠️ RESOLVED / superseded (2026-09-15).** These were first-pass probes; most are now answered in [`decisions-register.md`](decisions-register.md) and [`python-side-findings.md`](../python-side-findings.md): the v5 copy fast-path was never enabled (E2, undecryptable class empty); migrations are sqlx-owned (separate greenfield DB, no coexistence); range-aware fetch is the framed single-ranged-GET (doc 23/25); no direct chain-write path (HCFS owns usage→chain); no Redis (Postgres-queued). The remaining genuinely-open items are **Python-product** questions, not rewrite blockers. Kept below for history.

- **Crypto:** does the v5 copy fast-path actually reproduce (CID reuse vs destination-object_id AAD → possible `InvalidTag`)? Is it gated off in prod? [01]
- **Schema:** is `object_versions.status` vestigial (real state in `cephor_replication_status`)? Diff production `pg_indexes` for out-of-band index drift. Who owns migrations under Rust vs the drain's sqlx migrator? [02]
- **Data plane:** is range-aware backend fetch feasible (the cold-range full-chunk gap)? `meta.json` unknown-field policy for forward-compat? [03]
- **Queues:** does a **download queue path** still exist (evidence in `errors.py`/`queue_metrics.py` despite no `run_download_*`)? Arion-only vs multi-backend day one? [04]
- **Auth/billing:** the pay-as-you-go `can_upload` caller-vs-owner keying; whether rate-limiting (currently unwired) moves worker-side. [05]
- **Protocol:** GetObjectAttributes/PostObject absence — port or skip? object-lock/S4 reconciliation. [06]
- **Chain:** should the rewrite gain a direct chain-write path at all (product change)? Is `HIPPIUS_VALIDATOR_REGION` dead config? Are the credit chain and thebrain the same runtime? [07]
- **Harvest:** auth boundary (gateway vs S3 layer), `ObjectMeta` reconciliation, checksum scope, SigV2 support, uuid ownership. [08]
- **Ops:** API worker/connection budget, gateway/api merge status, external alert thresholds (in `thenervelab/hippius-otel`), single vs HA `redis-queues`. [09]

## "Fix in Python first" candidates surfaced by the research

Independent of the rewrite, worth fixing in the live product (and they double as the rewrite's behavioral oracle):

- **S4 append bypasses object-lock** — a compliance/correctness hole for any WORM user (e.g. sn85). [06]
- **~200k NULL-envelope `object_versions` rows 500 on read** (P0); add a `v5_requires_envelope` CHECK going forward. [01][02]
- **Per-prefix / finer-grained access policy** — the real sn85 gap (buckets are public-or-private at the bucket grain). [05][06]
- Deploy-lag: the If-None-Match (#523) and Content-MD5 (#522) fixes are on `main` (2026-09-13) but sn85 likely tested an older env — verify the deployment. (See assessment §1.5.)

---

*Each section doc is self-contained and cites `file:line`. Where a section and a committed `CLAUDE.md`/README disagree, the section documents the **code** (per this project's "verify against code, not config" rule) and flags the stale doc.*
