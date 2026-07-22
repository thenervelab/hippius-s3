# Audit Report — `hippius-drain` service + staging wiring

> **Commit range:** `3a3bcaf..751eae4` (staging branch)
> **Date:** 2026-06-18
> **Auditor:** Claude (Opus 4.8) — two independent review sweeps (by-component, then by-concern)
> **Scope:** new Rust `hippius-drain` workspace (3 crates, ~11k LOC), `Dockerfile.drain`, staging CI build/deploy, k8s staging manifests, gated Python e2e suite.

---

## 1. Executive summary

`hippius-drain` is a new SSD→CephFS drain daemon for the S3 ingest path (replacing the Python background replicator + janitor for the node-local→CephFS hop, plus GC). It comprises three crates: `hippius-drain-core` (domain logic + Postgres layer + migrations), `hippius-drain-agent` (per-node DaemonSet), and `hippius-drain-allocator` (singleton leader-elected budget allocator).

**Overall assessment: high-quality, defensively-written code that is safe to run in staging.** The load-bearing correctness paths are sound: crash-safe drain (verify-before-unlink, file + parent-dir fsync, atomic rename, typestate-enforced commit ordering), AIMD allocator math (overflow-guarded, ceiling-clamped), leader-election epoch fencing (no split-brain write window), path-traversal defense (UUID-shaped object IDs + component validation on both render and parse), and fully parameterized SQL. The risky/incomplete pieces (live Ceph probe, CephFS GC) are correctly feature-gated off.

**No data-loss bug and no deploy-blocker were found.** However there is **one functional P1** (large objects stall the drain indefinitely) and **two process/enforcement P1s** (CI never runs the Rust lint/test/supply-chain gates; a README safety claim isn't backed by the manifests). The remaining findings are hardening items and documentation/intent mismatches.

### Findings at a glance

| # | Severity | Area | Title |
|---|----------|------|-------|
| F1 | **P1** | agent / rate control | Part larger than one-second burst stalls forever |
| F2 | **P1** | CI / supply chain | Rust lints, tests, and `cargo deny` are never enforced in CI |
| F3 | **P1 (doc/safety)** | k8s | README promises node-isolation guardrails the manifests don't implement |
| F4 | P2 | core / store | `PartClaimLost` does not fence a lease-expiry re-claim (doc/intent mismatch) |
| F5 | P2 | agent / fs | File copy/hash/open follow symlinks (no `O_NOFOLLOW`); daemon runs as root |
| F6 | P2 | allocator / store | Empty-plan allocation write is not epoch-fenced |
| F7 | P2 | agent / rate control | Stat failure charges 0 (or partial) bytes, under-metering the rate gate |
| F8 | P2 | CI / deploy | Agent DaemonSet rollout-wait `|| echo` masks all failures, not just pod-capacity |
| F9 | P2 | workspace lints | `arithmetic_side_effects` / `indexing_slicing` not in the deny-set |
| F10 | P2 | agent / supervisor | No per-worker restart/backoff — any worker exit tears down the agent (confirm intent) |
| F11 | P2 | core / gc | `GcClaim` proves claim-exclusivity, not object-terminality (documented-deferred) |
| F12 | P2 | container | Image runs as root; the allocator has no cross-uid need |
| F13 | P2 | supply chain | `deny.toml` advisory coverage is thin (only `yanked` explicit) |
| F14 | nit | agent / concurrency | `DRAIN_CONCURRENCY` is effectively unused — drains are serial |
| F15 | nit | core / reconcile | Reconciler scan is unbounded / un-paginated (one DB round-trip per part) |
| F16 | nit | core / reconcile | TOCTOU over-counts the `recovered` metric (benign, no resurrection) |
| F17 | nit | core / reconcile | Post-replication S4 appends are not re-drained (design question) |
| F18 | nit | core / store | `mark_failed` leaves `claimed_at` populated |
| F19 | nit | agent / disk | Disk-pressure "backlog" metric assumes a dedicated ingest volume |
| F20 | nit | docs / manifests | Stale `cache-replicator` references in 3 staging manifests |
| F21 | nit | agent / main | Daemon always exits `0` even after a worker was force-aborted |
| F22 | nit | deps | Duplicate dependency major-versions (RustCrypto/rand generational split) |

**Note on the Ceph ceiling:** the live `CephProbe` is implemented and correct (decay-on-error never fabricates a NearFull; no lock held across `.await`; request timeout set), but it is **not enabled in staging** — `CEPHOR_CEPH_MGR_METRICS_URL` is commented out in `drain-allocator-deployment.yaml:53`, so staging runs `StaticCeiling(Open)` at the configured 1 GB/s. This is intentional per the remaining-work doc, but means the allocator is currently blind to real Ceph near-full in staging.

---

## 2. Findings (P1)

### F1 — Part larger than one-second burst stalls the drain forever  **[P1]**

**Location:** `crates/hippius-drain-agent/src/worker.rs:107`; `crates/hippius-drain-core/src/enforce.rs:172` (`try_take`); `crates/hippius-drain-agent/src/main.rs:39` (`burst = max_drain_rate`, default 100 MB).

**Description.** The worker charges a part's *entire* byte size to the token bucket via `try_drain(bytes)`. `try_take` admits only when `tokens >= bytes`, and `tokens` is capped at `burst`, which is set to `max_drain_rate` (one second of rate, default 100 MB). Therefore any part whose summed chunk bytes exceed the per-second rate can **never** be admitted, even with a completely full bucket: it is denied → `release_part` → re-claimed on the next wake → denied again, indefinitely. No error is surfaced (only a `debug!` line). A 100 MB+ simple-PUT object is a single part and triggers this.

**Impact.** Silent, permanent stall of any sufficiently large object on staging. The part never drains off the SSD, so it is never durable on CephFS and the SSD cache is never reclaimable for it.

**Recommendation.** Decouple the admission charge from the burst ceiling. Options, in order of preference:
1. Clamp the charge to the burst (`try_take(min(bytes, burst))`) and reconcile the remainder against the next refill, **or** allow a one-shot overdraft when a single part exceeds `burst` so it can always make progress.
2. Guarantee `burst >= max_part_size` at config time and validate it.

Add a regression unit test: a part with total bytes `> burst` must eventually drain rather than loop on `Denied(RateLimited)`.

---

### F2 — Rust lints, tests, and `cargo deny` are never enforced in CI  **[P1]**

**Location:** `.github/workflows/*` (verified: zero `cargo clippy|test|deny|fmt|build` steps); the only drain CI job is `build-drain` in `.github/workflows/staging-deploy.yaml:113`, which solely runs `docker build -f Dockerfile.drain`.

**Description.** The workspace defines strong guardrails — `forbid(unsafe_code)`, `deny(unwrap_used)`, `deny(panic)`, clippy pedantic, and a `deny.toml`. None of them run in CI. There is no job that executes `cargo clippy -D warnings`, `cargo test`, `cargo fmt --check`, or `cargo deny check`. The Dockerfile builds with plain `cargo build --locked` (no clippy/test gate).

**Impact.** Every quality and supply-chain guarantee in the remaining-work doc ("clippy clean, 193 tests pass, advisory gates") rests on enforcement that does not exist in the pipeline. A `deny`-level lint regression, a failing test, or a yanked/vulnerable dependency would ship to staging silently.

**Recommendation.** Add a Rust CI job (gating the staging deploy alongside `build-drain`) that runs, against the pinned toolchain: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo deny check`, and the pure-logic test tiers. Gate the Postgres `#[sqlx::test]` cases behind a Docker/testcontainers service as the spec intended, so the non-DB tiers still run without Docker.

---

### F3 — README promises node-isolation guardrails the manifests don't implement  **[P1 — doc/safety]**

**Location:** `k8s/staging/README-local-ingest-trial.md`; `k8s/staging/drain-agent-daemonset.yaml`; `k8s/staging/api-local-deployments-staging.yaml`.

**Description.** The README's risk section asserts ingest is kept off the wrong nodes by "a nodeAffinity allow-list + psql taints." Neither the drain-agent DaemonSet nor the api-local Deployment contains any `nodeAffinity` allow-list or `tolerations`. Placement relies *entirely* on the `s3-staging-local-ingest=true` `nodeSelector` label being applied to the right nodes (node1/2/3) and nowhere else.

**Impact.** Given this cluster's history (the node6-cache SPOF outage), the documented safety story is materially stronger than reality. A single mislabel would land ingest + drain on an unintended node (e.g. a psql or cache node) with zero manifest-level guardrail.

**Recommendation.** Either (a) add the `nodeAffinity` allow-list and psql-node tolerations the README describes, making placement enforced rather than convention; or (b) correct the README so the safety story matches the manifests (placement depends solely on label discipline). (a) is preferred for a production-bound service.

---

## 3. Findings (P2)

### F4 — `PartClaimLost` does not fence a lease-expiry re-claim  **[P2 — doc/intent mismatch]**

**Location:** `crates/hippius-drain-core/src/store.rs:621` (`mark_replicated` guard), `:546` (`claim_part` re-claim), `:47` (`StoreError::PartClaimLost` doc); `crates/hippius-drain-core/migrations/0005_replication_status.sql` header.

**Description.** Commit is guarded by `WHERE status = 'draining'`, and the `PartClaimLost` doc plus the migration comment cite "a re-claim after lease expiry" as the protected case. But `claim_part` re-claims a stale row by setting its status *back* to `draining`. So a hung-then-resumed original (zombie) agent's `mark_replicated` matches the reclaimer's `draining` row and succeeds — `PartClaimLost` never fires for exactly the scenario the docs name. Two agents can both "successfully" commit and unlink the same part.

**Impact.** Not data loss in practice: chunk content is deterministic per `(object, version, part, chunk_index)`, both agents write identical bytes to identical pool paths via atomic rename, and each agent byte-verifies its own full copy before committing. The pool always ends with a complete, verified copy. The defect is that the advertised fencing guarantee is absent and the safety actually rests on content-determinism — an undocumented and fragile assumption.

**Recommendation.** Add a monotonic claim token: bump an integer `claim_seq` (or reuse a per-claim UUID) in `claim_part`, return it in `ClaimedPart`, and add `AND claim_seq = $N` to the `mark_replicated` guard — mirroring the `fencing_epoch` pattern already used correctly for allocations. If the team prefers to keep determinism-only safety, rewrite the `PartClaimLost` doc and migration comment to state that explicitly and remove the misleading "re-claim after lease expiry" example.

---

### F5 — File copy/hash/open follow symlinks; daemon runs as root  **[P2]**

**Location:** `crates/hippius-drain-agent/src/localfs.rs:176` (`hash_file` open), `:200` (`fs::copy`), `:201`/`:204` (open/rename) — none use `O_NOFOLLOW`/`symlink_metadata`.

**Description.** The drain runs as root and uses symlink-following filesystem operations on the chunk tree. Source paths derive from a UUID-validated `PartKey`, so this is not reachable directly from an S3 client. But if any other process (or a compromised ingest path) plants a symlink at `<ssd_root>/<uuid>/v<n>/part_<n>/chunk_<i>.bin`, the drain would read through it: it could copy an arbitrary root-readable file (e.g. `/etc/shadow`) into the CephFS pool under a predictable path, and the byte-verify would still pass (both hashes read the same followed target).

**Impact.** Information disclosure / pool poisoning, gated by "who can write the SSD cache tree." The S3 writer owns that tree today, so exposure is low — but the drain has no defense if that trust boundary is ever crossed.

**Recommendation.** Open chunk files with `O_NOFOLLOW` (via `OpenOptionsExt::custom_flags(libc::O_NOFOLLOW)`) for both hash and copy, and reject symlinked directory components, or document the SSD-tree trust boundary explicitly as a relied-upon invariant. See also F12 (drop root for the allocator).

---

### F6 — Empty-plan allocation write is not epoch-fenced  **[P2]**

**Location:** `crates/hippius-drain-core/src/store.rs:350` (`write_allocations` trailing zeroing UPDATE).

**Description.** When the allocation plan is empty, the per-row upsert loop is skipped, so the `Fenced` early-return (which only triggers on a fenced upsert) cannot fire. The trailing `UPDATE ... WHERE fencing_epoch <= $1 AND node_id <> ALL($2) AND budget_bytes <> 0` still runs with `present = []`; `node_id <> ALL('{}')` is TRUE for every row, so it zeroes every allocation at `fencing_epoch <= $1`. A deposed leader producing an empty plan can therefore still zero a current leader's allotments.

**Impact.** A stale/deposed allocator can briefly zero the fleet's budgets, throttling all agents to floor until the live leader's next tick re-writes them. Bounded and self-healing, but a real correctness gap in the fencing story.

**Recommendation.** Skip the zeroing entirely when the plan is empty, or require the leader to re-confirm its epoch (e.g. fold the zeroing into the same fenced transaction and treat zero-rows-affected as `Fenced`). Add a test: a deposed leader (lower epoch) with an empty plan must not zero a current leader's rows.

---

### F7 — Stat failure charges 0 (or partial) bytes, under-metering the rate gate  **[P2]**

**Location:** `crates/hippius-drain-agent/src/worker.rs:72` (`part_size`).

**Description.** `part_size` returns 0 if `list_chunks` fails, and sums per-chunk `metadata().map_or(0, len)` otherwise. On a transient stat error the gate admits the part at zero (or undercounted) cost, then `drain_part` re-lists successfully and drains the full part outside the budget. The doc claims the failure path "surfaces the real error," which holds for a genuinely missing part but not for a metadata race where the file is actually present.

**Impact.** The rate limiter can admit and drain unmetered (or under-metered) bytes precisely under SSD I/O pressure — defeating rate control exactly when it matters, silently rather than failing.

**Recommendation.** On a stat/list error, deny admission (charge `burst`/treat as over-budget) and let the next wake retry, rather than charging 0. Alternatively measure actual bytes drained and reconcile the bucket post-transfer.

---

### F8 — Agent rollout-wait `|| echo` masks all failures, not just pod-capacity  **[P2]**

**Location:** `.github/workflows/staging-deploy.yaml` (drain-agent rollout step, commit 751eae4).

**Description.** The intent — don't fail the deploy when a labeled node at pod capacity can't host the agent — is reasonable, but `kubectl rollout status ... || echo "::warning"` swallows *every* non-zero exit: image-pull failure, CrashLoopBackOff, bad config, RBAC denial. A genuinely broken agent rollout reports green. (The allocator wait above it is correctly strict, so this does not mask allocator failures — the commit's stated goal holds.)

**Impact.** Real agent rollout failures are invisible in CI; only the allocator is actually gated.

**Recommendation.** Gate on "at least 1 agent pod Ready" (e.g. poll `kubectl get ds drain-agent -o jsonpath='{.status.numberReady}'` for `>= 1`) instead of swallowing all exits, or grep the rollout output for the specific capacity condition and only warn on that.

---

### F9 — `arithmetic_side_effects` / `indexing_slicing` not in the deny-set  **[P2]**

**Location:** `Cargo.toml` (workspace `[lints]`).

**Description.** The workspace denies `unwrap_used`/`panic` but not `clippy::arithmetic_side_effects` or `clippy::indexing_slicing`. Raw `+`/`-`/`*` and `slice[i]` are therefore not lint-caught. All current arithmetic sites were verified safe (each subtraction is guarded by a preceding `>=`/early-return; products use `u128` intermediates; shifts cap at 63), but the "no panic" guarantee has a hole: a future edit introducing an overflowing operation would compile clean, panic in debug, and **wrap silently in release** (the daemon runs release builds).

**Impact.** Latent. No live bug, but the guardrail the codebase advertises is incomplete.

**Recommendation.** Add `arithmetic_side_effects = "warn"` (or `deny`) and `indexing_slicing = "warn"` to the workspace lints, and convert the verified-safe sites to explicit `checked_*`/`saturating_*` or `get()` with `#[expect]` justifications where the invariant is non-obvious.

---

### F10 — No per-worker restart/backoff; any worker exit tears down the agent  **[P2 — confirm intent]**

**Location:** `crates/hippius-drain-agent/src/supervisor.rs:188-212`.

**Description.** The file is described as a "supervised runtime / restart loop," but the implementation classifies any first worker exit — clean, early, or panic — as `ShutdownTrigger::WorkerExited` and cancels the entire supervisor. There is no in-process restart or backoff. (This matches the file's own doc comments, which describe escalate-to-orderly-shutdown rather than restart.)

**Impact.** A worker that panics on startup yields a process-level crash loop bounded only by k8s restart backoff, not in-process backoff. If restart-with-backoff was intended (as the "restart loop" framing implies), it is absent.

**Recommendation.** Confirm the intended semantics. If escalate-to-shutdown is correct, rename/clarify the "restart loop" language. If per-worker restart is desired, add bounded exponential backoff and a crash-loop circuit breaker. Pairs with F21 (exit non-zero on unclean shutdown so k8s actually restarts).

---

### F11 — `GcClaim` proves claim-exclusivity, not object-terminality  **[P2 — documented-deferred]**

**Location:** `crates/hippius-drain-core/src/gc.rs:44-50`, `:174-177`.

**Description.** GC requires a `GcClaim`, constructed only by the store's SKIP-LOCKED claim winner. The doc itself notes the claim proves "won the claim" but not "the object is terminal" — the api↔GC write-fence contract is deferred. So today the capability proves exclusivity, not that the object is actually abandoned/terminal.

**Impact.** Latent and currently mitigated: CephFS pool reclaim is feature-gated **off** (`gc-cephfs`, verified default `[]`), and SSD-local GC has no shared-write hazard. The risk materializes only when CephFS GC is enabled before the write-fence lands.

**Recommendation.** Keep `gc-cephfs` off until the api↔GC write-fence (no writes after terminal commit, or object lock, or `terminal_at + δ` grace) is confirmed, as the remaining-work doc already gates. Bind terminality into the `GcClaim` construction (require a terminal-state predicate) before flipping the feature on.

---

### F12 — Image runs as root; the allocator has no cross-uid need  **[P2]**

**Location:** `Dockerfile.drain:49-56`; both staging manifests (no `runAsNonRoot`/`securityContext`).

**Description.** The single image serves both binaries and runs as root. The agent's root need (cross-uid reads/unlinks of api-written chunk files on the shared volume) is real and acknowledged in a comment. The allocator, however, only talks to Postgres + HTTP and never touches chunk files, yet inherits the same root image and pod security posture.

**Recommendation.** Set `runAsNonRoot: true` (+ a non-root `runAsUser`) on the allocator pod specifically, and add a `securityContext` dropping unneeded capabilities to both. Revisit the agent's root requirement once the api uid is aligned, as the Dockerfile comment anticipates.

---

### F13 — `deny.toml` advisory coverage is thin  **[P2]**

**Location:** `deny.toml` (`[advisories]`).

**Description.** `[advisories]` sets only `yanked = "deny"`. `vulnerability` and `unmaintained` are left to cargo-deny's version-dependent defaults. Combined with F2 (cargo-deny never runs), the advisory gate is doubly weak. The licenses/bans/sources sections are otherwise reasonable (`unknown-registry`/`unknown-git`/`wildcard-dependencies` denied; everything is `=`-pinned).

**Recommendation.** Explicitly set `vulnerability = "deny"` and `unmaintained = "warn"` (or `deny`), and wire `cargo deny check` into CI per F2.

---

## 4. Findings (nits)

- **F14 — `DRAIN_CONCURRENCY` effectively unused.** `runtime.rs:35` defines concurrency 4 and feeds `ConcurrencyLimiter`, but `drain_until_empty` claims one row per query and loops serially, so a single worker never exploits the gate. Confirm whether parallel drains were intended; if so, claim in batches.
- **F15 — Reconciler scan unbounded.** `reconcile.rs` loads the entire `scan_parts()` Vec and issues one `status()` round-trip per part with no LIMIT/pagination (unlike the DB-side `list_landed_pending_parts(limit)`). Bounded by disk, not attacker, but a large part count is a memory + query spike per tick.
- **F16 — Reconciler TOCTOU over-counts `recovered`.** `reconcile.rs:138` reads `status()==None` then `record_landed` (`INSERT ... ON CONFLICT DO NOTHING`). A row appearing between the two awaits is not resurrected (the no-op INSERT protects it) but is still tallied as `recovered`. Cosmetic metric skew only.
- **F17 — Post-replication S4 appends not re-drained.** A `replicated` part whose SSD dir is later re-touched by an S4 append is counted `replicated_orphan` and left alone — appended chunks are not re-drained. This is an api-contract question for the design owners, not a bug in this code.
- **F18 — `mark_failed` leaves `claimed_at` populated** (`store.rs:644`), inconsistent with `release_part` which nulls it. Benign today; would bite a future query keying off `claimed_at` independent of status.
- **F19 — Disk-pressure "backlog" assumes a dedicated volume.** `disk.rs` reports total occupied bytes as the drain backlog; on a shared volume the allocator mis-weights the node. The math is correct; the assumption is the risk (pressure correctly uses `f_bavail`).
- **F20 — Stale `cache-replicator` references** in `api-local-deployments-staging.yaml`, `drain-agent-daemonset.yaml`, and `ingest-node-labels-staging.yaml` comments. Cosmetic.
- **F21 — Daemon always exits `0`** even after a worker was force-aborted (`main.rs:63`), so k8s won't restart a pod whose drain worker died. Consider exiting non-zero when `!report.clean`. Pairs with F10.
- **F22 — Duplicate dependency major-versions** (`sha2` 0.10+0.11, `digest`, `rand`/`rand_core`, `getrandom`, `hashbrown`) from the RustCrypto/rand generational split. Benign bloat; `multiple-versions = "warn"` would flag it once cargo-deny runs (F2). The agent could adopt sqlx's existing `sha2 0.10` to collapse one duplicate.

---

## 5. Verified correct (spot-checked, not merely trusted)

- **Crash-safe drain ordering:** persist each chunk (copy → `fsync` file → `fsync` parent dir → atomic rename) → independent re-hash of the pool copy → byte-compare → persist `meta.json` last → commit (`mark_replicated`) → unlink SSD. No path unlinks the source before a verified durable copy exists; every failure variant retains the SSD copy. The `PartVerified` typestate makes "commit before verify" a compile error.
- **AIMD `allocate()`:** all products via `u128`, every divisor zero-guarded, `weight.max(1)` prevents stall, capacity hard-clamped to `ceiling.budget()` (`alloc.rs:313`); `Critical` ⇒ zero budget. So a decayed `Open(rate)` ceiling is a genuine hard cap, not cosmetic. Proptest-covered (capacity-bound, demand-cap, conservation).
- **Leader-election fencing:** epoch bumps on every lease takeover including self-reacquire after own expiry (`store.rs:469`); the allocation upsert fence (`fencing_epoch <= $3`) rolls back a deposed leader's transaction. Survives the STW-pause split-brain. (Exception: the empty-plan path — see F6.)
- **Live `CephProbe`:** decay-on-error preserves the band and never fabricates a NearFull from Open on first failure; no lock held across `.await`; request `timeout` set so a hanging mgr can't block the tick. Correct — but disabled in staging (env var commented).
- **Prometheus parser (`mgr.rs`):** single-pass, bounded memory, panic-free on arbitrary text (proptest); `OSD_FULL` vs `OSD_BACKFILLFULL` matched on exact label; missing cluster-bytes ⇒ `MissingMetric` (folded to decay, not read as healthy); zero-total guarded against div-by-zero; threshold boundary `>=` inclusive into the higher band.
- **Path traversal:** `ObjectId` requires strict 8-4-4-4-12 hyphenated-UUID shape; `parse_part_dir` admits only `Component::Normal` (rejects `..`/root/prefix) and re-validates each of exactly 3 components; `safe_component` guards the GC/FileId path. No escape found on read, write, or unlink. Proptest `parse_never_panics_on_arbitrary_input`.
- **SQL:** every query uses positional bind params; no string interpolation of any value. No injection surface.
- **Backfill idempotency:** `record_landed` is `INSERT ... ON CONFLICT DO NOTHING`; a `draining`/`replicated`/`failed` part is never reset to `pending`. Mid-write parts are gated by the `meta.json`-present check (api writes meta last).
- **Feature-flag hygiene & TLS posture:** `http`/`pg`/`gc-cephfs` gate both deps and code; the agent does not link reqwest; `gc-cephfs` is off by default; neither `reqwest` (built `default-features = false`) nor `sqlx` pulls OpenSSL/rustls/any TLS backend. Reproducible build: toolchain pinned `1.95.0` matching the builder image, exact-pinned deps, committed `Cargo.lock`, `--locked`, no git/path deps.
- **k8s:** PVC is RWX `ceph-filesystem` (no RWO-shared-across-DaemonSet hazard); `DATABASE_URL` secret ref matches the api; allocator is `replicas: 1` + `strategy: Recreate` (correct singleton); no dangling refs to the removed PV/PVC/replicator; removal risks no data loss.
- **Python e2e suite:** properly gated — skips unless `HIPPIUS_DRAIN_LIVE=1` and AWS creds are set. Won't break CI.

---

## 6. Recommended actions before promoting past staging

**Must fix (P1):**
1. F1 — fix the oversize-part stall + add a regression test.
2. F2 — add a Rust CI job (fmt/clippy/test/deny) gating the deploy.
3. F3 — reconcile the README node-isolation claim with the manifests (prefer enforced `nodeAffinity` + tolerations).

**Should fix (P2):** F4 (claim fencing token or doc fix), F6 (empty-plan fence + test), F7 (deny on stat failure), F8 (rollout-wait gating), F5/F12 (symlink-safe opens + drop root for allocator), F9 (lint deny-set), F10 (clarify/implement supervisor restart), F13 (advisory coverage). Keep F11's `gc-cephfs` off until the write-fence lands.

**Suggested tests for the critical paths:**
1. Oversize part (`bytes > burst`) must drain, not stall (F1).
2. Lease-expiry re-claim: original agent's `mark_replicated` must not silently succeed against a reclaimer's row — or assert+document the double-commit is benign (F4).
3. Deposed leader with an empty plan must not zero a current leader's allotments (F6).

---

*End of report.*
