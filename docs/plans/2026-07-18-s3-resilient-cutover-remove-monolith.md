# S3 Resilience: Promote the Staging Architecture, Remove the node6 Monolith

> **Status:** DRAFT for team evaluation — 2026-07-18. Phase 0 (evaluation) is DONE; findings are inline.
> Nothing below has been executed. Decisions flagged **[TEAM DECISION]** need a group call before we act.
> **For Claude (execution):** REQUIRED SUB-SKILL: superpowers:executing-plans.

**Goal:** Make production S3 survive the loss of any single node by replacing the node6-cache-pinned
"monolith" with the staging local-ingest + drain-to-Ceph architecture, promoting that code to the
source-of-truth and prod deploy branches, and freeing node capacity by cleanup rather than raising limits.

**Architecture (target):** `api-local` runs one pod per SSD ingest node (`hostPath` local disk + Ceph
read-fallback); a per-node `drain-agent` DaemonSet copies each node's local SSD → CephFS; a leader-elected
`drain-allocator` manages drain budget; all workers run on Ceph. Losing a node degrades, never blacks out.

**Tech stack:** Kubernetes (RKE2 1.34), Kustomize overlays, GitHub Actions (branch-per-env deploy),
CephFS/RBD (rook, 30 OSDs), Python api/workers, Rust drain stack (`Dockerfile.drain`), Postgres (CNPG), Redis.

---

## 1. TL;DR

- **What broke (2026-07-18):** node6-cache became unavailable; because the **entire S3 write path is pinned
  to that one node** by its 40 TB local disk, all of S3 returned 5xx across **both** ATS edge regions
  (`AtsClient5xxOutage`). Root cause of the node failure = an **abrupt host freeze** (OVH attributes to
  hardware; see §2 for the honest evidence). Random host failure is unavoidable — the unacceptable part is
  that it took the whole platform down.
- **The fix already exists on `staging`:** the s3-2.0 local-ingest + drain design. It is **staging-only**;
  a prod overlay must be authored and the code promoted.
- **Good news from evaluation:** Ceph has capacity (75 TiB free); every node has enough local disk; the
  drain-gating blocker (PR-7 / #264) is already merged; and the pod-capacity crunch is solvable by
  **cleaning up dead pods — no need to raise the per-node pod cap.**
- **Effort shape:** mostly (a) branch reconciliation, (b) author a prod Kustomize overlay + CI drain build,
  (c) a reversible cutover gated on the existing `s3-2.1-todo.md` checklist.

---

## 2. Background — the incident & root cause (evidence-based)

**Timeline (node6-cache, from its own journal):**
- Healthy and serving until **12:07:53 UTC** (systemd starting pods, sshd logging) — then the journal goes
  **instantly silent**, mid-normal-operation. Kubelet had stopped posting to the API at 12:03:45 (marked
  `NotReady` 12:09).
- Recovered **only by an external OVH power-cycle** ~13:37, which also bumped the kernel `6.8.0-124`→`136`.

**What the logs prove:** no OOM-killer (global), no MCE, no EDAC, no hung-task, no panic anywhere near the
crash. That **rules out a software OOM / kernel panic / clean reboot.** An abrupt, unlogged freeze recovered
by power-cycle is consistent with hardware — which matches OVH support's "hardware" attribution.

**What we could NOT confirm (stated honestly):** the *specific* hardware component. Post-reboot EDAC ECC
counters are clean (but reset on reboot), and there is **no `mcelog`/`rasdaemon`** installed to have captured
a pre-crash MCE. So "hardware fault" rests on OVH's word + elimination, not a component-level diagnosis.
**Action:** request OVH's intervention report / IPMI SEL, and install `rasdaemon` so the next event is
diagnosable (§ Phase 1).

**Chronic side-condition (NOT the trigger):** node6 is memory-undersized — **192 GB vs 251 GB** on peer
nodes — while carrying the whole python-heavy write path; Jul 13 shows a burst of pod-level (cgroup-limit)
OOM-kills of workers. Worth fixing, but the kernel log proves it did not cause the freeze.

**Why one node took down everything (the real target of this plan):**
`k8s/production/local-cache-patch.yaml` pins `api` + `arion-uploader` + `arion-downloader` + `backup` +
`hydrator` + `janitor` to `k8s-v3-node6-cache`, because `local-cache-pvc` is a **local PV** (40 TB ZFS,
`nodeAffinity` → node6). Node down → `api` 0/5 → S3 5xx on every ATS POP.

---

## 3. Current vs target architecture

| Aspect | Current (monolith) | Target (staging design) |
|---|---|---|
| API serving | `api` ×5 pinned to node6 | `api-local`, 1/ingest node, `hostPath` SSD + Ceph read-fallback |
| Workers | pinned to node6 (local-cache) | un-pinned, read CephFS across the pool |
| Durable cache | 40 TB node-local PV on node6 | CephFS `object-cache-pvc` (already 9.7 TiB, RWX) |
| Local→durable | n/a (single disk) | `drain-agent` DaemonSet (per node) + `drain-allocator` (singleton) |
| Single-node loss | **total S3 outage** | degraded (other ingest pod + Ceph read-fallback) |
| Lives in repo | `k8s/production/` | `k8s/staging/` only — **prod overlay must be authored** |

---

## 4. Ground truth (evaluated 2026-07-18 — verified against the tree & live cluster)

**Branch topology (deploy triggers):** `staging`→staging env; **`k8s-production`→production env**;
`main`→tests only (**no deploy**). *Implication: "push to main" does NOT change prod — the resilient code +
overlay must reach `k8s-production`.*

**Divergence:** `main..staging` = **280 commits / 290 files / +44 353 / −1 544**.
- `staging..main` = 1 commit `#265` (metrics fix) — **superseded** by staging's otel rework (staging already
  keeps `service.instance.id = hostname`). Do not cherry-pick.
- `staging..k8s-production` = 2 commits **not in staging → must carry forward**: `#258` (prod delete/unpin +
  auth/cache/observability batch), `#260` (flag-gated batch-delete unpinner, +420 lines).

**Prod deploy mechanics (`production-deploy.yaml`):** rebuild `hippius-s3-secrets` (full literal set) →
`kustomize edit set image` → **delete old `db-migrations` job (migrations run as a Job automatically)** →
`kubectl apply -f k8s/redis-cluster/production.yaml` → **`kubectl apply -k k8s/production`** → `rollout
status` gateway/**api**/arion-* → `rollout restart` hydrator/backup. *Implication: the `api` rollout-wait must
become `api-local`, plus add `drain-allocator`/`drain-agent` waits.*

**Drain image gap:** `staging-deploy` has a rust-gated `build-drain` job producing
`ghcr.io/thenervelab/hippius-s3/drain`; **`production-deploy` has none.** Must port it (exclude staging-only
chaos-mesh/toxiproxy).

**Ceph capacity — GO:** RAW `ssd` **105 TiB, 75 TiB free (28.7 % used)**; CephFS data pool 7.3 TiB stored,
**20 TiB MAX AVAIL**. Ample for the ex-node6 tier. **Caveat:** `HEALTH_WARN — mons f,g,h low on available
space` (mon-store disk, not data) → fix separately, not a cutover blocker. Client IO already ~817 MiB/s rd —
moving cache reads onto Ceph adds read load; watch latency.

**Local disk — sufficient everywhere:** every worker has **833 GiB allocatable root disk** (enough for the
transient drain buffer; staging proves it on node2/node3 with no dedicated NVMe). **node6-cache** additionally
has **12× 3.5 TB NVMe (~40 TB ZFS)** — the old cache, **freed** when the monolith is retired → ideal ingest
node #1. **The constraint is pod slots, not disk.**

**Pod-capacity crunch is a cleanup problem, not a limits problem:** general workers sit at the **110-pod cap**
(node4=108, node5=109; a probe pod got `OutOfpods`) while **CPU is 3–10 % and RAM 25–56 %.** But those nodes
are clogged with **terminal junk**: ~16 reapable pods on node4, ~9 on node5 (finished/failed Jobs, stuck
pods, debug leftovers). Cluster-wide: **43 Completed + 15 Error + 11 CrashLoopBackOff.** Reaping terminal
pods frees far more than the 2 slots/node the ingest tier needs — **so we do NOT need to raise `maxPods`.**
Contributing factors: indexer/marketing Jobs lack `ttlSecondsAfterFinished`; **Sentry** runs 71 pods with 9
crash-looping for 80 days; **staging S3 runs 39 pods on the prod general pool.**

**Prereqs already captured in `s3-2.1-todo.md`** (the authoritative cutover checklist — this plan
operationalizes it). **PR-7 drain-gating (#264) is merged into staging** (earlier blocker resolved).

---

## 5. [TEAM DECISION] Open questions to evaluate together

1. **Git strategy:** make `main` the trunk (merge staging→main, then fast-forward `k8s-production` from it),
   or keep branch-per-env and merge staging→`k8s-production` directly? (280-commit promotion either way.)
2. **Ingest topology:** confirm **node6-cache = ingest #1**; pick ingest #2 from node4/node5 **after cleanup**
   (no `maxPods` raise) — or a third option. Confirm labels (`s3-local-ingest=true`), the prod hostPath path
   (`/var/lib/hippius/local_ingest_prod`), taints, and DaemonSet/singleton placement.
3. **Capacity policy:** ratify "cleanup + Job TTL, no `maxPods` raise." Decide the fate of **Sentry** (biggest
   single reclaim if unused) and whether to **taint the 2 ingest nodes** so staging/dev can't recompete.
4. **node6 RAM:** rightsize (192→256 GB to match peers) or keep it as an ingest node with load spread — given
   it's no longer the sole write-path host.
5. **From `s3-2.1-todo.md`:** SLO ratification with Ops; `O_NOFOLLOW` trust boundary (is the SSD cache
   our-pods-only?); chaos-tier gating (is F2/F4/F5 enough to ship, full matrix as fast-follow?).
6. **OVH RCA:** who chases the intervention report / IPMI SEL for the actual node6 fault.

---

## 6. The plan

### Phase 0 — Evaluation ✅ DONE (2026-07-18)
All findings are in §2–§4. No changes were made. Outputs: branch/deploy mechanics mapped, drain-image gap
found, commits reconciled, Ceph GO, local disk sufficient, capacity = cleanup problem.

### Phase 1 — Reclaim node capacity by cleanup (no `maxPods` raise) [decision-gated by §5.3]
Read-only inventory is done; this phase is the *action*.
- **Task 1.1** Reap terminal pods (safe, won't respawn): all `Completed`/`Error` Job pods and stuck pods
  (indexer backfills 30–53 d, `kafka-tools` ImagePullBackOff, `arion-staging/gateway-0`+`warden-0` Init,
  `finney-light`, `harbor/curl-tmp`, `proxy-prod/debug-arion*`). Verify slots freed on node4 & node5.
- **Task 1.2** Add `ttlSecondsAfterFinished` to the indexer/marketing Jobs so junk stops re-accumulating.
- **Task 1.3** Owner decision on the 80–109-day crash-loopers (Sentry ×9, `loki-0`, `lightnode-3`,
  `finney`); scale down/remove what's dead (pod-delete alone respawns — must fix the controller).
- **Task 1.4** Install `rasdaemon` on the worker nodes (host-provisioned) so the next hardware event is
  diagnosable; chase OVH's node6 intervention report.
- **Verification:** node4 & node5 each show ≥ 4 free pod slots with no `maxPods` change; `kubectl get pods -A`
  shows 0 stale `Completed`/`Error`.

### Phase 2 — Reconcile source of truth [decision-gated by §5.1]
- **Task 2.1** Record the chosen git strategy.
- **Task 2.2** Create integration branch from `staging`; **carry forward `#258` and `#260`** (cherry-pick
  from `k8s-production`, resolve conflicts, run touched tests). **Do NOT** cherry-pick `#265` (superseded).
- **Task 2.3** Open the promotion PR (per strategy); CI `test-and-lint` green. Hold merge until Phase 3 lands
  in the same train.

### Phase 3 — Author the prod overlay + CI drain build; delete the monolith
Mirror `k8s/staging/` with prod denominators; build in the integration branch.
- **Task 3.1** `k8s/production/ingest-node-labels.yaml` — the chosen ingest nodes labelled `s3-local-ingest=true`.
- **Task 3.2** `k8s/production/api-local-deployments.yaml` — replicas = ingest-node count; `hostPath
  /var/lib/hippius/local_ingest_prod`; `nodeSelector` + required hostname allow-list; keep Ceph RWX mounts.
- **Task 3.3** `k8s/production/drain-agent-daemonset.yaml` + `drain-allocator-deployment.yaml` — same ingest
  nodes; image `ghcr.io/thenervelab/hippius-s3/drain`.
- **Task 3.4** Add the `build-drain` job (rust-gate) to `production-deploy.yaml`; set `deploy needs:
  [build-images, build-drain]`; inject the drain image tag; **change the `api` rollout-wait to `api-local`
  and add `drain-allocator`/`drain-agent` waits.**
- **Task 3.5** Switch `k8s/production/kustomization.yaml` to api-local + drain + labels; flip the `api`
  Service selector → `app: api-local`; in `resource-limits.yaml` scale base `api`→0 and **un-pin**
  `arion-uploader`/`arion-downloader`/`janitor`/`backup`/`hydrator`; **delete** `local-cache-patch.yaml`,
  `pv-local-cache.yaml`, `pvc-local-cache.yaml`, `local-cache-configmap-patch.yaml`.
- **Verification:** `kubectl kustomize k8s/production` renders **no** `local-cache-*` and **no**
  `hostname: k8s-v3-node6-cache`; base `api` 0, `api-local` serving, Service → `api-local`.

### Phase 4 — Cutover prerequisites (operationalize `s3-2.1-todo.md` "TO SHIP")
- **Task 4.1** Data-plane: run migrations (`object_versions.completed_part_numbers`); confirm `cephor`
  schema; set `redis-queues` `noeviction` + restart; full worker env/secret set; MPU-reaper indexes.
- **Task 4.2** Alerts + runbooks: redis-queues memory alert; drain lag/backlog/SSD-pressure/DLQ/leader-loss;
  rollout/rollback runbook (**drain image ships before the api image that stops PUT-enqueue; rollback = image revert**).
- **Task 4.3** Staging ship-gate (NO-GO if any red): `inv-guard` green; S8 byte-identical durability;
  S10/S11 single-leader + allocator-failover (F2 / T4-C); R2 index-only queries; chaos F4 (CephFS) + F5 (SSD fill).

### Phase 5 — Production cutover (reversible, order is load-bearing)
- **Task 5.1** Merge the promotion+overlay train → `k8s-production`; watch build (incl. `drain`) + deploy.
- **Task 5.2** Label ingest nodes; **bring up the drain stack FIRST** (`drain-allocator` + `drain-agent` N/N)
  — drain live before API stops PUT-enqueue.
- **Task 5.3** Roll `api-local`; Service → `api-local`; base `api`→0; workers reschedule onto Ceph.
- **Task 5.4** Production smoke + soak: 5xx at baseline (`AtsClient5xxOutage` quiet), drain lag bounded, DLQ ~0.
- **Rollback:** revert the api image (restore PUT-enqueue) + Service selector; base `api` back up. Data-safe
  (janitor never evicts an un-replicated chunk).

### Phase 6 — Prove resilience & retire the monolith
- **Task 6.1** **Single-node-kill drill** (acceptance test): cordon+drain OR stop ONE ingest node; S3 stays
  serving, 5xx at baseline, drain resumes on recovery.
- **Task 6.2** Reclaim node6's 40 TB (`Retain` PV → release); repurpose node6 (rightsized) or return to OVH.
- **Verification:** no `local-cache-*` / node6 hostname references remain in `k8s/production`.

---

## 7. Rollback & safety (whole effort)
The monolith stays intact until Phase 6, so any earlier phase rolls back by reverting the deploy commit
(re-applies the pinned overlay) or just the api image (resumes PUT-enqueue). Cutover is an image revert; the
janitor never evicts an un-replicated chunk → rollback is data-safe.

## 8. Definition of done
- Losing any single node no longer causes an S3 outage (Phase 6.1 passes).
- `k8s/production` has **no** node6 pin and **no** `local-cache` PV/PVC.
- The staging architecture is the source of truth on `main` + the prod deploy branch, with `#258`/`#260`
  preserved and `#265` confirmed superseded.
- Node capacity was reclaimed by cleanup + Job TTL (no `maxPods` raise); ingest nodes protected.
- `s3-2.1-todo.md` "TO SHIP" items checked; fast-follow items tracked separately.

## 9. Appendix — evidence pointers
- Incident RCA detail: node6 journal (freeze at 12:07:53 UTC, kernel `124→136`, no OOM/MCE).
- SPOF: `k8s/production/local-cache-patch.yaml`, `pv-local-cache.yaml`, `pvc-local-cache.yaml`.
- Target design: `k8s/staging/README-local-ingest-trial.md` + the `api-local`/`drain-*` manifests.
- Cutover checklist: `s3-2.1-todo.md`. Drain-gating: PR #264. CI: `.github/workflows/{production,staging}-deploy.yaml`.
- Reap list (Phase 1): terminal pods per node from the 2026-07-18 cluster audit (regenerate before acting).
