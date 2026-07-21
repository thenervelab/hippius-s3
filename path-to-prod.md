# path-to-prod — promoting s3-2.1 (drain-direct) from staging to production

**Audience:** whoever runs the cutover. **Goal:** move prod from the current
monolithic pipeline to the s3-2.1 **drain-direct** architecture with **zero upload/download
downtime**, and a rollback that is a single `kubectl` revert.

Companion docs: `s3-2.1-todo.md` (what's shipped + what's left, incl. the code-promotion/CI/monolith-removal
steps) · `2026-07-18-s3-resilient-cutover-remove-monolith.md` (incident + strategic plan) ·
`s3-prod-drain-capacity-plan.md` (node sizing/why) · `stress-test/plan.md` (harness). Read `s3-2.1-todo.md`
→ "PROD LOGISTICS" for the node-prep runbook; this doc is the **rollout mechanics**.

## Why (the SPOF)
Prod's entire write path (`api` + `arion-uploader`/`downloader` + `backup` + `hydrator` + `janitor`) is
**pinned to `node6-cache`** because the cache is a 40 TB node-local PV (`local-cache-pvc`) on that one node.
On 2026-07-18 node6 froze and **all** of prod S3 5xx'd across both edge regions. This cutover spreads ingest
across nodes with a CephFS read-fallback so a node loss **degrades instead of blacking out**. Removing that
pin — un-pinning the workers and deleting the `local-cache-*` manifests — is part of the cutover, not a
follow-up.

---

## 0. What actually changes (the delta)

| | **Today (prod)** | **After cutover** |
|---|---|---|
| Where the api stages a PUT | **node6-local** cache PV (`local-cache-pvc`, 40 TB ZFS on `node6-cache`) | dedicated **NVMe** per ingest node (host `/s3-data`) |
| Write-path placement | `api` + workers **pinned to node6-cache** (SPOF) | `api-local` 1/ingest-node; workers un-pinned onto Ceph |
| Who enqueues the backend upload | **the api itself** (`enqueue_upload_to_backends`) | **the drain-agent** (sole producer, after SSD→CephFS replication) |
| Upload queue + payload | `{backend}_upload_requests`, `UploadChainRequest` | **identical** — only the producer moved |
| New components | — | `drain-allocator` (singleton) + `drain-agent` (DaemonSet) + `mpu-reaper` + `api-local` |
| Cross-node fresh reads | n/a (single pinned cache) | `DualFileSystemPartsStore` pool fallback during the SSD→pool window |

The upload queue and its `UploadChainRequest` payload are **unchanged** — the Rust
`crates/hippius-drain-agent/src/enqueue.rs` shape is golden-fixture-tested against
`hippius_s3/queue.py::UploadChainRequest`. This equivalence is what makes the hybrid below safe.

---

## 1. Is a hybrid (old + new in parallel) possible? — **Yes, and it's the recommended path**

We can run the **old api image (ceph-cache + self-enqueue)** and the **new `api-local` image
(drain-direct + SSD)** side by side behind the same `api` Service, then scale the old fleet down
incrementally once the new one is proven. Reasons it's safe:

- **Two self-contained write pipelines, one shared sink.** Old api → node6-local cache → self-enqueue.
  New api-local → node SSD → drain replicates → drain enqueues. Both LPUSH the **same
  `UploadChainRequest`** to the **same `{backend}_upload_requests`**, so the (new) `arion-uploader`,
  `chunk_backend`, janitor, and Postgres serve both transparently.
- **Additive, backward-compatible schema.** The only new column, `object_versions.completed_part_numbers`,
  is nullable (= "all parts"), so the old image tolerates the migrated DB.
- **Gateway is stateless.** Old and new gateway pods can serve in parallel with no coordination.

### The one hard rule
**A NEW-image api must NEVER write to the old node6 cache.** The new api does not self-enqueue, and the
drain only scans node-local **SSD ingest** dirs — so a new-image api on the old cache produces uploads that
stay `pending` forever. Therefore in a hybrid: the old-cache writer is the **OLD** image; the SSD writer is
the **NEW** `api-local`. And **deploy the drain stack before any `api-local` pod takes traffic**, or its
uploads stage on SSD and never replicate.

### Routing mechanism (how both fleets sit behind one Service)
The gateway forwards to `http://api:8000` (the `api` Service, `base/services.yaml`, selector
`app: api`). To fan traffic across both fleets:

1. Add a shared pod label — `role: s3-api` — to **both** the base `api` Deployment template and the
   `api-local` template (a pod-template label; does not change either Deployment's own `selector`).
2. Patch the `api` Service selector from `app: api` → `role: s3-api`.

Now every ready pod of **both** deployments is a Service endpoint; k8s load-balances per-pod, so the
old/new traffic split ≈ the ready-pod ratio. **Weight = replica counts.** No new Service, no new
gateway URL.

### The rollout, in increments
| Phase | base `api` (old img, ceph) | `api-local` (new img, SSD) | ~ new-traffic share | Gate to advance |
|---|---|---|---|---|
| 0 (today) | 5 | 0 | 0% | drain stack healthy, `leader_count=1`, all 5 ingest nodes prepped + labeled (done, #292) |
| 1 canary | 5 | 1 | ~17% | new uploads drain (`replicated`), reads OK, `corrupt=0`, no DLQ growth |
| 2 | 5 | 3 | ~38% | replication-lag p99 within SLO, SSD backlog bounded |
| 3 | 2 | 4 | ~67% | error rate flat, smoke green |
| 4 full | 0 | 5 | 100% | soak clean → optionally repoint Service to `app: api-local` and drop the shared label |

Rollback at any phase: **scale `api-local`→0** (or scale base `api` back up). Acked bytes on SSD are
never lost — the janitor's replication gate never evicts an un-replicated chunk. Fully drained SSD
data is already in the ceph pool + backends, readable by the old fleet.

### Cross-read caveat (accepted, bounded)
An **old**-image api pod reading an object that was **just** written by an `api-local` pod, during
the 0–15 s reconcile + drain SSD→pool window, can miss — the pool copy doesn't exist yet, the old
image can't see another node's SSD, and it lacks the #286 FS re-poll. This is bounded (retryable,
not a hang on the new side) and shrinks to zero as base `api`→0. For a canary it's acceptable;
if any client is sensitive to read-after-write across pods, advance phases faster or gate reads.

### Simplest alternative (if you don't want two images)
Skip the old fleet entirely and do the staging-style **full swap** in one step (base `api`→0 +
Service→`app: api-local`). Rollback is still just the revert. Choose this only if a brief
new-only exposure is acceptable; the hybrid above is strictly safer for a first prod cutover.

---

## 2. Pre-flight checklist (before touching routing)

**Code + CI + monolith (prod deploys from the `k8s-production` branch, not `staging`):**
- [ ] **Promote the code** — `staging` → `k8s-production`, carrying forward the prod-only commits (#258, #260,
      and the redis-queues `noeviction`/cap hotfixes #287 + 2). Do **not** cherry-pick #265 (superseded).
- [ ] **Add the `build-drain` CI job** to `production-deploy.yaml` (it has none) and change the rollout-wait
      from `deployment/api` → `api-local` + add `drain-allocator`/`drain-agent` waits.
- [ ] **Remove the SPOF in the overlay** — un-pin `arion-uploader`/`downloader`/`janitor`/`backup`/`hydrator`
      off `node6-cache`; delete `local-cache-patch.yaml` + `pv-/pvc-local-cache.yaml` +
      `local-cache-configmap-patch.yaml`. Verify `kubectl kustomize k8s/production` shows no `local-cache-*`,
      no `k8s-v3-node6-cache`.
- [ ] **Free pod slots by cleanup** (not a `maxPods` raise) — reap terminal pods on the target ingest nodes +
      add `ttlSecondsAfterFinished` to the TTL-less Jobs.

**Data plane + nodes:**
- [x] **Ingest nodes prepped (done, #292).** `k8s-v3-node1..5` each have a dedicated 3.84 TB NVMe mounted at
      host `/s3-data` and are labeled `s3-prod-local-ingest=true` on the live cluster; the manifests point at
      `/s3-data`, replicas=5. (No Ceph-OSD repurpose was needed — the disks were already there.) staging's
      ingest on node2/node3 uses a different disk (node-root `local_ingest_staging`), so they don't collide.
- [ ] **Grow the CephFS shared cache** — bump `object-cache-pvc` **9.5 TiB → 20 TiB** (in
      `resource-limits.yaml`). Retiring the node6 40 TB local cache (**9.1 TB hot working set**) makes this
      CephFS RWX pool the shared *read* cache; 9.5 TiB has no headroom for it. Online-expandable
      (`ceph-filesystem allowVolumeExpansion=true`) — non-disruptive, so do it **early** (safe on the current
      monolith too). Actual usage stays janitor-GC-bounded to ~9–10 TB. **Overcommit caveat:** the pool
      backs ~16 TiB of *actual* data today (MAX AVAIL ~14 TiB); 20 TiB is a quota ceiling, not fully
      backable until Ceph OSDs are added (e.g. fold in node6's freed disk). Imperative alternative:
      `kubectl patch pvc object-cache-pvc -n hippius-s3-prod -p '{"spec":{"resources":{"requests":{"storage":"20480Gi"}}}}'`.
- [ ] **App image ≥ #265** (metrics-collision + double-count fix, PR #284) — else OTel metrics are
      inflated ~100–650× and the drain dashboards/alerts lie.
- [ ] **Run the Python migrations** (`db-migrations` Job): `object_versions.completed_part_numbers`
      (`20260706130000`), the reaper indexes (`20260702000000_multipart_uploads_initiated_at`,
      `20260706000000_parts_upload_uploaded_at`). Workers init-gate on `schema_migrations`.
- [ ] **`redis-queues` = `noeviction`, then restart it** (the policy is inert until restart); confirm
      `CONFIG GET maxmemory-policy`. It holds the work queues + pub/sub + `cephor:*` lease/fence keys —
      a full instance must reject writes loudly, never evict. Sequence in a window (AOF reload blips `cephor:*`).
- [ ] **Every worker + the drain pods have the full env/secret set** (config validation is whole-config).
      The drain reuses `hippius-s3-secrets`: `DATABASE_URL`, `REDIS_QUEUES_URL`, `HIPPIUS_UPLOAD_BACKENDS`.
- [ ] **Alerts wired + rehearsed** (already in `monitoring/grafana/.../alert-rules.yml`): DLQ depth +
      15 m growth + `dlq_dropped_total`; `drain_leader` (split-brain), `drain_leader_epoch` (decrease),
      `drain_ssd_pressure`, `janitor_aged_pending_orphans`, `drain_corrupt_parts`,
      `drain_reclaim_backing_errors`. Confirm the redis-queues memory alert thresholds (warn ~70% / page ~85% of 2 GB).
- [ ] **Ratify the data-safety SLOs with Ops** (S8 durability + S10/S11 single-leader are non-overridable).
- [ ] **Ship gate green on staging** on this exact image: S8 byte-identical re-GET; single-leader / no
      split-brain (F2 + T4-C); R2 prod-scale query gate (index scans only); focused chaos F4 + F5.

---

## 3. Manifest wiring — done in #292; one uncomment remains

The node names + path are already wired (PR #292): `ingest-node-labels-production.yaml`, both nodeAffinity
allow-lists (`api-local`, `drain-agent`), and `hostPath: /s3-data` all list `k8s-v3-node1..5`; `api-local`
`replicas: 5`. The node list is identical across the three files. Path/label deltas vs staging (`/s3-data`,
`s3-prod-local-ingest`, `postgres-nvme-rw`, `ENVIRONMENT=production`, `hostPath type: Directory`) are baked in.

**The only remaining manifest step (at cutover):** uncomment the drain block **and** the `# - name: …/drain`
image in `k8s/production/kustomization.yaml`.

---

## 4. Env vars to set / tune (prod)

Already in the prod manifests (carried from staging, live-validated):

| Var | Value | Where / why |
|---|---|---|
| `CEPHOR_ALLOC_MIN_TOTAL_BPS` | `50000000` (50 MB/s) | allocator — floor above the AIMD collapse threshold |
| `CEPHOR_ALLOC_TARGET_P99_MS` | `8000` | allocator — headroom over real per-part copy time |
| `CEPHOR_DRAIN_CONCURRENCY` | `16` | agent — overlaps per-part commit fsyncs (main throughput lever) |
| `CEPHOR_DRAIN_POLL_SECS` | `1` | agent — cheap partial-index claim; fast pickup |
| `CEPHOR_RECONCILE_POLL_SECS` | `15` | agent — the dominant fresh-read latency tail (0–15 s). Lower only via the Tier-1 landing fast-path |
| `CEPHOR_RECLAIM_POLL_SECS` / `_GRACE_SECS` | `300` / `3600` | agent — SSD junk reclaim |
| `HIPPIUS_OBJECT_CACHE_DIR` | `/var/lib/hippius/local_object_cache` | api-local — the SSD ingest dir |
| `HIPPIUS_OBJECT_CACHE_FALLBACK_DIR` | `/var/lib/hippius/object_cache` | api-local — read fallback to the ceph pool (DualFS) |

To decide / tune at cutover:

| Var | Note |
|---|---|
| `HIPPIUS_UPLOAD_BACKENDS` | drain sources this from the secret; if a backup backend is added it MUST be here or the janitor gate never clears → unbounded SSD growth |
| `CEPHOR_MAX_DRAIN_RATE_BPS` | default 100 MB/s/node × 5 nodes ≈ 870 MB/s fleet — ample over the ~573 MB/s peak; no raise needed |
| `HIPPIUS_MPU_REAPER_INTERVAL_SECONDS` | raise off the 120 s drain-down value once the reaper indexes are confirmed applied |
| `CEPHOR_CEPH_MGR_METRICS_URL` | leave unset for first bring-up (static ceiling); enable once cross-ns reach to `rook-ceph-mgr` is confirmed |

---

## 5. Cutover steps (ordered)

1. **Nodes are prepped + labeled** (done, #292). At cutover, uncomment the drain block + `drain` image in
   `k8s/production/kustomization.yaml` (§3).
2. **Migrations Job** → wait green.
3. **`noeviction`** on redis-queues + restart (§2).
4. **Deploy the drain-allocator first** (owns the `cephor_*` schema; the agent + reaper init-gate on
   it). Confirm `leader_count == 1`, `cephor_replication_status` table exists.
5. **Deploy `drain-agent` (DaemonSet) + `mpu-reaper`.** Confirm one agent per labelled node, Ready.
6. **Add the shared `role: s3-api` label** to the base `api` template and the `api-local` template;
   **patch the `api` Service selector → `role: s3-api`** (§1). Base api still `app: api`, still serving.
7. **Bring up `api-local` at 1 replica** (Phase 1). Verify (§6). Then walk Phases 2→4, gating on §6
   each step, scaling `api-local` up and base `api` down.
8. **At Phase 4** (base api = 0): optionally repoint the Service to `app: api-local` and remove the
   shared label, matching the staging end-state.
9. **Run `tests/smoke`** against prod after each meaningful phase.

Gateway image: roll normally (stateless). If you want it strictly parallel too, give the new gateway
Deployment the `app: gateway` label so both are in the `gateway` Service, then scale the old one down.

---

## 6. Verify at each phase

- **Uploads drain:** new PUTs land `pending` → `draining` → `replicated` in `cephor_replication_status`;
  `corrupt = 0`; `aged_pending_orphans` not rising.
- **Backend still ships:** `{backend}_upload_requests` drains; `chunk_backend` rows appear; DLQ flat.
- **Reads:** GET of a freshly-written object (both fleets) succeeds; cross-node fresh read bounded (§1 caveat).
- **Coordination:** `leader_count == 1`, `epoch` monotonic (never decreases).
- **SSD:** `drain_ssd_pressure` low; no node 503-ing PUTs.
- **Durability:** byte-identical re-GET (client md5 / `x-amz-checksum`, not ETag).
- **/s3-prod-health** snapshot clean (it already probes the drain stack).

---

## 7. Rollback

Write path is drain-direct + **flagless** → rollback = revert:
- **Phase 1–3:** `kubectl scale deploy/api-local --replicas=0` (traffic falls back to base api) — or
  scale base api up. Instant.
- **Full revert:** re-point the `api` Service selector to `app: api`, scale base `api` back to 5,
  scale `api-local`→0, revert the kustomization image tags. The drain stack can stay up (idle) or be
  scaled down.
- **Data safety:** acked bytes are on ingest SSD; the janitor never evicts an un-replicated chunk; the
  migration column is nullable. No acked write is lost by a rollback.

---

## 8. Post-cutover (fast-follow, not blocking)

Tracked in `s3-2.1-todo.md` → FAST-FOLLOW: the Tier-1 landing fast-path (kills the 0–15 s fresh-read
tail), the `throughput==0 ∧ backlog>0` stall alert, the full chaos matrix (F1/F3/F6/F7/F8), the 6 h
soak, the load-driver knee certification, and the residual Rust hardening (C1c read-side epoch check,
`O_NOFOLLOW`). None gate the cutover — the promotion is a reversible image/route change.
