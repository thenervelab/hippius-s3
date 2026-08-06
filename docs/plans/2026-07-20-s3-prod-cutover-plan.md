# Production cutover: promote the staging architecture, retire the node6 monolith

**Status:** proposal for team review — 2026-07-20
**Supersedes:** `2026-07-18-s3-resilient-cutover-remove-monolith.md` (three of its premises were wrong; see §9)
**Nothing in this document has been executed.** All cluster inspection was read-only.

**Goal:** production survives the loss of any single node, by replacing the node6-cache-pinned write path
with the staging local-ingest + drain-to-Ceph architecture — without downtime.

---

## 1. TL;DR

- **The problem:** 46 production pods — the entire data plane — are pinned to one node. On 2026-07-18
  that node froze and S3 returned 5xx across both ATS edge regions.
- **The fix exists and runs on `staging` today.** Production is 378 commits behind it.
- **Zero downtime is achievable.** The mechanism already exists in the staging overlay: run `api-local`
  alongside the pinned `api` fleet, then flip one Service selector. Atomic, instant, reversible.
- **But not today — three hard blockers**, all fixable without downtime, all sequencing rather than
  engineering: prod's DB is 4 migrations behind, the drain schema doesn't exist in prod, and
  `k8s/production` contains zero drain manifests.
- **The load-bearing constraint:** the new code is a *flagless* cutover. The API no longer enqueues
  uploads at PUT; the Rust drain is the sole producer. **The drain stack must be live in prod before the
  new api image rolls**, or uploads silently stall.
- **Capacity is the awkward part.** Four of six worker nodes have 1–4 free pod slots. The 07-18 plan's
  "reap terminal pods" answer does not work (terminal pods hold no slots). Real options are in §7.

---

## 2. Why — the incident

**node6-cache, 2026-07-18:** healthy and serving until 12:07:53 UTC, then the journal goes instantly
silent mid-operation. Kubelet stopped posting at 12:03:45, `NotReady` at 12:09. Recovered only by an
external OVH power-cycle ~13:37, which also bumped the kernel `6.8.0-124`→`136`.

**What the logs rule out:** no OOM-killer, no MCE, no EDAC, no hung-task, no panic. Not a software OOM,
not a kernel panic, not a clean reboot. An abrupt unlogged freeze recovered by power-cycle is consistent
with hardware, matching OVH support's attribution.

**What we could not confirm:** the specific component. Post-reboot ECC counters are clean but reset on
reboot, and there is no `mcelog`/`rasdaemon` installed to have captured a pre-crash MCE. "Hardware fault"
rests on OVH's word plus elimination, not a component-level diagnosis. → **Action:** request the OVH
intervention report / IPMI SEL, and install `rasdaemon` so the next event is diagnosable.

**Chronic side-condition, not the trigger:** node6 is memory-undersized (192 GB vs 251 GB on peers)
while carrying the whole Python write path. Worth fixing; the kernel log proves it did not cause the freeze.

**Why one node took down everything — this is the actual target of this plan.**
`k8s/production/local-cache-patch.yaml` pins workloads to `k8s-v3-node6-cache` because `local-cache-pvc`
is a *local* PV (40 TB ZFS, nodeAffinity → node6). Verified live, and broader than the patch file suggests
(`backup` and `hydrator` were added later):

| Deployment | Replicas | Pinned to node6 |
|---|---|---|
| api | 5 | yes |
| arion-uploader | 10 | yes |
| arion-downloader | 10 | yes |
| backup | 10 | yes |
| hydrator | 10 | yes |
| janitor | 1 | yes |

**46 pods on one node.** All 5 `api` replicas confirmed on node6 (`10.42.33.*`). Only `gateway` is spread.
Aggravating: **there are no PodDisruptionBudgets on `api` or `gateway`** — prod PDBs cover Postgres only.

---

## 3. Current vs target

| Aspect | Current (monolith) | Target (staging design) |
|---|---|---|
| API serving | `api` ×5 pinned to node6 | `api-local`, 1/ingest node, hostPath SSD + Ceph read-fallback |
| Workers | pinned to node6 | un-pinned, read CephFS across the pool |
| Durable cache | 40 TB node-local PV on node6 | CephFS `object-cache-pvc` (**already bound in prod, 9728Gi RWX**) |
| Local→durable | n/a (single disk) | `drain-agent` DaemonSet + leader-elected `drain-allocator` |
| Upload producer | api enqueues at PUT | **drain, after Ceph replication (no flag, no fallback)** |
| Single-node loss | total S3 outage | degraded — other ingest pod + Ceph read-fallback |
| Lives in repo | `k8s/production/` | `k8s/staging/` — prod overlay exists but is commented out |

---

## 4. Verified ground truth (2026-07-20, read-only)

### 4.1 Branch topology

Deploy triggers: `staging`→staging env, **`k8s-production`→production**, `main`→tests only (no deploy).
*Pushing to `main` does not change production.*

- `k8s-production..staging` = **378 commits.** Prod image `d495dd1`, staging `e6b26c6`.
- `staging..k8s-production` = **5 commits that must carry forward**: #258 (prod delete/unpin + auth/cache/
  observability), #260 (flag-gated batch-delete unpinner), and the redis-queues `noeviction` fix (#287,
  `f5726e5` + `190320a`).
- `staging..main` = 2 commits (#265, #284, metrics) — **superseded** by staging's otel rework. Do not
  cherry-pick.

### 4.2 The flagless cutover — the constraint that orders everything

`hippius_s3/writer/queue.py` was **deleted in `017d057`**. On staging, PutObject, CompleteMPU and S4
append no longer LPUSH to `arion_upload_requests`; they persist `object_versions.address` only. The Rust
drain agent produces the upload after Ceph replication (`crates/hippius-drain-agent/src/enqueue.rs:139`).

There is **no feature flag** — nothing in `config.py` gates this. `s3-2.1-todo.md:77` states it plainly:
*"Cutover is flagless (no producer toggle; rollback = image revert)."*

**Consequence: the drain stack must be live and consuming before the new api image rolls.**

**The good news — a rolling update is safe.** The two producer paths are mutually exclusive per object
version, keyed on `address`:
- Old pods LPUSH at PUT and never write `address` → the drain's `load_upload_context`
  (`store.rs:613-645`) returns `None` → deferred harmlessly (already enqueued).
- New pods write `address` and never LPUSH → invisible to the old path.

**No object is ever enqueued twice.** Mixed old/new api pods behind one Service will not duplicate or
corrupt uploads.

**One narrow divergence during the roll:** `object_versions.completed_part_numbers` is written only by new
pods (`object_writer.py:937`) and filtered only by new readers (`services/parts_catalog.py:47`). If a *new*
pod completes an MPU naming a strict subset of parts and an *old* pod later serves that GET, the old reader
ignores the column and serves **all** parts → wrong bytes, wrong multipart ETag. Subset-completion MPUs
only; the reverse direction is safe (NULL = "all parts"). Mitigation: keep the roll window short.

### 4.3 The zero-downtime lever

Staging's `api` Service selects `app: api-local` (a Kustomize selector patch). The gateway's
`GATEWAY_BACKEND_URL=http://api:8000` is unchanged and never notices. So the cutover is:

> bring `api-local` up alongside the pinned `api` → flip one Service selector → scale base `api` to 0

Atomic, instant, and rollback is flipping it back.

### 4.4 Runtime dependencies — mostly clear

- **`cephor_*` schema is NOT required by the api.** The only api-side write is AbortMPU's
  `fail_version_replication` (`api/s3/multipart.py:791`), wrapped in `contextlib.suppress`. It is the
  separate **mpu-reaper** that init-gates on `to_regclass('cephor_replication_status')`.
- **hostPath ingest dir is optional** — only `HIPPIUS_OBJECT_CACHE_DIR` / `_FALLBACK_DIR` matter. The new
  api on a plain Ceph PVC works unchanged.
- **FS chunk path layout is identical** — `git diff k8s-production...staging -- hippius_s3/cache/fs_store.py`
  is empty. A shared cache during the roll is safe.
- **Queue payloads compatible** — `RetryableRequest` uses `ConfigDict(extra="ignore")` on both branches;
  queue names unchanged.

### 4.5 Config and secrets drift — smaller than expected

`hippius-s3-defaults`: **zero keys present in staging but missing in prod.** Prod-only keys are exactly the
three from `local-cache-configmap-patch.yaml` (`HIPPIUS_OBJECT_CACHE_DIR`,
`HIPPIUS_OBJECT_CACHE_FALLBACK_DIR`, `HIPPIUS_FS_CACHE_GC_MAX_AGE_SECONDS`) — **that patch must be dropped
at cutover** so the cache dir reverts to the CephFS path.

`hippius-s3-secrets`: **key sets identical, 28 keys each** (names compared only). All three secretKeyRefs
the drain-agent needs exist in prod. Upload backends already match staging (`arion,ovh`).

Meaningful value drift — prod's uploader retry policy is far shallower and was tuned for the old inline path:

| Key | prod | staging |
|---|---|---|
| `HIPPIUS_UPLOADER_MAX_ATTEMPTS` | 2 | 7 |
| `HIPPIUS_UPLOADER_BACKOFF_BASE_MS` | 100 | 500 |
| `HIPPIUS_UPLOADER_BACKOFF_MAX_MS` | 500 | 60000 |

### 4.6 redis-queues — correct in prod; the ConfigMap lies

Live: `maxmemory-policy=noeviction`, `maxmemory=4 GiB`, `evicted_keys=0`, used 5.22M / peak 9.45M. Passed as
StatefulSet args. **Trap:** the `redis-queues-config` ConfigMap still reads `1gb` / `allkeys-lru` in *both*
namespaces and is **not mounted** — dead config that will generate false findings. Delete it.

### 4.7 Health baseline

All prod pods Running/Ready except one `OutOfpods` uploader orphan (cosmetic, from a scaled-to-0 ReplicaSet).
~10–16k gateway ops per 5 min. **Zero 5xx in the last 30 min; zero api-level ERROR/Traceback.** No HPAs.
This is a clean baseline to cut over from.

### 4.8 Ceph

```
health: HEALTH_WARN (mons f,g,h low on available space — mon store, not data)
osd: 29 up/in;  usage 30 TiB used / 101 TiB avail
1,269,634 objects misplaced (4.39%) — 7 backfilling, 14 backfill_wait, recovery 333 MiB/s
ceph-filesystem-data0: 7.4 TiB stored, 28.56% used, 19 TiB MAX AVAIL
```

Prod **already has** the CephFS RWX PVC: `object-cache-pvc  Bound  9728Gi  ReadWriteMany  ceph-filesystem`
(2.3 T used). node6's local ZFS holds 7.0 T of 42 T.

**Two cautions given the in-flight 30 TB decom (§7.3).**

---

## 5. Blockers — all fixable with zero downtime, none resolved today

**B1 — prod DB is 4 migrations behind; `object_versions.address` does not exist.**
```
prod:    20260715000000, then 20260528120800...   (out of order = cherry-pick signature)
staging: 20260718000000, 20260715000000, 20260706130000, 20260706000000, 20260702000000, 20260622122700
prod object_versions:    NO address, NO completed_part_numbers
```
The new API calls `set_object_version_address` on every write. **Deploying the new image today fails on
every PUT.** Root cause is branch drift, not a failed migration.

**B2 — the drain schema does not exist in prod.** `cephor_gc_state` / `cephor_replication_status` come from
`crates/hippius-drain-core/migrations/0001..0012` (sqlx, run by the drain binaries).
`python -m hippius_s3.scripts.migrate` will **not** create them.

**B3 — `k8s/production` has zero drain manifests.** The overlay files exist on `staging` but are
deliberately commented out in `k8s/production/kustomization.yaml`. Additionally
`production-deploy.yaml` has **no `build-drain` job** and still waits on `deployment/api`.

**B4 (not in the 07-18 plan) — no PDBs on `api`/`gateway`.** Without them, moving pods during the
transition is not zero-downtime.

---

## 6. Migrations

Five new migrations; all **additive and safe against the running old image**.

| Migration | Effect | Note |
|---|---|---|
| `20260622122700_object_versions_address` | `ADD COLUMN address TEXT` | nullable; required by the drain promoter |
| `20260702000000_multipart_uploads_initiated_at_index` | partial index | MPU reaper |
| `20260706000000_parts_upload_uploaded_at_index` | **`CREATE INDEX CONCURRENTLY`** on `parts` | ~94M rows — see below |
| `20260706130000_object_versions_completed_parts` | `ADD COLUMN completed_part_numbers integer[]` | NULL = "all parts" |
| `20260718000000_objects_bucket_prefix_active_index` | **`CREATE INDEX CONCURRENTLY`** on `objects` | partial, `deleted_at IS NULL` |

The two `CONCURRENTLY` builds do not take a write lock, but they take real time at prod cardinality. Both
migration files document the recovery path: a build that fails midway leaves an **INVALID** index that
`IF NOT EXISTS` will then *skip* — it must be dropped or `REINDEX CONCURRENTLY`'d manually before re-running.
**Run these as a standalone step, watched, not inside the cutover deploy.**

---

## 7. Capacity — the awkward constraint

### 7.1 Correcting the 07-18 plan

That plan said the crunch is solved by reaping terminal pods, no `maxPods` raise. **Succeeded/Failed pods
hold no scheduling slot.** Proof: node1 had 107 non-terminated + 10 terminal = 117 > cap 110. The 62
terminal pods cluster-wide are `kubectl get` noise; reaping them frees nothing.

**Real free slots (cap 110 on all workers):**

| node1 | node2 | node3 | node4 | node5 | node6-cache |
|---|---|---|---|---|---|
| 1 | 1 | **20** | 1 | 4 | **14** |

The ingest tier needs ~2–3 slots per node **plus rolling-update surge headroom**. Only **node3** and
**node6-cache** have room today.

### 7.2 Reclaimable running pods — 110 across the pool

| Group | N | node1 | node2 | node3 | node4 | node5 | node6 |
|---|---|---|---|---|---|---|---|
| **T1** indexer bare backfill pods (28–60d, no owner) | 9 | 2 | · | · | 5 | 2 | · |
| **T1** arion-staging stuck Init 97–111d | 2 | 1 | · | · | · | 1 | · |
| **T1** arion `loki-0` crashloop 91d / 25,624 restarts | 1 | · | · | · | 1 | · | · |
| **T1** finney-light stuck + crashlooping | 2 | 1 | · | · | 1 | · | · |
| **T1** prod `redis-download-cache-0` (decommissioned) | 1 | · | · | · | · | 1 | · |
| **T1** prod `redis-chain-0` (decommissioned) | 1 | 1 | · | · | · | · | · |
| **T2** sentry (whole stack) | 65 | 28 | 11 | 15 | 10 | 1 | · |
| **T3** staging `redis-cluster*` (unreferenced) | 12 | 3 | 4 | 1 | 3 | 1 | · |
| **T3** staging test-restore PG clusters | 2 | 2 | · | · | · | · | · |
| **T3** staging chaos-mesh + toxiproxy | 14 | 2 | 1 | 2 | 3 | 2 | 1 |
| **T3** staging `redis-chain-0` | 1 | 1 | · | · | · | · | · |
| **Free after T1 only** | | 6 | 1 | 20 | 8 | 8 | 14 |
| **Free after T1+T2+T3** | | 42 | 17 | 38 | 24 | 12 | 15 |

**T1 (16 pods) is unambiguously dead** — no owner decision needed. The 9 indexer backfills have no
ownerReference at all, so nothing respawns them. `redis-download-cache` and `redis-chain` are documented as
decommissioned in CLAUDE.md §5.3 and confirmed unreferenced by any Deployment env.

**T2 Sentry needs an owner decision, not a reap.** 11 consumers CrashLoopBackOff for 82 days with 11k–15k
restarts each. **But prod S3's `SENTRY_DSN` points at `sentry-relay.sentry.svc.cluster.local:3000/3`** —
Sentry *is* wired into production. With the snuba pipeline dead for 82 days, relay is almost certainly
accepting events that get dropped downstream: wired but not delivering. Decide fix-or-remove.

**T3 caveat:** chaos-mesh + toxiproxy (14 pods) were added ~7 days ago for the drain chaos matrix. **Do not
remove until the F2/F4/F5 chaos gates are signed off** — they are needed for this cutover.

**T4, not counted:** `hippius-indexer` runs 116 pods, nearly all redeployed today, so the fleet is live. But
there are visible v1/v2 pairs (e.g. `account-creations-consumer` 55d alongside
`hippius-subsquid-account-creations-consumer-v2` 0d). If the v1s are superseded that is a meaningful
reclaim — needs the indexer owner.

Separately: `hippius-indexer` / `hippius-marketing` Jobs lack `ttlSecondsAfterFinished`, which is why 62
terminal pods accumulated. Worth fixing for readability; not capacity.

### 7.3 The 30 TB Ceph decom (in progress)

Reclaiming OSD NVMe for node-local ingest disk is the right move and unblocks the disk side. Two cautions:

1. **Local disk does not move the pod-slot constraint.** Put the reclaimed NVMe on nodes that also have
   slots, or you get disk you cannot schedule onto.
2. **Capacity math (estimate, needs verification).** At ~3× replication, removing 30 TiB raw takes CephFS
   `MAX AVAIL` from 19 TiB to roughly **9–10 TiB**. Post-cutover, CephFS must absorb what node6's ZFS holds
   today — **7.0 TiB and growing**. That is tight enough to verify the drained steady-state footprint
   *before* retiring node6, not after.
3. **Do not stack the operations.** Ceph is `HEALTH_WARN` and mid-backfill (4.39% misplaced, 333 MiB/s).
   Removing OSDs adds backfill, and the cutover moves cache reads *onto* Ceph. Let it return to
   `HEALTH_OK` between the decom and the cutover.

---

## 8. Open decisions for the team

1. **Git strategy** — make `main` the trunk (merge `staging`→`main`, fast-forward `k8s-production`), or
   merge `staging`→`k8s-production` directly? 378-commit promotion either way. Either way, **#258, #260 and
   the redis `noeviction` fix must be carried forward**; #265/#284 are superseded.
2. **Ingest topology** — confirm **node3 + node6-cache** (the only pair with slots today), or free slots
   elsewhere first. Confirm the label (`s3-prod-local-ingest=true`), hostPath
   (`/var/lib/hippius/local_ingest_prod`), and whether to **taint** the ingest nodes so staging/dev cannot
   recompete.
3. **Sentry** — fix or remove. Biggest single reclaim (65 pods, 28 on node1) and currently a broken
   dependency of prod error reporting either way.
4. **node6 RAM** — rightsize 192→256 GB to match peers, or accept it as an ingest node now that it is no
   longer the sole write-path host?
5. **Uploader retry policy** — reconcile prod's 2 attempts / 500 ms max with staging's 7 / 60 s. The drain
   changes retry dynamics; prod's values were tuned for the old inline path.
6. **Chaos gating** — is F2 (allocator failover) + F4 (CephFS degraded) + F5 (SSD fill) enough to ship, with
   the full matrix as fast-follow?
7. **OVH RCA** — who chases the intervention report / IPMI SEL.

---

## 9. What changed from the 07-18 draft

| 07-18 claim | Verified reality |
|---|---|
| "Reap terminal pods, no `maxPods` raise" | Terminal pods free **zero** slots. Only node3 + node6 have room. |
| `main..staging` = 280 commits | `k8s-production..staging` = **378** |
| Prereqs mostly captured | **Prod DB is 4 migrations behind** and `cephor_*` is absent — neither was listed |
| PDBs not mentioned | **No PDBs on `api`/`gateway`** — a zero-downtime blocker |
| Ceph "GO, 75 TiB free" | True, but `HEALTH_WARN` + mid-backfill, and the 30 TB decom changes the math |

Also worth knowing: the stress-test **G4 "sole-producer" gate is vacuously green** —
`stress-test/inv/inv_det.py:41` greps for `enqueue_upload(`, a function deleted in `017d057`. Do not treat
it as verification of §4.2.

---

## 10. The plan

### Phase 1 — Capacity and hygiene (no downtime, no dependencies)
- **1.1** Reap T1 (16 pods). Unambiguously dead, nothing respawns.
- **1.2** Owner decision on Sentry (§8.3) and indexer v1/v2 (§7.2 T4); act on the outcome.
- **1.3** Add `ttlSecondsAfterFinished` to indexer/marketing Jobs.
- **1.4** Delete the stale, unmounted `redis-queues-config` ConfigMap in both namespaces.
- **1.5** Install `rasdaemon` on the worker nodes; chase the OVH intervention report.
- **Gate:** the two chosen ingest nodes each show ≥ 4 free slots.

### Phase 2 — Database (no downtime, safe against the running old image)
- **2.1** Apply the 5 migrations to prod as a standalone watched step, **not** inside a deploy. Expect the
  two `CONCURRENTLY` builds to take time; know the INVALID-index recovery (§6).
- **2.2** Verify `object_versions.address` and `completed_part_numbers` exist.
- **Gate:** prod `schema_migrations` matches staging; prod api still serving at baseline.

### Phase 3 — Reconcile source of truth
- **3.1** Record the chosen git strategy (§8.1).
- **3.2** Integration branch from `staging`; carry forward **#258, #260, redis `noeviction`**. Do **not**
  cherry-pick #265/#284.
- **3.3** Promotion PR; CI `test-and-lint` green. Hold merge until Phase 4 lands in the same train.

### Phase 4 — Author the prod overlay + CI drain build
- **4.1** `ingest-node-labels-production.yaml` — replace the `REPLACE-prod-ingest-node-*` placeholders with
  the chosen nodes; same in both nodeAffinity allow-lists.
- **4.2** Uncomment the drain block and the `drain` image in `k8s/production/kustomization.yaml`.
- **4.3** Add the rust-gated `build-drain` job to `production-deploy.yaml`; set
  `deploy needs: [build-images, build-drain]`; **change the `api` rollout-wait to `api-local`** and add
  `drain-allocator` / `drain-agent` waits.
- **4.4** Add PDBs for `api`, `api-local`, and `gateway`.
- **4.5** Flip the `api` Service selector → `app: api-local`; scale base `api`→0; un-pin
  `arion-uploader`/`arion-downloader`/`janitor`/`backup`/`hydrator`; **delete** `local-cache-patch.yaml`,
  `pv-local-cache.yaml`, `pvc-local-cache.yaml`, `local-cache-configmap-patch.yaml`.
- **Gate:** `kubectl kustomize k8s/production` renders **no** `local-cache-*` and **no**
  `hostname: k8s-v3-node6-cache`; base `api` 0; Service → `api-local`.

### Phase 5 — Prerequisites
- **5.1** Node prep: mount reclaimed NVMe at `/var/lib/hippius/local_ingest_prod`; label
  `s3-prod-local-ingest=true`; decide taints.
- **5.2** Confirm Ceph back to `HEALTH_OK` post-decom, and verify the drained steady-state footprint fits
  the post-decom `MAX AVAIL` (§7.3).
- **5.3** Alerts + runbooks: drain lag / backlog / SSD pressure / DLQ / leader-loss; redis-queues memory;
  rollout+rollback runbook.
- **5.4** Staging ship-gate — **NO-GO if any red:** `inv-guard` green; S8 byte-identical durability;
  S10/S11 single-leader + allocator failover; R2 index-only queries; chaos F4 + F5.

### Phase 6 — Cutover (reversible; order is load-bearing)
- **6.1** Merge the promotion + overlay train → `k8s-production`. Watch the build, including `drain`.
- **6.2** **Bring the drain stack up FIRST.** `drain-allocator` first (it owns and creates the `cephor_*`
  schema, resolving B2), then `drain-agent` N/N. Confirm healthy and consuming **before** anything else.
- **6.3** Roll the api image. Mixed old/new pods coexist safely (§4.2). Keep the window short.
- **6.4** Scale `api-local` up alongside the pinned `api`; flip the Service selector; drop
  `local-cache-configmap-patch.yaml`; scale base `api`→0; workers reschedule onto Ceph.
- **6.5** Smoke + soak: 5xx at baseline (`AtsClient5xxOutage` quiet), drain lag bounded, DLQ ~0.
- **Rollback:** revert the api image (restores PUT-enqueue) + flip the Service selector back; base `api`
  back up. Data-safe — the janitor never evicts an un-replicated chunk.

### Phase 7 — Prove resilience, retire the monolith
- **7.1** **Single-node-kill drill** (the acceptance test): cordon+drain one ingest node. S3 stays serving,
  5xx at baseline, drain resumes on recovery.
- **7.2** Reclaim node6's 40 TB (`Retain` PV → release); rightsize or repurpose the node.

---

## 11. Definition of done

- Losing any single node no longer causes an S3 outage (7.1 passes).
- `k8s/production` has no node6 pin and no `local-cache` PV/PVC.
- The staging architecture is source-of-truth on the prod deploy branch, with #258/#260/`noeviction`
  preserved and #265/#284 confirmed superseded.
- Prod `schema_migrations` matches staging; `cephor_*` present.
- PDBs exist on `api-local` and `gateway`.
- Ingest nodes labelled, disk-backed, and protected from recompeting workloads.

---

## 12. Evidence pointers

- SPOF: `k8s/production/local-cache-patch.yaml`, `pv-local-cache.yaml`, `pvc-local-cache.yaml`
- Target design: `k8s/staging/README-local-ingest-trial.md` + the `api-local` / `drain-*` manifests
  (its "Uploads" section is **stale** — pre-PR-7 text)
- Flagless cutover: `put_object_endpoint.py`, `multipart.py:1131`, `append.py:150`; deletion in `017d057`
- Drain promoter: `crates/hippius-drain-core/src/store.rs:613-645`, `crates/hippius-drain-agent/src/enqueue.rs:139`
- Cutover checklist: `s3-2.1-todo.md`; gates: `docs/audits/s3-2.1-drain-road-to-prod.md`
- CI: `.github/workflows/{production,staging}-deploy.yaml`
- Reclaim list: regenerate before acting — pod state moves.

## 13. Known gaps, stated honestly

- The Ceph post-decom capacity number in §7.3 is an **estimate** from the observed ratio, not a measurement.
  Verify against actual `MAX AVAIL` after the OSDs are out.
- The node6 hardware fault is attributed, not diagnosed (§2).
- After the drain's Redis LPUSH there is still **no reconciler** — `object_versions.status` never leaves
  `'publishing'` and nothing re-drives a lost upload request. Pre-existing, not introduced here, but it is
  the sharpest remaining edge in the new write path.
- No load test of the CephFS read path at prod request rates has been run. Client IO is already ~817 MiB/s
  read; the cutover adds the ex-node6 cache reads on top.
