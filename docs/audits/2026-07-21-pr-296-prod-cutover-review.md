# PR #296 review — s3-2.1 production cutover readiness

**Reviewed:** 2026-07-21
**PR:** [#296 `release(1/3): promote staging → k8s-production`](https://github.com/thenervelab/hippius-s3/pull/296) — draft, `release/promote-staging` → `k8s-production`
**Stack:** #296 ← [#297 `enable-drain`] ← [#298 `full-swap`]; sibling `s3-backup#18`
**Scope:** 378 files, +51,652 / −1,913, 398 commits ahead of `k8s-production` (merge-base `d495dd1`)
**Method:** branch diff against `origin/k8s-production`, plus read-only verification against the live
`hippius` kubectl context (`hippius-s3-prod`, `hippius-s3-staging`) and the prod Postgres primary.

---

## Verdict

**Do not merge, and do not run the hybrid cutover yet.** Three blocking issues and five significant
ones. The most important finding is not in the diff's code — it is a structural property of the
cutover: **there is no partially-deployed state of this stack that preserves upload durability.**

The stated goal — *bring the v2 service up on the cluster and test it without interrupting
production* — is not achievable with these three PRs as written. Every intermediate configuration,
including the operator-driven "zero-downtime hybrid", silently stops replicating some or all
production uploads. Worse, the safety gates built to protect the hybrid are structurally incapable
of detecting it.

| # | Severity | Finding |
|---|---|---|
| B1 | Blocker | Merging #296 alone dark-uploads 100% of prod traffic |
| B2 | Blocker | No safe hybrid state: drain triggers on SSD scan only; base `api` writes to CephFS |
| B3 | Blocker | Ingest-node slots are unprotected; CI downgrades a partial drain-agent roll to a warning |
| S1 | Significant | `rollout.sh` hybrid gates are blind to the B2 failure and report green |
| S2 | Significant | 9.2 TiB hot cache abandoned at cutover → cold-cache re-fetch storm |
| S3 | Significant | `rollout.sh` is not committed to any branch |
| S4 | Significant | `rollout.sh` lacks `set -e`; an unchecked patch can empty the `api` Service |
| S5 | Significant | Rollback is not durable against the next CI deploy, and vice versa |
| M1–M4 | Minor | Migration concurrency, `/s3-data` unverified, stuck pod, dead Redis StatefulSets |

---

## 1. Live production baseline (verified 2026-07-21)

Prod runs image tag `d495dd1` — exactly `k8s-production` HEAD, no drift.

| Signal | Value |
|---|---|
| Workloads | api 5/5, gateway 5/5, arion-uploader 10/10, arion-downloader 10/10, arion-unpinner 3/3, janitor 1/1, backup 10/10, hydrator 10/10, cleanup 3/3 |
| Pinned to `k8s-v3-node6-cache` | 46 pods: 10 hydrator, 10 backup, 10 uploader, 10 downloader, 5 api, 1 janitor |
| `schema_migrations` head | `20260715000000`, then jumps back to `20260528120800` — **4 migrations missing** |
| `object_versions.address` | **absent** |
| `object_versions.completed_part_numbers` | **absent** |
| `cephor_*` tables | **none** |
| `object_versions` by status | publishing 131,957,697 · failed 498,876 · uploaded 75,522 |
| redis-queues | `maxmemory=4GiB`, `maxmemory-policy=noeviction` ✅ |
| `LLEN arion_upload_requests` | **0** ✅ |
| `object-cache-pvc` | 9728Gi (PR targets 20480Gi) |
| local NVMe cache (node6) | 42T capacity, **9.2T used** |
| CephFS object cache | 9.5T capacity, 2.3T used |
| Ingest labels `s3-prod-local-ingest=true` | already applied to node1–node5 ✅ |

**Replication health is good.** A bounded probe of the 200k most recent `part_chunks` found only
**169** without an active `chunk_backend` row — consistent with normal in-flight work, not a backlog.
Combined with an upload queue depth of 0, this is a clean point at which to abandon the node6 cache
*from a durability standpoint*.

Staging has soaked the target architecture for **32 days**: `drain-agent` 2/2 (node2, node3),
`drain-allocator` 1/1, `mpu-reaper` 1/1, `api-local` 2/2. That is real evidence the end state works.
It does **not** cover the mixed-fleet transition, which is where every blocker below lives.

---

## 2. Blockers

### B1 — Merging #296 alone stops all upload replication

The promoted code deleted `hippius_s3/writer/queue.py`. `PUT`, multipart-complete, and append no
longer `LPUSH` to `arion_upload_requests`; they only persist `object_versions.address`
(`hippius_s3/api/s3/objects/put_object_endpoint.py:195`). The Rust drain is now the sole upload
producer — and in this PR it is **commented out** in `k8s/production/kustomization.yaml:12-22`.

Consequence: every new object lands in cache, is never uploaded to Arion, is never published to
chain, and — because the janitor's replication gate refuses to evict an un-replicated chunk — is
never evictable. Uploads fail silently and the cache fills.

The PR body says "Do NOT merge alone". That is the correct intent, but it is enforced only by prose.
This is an **open PR whose base branch is the deploy trigger**; a single merge is a production
durability incident. `isDraft=true` and `mergeable=MERGEABLE` today.

> **Recommendation:** add a branch-protection rule or a required check on `k8s-production` that fails
> when `kustomization.yaml` has the drain block commented out *and* `writer/queue.py` is absent.
> Prose in a PR description is not a safety mechanism.

### B2 — There is no safe hybrid state (the load-bearing finding)

This is the non-obvious one, and it invalidates the "bring it up alongside prod and test" plan.

Three facts compose into it:

1. **The drain's only trigger is an SSD scan.** `crates/hippius-drain-core/src/reconcile.rs:4` —
   *"In the api part model there is no NOTIFY fast path: the reconciler is the sole drain trigger."*
   The agent scans `CEPHOR_SSD_ROOT=/var/lib/hippius/local_object_cache`, which is the `/s3-data`
   hostPath. `CEPHOR_POOL_ROOT=/var/lib/hippius/object_cache` (CephFS) is the drain **destination**.
   *Nothing ever scans CephFS for new parts.*

2. **`address` is not a location.** `set_object_version_address` stores
   `request.state.account.main_account` — the substrate account. It lets the drain rebuild an
   `UploadChainRequest` for a part it has already found on its own SSD. It cannot help the drain
   *find* a part.

3. **#296 moves the base `api` onto CephFS.** Deleting `local-cache-patch.yaml` and
   `local-cache-configmap-patch.yaml` un-pins api/uploader/downloader/janitor from node6 and reverts
   `HIPPIUS_OBJECT_CACHE_DIR` to the CephFS path. (Confirmed: the live ConfigMap's
   `kubectl.kubernetes.io/last-applied-configuration` contains `HIPPIUS_OBJECT_CACHE_DIR`, so
   `kubectl apply` will strip the key rather than leave it.)

Therefore **any part written by a base `api` pod running the new image is invisible to every
drain-agent** — permanently dark. And because the `api` Service still selects `app: api` until #298,
that is 100% of traffic:

| Deployed state | Uploads replicated? |
|---|---|
| #296 alone | ✗ 0% — drain not deployed |
| #296 + #297 (drain up, routing not flipped) | ✗ 0% — all traffic on base `api` → CephFS |
| Hybrid shift (`rollout.sh`) | ✗ proportional to remaining base-api replicas |
| #296 + #297 + #298 (full swap) | ✅ correct |

**The three PRs are one atomic change.** Reviewing them as independent layers is fine; deploying them
independently is not. The PR body describing #297 as *"Enables the SSD-ingest tier"* reads like a
meaningful standalone step — it should say explicitly that no intermediate state is durability-safe.

### B3 — Ingest-node slots are unprotected, and CI treats the failure as a warning

Live capacity (allocatable 110 pods/node):

| Node | Running | Free |
|---|---|---|
| k8s-v3-node1 | 108 | **2** |
| k8s-v3-node2 | 106 | 4 |
| k8s-v3-node3 | 82 | 28 |
| k8s-v3-node4 | 105 | **5** |
| k8s-v3-node5 | 100 | 10 |
| k8s-v3-node6-cache | 47 | 63 |

Cluster-wide there is room. The problem is **placement asymmetry**: `api-local` and `drain-agent` are
constrained to node1–node5 (`nodeSelector: s3-prod-local-ingest=true` plus a hostname allow-list),
while the 46 pods un-pinned from node6 by #296 can land anywhere — including consuming the exact
ingest-node slots the drain tier needs. Nothing prevents this: the ingest nodes carry **no taint**,
and neither `api-local` nor `drain-agent` carries a `priorityClass`. node1, with 2 free slots, needs
2 (one `api-local` + one `drain-agent`) and has no margin.

This is made dangerous rather than merely annoying by `.github/workflows/production-deploy.yaml:307-318`,
which downgrades a partial DaemonSet rollout to a **warning**:

```
::warning::drain-agent rolled ${ready}/${desired} (a labeled node likely at pod capacity); proceeding
```

Because `api-local` selects the *same label*, an ingest node can end up running `api-local` with **no
drain-agent**. Every upload served by that pod stages to its local SSD and is never drained — and CI
reports the deploy green. This is the single most dangerous line in the diff.

> **Recommendation:** taint the ingest nodes and add matching tolerations to `api-local`/`drain-agent`
> so general workloads cannot occupy those slots; give both a `priorityClass`; and make the deploy
> fail unless `drain-agent` Ready count equals the number of nodes hosting `api-local`. Do the
> pod-slot cleanup (listed as pre-flight, not yet done) before any cutover.

---

## 3. Significant issues

### S1 — `rollout.sh`'s hybrid gates cannot detect B2

`rollout.sh` STEP 4–5 adds a shared `role: s3-api` label to both fleets, repoints the `api` Service at
it, then shifts replicas `api-local 1→5` / `base api 5→0`, gating each phase on:

- `assert_safe` (lines 81–87) — asserts `corrupt = 0` and `cephor:leader` exists;
- `watch_convergence` (lines 89–95) — watches `cephor_replication_status` `pending + draining`.

Both are blind to the failure. A base-api dark upload **never creates a `cephor_replication_status`
row at all**, so `undrained` stays 0 and every phase prints `✓ phase healthy`. At the first phase
(`api-local=1`, `base-api=5`) roughly **83% of uploads are dark** while the script reports
`~16% on the new path`.

The hybrid is therefore not the cautious option — it is the most dangerous one, because it runs a
durability gap for the entire duration of the shift behind green indicators.

A gate that would actually catch it — parts that the api has acked but no drain has recorded:

```sql
-- Must be 0 before advancing a phase. Counts object versions the api made serveable
-- (address written) that no drain-agent has ever seen, past a grace window.
SELECT count(*)
FROM object_versions ov
WHERE ov.address IS NOT NULL
  AND ov.last_modified < now() - interval '10 minutes'
  AND NOT EXISTS (
        SELECT 1 FROM cephor_replication_status crs
        WHERE crs.object_id = ov.object_id
          AND crs.object_version = ov.object_version);
```

Bind the exact join to `cephor_replication_status`'s real part key before use.

### S2 — `rollout.sh` is not committed

The PR body cites it three times — as the gated alternative to the big-bang merge and as a rollback
path (`rollout.sh rollback`). It exists on the author's machine only; it is absent from
`release/promote-staging`, `release/enable-drain`, `release/full-swap`, and `staging`. One of the two
documented rollback mechanisms is currently unreviewable. (`path-to-prod.md`, `s3-2.1-todo.md`, and
`s3-prod-drain-capacity-plan.md` *are* present.)

### S3 — `rollout.sh` omits `set -e`

Line 22 is `set -uo pipefail`; the repo standard is `set -euo pipefail`. STEP 4 (lines 162–166) patches
base `api`, patches `api-local`, waits for both rollouts, then patches the Service selector — with no
exit-code checks. If the base-api label patch fails, the script proceeds and repoints the Service
anyway, potentially selecting **zero endpoints**: a full outage from an unchecked return code. STEP 5's
`kc scale` calls are likewise unchecked.

Note the script *is* otherwise defensive — the signal helpers fail closed, since an empty result makes
`assert_safe`'s string comparison abort. The gap is specifically in the mutating steps.

### S4 — 9.2 TiB hot cache is abandoned instantly

The node6 NVMe holds 9.2 TiB of hot working set. After #296, pods un-pin from node6 and the cache dir
reverts to CephFS, which holds 2.3 TiB. Durability is not at risk (see the replication probe), but
every read for the abandoned working set becomes a miss that re-fetches from Arion, at full fleet
concurrency, against a CephFS pool that must simultaneously absorb the writes.

The PVC grow to 20480Gi ships in `resource-limits.yaml` with an honest overcommit note (20 TiB exceeds
the pool's currently backable ~16 TiB; it is bounded in practice by janitor GC). That reasoning is
sound, but it makes **janitor health a hard dependency** during precisely the window when the cache is
churning hardest. Plan for the latency spike; consider a staged warm-up rather than a cliff.

### S5 — Rollback is not durable, and CI fights the hybrid

Two directions of the same problem:

- `rollback()` (lines 197–207) patches the live Service selector and replica counts. If #298 is merged,
  the next CI `kubectl apply -k` re-applies `selector: app: api-local` and `replicas: 0` — silently
  re-cutting over. **Rollback must include reverting the branch**, not just the cluster.
- Conversely, during a hybrid window the base `services.yaml` selector is `app: api`, so *any*
  unrelated CI deploy reverts the `role: s3-api` selector mid-shift. **Freeze deploys** for the
  duration, or the routing is not stable.

---

## 4. Minor / to verify

- **M1 — Migration concurrency (unverified).** `start-api.sh` runs `python -m hippius_s3.scripts.migrate`
  before uvicorn, so all 5 api replicas run it, concurrently with the `db-migrations` Job. I found no
  advisory lock in `migrate.py`. This rollout applies 4 migrations to prod for the first time; confirm
  the runner is race-safe.
- **M2 — `/s3-data` mount unverified.** I could not confirm from kubectl that the NVMe is mounted on
  node1–node5 (kubelet stats does not expose unused host mounts). With `hostPath type: Directory`, a
  missing mount leaves `api-local` in `ContainerCreating`. `rollout.sh:110` only warns. Verify by hand.
- **M3 — Stuck pod.** `arion-uploader-556d5999cd-8vbc8` has been `Failed` /
  `ContainerStatusUnknown` (exit 137) for 3d2h. Clean it up before counting node slots.
- **M4 — Dead StatefulSets still running.** `redis-chain` and `redis-download-cache` are documented as
  decommissioned in `CLAUDE.md` but are live in prod, consuming node slots that B3 needs.

---

## 5. What is correct (verified, do not re-litigate)

- **Migration ordering is properly handled.** `start-api.sh` migrates before serving, and workers gained
  a `wait-for-migrations` initContainer (`scripts/wait_for_migrations.py`) that blocks until the newest
  baked-in migration appears in `schema_migrations`. This closes a real gap — my initial read that the
  api had no migration gate was wrong.
- **The 4 missing migrations are in the branch**, including `20260622122700_object_versions_address.sql`.
- **redis-queues pre-flight is already done**: `noeviction` at 4 GiB (superseding the previously recorded
  1 GiB `allkeys-lru`), with upload queue depth 0.
- **Leader election is Redis-based and the script's gate is correct.** `coordination.rs:275-281` derives
  `cephor:leader` / `cephor:epoch` from `DEFAULT_PREFIX = "cephor:"`; both keys are live on staging. The
  Postgres `cephor_leader_lease` from migration 0003 was dropped by 0009.
- **Ingest node labels already applied** to node1–node5.
- **`failoverDelay: 15`** on the Postgres cluster is a genuine fix for the 2026-06-25 flap.
- **`hostPath type: Directory`** (not `DirectoryOrCreate`) is the right safety latch.
- **CI quality gate**: `rust-gate` (fmt, clippy `-D warnings`, `cargo-deny` on Rust 1.95.0) blocks the
  drain image build, which blocks the deploy.
- **Staging soak**: 32 days on the full target architecture.

---

## 6. Recommended path

Ordered, with the gate that must be green before advancing.

1. **Fix the blind gate first.** Add the S1 dark-upload query to `rollout.sh preflight` *and* as a hard
   per-phase gate in the shift loop. Nothing else is safe to attempt until the cutover can *see* B2.
2. **Commit `rollout.sh`** with `set -euo pipefail` and exit-code checks on every mutating step (S2, S3).
3. **Protect the ingest nodes** — taint + tolerations, `priorityClass`, and a hard drain-agent Ready
   gate in CI. Clean up M3/M4 to reclaim slots. Verify `/s3-data` on all five nodes (M2). (B3)
4. **Apply the Python migrations** to prod ahead of the image roll. They are additive and safe against
   the current image; doing this first removes them from the critical path. Confirm M1 first.
5. **Freeze CI deploys** to `k8s-production` for the cutover window (S5).
6. **Choose the shape deliberately:**
   - *Full swap* (#296+#297+#298 merged together) — brief, atomic, no durability gap, but a hard cutover
     with a cold cache (S4).
   - *Hybrid via `rollout.sh`* — only after step 1, and understanding that every base-api replica still
     serving during the shift is producing dark uploads. The gate makes that visible; it does not make
     it stop.
7. **Land `s3-backup#18` in the same window**, as the PR body notes.

If the actual goal is to **test v2 without risking prod**, none of the above delivers it. Do that in a
separate namespace against a restored copy, or extend the drain reconciler to also scan the CephFS pool
so that a mixed fleet is genuinely safe. The latter is the change that would turn this all-or-nothing
cutover into an incremental one.

---

## Appendix — key references

| Claim | Evidence |
|---|---|
| Drain triggers on SSD scan only | `crates/hippius-drain-core/src/reconcile.rs:4` |
| Drain source/destination roots | `k8s/production/drain-agent-daemonset.yaml:114-119` |
| `address` is the account, not a path | `hippius_s3/api/s3/objects/put_object_endpoint.py:195-200` |
| Drain commented out in prod overlay | `k8s/production/kustomization.yaml:12-22`, `:41-43` |
| Local-cache patch deleted | `k8s/production/kustomization.yaml` (patches list), `local-cache-patch.yaml` removed |
| Partial DaemonSet roll = warning | `.github/workflows/production-deploy.yaml:307-318` |
| API migrates before serving | `start-api.sh:12-13` |
| Worker migration gate | `k8s/base/workers-deployments.yaml:27-42`, `scripts/wait_for_migrations.py` |
| Leader key derivation | `crates/hippius-drain-core/src/coordination.rs:275-281` |
| PVC grow + overcommit note | `k8s/production/resource-limits.yaml:128-142` |
| Full-swap routing flip | `k8s/production/kustomization.yaml` (#298), `resource-limits.yaml:21-27` |
