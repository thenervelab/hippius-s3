# Incident Report — 2026-06-13/14 hippius-s3-prod OOM + availability events

**Date:** 2026-06-13 → 2026-06-14
**Severity:** Mixed — §1–§8 Low (self-healed / no user impact). **§9 was HIGH — customer-facing S3 outage ~09:24–09:45 EDT (~21 min); now RESOLVED.**
**Status:** All five resolved/self-healed. Postgres replica recovered (§1–§6). Observability stack (Tempo + otel-collector) self-resolved 02:04 UTC 2026-06-14 — preventive fixes still recommended (§7.5). **`k8s-v3-node6-cache` RECOVERED — node hard-rebooted at 09:41 EDT, `api` data path restored ~09:45. Root cause: abrupt host power/hardware loss on the OVH node (§9.7). Team notified.** **`postgres-nvme` primary failover 2026-06-14 ~21:36 EDT — auto-healed in min, 3/3 ready, ~12 s write blip; root cause = primary stall (same nvme mis-sizing as §1, NOT OOM this time) (§11). Remediation §5/§11.5 still NOT applied.**
**Author:** Camden + Claude

This report covers **five events** observed on prod across the two days:
1. **`postgres-nvme-1`** — async DB replica, node-level `SystemOOM`, self-healed in ~2 min (§1–§6).
2. **`otel-collector` + `tempo-0`** — observability stack, per-container OOM, still flapping (§7).
3. **`k8s-v3-node6-cache` node failure (2026-06-14)** — kubelet went silent; the entire internal `api` + uploader/downloader/hydrator fleet is hard-pinned to this one node → **S3 data path down** (§9).
4. **`api`/`gateway` liveness-probe death spiral (2026-06-14 PM)** — load surge → too-aggressive liveness probe kills busy-but-healthy pods, flapping (§10).
5. **`postgres-nvme` primary failover (2026-06-14 night)** — the §5-predicted escalation: the **primary** `nvme-2` stalled (unresponsive to health probe, NOT OOM), CNPG auto-failed-over to `nvme-1`; ~12 s write blip, self-healed (§11).

Events 1, 2 and 5 share a common root pattern (memory ceilings without headroom, §8; event 5 the same `postgres-nvme` mis-sizing as event 1). Events 3–4 are a separate failure mode — single-node concentration of the hot path — but reinforce the same theme: insufficient blast-radius isolation on this cluster.

---

## 0. Executive summary — Sun 2026-06-14 (leadership / non-oncall)

**TL;DR:** One ~21-minute customer-facing S3 outage this morning, caused by an OVH node hard-failing.
It self-recovered. Two unrelated observability issues self-resolved overnight. **No data loss in any.**

### S3 data-path outage — 09:24–09:45 EDT (~21 min) — customer-facing
- **What:** OVH node `k8s-v3-node6-cache` died at 09:24 and went completely dark. It hosts our *entire*
  internal hot path — all `api`, `arion-uploader`, `arion-downloader`, `hydrator` pods. With it gone, the
  `api` service had zero healthy endpoints and the gateway returned 500s on reads and writes.
- **Why it couldn't auto-heal:** those pods are hard-pinned to that one node (they depend on a 40 TiB
  local NVMe cache that only exists there), so Kubernetes couldn't reschedule them elsewhere, and the
  other nodes were near memory capacity. No way to route around it from our side.
- **Recovery:** the node hard-rebooted on its own at 09:41 (OVH/hardware watchdog); pods came back and the
  data path was restored by ~09:45. **No manual intervention** — we couldn't have been faster without OVH
  access, which we don't currently have.
- **Root cause:** abrupt host power/hardware loss on the OVH box. Confirmed from the node's own logs — the
  kernel log stops mid-line at 09:24 with **no panic, no OOM, no error**, then a fresh boot 16 min later.
  That signature = a hard power-off / hardware fault, **not our software**. Confirming the exact physical
  cause needs OVH Manager access to the server's hardware log for the 09:24 window. (Detail: §9.)
- **Real lesson:** the trigger was outside our control, but the *blast radius* was our architecture — the
  whole user-facing path sits on one un-replicated node. A host will fail again; the fix is removing that
  single point of failure.

### S3 `api`/`gateway` flapping — Sun ~15:30 EDT onward — minor, self-recovering
- **What:** under a heavy traffic surge (gateway hit 20–27k req/min, ~5–7× baseline), `api` and `gateway`
  pods began intermittently restarting. Each restart caused a brief (~1–2 min) ~95% throughput dip; the
  service recovered itself each time and stayed 5/5 ready.
- **Why:** the pods are killing *themselves*, not crashing. The Kubernetes liveness probe is too aggressive
  — when a pod is busy under load it can't answer the trivial health check within 5 s, so Kubernetes
  assumes it's dead and restarts a perfectly healthy pod, which makes the load worse on the rest. **Not an
  OOM, not a hardware fault, not a database problem** — there is spare CPU and memory on the box.
- **Status:** confirmed root cause, system stable but flapping. **No changes made yet** (documented first).
  Low-risk fix: make the health-check more patient and add a little capacity. Same underlying weakness as
  the morning outage — the whole hot path runs on too few pods on one node. (Detail: §10.)

### Observability OOM (Tempo + otel-collector) — self-resolved overnight, no user impact
- Tempo (trace store) hit a one-time memory spike compacting a backlog, crash-looped, and backed up the
  otel-collector until it crash-looped too. **Only effect: dropped traces** — metrics, logs, and all S3
  functionality were unaffected. Both recovered on their own ~02:04 UTC. (Detail: §7.)

### Action items
1. **Remove the single-node pin** of the api/worker hot path so a host failure degrades instead of fully
   outages S3 — *biggest one; directly prevents a repeat of today.*
2. **Get break-glass OVH Manager access** for on-call — recovery today was blocked purely on access.
3. **Add alerting** on NotReady nodes / `api` zero healthy endpoints (caught via a menu-bar monitor, not a page).
4. **Preventive:** more memory headroom for Tempo + raise the otel-collector limit so the observability OOM
   doesn't recur.
5. **Loosen the api/gateway liveness probe** (timeout 5→10 s, failureThreshold 3→5) + add event-loop
   capacity (WEB_CONCURRENCY>1 or more replicas) so a load surge degrades gracefully instead of triggering
   self-inflicted restarts — *directly prevents a repeat of the afternoon flapping.*
6. **Fix the `postgres-nvme` memory mis-sizing** (now overdue — caused *two* incidents in 48 h: 06-13 replica
   OOM + 06-14 night primary failover). Low-risk P1: cut `work_mem` 128MB→32–48MB online (no restart). Then
   add a PgBouncer/Pooler (kills the 253-idle-connection pile-up) and enable `pg_stat_statements`. (Detail: §11.5.)

---

## 1. Summary

At **19:07–19:08 EDT** the CloudNativePG **replica** `postgres-nvme-1` (cluster `postgres-nvme`,
node `psql-s3-1`) was killed by the **kernel OOM-killer** (node-level `SystemOOM`), restarted
twice, replayed WAL from archive, rejoined as a streaming replica, and was healthy again by
**19:10**. The SwiftBar podwatch monitor caught the restart-delta and notified — working as designed.

There was **no impact to the primary** (`postgres-nvme-2`) and **no write-path disruption**: the
cluster uses asynchronous replication, so an OOM'd async replica does not block commits. No data
loss — the replica recovered entirely via WAL replay.

**Root cause:** the `postgres-nvme` cluster is configured to allow far more memory than its
dedicated nodes physically have. A transient burst of concurrent heavy read queries on the replica
drove total Postgres memory past the node's ~62 GiB of RAM, and the kernel OOM-killer chose a
postgres process as the victim.

---

## 2. Timeline (EDT)

| Time | Event |
|---|---|
| ~19:07:30 | Readiness probe to `:8000/readyz` fails with `write: broken pipe` — instance under memory pressure |
| 19:07:32 | postgres container terminated (1st); node `psql-s3-1` logs `SystemOOM`, victim: postgres pid 1744135 |
| 19:07:53 | podwatch records `NOTREADY` + `RESTART (1x)` for `postgres-nvme-1` |
| ~19:08:24 | container exits again; restart #2 |
| 19:08:39 | postgres container started (final); begins crash recovery / WAL replay from archive (LSN 517/…) |
| 19:09:51 | "consistent recovery state reached"; "ready to accept read-only connections" |
| 19:09:57 | "started streaming WAL from primary at 517/A3000000 on timeline 16" — replica back in sync |
| 19:10:22 | podwatch records `RECOVERED — all monitored namespaces healthy` |

---

## 3. Evidence

- **Node event:** `psql-s3-1` → `Warning SystemOOM … System OOM encountered, victim process: postgres, pid 1744135`.
  This is a **node-wide kernel OOM**, *not* a cgroup/pod-limit eviction.
- **Pod state:** `postgres-nvme-1` `RESTARTS 2`, last state Terminated; no further restarts since.
- **Cluster status:** `postgres-nvme` → 3/3 ready, primary `postgres-nvme-2`, *"Cluster in healthy state."*
- **Current load (post-incident):** replica essentially idle — 1 active client backend (our diagnostic
  query), 8 total backends. The spike was transient, not sustained.
- **No temp-file spills** recorded on the instance → `work_mem` is high enough that large sorts/hashes
  stay **resident in RAM** rather than spilling to NVMe. Good for latency, bad for memory ceilings.
- **No connection pooler** deployed (no CNPG `Pooler`, no PgBouncer). Apps connect directly via
  `postgres-nvme-rw` / `-ro` / `-r` services. Primary currently holds **359 / 1000** backends.
- **`pg_stat_statements` is NOT installed** → we could not identify the exact culprit query this time.

---

## 4. Root-cause analysis — why memory ballooned

Postgres does **not** enforce a global memory ceiling. Memory is the sum of a fixed floor plus a
per-operation grant that scales with concurrency:

```
total ≈ shared_buffers (fixed)  +  Σ over active queries ( work_mem × mem-nodes × parallel workers )
```

For the `postgres-nvme` cluster ([k8s/production/postgres-nvme-cluster.yaml](k8s/production/postgres-nvme-cluster.yaml)):

| Setting | Value | Effect |
|---|---|---|
| `shared_buffers` | **16 GB** | Always allocated — the floor |
| `work_mem` | **128 MB** | Per sort/hash node, **per backend, per parallel worker** |
| `hash_mem_multiplier` | 2 | Hash nodes may use **256 MB** each |
| `maintenance_work_mem` | **4 GB** | × 3 autovacuum workers ⇒ up to **12 GB** burst |
| `max_connections` | **1000** | The multiplier on `work_mem` |
| `max_parallel_workers_per_gather` | 4 | A query fans to 5 procs, each with its own `work_mem` |
| Pod memory **limit** | **56 GiB** | = **90% of node RAM** |
| Node (`psql-s3-*`) RAM | **~62.4 GiB** (65442956 Ki) | Only ~6 GiB left for OS/kubelet/containerd + page cache |

**Worst-case math (conservative):** at the 359 connections seen, even one in-memory sort each is
`359 × 128 MB ≈ 46 GiB`, plus the 16 GiB `shared_buffers` ≈ **62 GiB ≈ the entire node**. Factor in
the 2× hash multiplier and up-to-5× parallel fan-out and you reach OOM with **far fewer** concurrent
heavy queries. The 56 GiB cgroup limit sits *above* `node_RAM − shared_buffers − OS`, so a spike
tips the **node** into `SystemOOM` before the pod's own limit cleanly evicts it — which is exactly
what the kernel event shows.

### The damning contrast: the sibling cluster is sized sanely

The other prod cluster `postgres` ([k8s/production/postgres-tuning.yaml](k8s/production/postgres-tuning.yaml)) runs the
*same workload class* but is configured conservatively **and** lives on much larger general-purpose nodes:

| Dimension | `postgres-nvme` (OOM'd) | `postgres` (sibling, healthy) |
|---|---|---|
| Node RAM | **~62 GiB** (dedicated `psql-s3-*`) | **256–395 GiB** (general workers) |
| Pod memory limit | **56 GiB (≈90% of node)** | 32 GiB (≈8–12% of node) |
| `shared_buffers` | **16 GB** | 8 GB |
| `work_mem` | **128 MB** | 64 MB |
| `maintenance_work_mem` | **4 GB** | 2 GB |
| `max_connections` | 1000 | 1000 |

Every per-operation memory knob on `postgres-nvme` is **2× larger**, on a node with **~¼ the RAM**,
with a limit set to **90%** of that node. The most aggressive memory config is on the box least able
to absorb it. This OOM was the predictable realization of that mismatch, not a fluke.

---

## 5. Remediation

### Priority 1 — stop the bleeding (low-risk config change)
1. **Cut `work_mem` 128 MB → 32–48 MB** in `postgres-nvme-cluster.yaml:48`.
   This is the single biggest lever (it multiplies by hundreds of connections). On NVMe the worst
   case is a cheap on-disk spill instead of a node OOM. Reload is online (no restart for `work_mem`).
2. **Lower `maintenance_work_mem` 4 GB → 2 GB** (`:35`) — caps the autovacuum burst from ~12 GiB to ~6 GiB.

### Priority 2 — make failures bounded and predictable
3. **Set memory request = limit ≈ 48–50 GiB** (`:60`/`:63`) so the pod is Guaranteed QoS and a spike
   triggers a **clean cgroup OOM of just this pod** (single restart) instead of a node-wide
   `SystemOOM` where the kernel picks an arbitrary victim. Leaves ~12 GiB headroom on the 62 GiB node.
   (Combine with P1 so real usage stays well under the new limit.)

### Priority 3 — bound real concurrency
4. **Deploy a connection pooler** — CNPG `Pooler` (PgBouncer) in transaction mode in front of the
   `-rw`/`-ro` services, and reduce effective server-side concurrency. `max_connections=1000` with no
   pooler and 359 live backends is the multiplier behind the worst case. Target ~150–200 server conns.
5. Optionally drop `max_parallel_workers_per_gather` 4 → 2 to limit per-query memory fan-out.

### Priority 4 — observability / forensics
6. **Enable `pg_stat_statements`** (add to `shared_preload_libraries`) so the next spike can be traced
   to a specific query. We were blind to the culprit this time.
7. **Add node-memory alerting** on `psql-s3-*` at ~80% used, plus a "node memory headroom" panel, so we
   get ahead of OOM rather than reacting to a restart.
8. Consider `huge_pages` for `shared_buffers` to keep the 16 GB pinned and out of the OOM-killer's
   general accounting.

---

## 6. Blast radius / why it stayed low
- Hit an **async replica**, not the primary → no commit blocking, no data loss.
- `psql-s3-1` is a **dedicated DB node** (only this pod) → no collateral damage to other workloads.
- CNPG auto-recovered via WAL replay and re-streamed from the primary within ~2 minutes.
- The same risk applies to **all three** `postgres-nvme` instances (identical spec). If the spike had
  landed on the **primary** (`postgres-nvme-2`), the impact would have been materially worse —
  a primary OOM forces a failover/restart and a brief write outage. **This is the reason to fix it
  now rather than treat it as a benign one-off.**

---

## 7. Second concurrent OOM — observability stack (`otel-collector` + Tempo)

**Status: SELF-RESOLVED 02:04 UTC 2026-06-14 (§7.5). Preventive fixes still recommended.**

Surfaced later the same day by podwatch: the prod **`otel-collector`** (`hippius-s3-prod`) was
`OOMKilled` and crash-looping. Investigation showed it was a **symptom**; the root was **`tempo-0`**
in the `monitoring` namespace, which was itself OOM-crash-looping.

### 7.1 The chain

1. **`monitoring/tempo-0` — root cause.** `OOMKilled` (exit 137), 46 restarts, actively flapping
   (restarts every few minutes). On each boot it replays its WAL and immediately compacts two large
   blocks (1,102,689 + 2,111,521 objects, ~735 MB) — the compaction memory spike kills it before it
   stabilizes, so it never stays up long enough to drain ingest.
2. **`otel-collector` traces pipeline backs up.** With Tempo flapping, the `otlp/tempo` exporter fails
   (`context deadline exceeded`); its `sending_queue` (size 100) fills and trace batches accumulate
   **in memory**.
3. **Collector OOMs.** `memory_limiter` is set to `limit_mib: 768` / `spike_limit_mib: 128`, but the
   container memory **limit is only 1 GiB**. The trace backlog blows past 1 GiB → `OOMKilled`
   (exit 137), **57 restarts**. Node4 (where it runs) is at 27% memory — this is per-container limits
   + a dead downstream, **not** node pressure.

### 7.2 Evidence

- `tempo-0`: `lastState.terminated reason=OOMKilled exit=137`, `restarts=46`, last restart seconds ago.
  Logs show large `vParquet4` compaction cycles immediately on startup.
- `otel-collector-…-42s8z`: `lastState.terminated reason=OOMKilled exit=137`, `restarts=57`. Logs:
  repeated `Exporting failed … otlp/tempo … sending queue is full`, `DeadlineExceeded`, and
  `memory_limiter … Memory usage is above hard limit. Forcing a GC` climbing to `cur_mem_mib: 907`
  before the kill.
- Collector resources: `requests cpu=250m/mem=512Mi`, `limits cpu=1/mem=1Gi`.
- Collector config (`cm/otel-collector-config`): traces pipeline `[memory_limiter, batch] → otlp/tempo`,
  `sending_queue.queue_size: 100`, `retry_on_failure.max_elapsed_time: 300s`.

### 7.3 Impact

- **Distributed traces for prod are being dropped** (rejected by the full queue / lost across restarts).
- Metrics (Prometheus exporter) and logs (`loki.monitoring`, healthy) are **unaffected**.
- **No user-facing S3 impact** — the `hippius-s3-prod` app (api, gateway, uploader, downloader,
  unpinner, account-cacher, redis) is fully `Running`.

### 7.4 Remediation

**P1 — fix the root (`tempo-0`):** the collector cannot recover while Tempo drops traces.
1. **Raise `tempo-0`'s memory limit** — it OOMs during compaction of multi-hundred-MB blocks; give it
   enough headroom to finish a compaction cycle.
2. **Tune compaction** to cap the spike — smaller `max_block_bytes` / `max_compaction_objects` so a
   single cycle can't balloon memory.

**P2 — make the collector resilient to a dead downstream:**
3. **Raise the collector's 1 GiB limit** (or lower the trace volume / queue) so a Tempo outage drops
   data gracefully instead of OOM-looping. Aligning the container limit with `memory_limiter`
   (768+128 MiB → e.g. a 1.5–2 GiB limit) gives the limiter room to actually shed load before the
   kernel kills the container.

### 7.5 Resolution (self-resolved, 2026-06-14)

**Both recovered on their own overnight — no manual intervention.** Verified ~13:54 UTC 2026-06-14:

- **`tempo-0`**: `Running 1/1`, continuous uptime since **02:04 UTC**; steady-state memory **~1.3 GiB
  under its 2 GiB limit**. The 46 restarts are cumulative over 69 days, not active flapping.
- **`otel-collector`**: `Running 1/1`, last restart ~02:00–03:00 UTC; **zero** export-error lines in a
  2-min sample (no "sending queue is full", no `DeadlineExceeded`, no "above hard limit"). The
  `memory_limiter` now oscillates ~260–730 MiB and GCs back down, staying under the 1 GiB limit — normal
  operation, not the death spiral.

**Why it stopped:** the trigger was a **one-time compaction catch-up**, not steady load. Once `tempo-0`
finished compacting the large block backlog (§7.1) at ~02:04 UTC, its steady-state memory settled under
the 2 GiB limit and it stopped OOMing; with Tempo accepting traces again, the collector's `otlp/tempo`
queue drained and it stopped OOMing too. They recovered together.

**Still warm / still worth the preventives (§7.4):** the collector continues to flirt with its soft
limit (~731 vs 768 MiB) and GC frequently. It is healthy now, but the same spiral recurs the next time
Tempo accumulates a large compaction backlog. The §7.4 P1/P2 fixes (Tempo compaction headroom; raise the
collector limit to 1.5–2 GiB) remain cheap insurance against a repeat — **open as preventive work, not an
active incident.**

---

## 8. Common thread across both events

Neither incident caused user-facing impact, but both are the same failure shape and worth fixing as a class:

- **Memory ceilings set without enough headroom for the real workload.** `postgres-nvme` allows ~90% of
  node RAM with 2× per-op memory knobs; the otel-collector's container limit (1 GiB) sits *below* the
  memory its trace backlog reaches under a downstream stall. In both cases the limit didn't bound the
  failure cleanly — it produced a node `SystemOOM` (postgres) or a tight crash-loop (collector).
- **Observability blind spots compound.** The postgres report notes `pg_stat_statements` is missing, so
  we were blind to the culprit query. The Tempo/otel failure means **traces are now being dropped too** —
  during exactly the window we'd want them for forensics. Restoring Tempo (§7.4 P1) also restores the
  trace signal that would help diagnose the next postgres spike.
- **Both were caught by podwatch restart-deltas**, not by proactive memory alerting — reinforcing §5 P4
  item 7 (node/container memory headroom alerts ahead of OOM).

---

## 9. Event 3 (2026-06-14) — `k8s-v3-node6-cache` node failure → S3 data-path outage

**Severity: HIGH — active customer-facing outage at time of writing.** Unlike §1–§8, this one has direct user impact.

### 9.1 What happened

At **09:24:02 EDT (13:24:02Z)** the kubelet on **`k8s-v3-node6-cache`** stopped posting node status.
Kubernetes marked the node `NotReady` with all conditions `Unknown` ("Kubelet stopped posting node
status") — i.e. a **host-level failure** (kernel panic/hang, hardware, or vRack NIC drop), *not* a pod
OOM or a cgroup eviction. The node did **not** self-recover; heartbeat remained frozen 7+ min later.

This node is on **OVH dedicated/bare-metal**, and **we do not have OVH Manager/API access** to reboot
it. The team has been notified to chase down OVH access.

### 9.2 Why it took down the S3 data path

The entire internal hot path is **hard-pinned to this single node** via
`nodeSelector: {kubernetes.io/hostname: k8s-v3-node6-cache}`:

- all 5 `api` replicas
- all `arion-uploader`, `arion-downloader`, and `hydrator` pods

Consequences:
- **`api` Service had zero ready endpoints** — all 5 went `NotReady` when the node went unreachable.
- **`gateway` (healthy, spread across node2–5) returned `500`s** forwarding to `api`:
  `httpx.ConnectError: All connection attempts failed` / `ConnectTimeout`
  (e.g. `GET /teutonic-sn3/index.html → 500`, observed 13:31Z).
- Because the pin is a hard `nodeSelector`, the pods **cannot reschedule** — replacement pods would be
  `Pending`/unschedulable until node6 returns. The 5-min unreachable-eviction does not help.

### 9.3 Why we can't trivially route around it

Investigated a k8s-side relocation; it is **not a safe quick fix**:

| Volume | StorageClass / Mode | Mount | Movable? |
|---|---|---|---|
| `object-cache-pvc` | `ceph-filesystem` / **RWX** | `/var/lib/hippius/object_cache` | Yes (network-backed) |
| `persist-pvc` | `ceph-filesystem` / **RWX** | `/var/lib/hippius/persist` | Yes |
| `dlq-pvc` | `ceph-filesystem` / **RWX** | `/tmp/hippius_dlq` | Yes |
| `local-cache-pvc` | **RWO**, static `local-cache-pv` | `/var/lib/hippius/local_object_cache` | **No** |

- The shared chunk cache (`object_cache`), persist, and dlq are CephFS-RWX and *would* mount elsewhere.
- The blocker is **`local-cache-pvc`: a 40 TiB local NVMe volume (`/cache`) with hard node-affinity to
  node6.** It is the primary hot tier and almost certainly holds **staged-but-not-yet-uploaded chunks**.
  It cannot move or be recreated elsewhere; data on it is stranded (not lost) until node6 returns.
- **No capacity to absorb the fleet anyway:** healthy nodes are already at **95–111 % memory requests**
  (node3 111 %, node5 109 %, node4 105 %). Rescheduling node6's ~65-pod fleet would leave most `Pending`.

A surgical `api`-only relocation (drop the `nodeSelector`, swap `local-cache` for `emptyDir`, serve reads
from the CephFS `object_cache`) is *theoretically* possible but only restores the front door — the
uploader/downloader are also on node6, so the upload/download pipeline stays broken — and it requires
verifying the Dual cache store tolerates an empty local tier plus scaling replicas down to fit. **Not
attempted; would need code verification + sign-off.** No cluster changes were made.

### 9.4 Status / next steps

- **Immediate:** recover node6 via **OVH Manager/API reboot** (soft → hard/netboot-rescue). This is the
  clean fix — once kubelet rejoins, all pinned pods rebind and endpoints repopulate automatically. The
  blocker is OVH access; team notified.
- Check OVH for a host-side incident/hardware intervention (`travaux.ovh.com`, `/me/task`,
  `/dedicated/server/{name}/task`) — rule out a panic vs. a network/hardware fault.
- Pinned data on the 40 TiB local cache is **safe** (still on disk, janitor won't evict unreplicated
  chunks) but **inaccessible** until node6 is back.

### 9.5 Eviction cascade (~5 min after node went NotReady)

Once node6 had been unreachable for the default **5-minute** `node.kubernetes.io/unreachable`
toleration, Kubernetes evicted its pods and the ReplicaSets created replacements — playing out exactly
as §9.3 predicted:

- **Original node6 pods → stuck `Terminating`.** Because the kubelet is dead, the API server cannot
  confirm deletion, so they hang in `Terminating` indefinitely (they clean up only when node6 returns).
- **Replacement pods → `Pending`, unschedulable.** The hard `nodeSelector` + node-local RWO
  `local-cache-pvc` mean no other node qualifies. Scheduler verdict:
  `0/12 nodes are available: 5 node(s) didn't match Pod's node affinity/selector, 4 node(s) had
  untolerated taint(s), 3 node(s) were unschedulable.`
- **Prod pod state:** ~**86 Terminating + 86 Pending + 39 Running**. `api` ready endpoints still `[]`
  (data path still down). All 5 `gateway` pods `Running`; one had a transient readiness blip
  (`/health` deadline exceeded) from hammering the dead `api` on forward — gateway itself is healthy.

**This is noise, not a new fault.** Underneath it is still the single node6 failure; the eviction merely
converted "5 NotReady api pods" into ~172 stuck/pending pods. **No new root cause, no change to the fix.**
When node6 rejoins, the `Terminating` pods are reaped and the `Pending` ones schedule back onto it
automatically — service restores with no manual intervention.

**Operational caution:** do **not** force-delete the `Terminating` pods (`--force --grace-period=0`).
It does not help — replacements remain unschedulable (still pinned to node6) — and force-deleting pods
that hold the RWO `local-cache-pvc` mid-outage risks volume-attachment cleanup conflicts when node6
returns. Leave them; let the node come back.

### 9.6 Root cause / the architectural debt

The whole user-facing api + worker fleet, plus a 40 TiB RWO local cache, lives on **one OVH bare-metal
node with no failover.** A single host fault = full S3 data-path outage with **no automated recovery
path** (hard `nodeSelector` + node-local RWO PV). This is the §8 theme escalated: **zero blast-radius
isolation for the hot path.**

**Remediation (post-incident):**
1. **Eliminate the single-node pin.** Spread `api` across multiple nodes; move its working set onto the
   CephFS-RWX `object_cache` so replicas aren't bound to one host's local NVMe. The
   `local_object_cache` should be a *performance* tier, never a *correctness/availability* dependency.
2. **Stand up the regional/multi-node cache stack.** This is exactly the `deploy-cache-production` job
   that is **currently disabled (`if: false`)** in [production-deploy.yaml](.github/workflows/production-deploy.yaml)
   pending the per-region NVMe PVC story — this outage is the case for finishing it.
3. **Don't co-locate api + uploader + downloader + hydrator on one node.** Distribute so a single host
   loss degrades rather than fully outages the pipeline.
4. **Ensure OVH break-glass access** exists for on-call (Manager login / API keys / reboot rights) — this
   incident's recovery was blocked purely on access; it self-recovered before access was obtained.
5. **Alert on `NotReady` nodes** (and on `api` ready-endpoint count hitting 0) so this pages immediately
   rather than being noticed via podwatch deltas.

### 9.7 Resolution + confirmed root cause (2026-06-14)

**Resolved — node self-recovered via reboot; no manual cluster intervention was performed.**

**Recovery timeline (EDT):**

| Time | Event |
|---|---|
| 09:24:02 | node6 kubelet stops posting status; node → `NotReady` (all conditions `Unknown`) |
| 09:24:30 | **last log entry on the old boot** — host goes completely dark |
| ~09:29 | 5-min unreachable-eviction fires; node6 pods → stuck `Terminating`, replacements `Pending` (§9.5) |
| 09:41:08 | **host back up on a NEW boot id `d0ff6464…`** — node rebooted (`Warning Rebooted`, `Starting kubelet`) |
| ~09:42 | `Terminating` pods reaped (`EXIT` events); fleet reschedules onto node6 (brief `ImagePullBackOff`, resolved in ~400 ms) |
| ~09:45 | all 5 `api` pods `1/1 Ready`; `api` Service has 5 ready endpoints; gateway 5xx clears — **data path restored** |

Total user-facing outage ≈ **21 min** (09:24 → ~09:45).

**Root cause — abrupt host power/hardware loss on the OVH node (not an OOM, not our stack):**

Pulled directly from node6's own journal after recovery (`kubectl debug node/…` → `chroot /host`):
- **journald boot history:** previous boot `8036f2af…` **last entry 13:24:30 UTC**, then nothing; new boot
  `d0ff6464…` **first entry 13:41:08 UTC** → host was **dark ~16.5 min**.
- **Crash-signal scan of the previous boot's kernel log returned EMPTY** — no panic, no oops, no
  `oom-kill`, no machine-check/MCE, no thermal, no hung-task/soft-lockup, no NIC `link down`. The journal
  simply **stops mid-stream at 13:24:30 with zero warning.**

A clean log cutoff with **no crash trace** is the fingerprint of an **abrupt power-off / hard reset /
hardware fault** — the kernel died too fast (or lost power) to log anything; ~16 min later an external
watchdog (OVH/hardware) hard-rebooted it. Conclusions:
- **Effectively an OVH host hardware/power incident** on that dedicated server. Not a Kubernetes, app, or
  config fault (those cannot stop a journal mid-line).
- **Unrelated to the §1–§8 OOM theme** — zero memory-pressure / oom-kill in the kernel log.

**Open gap (needs OVH-side access to close):** the node side cannot distinguish *hardware fault* vs.
*power loss* vs. *unannounced OVH intervention* — all look identical (instant death, no logs). To confirm
the physical cause, someone with OVH Manager access should check, for the **13:24 UTC** window:
- the dedicated server's **hardware event log / IPMI SEL** and status page, and
- **travaux.ovh.com** / OVH incident feed for that datacenter.

**Why this still warrants the §9.6 fixes:** the *trigger* was external and outside our control, but the
*blast radius* (full S3 data-path outage from one host dying) was entirely our architecture — the hard
single-node pin of the whole hot path. A host will fail again; the fix is removing the single point of
failure, not preventing OVH reboots.

---

## 10. Event 4 (2026-06-14 afternoon) — `api`/`gateway` liveness-probe death spiral under load

**Status: confirmed root cause, system stable (5/5 ready) but flapping. NO changes applied — Camden
asked to document first.** This is a **different failure mode** from §9 (no node died, no OOM, no
hardware fault) on the **same architectural weakness** (the §9.6 single-node pin starving the hot path).

### 10.1 What happened

Starting ~**15:30 EDT** the SwiftBar podwatch monitor flagged a cluster of `api` + `gateway` restarts.
Over ~20 min: `gateway-…-7jdfl` (×2, 15:32), `api-…-zr8gg` (15:38), `api-…-p7nsd` (15:43),
`gateway-…-mlmg7` (15:51), plus liveness failures on `gateway-…-lmnrc`. All exited **137 (SIGKILL),
reason `Error`** — **not `OOMKilled`**. Every restart was preceded by the same kubelet event:

> `Liveness probe failed: Get ".../health": context deadline exceeded` → `Container … failed liveness probe, will be restarted`

### 10.2 Root cause — self-inflicted liveness kills under event-loop saturation

The `/health` handlers on **both** services are trivial static returns with **zero I/O**
([`gateway/main.py:184`](gateway/main.py), [`hippius_s3/main.py:361`](hippius_s3/main.py)) — they
return `{"status":"healthy"}` and touch no DB/Redis. A no-op async handler can only time out if the
**asyncio event loop itself is starved**, i.e. the loop is too busy to schedule the probe coroutine.

The chain:
1. **Heavy load surge.** Gateway request rate (audit-success/min, Loki) ran a ~3–4k/min baseline but
   spiked to **20,551/min at 14:00** and **27,495/min at 14:20** (~5–7×), driven by bulk
   `hippius-juicefs-data/…/chunks/…` writes + heavy `tora-m365/…/manifests/mail/…` manifest spam.
2. **Too few event loops.** The `api` runs **5 pods, single uvicorn worker each** (`WEB_CONCURRENCY`
   unset → one event loop per pod), **all pinned to node6** — the entire prod data plane on 5 event loops.
3. **Saturation → missed probe.** Under burst, each loop can't service `/health` within the **5 s**
   liveness timeout. With `periodSeconds=60, failureThreshold=3`, **3 consecutive misses over ~3 min**
   → kubelet kills a **healthy-but-busy** pod.
4. **Cascade.** Each kill removes capacity → load concentrates on survivors → next loop trips. The
   gateways aren't node-pinned (node3/4/5) but trip the same way: when `api` responses slow, the
   gateway's shared `httpx` pool (`max_connections=100`) backs up and stalls *its* event loop, so
   `gateway`'s own no-op `/health` also times out → gateway liveness kills.

### 10.3 What it is NOT (ruled out)

- **Not OOM** — exit reason `Error`, not `OOMKilled`; api pods at ~460–620 MiB vs an **8 Gi** limit.
- **Not node pressure** — node6 `MemoryPressure/DiskPressure/PIDPressure = False`; node at **~20% CPU /
  ~8% mem** (16 GiB of ~190 GiB).
- **Not CPU throttling** — `cpu.stat` shows **`nr_throttled 0`** on api pods (4-core limit, 0 throttled
  over 6 h).
- **Not Postgres** — `postgres-nvme` nodes at 29–58% mem, low CPU, healthy; the §1 OOM is unrelated.
- **Not node6's morning reboot** — the uniform `6h22m` pod age is just the §9 recovery (node6 came back
  09:41); this event is fresh load at 15:30+.

### 10.4 Customer impact

Real but transient and self-recovering. Each liveness kill caused a **~1–2 min ~95% throughput
collapse**: gateway success/min dropped from the ~3–5k baseline to **356 (15:43), 248 (15:50), 162
(15:51)**. Both deployments returned to **5/5 ready** between kills. No data loss (writes that 503 are
retried client-side; PUT path is idempotent on object key).

### 10.5 Remediation (proposed, NOT applied)

- **P1 — loosen the liveness probe (lowest-risk, stops the self-inflicted cascade).** A liveness probe
  should detect a *dead* process, not a *busy* one; load-shedding is readiness' job. Bump api+gateway
  `livenessProbe` to ~`timeoutSeconds: 10`, `failureThreshold: 5` (and consider decoupling: keep a
  tight *readiness* probe so a saturated pod is pulled from the Service instead of killed). Killing a
  busy pod under load is strictly counterproductive — it deletes capacity at peak.
- **P2 — add event loops / capacity.** node6 has CPU+mem headroom: either set `WEB_CONCURRENCY>1`
  (multiple uvicorn workers per pod → more loops without more pods, sidesteps the RWO pin) or scale api
  5→8–10 (still node6-bound by the local-cache PVC) and gateway 5→8 (not pinned, schedules anywhere).
- **P3 — the real fix is §9.6:** removing the single-node pin so the data plane can spread; this event
  is another symptom of the same concentration.
- **P4 — observability:** the trivial `/health` hides downstream health (noted as a P1 in `gateway` docs
  / `ha.md`); separately, an event-loop-lag / request-queue-depth metric would have flagged this before
  the kills. Alert on api/gateway restart-rate, not just NotReady nodes.

### 10.6 Common thread

§9 (hardware death) and §10 (load-induced probe kills) are unrelated triggers but the **same
architectural debt**: the entire user-facing hot path is concentrated on 5 single-worker pods pinned to
one node. §9 took it down via the node dying; §10 degrades it via the pods killing themselves under
load. Both are fixed by the §9.6 work (spread the hot path) plus, here, a more lenient liveness probe.

---

## 11. Event 5 (2026-06-14 night) — `postgres-nvme` automatic primary failover (the §5 prediction came true)

**Severity:** Low — self-healed in ~3–7 min, no manual action, no data loss; ~12–20 s write-unavailability
window. But it's the **second `postgres-nvme` incident in 48 h** and the exact escalation §5 warned about:
"if the spike hits the **primary** next time it forces a failover + brief write outage."

### 11.1 What happened

At **2026-06-14 ~21:36 EDT (01:36:06 UTC 06-15)** CloudNativePG declared the `postgres-nvme` cluster
unhealthy and ran an **automatic failover**, promoting **`postgres-nvme-1` (node `psql-s3-1`) to primary**.
The previous primary was **`postgres-nvme-2` (node `psql-s3-2`)**.

The trigger was **not** a kill — it was the primary going **unresponsive to its health probe**. The CNPG
operator's per-instance status poll timed out:

```
pod status (3 of 3): name=postgres-nvme-2 isPodReady:false
  statusCollectionError: Get "https://10.42.213.102:8000/pg/status":
  context deadline exceeded (Client.Timeout exceeded while awaiting headers)
→ "Cluster has become unhealthy" → "Failing over" newPrimary=postgres-nvme-1
```

Both replicas (`nvme-1`, `nvme-3`) were healthy and caught up at the same instant (`replayLsn 530/8E65CE20`,
`isPodReady:true`), so this was **not a network partition** of `psql-s3-2` — it was specifically the
postgres/instance-manager on `nvme-2` stalling long enough to miss the probe deadline.

### 11.2 Sequence

- `01:36:06` — operator: primary `nvme-2` `/pg/status` deadline-exceeded → "Cluster has become unhealthy" → "Failing over" to `nvme-1`.
- `01:36:18` — "Setting primary label" on `nvme-1`; new primary promoted. **Write-unavailability window ≈ 12 s** (reads from replicas uninterrupted throughout).
- `01:36:26` — old primary `nvme-2` postgres gracefully shut down (pg_controldata `Database cluster state: shut down`, **exit 0**), then began rejoining as a **replica**.
- `01:37–01:38` — `nvme-2` replayed WAL from archive (`000000110000053000000080`…`9B`). During this long replay its **startup probe returned HTTP 500** ("database system is starting up / not yet accepting connections"), so the kubelet **restarted the postgres container twice (BackOff)** — cosmetic restarts driven by the probe, not new faults.
- `01:38:43` — `nvme-2`: `started streaming WAL from primary at 530/9C000000 on timeline 17` → caught up, went **2/2 Ready**.
- End state (verified): `cluster phase = Cluster in healthy state`, **3/3 ready**, `currentPrimary = postgres-nvme-1` (`pg_is_in_recovery() = f`, accepting writes). Node `psql-s3-2` back to **9 % mem / 0 % CPU**.

### 11.3 Why this is NOT the 06-13 node-OOM (ruled out)

- **No `SystemOOM` event** in the retained window (this was investigated ~5 min after the event, well within event TTL — a node OOM would still have been listed).
- Old primary exited **0 (graceful)**, container reason `Completed` — **not `OOMKilled`** (137).
- Node `psql-s3-2` shows no current memory pressure (9 %).

So the primary **stalled**, it wasn't kicked by the kernel. Most likely the same underlying cause as §4
(unbounded `work_mem` × concurrency starving the box / I-O stall under a heavy-query or checkpoint burst),
but severe enough to freeze the primary's control-plane responder rather than OOM a backend. `pg_stat_statements`
is still not installed (§5 P4), so the exact stalling query remains unidentified.

### 11.4 Notable: idle-connection pile-up confirms the no-pooler problem

Immediately after failover the new primary held **253 idle + 2 idle-in-transaction connections vs only 4
active** (`pg_stat_activity`). That's the `max_connections=1000` + **no PgBouncer/Pooler** design from §5 P3:
every app pod hoards idle connections, each reservable for a full `work_mem` allocation — exactly the
amplifier behind both the 06-13 OOM and this stall.

### 11.5 Remediation

**Same as §5 — still NOT applied as of 2026-06-14.** This event re-prioritizes them; the failover machinery
itself worked perfectly (good HA), but the primary keeps stalling under load:

- **P1 (now overdue) — cut `work_mem` 128MB→32–48MB + `maintenance_work_mem` 4GB→2GB.** Online `ALTER SYSTEM` + reload, no restart, low risk. Directly shrinks the per-op memory that starves the box.
- **P2 — add a CNPG Pooler/PgBouncer** in front of `postgres-nvme` and cut effective app-side concurrency; kills the 253-idle-connection amplifier.
- **P3 — enable `pg_stat_statements`** so the next stall/OOM has a named culprit query (currently flying blind on both events).
- **P4 — node-mem + CNPG-failover alerting** on the `psql-s3-*` nodes (this failover was caught via podwatch, not a page); alert at 80 % node mem and on any `postgres-nvme` failover event.

### 11.6 Common thread

§1–§6 (06-13 replica OOM) and §11 (06-14 primary stall→failover) are the **same root cause on the same
cluster**: `postgres-nvme` runs an aggressive memory config (`work_mem=128MB`, `shared_buffers=16GB`, no
pooler, `max_connections=1000`) on the **smallest nodes in the fleet** (~62 GiB), with no global memory cap.
06-13 it OOM-killed a replica; 06-14 it stalled the primary into a failover. Two symptoms, one fix: §5 P1–P4.
The good news this time — **CNPG's automatic failover did its job**: detected the bad primary, promoted a
replica, re-joined the old primary as a streaming replica, all self-healing in minutes with a ~12 s write blip.
