# hippius-s3 prod — SSD-drain architecture capacity plan

**Question:** to run the staging "SSD-draining cache" S3 stack in `hippius-s3-prod`, how many
API/ingest nodes with local NVMe do we need, and how big should the SSDs be, to keep up with
daily traffic + bursts?

**Answer (TL;DR):** the workload is **light on bytes, heavy on object count, and very bursty**
(~18× peak/avg). It is trivially served on CPU/RAM; the only real sizing levers are (1) enough
**drain throughput** to clear the sustained peak, and (2) an SSD sized for **endurance**, not
capacity (the resident backlog is ~10–25 GB, but you write 2.5–5 TB/day of churn to it).

> **Recommendation:** **3–4 ingest nodes**, each running `api-local` + a `drain-agent`, pinned by
> a `s3-prod-local-ingest=true` label, with **1–2 TB write-intensive NVMe (≥3 DWPD)** local disk.
> 4 nodes at the default 100 MB/s/node drain budget = 400 MB/s fleet (covers the p99 sustained
> peak with headroom, N+1 redundancy, and absorbs the rare 470–573 MB/s bursts on the SSD).
> 3 nodes work if you raise `CEPHOR_MAX_DRAIN_RATE_BPS` to ~150–200 MB/s/node (validate against
> the Ceph pool first). Drainers are a DaemonSet on those same nodes — **no separate drain hosts.**

---

## 1. Measured demand (ground truth)

⚠️ **The OTel byte/op metrics are NOT usable on prod today.** `rate(s3_operations_total{...put_object})`
reads **~30,700 put/s** and `s3_bytes_uploaded_total` reads **~13 GB/s** — vs a true **~53 ops/s**
and **~32 MB/s**. Prod runs a pre-#265 image (`api:07b1fb1`); every uvicorn worker shares one
`service.instance.id`, so N cumulative counters collide in one collector slot and each scrape reads
as a counter reset → ~100–650× inflated rates (see the incident behind PR #265, and PR #284 for a
residual 2× double-count). **All numbers below come from ground truth instead:** the
`object_versions` table (authoritative bytes/objects) and the gateway audit log in Loki
(authoritative request rate).

Measured over **2026-07-12 → 07-19** (`object_versions.size_bytes` by `created_at`):

| Metric | Value |
|---|---|
| Daily ingest | **1.3 – 5.2 TB/day** (peak day 07-13 = 5.2 TB; typical 2–3 TB) |
| Avg ingest rate | **31.8 MB/s** (≈ 2.7 TB/day) |
| Peak 5-min sustained | **470 MB/s** (p99 = 257 MB/s) |
| Peak 1-min burst | **573 MB/s** (p99 = 284, p99.9 = 494 MB/s) |
| Burstiness (peak/avg) | **~15× (5-min), ~18× (1-min)** |
| Object create rate | avg **21/s**, peak-hour 61/s, **peak-1-min 150/s** |
| Request mix (Loki) | PUT 18/s, GET 13.5/s, HEAD 22.5/s, DELETE 0.9/s |
| Current API CPU | **< 1 core total** across all 5 pods (75–202 mCPU each) |
| Reads (GET+HEAD) | light; served from the Ceph pool, **not** the local SSD ingest |

### Workload shape — bimodal, and it matters

| Size band | % of object **count** | % of **bytes** |
|---|---|---|
| < 64 KB | **91 %** | 0.3 % |
| 64 KB – 1 MB | 4 % | 1.3 % |
| **1 – 10 MB** | 5 % | **92 %** |
| > 10 MB | 0.1 % | 6 % (max seen 50 MB) |

p50 object = **496 bytes**, avg ≈ 116 KB–1.5 MB, p99 = 2 MB. This is an M365 mail-backup pattern
(`tora-m365/.../*.enc`): a flood of tiny manifests (drives **op/DB/part rate**) plus a smaller
population of 1–10 MB blobs (drives **SSD byte throughput**). **Neither dimension is large**, but
they stress different resources, so size for both.

---

## 2. How the drain architecture consumes this (the supply side)

Per the staging design (`crates/hippius-drain-*`, `k8s/staging/drain-*`):

- A client PUT lands on `api-local`, which writes the encrypted, chunked object to the **node-local
  SSD** (`/var/lib/hippius/local_ingest_*`) and returns 200. The SSD therefore holds only the
  **not-yet-drained** working set.
- A **`drain-agent`** (DaemonSet, one per ingest node) copies each complete part SSD → CephFS pool,
  commits `replicated` to `cephor_replication_status`, enqueues the backend upload, and unlinks the
  SSD copy. The singleton **`drain-allocator`** hands each node an AIMD write-budget; the
  **`mpu-reaper`** cleans abandoned MPUs.
- **Two throughput ceilings:** the AIMD **byte budget** (`CEPHOR_MAX_DRAIN_RATE_BPS`, default
  **100 MB/s/node**) and the per-part **commit** (`mark_replicated` = a WAL fsync on the app
  Postgres). On staging (Ceph-backed PG) this was commit-bound at ~0.68 parts/s; **prod's PG is
  NVMe (`postgres-nvme`)** so the commit is ~ms → the part rate ceiling is hundreds–thousands/s,
  far above the 150 parts/s peak. **On prod the drain is byte-budget-bound, not commit-bound.**

---

## 3. Sizing model

Formulas (see references): the SSD is a write-back staging buffer; backlog = `max_t(A(t) − D(t))`;
burst backlog ≈ `(R_in_peak − R_drain) × τ_burst`; **if sustained `R_drain ≥ R_in_sustained`, the
backlog is bounded and small — otherwise no SSD is big enough.** Size the drain first.

### 3a. Drain fleet (the binding constraint)

Must sustain the **5-min sustained peak** so backlog can't grow unboundedly:
- p99 5-min = **257 MB/s**; worst 5-min = **470 MB/s**.
- Target with headroom (keep utilisation ≤ ~65 %, per SRE): `257 / 0.65 ≈ 395 MB/s`, and be able
  to ride the worst 470 MB/s.
- At the default **100 MB/s/node** budget → **4 nodes = 400 MB/s** (covers p99 + headroom; the rare
  470–573 MB/s 1-min bursts are absorbed by the SSD buffer). Or **3 nodes** with the budget raised
  to ~150–200 MB/s/node (Ceph pool can take it; validate) = 450–600 MB/s.

### 3b. SSD size per node — driven by ENDURANCE, not capacity

**Resident backlog is tiny.** With drain ≈ sustained peak, the SSD only holds the trigger/dwell
window (reconciler poll 15 s + copy) times the burst rate:
`573 MB/s × ~45 s ≈ 26 GB total`, spread across nodes ≈ **~10 GB/node**; under the worst 5-min peak
with a 400 MB/s drain: `(470−400) × 300 s ≈ 21 GB total`. **Round to ~50 GB/node of usable backlog
even pessimistically.**

**Endurance is the real driver.** Every uploaded byte is written to SSD once, then deleted — a
high-churn logging pattern. Daily host writes/node (3-node fleet, WAF ≈ 2×):
- typical `2.7 TB/day ÷ 3 × 2 ≈ 1.8 TB/day/node`; peak-day `5.2 ÷ 3 × 2 ≈ 3.5 TB/day/node`.
- On a **1 TB** drive → **1.8–3.5 DWPD**; on **2 TB** → **0.9–1.8 DWPD**.
- 3-yr TBW: peak 3.5 TB/day ⇒ ~3.8 PBW. A **1 TB @ 5 DWPD** (5.5 PBW) or **2 TB @ 3 DWPD**
  (6.6 PBW) both clear it with margin; a read-intensive/consumer drive (≤1 DWPD) would **not**.

→ **1–2 TB write-intensive enterprise NVMe, ≥3 DWPD (prefer 5).** Capacity is irrelevant (only
~10–50 GB is ever used); you're buying **endurance + sustained-write bandwidth + burst headroom**.

### 3c. Node count = max of the bounds, + redundancy

| Bound | Requirement | Nodes |
|---|---|---|
| Ingress bandwidth | 573 MB/s peak ÷ ~1 GB/s (NVMe write) or 1.25 GB/s (10 GbE) | 1 |
| **Drain throughput** | ≥ ~400 MB/s (p99 5-min + headroom) @ 100 MB/s/node | **~4** |
| SSD resident set | ~26 GB total | 1 |
| Part/commit rate | 150 parts/s ≪ NVMe-PG capacity | 1 |
| CPU / RAM | < 1 core, ~1.4 GB/pod today | 1 |
| **Redundancy (N+1)** | survive 1 node down at peak | +1 already folded in |

**Binding bound = drain throughput → 3–4 ingest nodes.** The average load needs *one* node; the
**bursts + N+1 redundancy** are what justify 3–4.

### Host spec for a new ingest node
- **CPU:** 16–32 cores (huge headroom vs the <1 core used; covers AES-256-GCM + chunking + reads).
- **RAM:** 32–64 GB (api pods ~1.5 GB each + streaming buffers + page cache).
- **Local disk:** **1–2 TB write-intensive NVMe, ≥3 DWPD**, dedicated to `local_ingest_prod`.
- **NIC:** 10 GbE (peak 573 MB/s ≈ 4.6 Gbps ingress + read egress).
- (The existing `node1–5` are 96–128 core / 263–395 GB — already ample **if** local NVMe is added.)

---

## 4. Provisioning on kubectl in `hippius-s3-prod`

Mirror the staging manifests (`k8s/staging/drain-*`, `api-local-*`, `ingest-node-labels-*`) into a
prod overlay. Prod already has `postgres-nvme` (for `cephor_*`), `object-cache-pvc` (CephFS pool),
and `redis-queues` — the drain reuses all three.

1. **Attach + mount local NVMe** on the 3–4 chosen nodes at `/var/lib/hippius/local_ingest_prod`
   (a dedicated disk, `xfs`/`ext4`, `noatime`). Prefer a **Local PersistentVolume**
   (`volumeBindingMode: WaitForFirstConsumer`) or a `hostPath` with an app-level `sizeLimit` — **not**
   a bare hostPath with no limit (the scheduler ignores its size; a full disk causes node
   disk-pressure eviction).
2. **Label the ingest nodes:**
   ```bash
   kubectl label node <node> s3-prod-local-ingest=true    # x3–4 nodes
   ```
3. **Deploy the drain control plane + agents** (prod copies of the staging YAML, namespace
   `hippius-s3-prod`, `CEPHOR_DATABASE_URL` → `postgres-nvme`, `CEPHOR_POOL_ROOT` → the CephFS pool,
   `CEPHOR_SSD_ROOT` → `local_ingest_prod`):
   ```bash
   kubectl -n hippius-s3-prod apply -f k8s/production/drain-allocator-deployment.yaml   # singleton
   kubectl -n hippius-s3-prod apply -f k8s/production/drain-agent-daemonset.yaml         # nodeSelector s3-prod-local-ingest
   kubectl -n hippius-s3-prod apply -f k8s/production/mpu-reaper-deployment.yaml
   ```
   Carry over the tuned knobs already validated on staging (PR #250 + the Tier-2 decouple in #264):
   `CEPHOR_ALLOC_MIN_TOTAL_BPS=50MB/s`, `CEPHOR_ALLOC_TARGET_P99_MS=8000`,
   `CEPHOR_DRAIN_CONCURRENCY=16`, `CEPHOR_RECONCILE_POLL_SECS=15`, `CEPHOR_ENQUEUE_POLL_SECS=5`, and
   **raise `CEPHOR_MAX_DRAIN_RATE_BPS` toward 150–200 MB/s/node** if you go with 3 nodes.
4. **Deploy `api-local`** pinned to the labelled nodes (nodeSelector `s3-prod-local-ingest=true`),
   writing to `local_ingest_prod`; keep the classic `api` (Ceph-backed) as fallback during cutover.
5. **Set the 503 back-pressure below the kubelet eviction threshold** so
   `fs_cache_pressure_middleware` sheds load (503 + Retry-After) *before* the node evicts pods —
   with ~50 GB of real use on a 1–2 TB disk this is a non-issue in steady state, but keep the margin.
6. **Redeploy the API image ≥ #265** (`main` today) as part of cutover so the metrics stop lying
   (the inflation above is purely the old image; PR #284 removes the residual 2×).

---

## 5. Caveats & what to validate before cutover

- **Drain byte-budget vs Ceph:** the default 100 MB/s/node is a floor; going to 3 nodes needs
  ~150–200 MB/s/node — load-test the SSD→CephFS copy against the live pool before trusting it.
- **Node-loss exposure:** a client gets 200 once data is on one node's SSD. If that node dies before
  the part drains, the undrained bytes are stranded until it recovers (not lost — but not yet on
  Ceph). Keep the drain fast (it is, on NVMe-PG) and N+1 so a failure is absorbed; the exposure
  window is seconds.
- **Growth:** peak day was 5.2 TB (2× the median). If ingest doubles, the model is linear — add a
  node (drain-bound) rather than bigger SSDs (endurance-bound; bump DWPD/size).
- **Re-measure after #265/#284 deploy:** once prod runs a fixed image, `s3_bytes_uploaded_total` /
  `s3_operations_total` become trustworthy and you can drive this off Grafana directly instead of
  the DB.

---

## 6. Promoting to prod on EXISTING hardware (no new hosts)

We do not need to order hosts. The cluster already has the right disks — they're just all
inside Ceph. Full inventory of schedulable worker nodes:

| Node | CPU | RAM | Local NVMe | Ceph OSDs | Current role |
|---|---|---|---|---|---|
| **k8s-v3-node1** | 96 | 263 GB | 8× NVMe (6 data + 2 OS) | **6 × 3.84 TB** | Ceph |
| **k8s-v3-node2** | 96 | 263 GB | 8× NVMe | 6 × 3.84 TB | Ceph |
| **k8s-v3-node3** | 96 | 263 GB | 8× NVMe | 6 × 3.84 TB | Ceph |
| **k8s-v3-node4** | 128 | 395 GB | 8× NVMe | 6 × 3.84 TB | Ceph |
| **k8s-v3-node5** | 96 | 263 GB | 8× NVMe | 6 × 3.84 TB | Ceph |
| k8s-v3-node6-cache | 96 | 197 GB | OS disk only | 0 | api + redis cache |
| psql-s3-1/2/3 | 12 | 65 GB | — | 0 | Postgres (dedicated) |

- Disks are **enterprise datacenter NVMe, 3.84 TB each** (WD Ultrastar DC SN840 on node1–3/5,
  Intel/Solidigm D7-P5520 on node4). `bluestore_bdev_type=ssd`, `rotational=0`.
- **`rook-ceph` is `useAllDevices:true`** → every one of the 6 data NVMe per node is already an OSD;
  the 2 leftover NVMe (`nvme1n1`/`nvme2n1`) are the OS/boot pair. **There is no free NVMe to grab.**
- Ceph today: **30 OSDs, 105 TiB raw, 29 % used (30 TiB), 75 TiB free, size=3** (all PGs `active+clean`).

### The move: repurpose one OSD per node into a local-ingest disk

Pick **4 of node1–5** (they're identical; e.g. **node1, node2, node3, node5** — leave node4, the
fattest, whole). On each, **pull one OSD out of Ceph** and dedicate that 3.84 TB NVMe to
`local_ingest_prod`. That is 4 × 3.84 TB of enterprise NVMe reclaimed for free — **far more than
the 1–2 TB target** (only ~20 GB is ever used; the size buys endurance headroom).

**Ceph impact (safe):** 30 → 26 OSDs, raw 105 → ~91 TiB, usable (size 3) 35 → 30 TiB against
~10 TiB used → **~33 % used**, still far below the 85 % nearfull line. Each removal backfills ~1 TB
onto the remaining OSDs — **do it one OSD at a time, waiting for `HEALTH_OK` between each.**

**⚠️ Label, don't hard-taint.** These nodes keep running their other 5 OSDs, so a `NoSchedule`/
`NoExecute` taint would evict the co-located `rook-ceph` OSD pods (and everything else). Use a
**nodeSelector label** exactly like staging (`s3-prod-local-ingest=true`) to pin `api-local` +
`drain-agent` there while OSDs keep running. (A hard taint is only appropriate if you fully
evacuate Ceph from a node — not worth it here, it would remove 6 OSDs at once.)

### Per-node runbook (repeat on each of the 4 chosen nodes, one at a time)

```bash
# 1. Pick the OSD on this node to sacrifice (from: ceph osd df tree). Say osd.8 on node1.
kubectl -n rook-ceph exec deploy/rook-ceph-tools -- ceph osd out osd.8
#    wait for rebalance to finish:
kubectl -n rook-ceph exec deploy/rook-ceph-tools -- ceph -s   # until active+clean, HEALTH_OK

# 2. Stop + purge the OSD (rook: scale its deployment to 0, then purge)
kubectl -n rook-ceph scale deploy rook-ceph-osd-8 --replicas=0
kubectl -n rook-ceph exec deploy/rook-ceph-tools -- ceph osd purge 8 --yes-i-really-mean-it
#    remove the OSD from rook's device list so it isn't re-consumed (useAllDevices=true will
#    otherwise re-adopt the wiped disk!): pin the node's devices explicitly OR set the disk in
#    the CephCluster `storage.nodes[].devices`/`deviceFilter` to EXCLUDE nvmeXn1. This is the
#    one gotcha — without it rook re-creates the OSD on the freed disk.

# 3. Wipe the freed disk and mount it for ingest (nvmeXn1 = the purged OSD's device)
#    (run on the node via a privileged debug pod / node shell)
wipefs -a /dev/nvmeXn1 && mkfs.xfs -f /dev/nvmeXn1
mkdir -p /var/lib/hippius/local_ingest_prod
#    persistent mount (noatime) via /etc/fstab or a systemd mount unit:
mount -o noatime /dev/nvmeXn1 /var/lib/hippius/local_ingest_prod

# 4. Label the node for the s3 ingest role
kubectl label node k8s-v3-node1 s3-prod-local-ingest=true
```

Then deploy the drain stack + `api-local` pinned to `s3-prod-local-ingest=true` (see §4).

**Net for 4 nodes:** reclaim 4 × 3.84 TB dedicated NVMe (endurance to spare), cost Ceph ~5 TiB of
usable capacity it isn't using, add zero hardware. The chosen nodes then run 5 Ceph OSDs +
`api-local` + `drain-agent` side-by-side — trivially within their 96–128 cores / 263–395 GB.

### The one real gotcha

`useAllDevices:true` will **re-adopt the wiped disk as a new OSD** unless you first change the
`CephCluster` CR to exclude that device on that node (`deviceFilter` / explicit `devices` list, or
`useAllDevices:false` with an enumerated set). Do that **before** wiping, or rook will fight you for
the disk. This is the only step that needs care; everything else is standard drain/ceph ops.

---

## References

- **Google SRE Book — Capacity Planning / Provisioning** — provision to peak, never 100 %, N+1/N+2:
  https://sre.google/sre-book/software-engineering-in-sre/ ,
  https://research.google/pubs/sre-best-practices-for-capacity-management/
- **Brendan Gregg — USE Method / Thinking Methodically about Performance** — utilisation vs
  saturation; averages hide sub-minute spikes: https://www.brendangregg.com/usemethod.html ,
  https://queue.acm.org/detail.cfm?id=2413037
- **Little's Law for buffer/cache sizing** (`L = λ·W`): https://sookocheff.com/post/modeling/littles-law/
- **Token/leaky-bucket burst math** (bucket depth = `(fill−leak)·τ`):
  https://intronetworks.cs.luc.edu/current/html/tokenbucket.html
- **NERSC / Cray DataWarp burst buffer** — the closest architectural prior art (per-node building
  block fronting a ~100× slower durable tier):
  http://www.cs.umd.edu/class/fall2022/cmsc714/Readings/Bhimji-burst-buffer.pdf
- **Kubernetes Local Persistent Volumes (KEP-121)** — Local PV vs hostPath, ephemerality,
  `WaitForFirstConsumer`:
  https://github.com/kubernetes/enhancements/blob/master/keps/sig-storage/121-local-persistent-volumes/README.md
- **GKE local SSD / AKS ephemeral NVMe** — NVMe-backed scratch guidance:
  https://docs.cloud.google.com/kubernetes-engine/docs/concepts/local-ssd ,
  https://learn.microsoft.com/en-us/azure/aks/best-practices-storage-nvme
- **SSD endurance (DWPD / TBW / write amplification)** — why a churn workload needs write-intensive
  NVMe: https://techcommunity.microsoft.com/blog/filecab/understanding-ssd-endurance-drive-writes-per-day-dwpd-terabytes-written-tbw-and-/426024 ,
  https://www.atpinc.com/blog/ssd-tbw-dwpd-endurance

*Generated 2026-07-19 from live prod telemetry (object_versions + Loki audit) — metric-derived
numbers deliberately avoided due to the pre-#265 inflation.*
