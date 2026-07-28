# stress-test/ — drain-stack test harness & stress plan (build spec)

> **What this is.** The engineering build spec for the WI-19 prod-readiness harness: precisely what to build, the
> interfaces, and the pass/fail criteria — self-contained so a harness engineer can start cold. The *gate/decision*
> context (the full failure surface C1–C13, adjacent blind spots A1–A20, known-risks R1–R6, SLO ratification, GO/NO-GO,
> open decisions) was tracked in the s3-2.1 PRODUCTION-READINESS GATE (WI-19), now shipped (see git history). This doc is
> the *how to build the machine that runs that gate*.
>
> Ground truth was verified against the tree on 2026-07-02. Where this doc states a `file:line`, it was read.

---

## 0. Status & results so far (updated 2026-07-03)

The first increment of this harness (`stress-test/harness/` — the S3-facing functional/durability/concurrency
scenarios + cluster invariant probes over kubectl→Postgres/Prometheus) is **built and running against
`s3-staging.hippius.com`**. It is not yet the full plan below (no `inv-guard` background asserter, no chaos matrix, no
allocator-under-pressure rigs, no in-cluster load driver) — but it already drove four production bugs to fixed, merged,
and re-verified live. Runs are archived on PR #226 (`feat/stress-test-harness`, `results/run-*.md`).

### What the harness found → what we fixed (all merged to `staging`)

| Harness signal | Fix (merged) |
|---|---|
| `drain-convergence` slow; `drain_fleet_estimate_bps` pinned at 1 MB/s | **#227** — AIMD `target_p99` 50 ms → 2000 ms. `observed_p99` is a whole-part SSD→CephFS copy (~330–410 ms), so the 50 ms target fired the saturation back-off every tick and pinned the fleet write-budget at the `min_total` floor. **The bottleneck.** |
| T1 crashed on CreateBucket `UploadNotPermitted: Failed to fetch billing balance` | **#229** — classify a transient billing-lookup failure as retryable → retry, then 503 SlowDown, never a hard 402. |
| (new gate) `func-mpu-wrong-etag` | **#228** — CompleteMultipartUpload now validates client part ETags (`InvalidPart`) + ordering (`InvalidPartOrder`). |
| A1 read-path hang | **#230** — envelope-race cold version-fallback now enqueues a download instead of hanging on pub/sub to `HIPPIUS_CACHE_TTL`. |
| Replication lag stuck at ~5 s after the budget was unpinned | **#231/#232** — `CEPHOR_DRAIN_POLL_SECS` 5 s → 1 s (staging); the residual lag was the poll floor, not throughput. (#231 also tried `CEPHOR_DEFER_BACKOFF_SECS` → 1 s; **#232 reverted it to the 5 s default** — the defer only re-parks a not-yet-drainable part, so 1 s is pure claim churn on stuck/A21-orphan parts.) |

### Benchmark — before → after (same harness, same endpoint, live staging)

| | Baseline (pre-fix) | After #227–#230 | **After #231 (poll+defer 1 s)** |
|---|---|---|---|
| Harness verdict | 6 pass / 2 fail | 13 pass / 1 fail | 12 pass / 2 fail\* |
| Drain fleet write-budget | **1 MB/s** (pinned at floor) | ~1 GB/s (ramps to `max_total`) | ~1 GB/s |
| Replication lag p50 / p99 (SQL `updated_at−landed_at`) | ~5 s (under the 1 MB/s cap) | 4.6 s / 5.9 s | **1.33 s / 2.60 s** |
| Durability (non-overridable) | 105/105 | 105/105 | **105/105 byte-identical** |
| CreateBucket / MPU-wrong-ETag | ❌ crash / (n/a) | ✅ / ✅ rejected | ✅ / ✅ rejected |

> The 1.33 s / 2.60 s figures were measured with **both** poll and defer at 1 s. **#232** reverts defer to the 5 s
> default (keeps poll 1 s) — the poll is expected to carry the win for the common simple-PUT case; re-measure pending.

\* Both post-#231 "fails" are **not real regressions**: (1) `inv-G1-single-leader` read `sum(drain_leader)=2` because the
harness sampled *during the deploy rollout* — the departing allocator pod's series had not expired; it settled to `1`
immediately after (epoch fence guarantees one leader). (2) `drain-convergence` counts the two **negative-path MPU test
objects** (`mpu-badetag`, `mpu-abort`) whose Complete was correctly rejected/aborted → `object_versions.address` NULL →
the drain can't enqueue them **by design**. Every legitimate object (112/112 with an address) reached `replicated`,
0 pending.

### Where we are right now

- **Drain throughput is no longer the bottleneck.** Budget unpinned to the ~1 GB/s ceiling; measured demand in these runs
  was tiny (~1.4 MB/s) because the harness is **client-uplink-bound from a laptop over the internet** — so the *stack's*
  true per-node knee (S1) is still **unmeasured**. That needs an in-cluster load driver (§4.3) — the top open perf item.
- **Replication lag is now ~1.3 s p50 / 2.6 s p99** on staging, well inside S2/S3.
- **Two real gaps the runs surfaced, tracked in the todo:** (A21) aborted/rejected MPUs leak orphan
  `cephor_replication_status` pending rows the reaper never cleans (3 live, ~7 days old); and (WI-20/R1) the ingest-SSD
  orphan-byte leak (82 %/69 %). Both are P0/P1 for cutover.
- **Harness fix owed:** exclude the negative-path MPU objects from the `drain-convergence` set so a clean run reads 14/14.

Full narrative + morning next-steps: [`../s3-2.1-checkpoint-20260702.md`](../s3-2.1-checkpoint-20260702.md).

---

## 1. Why we are building this

The drain stack replaced the synchronous write path with an async **SSD → CephFS → backend** pipeline coordinated by a
leader-elected Rust allocator. This changed the durability contract from *"acked = on Ceph"* to
**"acked = on ingest SSD, the drain will replicate."** It is **flagless and live** on staging as the *sole* upload
producer, shipping to prod.

The entire risk of the cutover is one of two failures:
1. **Silent data loss / corruption** — an acked object's bytes are lost or corrupted between the client `200` and durable
   backend storage.
2. **Coordinator misbehaviour under fault/load** — the allocator double-allocates (Ceph write overrun) or stalls
   (backlog explosion → 503 storm).

The per-step safety invariants (copy → verify → commit → unlink ordering, claim-seq fencing, epoch fence, replication-gated
reclaim, AIMD back-off) are **heavily unit-tested but almost entirely against in-memory fakes on sequential harnesses**.
The coordinator split-brain core has **0 executed coverage under default `cargo test`** (its 8 tests are `#[ignore]`,
needing a real Redis). No chaos run, no continuous-invariant assertion, no real load has ever exercised the stack against
real localfs + CephFS + Postgres + Redis under fault. **This harness closes that gap** — it is the machine that proves the
8 catastrophic invariants hold *continuously* while real load and real faults run, and that performance stays inside
ratified SLOs.

**Design principle (non-negotiable):** *no throughput or allocator number is trusted unless the invariant guard was green
for the entire run that produced it.* Every dynamic run (chaos, allocator-stress, load, soak) executes **on top of**
`inv-guard`, which aborts the run the instant any invariant breaks.

---

## 2. Context primer (everything the harness assumes)

### 2.1 The system under test
- **`drain-allocator`** (Deployment, singleton-ish): leader-elected via a Redis `Coordinator`; each tick it computes a
  fleet write-budget and writes per-node allocations, fenced by an epoch.
- **`drain-agent`** (DaemonSet, one per ingest node): claims a `pending` part (`FOR UPDATE SKIP LOCKED`), copies SSD→pool,
  verifies every chunk, commits (`mark_replicated`, claim-seq fenced), enqueues an `UploadChainRequest`, unlinks the SSD
  copy; obeys the allocator's per-node byte budget; trips a Ceph breaker on genuine pool-write faults.
- **State machine** — `cephor_replication_status.status ∈ {pending, draining, replicated, failed}`; `replicated`/`failed`
  are terminal. Columns of interest: `status`, `claim_seq`, `claimed_at`, `updated_at`, `landed_at`, `node_id`, `version`.
- **Sole-producer invariant** — only the drain enqueues uploads (the old Python `enqueue_upload` producer is **dead code**,
  `hippius_s3/writer/queue.py:11-35`, 0 callers). The uploader ships to backends + records `chunk_backend`.

### 2.2 Timing constants (verified — the recovery/latency bounds derive from these)
tick **5 s** · lease TTL **30 s** · alloc-key TTL **15 s** · agent heartbeat TTL 30 s / poll 10 s · drain poll 5 s ·
agent alloc re-pull **2 s** · claim lease **5 min** · decay half-life 30 s / floor **1 MB/s** · breaker **5 fails / 10 s
cooldown** · Redis coord timeout **5 s** · CHUNK_COPY_ATTEMPTS **3**.

### 2.3 Metrics — what actually exists (gate ONLY on these)
Exported OTLP (feature `otel`, gated on `ENABLE_MONITORING`), **5 total**:
`drain_leader`, `drain_fleet_estimate_bps` (allocator); `drain_parts_replicated_total`, `drain_ssd_backlog_bytes`,
`drain_breaker_open` (agent — **breaker gauge is conditional**, only present when an enforcer is passed). Plus
`uploader_dlq_total`, `hippius_queue_length` (workers).
**Computed but NOT exported** → cannot be a Prometheus gate: `SnapshotCell.p99()` (drain latency),
`failed`/`error_bps`/`deferred`/`reclaimed`/`reconciler_recovered`.
- **Replication lag** ⇒ **SQL**, not a metric:
  `percentile_disc(0.99) WITHIN GROUP (ORDER BY updated_at - landed_at) FROM cephor_replication_status WHERE status='replicated' AND updated_at > now()-interval '5 min'`.
- **Per-node budget** ⇒ Redis `GET cephor:alloc:<id>`. **Sole-leader** ⇒ `sum(drain_leader) ≤ 1`. **Epoch** ⇒
  `GET cephor:epoch`.
- **Lag is derived from *completed* drains** → structurally blind to a total stall (see R5 / §Criteria "liveness").

### 2.4 Environment
- kube context `hippius`; staging ns `hippius-s3-staging`; monitoring ns `monitoring`.
- Prometheus: `kubectl -n monitoring port-forward svc/prometheus-server 9090:80`. Grafana NodePort 31337.
- Postgres: `kubectl -n hippius-s3-staging exec postgres-1 -c postgres -- psql -U postgres -d hippius -tAc "…"`.
- S3: `source ./.aws.cli.env` (never print); endpoint `https://s3-staging.hippius.com`; `aws` CLI v2.
- Live-drain tests gated on `HIPPIUS_DRAIN_LIVE=1`; Rust coordinator tests need `CEPHOR_TEST_REDIS_URL` + `--include-ignored`.
- **Prod-parity gap:** staging `redis-queues` = `allkeys-lru` 1 GB; **prod = `noeviction`.** The eviction/OOM cells need a
  `noeviction` override overlay or staging masks the prod failure mode.

---

## 3. Directory layout to build

```
stress-test/
├── plan.md                        # this doc
├── inv/
│   ├── inv_det.py                 # deterministic pre-deploy runner (§4.1)
│   ├── inv_guard.py               # continuous background asserter (§4.2)
│   ├── guards.py                  # G1–G8 predicates (probe + run-fails-if)
│   └── history_check.py           # offline Elle/Knossos over recorded op history
├── load/
│   ├── driver.py                  # warp/elbencho orchestrator + md5 manifest (§4.3)
│   ├── ledger.py                  # acked-object md5+size manifest (the durability oracle)
│   └── profiles.yaml              # A1/A2/GB/soak/backpressure/scaling ladders
├── faults/
│   ├── inject.py                  # thin kubectl/redis-cli/toxiproxy injectors (§4.5)
│   ├── matrix.yaml                # F1–F8 × workload cells + recovery bounds
│   └── run_chaos.sh
├── alloc-stress/
│   ├── monitor.py                 # 250 ms sampler → JSONL (drain_leader, epoch, alloc keys)
│   ├── gate.py                    # PASS/FAIL over JSONL + DB + md5
│   └── run_scenario.sh            # A–F scenarios (§4.6)
├── db/
│   └── prod_scale_gate.py         # R2: EXPLAIN(ANALYZE,BUFFERS) + HypoPG on a prod-scale dump (§4.7)
├── compose/
│   ├── docker-compose.alloc-stress.yml   # Rig A: N allocators + toxiproxy redis_queues proxy
│   └── docker-compose.faults.yml         # toxiproxy toxics + noeviction override for e2e
└── k8s/chaos/                     # Chaos Mesh CRDs (PodChaos/NetworkChaos/TimeChaos/IOChaos)

# lives in the crate, not here (Rust must compile with the code):
crates/hippius-drain-core/tests/it/alloc_stress.rs   # Rig B: deterministic fence-race + R3 failpoint (§4.6)
```

**Reuse, do not rebuild:** `tests/e2e` (the drain stack IS already in the e2e compose: allocator + agent + toxiproxy +
redis-queues), `tests/staging` (`HIPPIUS_DRAIN_LIVE=1`), `cargo test -p hippius-drain-core`. The harness *orchestrates*
these; it does not replace them.

---

## 4. Components to build (each: purpose · interface · acceptance)

### 4.1 `inv-det` — deterministic pre-deploy proof
- **Purpose:** one command that proves all 8 invariants deterministically before any dynamic run. The pre-flight gate.
- **Does:** runs `cargo test -p hippius-drain-core --include-ignored` (with `CEPHOR_TEST_REDIS_URL` — **without
  `--include-ignored` the 8 coordinator epoch/lease tests silently skip**), `pytest tests/unit tests/integration`,
  targeted e2e, a **static callsite audit** (assert no `enqueue_upload_to_backends` callsite exists outside the drain
  enqueue module → G4), and the `PartVerified` `trybuild` compile-fail proof.
- **Acceptance:** exits non-zero on any failure; prints a per-invariant G1–G8 pass table; CI-wired as a pre-deploy gate.

### 4.2 `inv-guard` — continuous background asserter (the backbone)
- **Purpose:** run during **every** T3/T4/T5 dynamic run; raise `GuardViolation` → abort the run and mark it NO-GO the
  instant any invariant breaks. This is what makes every throughput/chaos number trustworthy.
- **Design (from runtime-verification research — do not deviate):**
  - **Hard binary invariants are event-driven, no debounce** — tail the Postgres WAL / logical decode + the allocator
    logs; a fast violation between two polls is invisible to polling. Abort on the **first** occurrence.
  - **Metric SLOs are polled**, lookbehind **≥ 4× the scrape interval**, with **hysteresis** (separate trip/clear
    thresholds) to avoid flapping.
  - Blast-radius model = ChAP: the guard is the automated circuit breaker for the experiment.
- **Sources:** Prometheus (port-forward), Loki, Postgres, Redis.
- **Interface:** `inv_guard.py --guards G1,..,G8 --run-id <id> --out run-<id>.jsonl`; writes a JSONL event stream; exit
  code ≠ 0 ⇒ the enclosing run aborts.
- **The 8 guards (probe → run-fails-if):**

  | G | Probe | Run-fails-if |
  |---|---|---|
  | G1 single-leader+epoch | event: `sum(drain_leader)`, `GET cephor:epoch`, allocator `Fenced` log | `≥2` leaders; epoch decreases; any `cephor:alloc:<id>` epoch < current after a tick |
  | G2 replication gate | SQL sentinel: every `chunk_backend deleted=true` had a live row for all required backends (`upload_backends ∪ backup_backends`) at delete time; `pressure_mode==2` + `JANITOR_CRITICAL_PRESSURE_BLOCKED` = PASS | any soft-deleted chunk lacking full-union coverage |
  | G3 durability | rolling md5+size re-GET vs the ledger (§4.3); SQL stalled-row check | any GET byte/size mismatch; stalled `pending`/`draining` past 2× lag SLO |
  | G4 sole-producer | static audit (inv-det) + runtime SQL uniqueness on `(chunk_id, backend)` | duplicate `(chunk_id, backend)`; API-side enqueue callsite |
  | G5 at-least-once no-dup | uploads per `(object_id,version,part,backend)` ≤ 1 committed | a committed duplicate; a `Replicated` row never enqueued |
  | G6 terminal monotonicity | sampled diff of `cephor_replication_status` | any `replicated→*` / `failed→*` transition |
  | G7 AEAD determinism | inv-det: byte-identical ciphertext; overwrite-window GET → 200/206 via v-1 | non-deterministic ciphertext; a 500 on overwrite GET |
  | G8 reaper safety | inv-det + guard: reaper only touches DLQ-gated, NULL-address, age>stale versions | a paused/non-DLQ MPU reaped; reaper crash on NULL `object_version` |
- **Acceptance:** a synthetic violation of each guard (injected in a test) aborts the run within its detection bound;
  a clean run produces a green JSONL and exit 0.

### 4.3 Load driver + durability ledger
- **Purpose:** generate the load ladders AND maintain the acked-object md5 manifest that is the durability oracle.
- **Build on:** MinIO **`warp`** (mixed R/W, MPU, distributed `warp client`) for the A1/A2 ladders; **`elbencho`** for
  GB-scale (rich latency percentiles). Keep a **thin boto3 wrapper** for the adversarial T1 correctness cases that need
  exact malformed requests (wrong ETag, subset part list, zero-byte part, etc.).
- **Ledger (`ledger.py`):** on every acked PUT/MPU-complete, record `(bucket, key, plaintext-md5, size)`. **Oracle keys on
  client-side plaintext md5 / `x-amz-checksum-*`, NOT ETag** — envelope-encrypted + MPU ETags are not content hashes.
  `inv-guard` G3 re-GETs against this ledger continuously.
- **Profiles (`profiles.yaml`):**

  | Profile | Mix | Rungs | Dur | Purpose |
  |---|---|---|---|---|
  | A1 small-heavy | 100% 64 KiB–1 MiB PUT | 10→25→50→100→200 | 5 min | small-object knee; 503 onset |
  | A2 MPU-heavy | 70% MPU 50–500 MiB, 30% PUT 1–10 MiB | 10→25→50→75→100 | 5 min | MPU knee; lag under load |
  | GB-scale | 1 GiB + 10 GiB single-client MPU | 1 | until drained | per-node MB/s (S1) |
  | Soak | 30% PUT / 70% MPU | 80% of knee | 6 h | leak / steady-state |
  | Backpressure | 1 MiB flood + `dd` SSD fill | fill→release | ≤10 min | S5/S7 self-heal |
  | Scaling | A2 mix | 1 node → 2 nodes | 10 min | linearity curve |
- **Ramp discipline:** a rung must hold **all §Criteria SLO gates** for its full duration before advancing; the first rung
  to break a gate is the **certified knee** (last passing rung). All T3/T4 runs use **80% of the certified knee.**
- **Acceptance:** ledger re-GET of a known corpus is byte-exact; driver runs in-cluster (not laptop-uplink-bound) so S1 is
  representative.

### 4.4 Toxiproxy toxics + redis/PG proxies + mock fault modes
- **Purpose:** the fault-injection substrate for F3/F6/F8 and the allocator Redis-pathology cells. **Verified: today
  toxiproxy is used on/off only (zero toxics), with no proxy in front of redis-queues or Postgres, and the mocks have no
  fault modes.** This must be built.
- **Build (`tests/e2e/support/compose.py` + `compose/docker-compose.faults.yml`):**
  - `add_toxic()` / `_ensure_redis_proxy()` helpers; toxiproxy proxies fronting **redis-queues** and **Postgres**;
    toxics: `latency`, `timeout`, `bandwidth`, `limit_data` (truncated body for F8), `reset_peer`
    (`slicer` only fragments the TCP stream and preserves every byte — it does NOT corrupt/truncate).
  - **`noeviction` override overlay** for e2e redis-queues (flip from `allkeys-lru` → `noeviction` to match prod).
  - Mock fault modes on `Dockerfile.mock-{arion,kms,hippius-api}`: env/endpoint-toggled `500` / `slow` / truncated-body /
    fail-after-N / `can_upload=false`.
- **Acceptance:** each toxic is verifiable in isolation (a latency toxic delays a call by the injected amount; `reset_peer`
  surfaces a typed error within the 5 s coord timeout); the noeviction override makes a full redis-queues **reject writes
  loudly** (not silently evict).

### 4.5 Chaos executor + matrix
- **Purpose:** run F1–F8 (fault × workload) on staging under `inv-guard` + the ledger; assert recovery bound + zero
  invariant break per cell.
- **Build:** thin `inject.py` (kubectl / redis-cli / toxiproxy) + **Chaos Mesh** CRDs in `k8s/chaos/` for
  PodChaos/NetworkChaos/TimeChaos/IOChaos. `matrix.yaml` encodes cells + bounds:

  | Fault | Headline gate | Recovery | Tool |
  |---|---|---|---|
  | F1 agent-node kill mid-replication | no false-commit under stale `claim_seq` (G5/G6) | resume ≤ 60 s; stuck-claim ≤ 6 min | PodChaos / `docker kill` |
  | F2 allocator failover/fence (3 sub-injections) | single-leader + epoch fence (G1) | ≤ 45 s partition; ≤ 10 s clean | PodChaos + NetworkChaos partition |
  | F3 redis-queues blip/evict/hang/noeviction-full | leader survives lease eviction; no dup | coord ≤ 30 s; backlog ≤ 2 min | toxiproxy + `DEBUG SLEEP` + noeviction override |
  | F4 CephFS slow/degraded/down | no stuck breaker; no false `replicated` | breaker ≤ 20 s; drain ≤ 10 min | NetworkChaos → OSD/MDS (IOChaos on CephFS is risky) |
  | F5 SSD fill pressure/critical | clean 503 SlowDown; replication gate absolute | 503 ceases ≤ 2 min | `fallocate`/`dd`; Chaos Mesh disk-fill |
  | F6 postgres failover | leadership unaffected; breaker not tripped by PG-only fault | query ≤ 20 s; drain ≤ 10 min | toxiproxy PG proxy / CNPG switchover |
  | F7 clock skew (disposable pods only) | single-leader + epoch-mono | stable ≤ 2 min | Chaos Mesh TimeChaos (per-container) |
  | F8 corrupt/partial chunk on SSD | never commit corrupt as `replicated`; blast-radius = 1 part | detect ≤ 5 s; reclaim ≤ 1 h | toxiproxy `limit_data` (truncated body) / Chaos Mesh IOChaos `mistake` |
- **F2 is the headline** — 3 sub-injections: (a) clean SIGTERM → graceful relinquish; (b) NetworkPolicy partition-then-heal
  → forces a stale-epoch write, must be `Fenced`; (c) transient 2-replica race + optional toxiproxy Redis latency. Assert:
  `sum(drain_leader) ≤ 1` every sample, `cephor:epoch` strictly increasing, zero alloc keys below current epoch after a
  tick, deposed write logged `Fenced` not applied, `Σbudget ≤ ceiling`.
- **Preconditions:** F7 disposable pods only (TimeChaos per-container, `clockIds: [CLOCK_MONOTONIC]` if lease deadlines are
  monotonic); F4 shared-pool degradation via NetworkChaos/Ceph-native on a disposable pool; F3 noeviction cell needs the
  override. Reaper won't fire in a chaos run (`HIPPIUS_MPU_STALE_SECONDS=2 days`) → keep reaper safety in `inv-det` unless a
  shrunk-window cell is added.
- **Acceptance:** each cell reports PASS iff (recovery within bound) ∧ (zero `inv-guard` violation) ∧ (ledger 100%).

### 4.6 Allocator-under-pressure rigs (USER PRIORITY)
- **Rig A — `compose/docker-compose.alloc-stress.yml`:** N allocator instances sharing `REDIS_QUEUES_URL`, fronted by a
  toxiproxy `redis_queues` proxy (**the only way to inject coordinator↔Redis latency without patching the binary**).
- **Rig B — `crates/hippius-drain-core/tests/it/alloc_stress.rs` (build in the crate):** deterministic fence-race +
  lease-gap via a barrier-gated `CephCeilingSource`, reusing the `coord()` / `run_allocator` patterns. **Add a
  `fail`/`failpoint`** at the `ceiling()`→`write_allocations` boundary for deterministic **R3** injection (or SIGSTOP the
  deposed allocator there).
- **Sampler/gate (`alloc-stress/`):** `monitor.py` samples `sum(drain_leader)`, `cephor:epoch`, and every `cephor:alloc:<id>`
  epoch at **250 ms** → JSONL; `gate.py` evaluates PASS/FAIL over JSONL + DB + ledger.
- **Scenarios (each a binary gate):**
  - **A** N-node contention: `sum(drain_leader)` pinned at 1 every 250 ms; ≤3 epoch bumps; one shared epoch.
  - **B** forced leader churn (kill leader every ~30 s, 10 min): `sum(drain_leader) ≤ 1` always; per-key epoch monotonic;
    `rate(drain_parts_replicated_total)>0` through every leaderless gap.
  - **C — the R3 gap.** Tier-1 fence correctness (lower epoch after higher-written → `Fenced`). **Tier-2 the gap:**
    successor B holds lease (epoch N+1) with **no** alloc key written, deposed A finishes a slow `ceiling()` and writes
    epoch N. **PASS:** any stale epoch-N budget visible **≤ one alloc-TTL (15 s) AND ≤ one agent poll (2 s)**; enforced
    beyond that OR `Σbudget > ceiling` observed → **FAIL** (demands the fix). This gate doubles as the fix's acceptance test.
  - **D** Redis latency/saturation/eviction/reset under **both** allkeys-lru + `noeviction`: `sum(drain_leader) ≤ 1`
    throughout; recovery ≤ 3 ticks; noeviction fails loudly not silent split-brain.
  - **E** budget fairness/starvation/overrun: `Σbudget = min(cap, Σdemand)`, `B_i ≤ demand_i`, critical-node reservation
    floor, `Σbudget ≤ ceiling`.
  - **F** decay-on-Err under sustained Redis outage: agent rate decays to floor (never 0); `drain_breaker_open == 0` (Redis
    loss ≠ Ceph failure); zero loss on restore.
- **Acceptance:** A–F all produce a binary PASS with the sampler JSONL as evidence; Rig B's R3 failpoint deterministically
  reproduces C Tier-2 (red before the fix, green after).

### 4.7 Prod-scale query gate (R2)
- **Purpose:** prove the reaper + reconciler queries are index-driven at prod cardinality **before** enabling on prod —
  the nvme-postgres-instability incident class.
- **Build (`db/prod_scale_gate.py`):** load a **prod-scale** clean dump (`scripts/gen_clean_dump.py`) into a staging DB;
  `EXPLAIN (ANALYZE, BUFFERS)` the reaper `SELECT`, `claim_part`, and the reconciler `statuses()`/`part_states()`;
  **HypoPG/Dexter** to confirm the right indexes. **Rewrite batch predicates as `col = ANY($1::uuid[])`** (btree, PG17
  ~3×) — never `scalar = ANY(array_column)` (forces a seq scan, needs GIN).
- **Acceptance (gate on buffers, not wall-clock):** no `Seq Scan` on `cephor_replication_status`; no Hash `Batches > 1`
  spill; required indexes present; reaper cycle p99 < RATIFY bound. **Missing index = NO-GO.**

### 4.8 Rust invariant / property / concurrency tiers (in the crate)
The load-sensitive concurrency invariants are proven **only sequentially** today — a contention regression passes the whole
suite. Build (land in `inv-det`):
- **proptest:** AIMD `next_capacity` **decrease + saturation triggers** (`alloc.rs:297-315`; additive-increase is already
  tested — scope to multiplicative decrease + `latency_saturated`/`error`-sat, which no fixture fires); conservation under a
  **`NearFull`** binding ceiling (the small-`Open` case already exists as `infeasible_path_conserves_capacity`); large-fleet
  **50..1000 nodes** (currently capped 1..6); order-independence; `reclaim_ssd` unlinks exactly `Failed`+aged; commit ⇒
  chunk byte-equal; per-step `drain_part` safety; epoch monotonicity (> vs ≥).
- **loom:** permit conservation + breaker accounting under concurrent `drain_next` (`Arc<Mutex<Enforcer>>`); mutex never held
  across `.await`. *(loom does NOT apply to `coordination.rs` — inter-process Redis Lua.)*
- **fault-injection (real PG + real Redis):** concurrent `claim_part` N=2..16 → exactly one `Some`, distinct `claim_seq`;
  N-way lease election → one `Some(Lease)`; multi-key plan fenced all-or-nothing; **C1c** epoch-bypass (DEL/expire the alloc
  key, stale write → assert never effective + the agent compares `StoredAllocation.epoch`); Redis black-hole/`DEBUG SLEEP` →
  `Err` within timeout; noeviction-full OOM → typed `Err`; epoch-counter-loss (C1); fsync EIO → SSD retained; two same-part
  drains → untorn copy (C11); symlink refused (`O_NOFOLLOW`); `Failed`-terminal-sink; terminal-GC vs reclaim vs re-record
  ordering; same-`(object,version,part)`-two-nodes → one owner; **breaker not tripped by post-commit `Unlink` OR
  `Persist`-tagged SSD-read EIO (C13)** — note the fix must split `Persist` (SSD-read vs Ceph-write); `Cleanup` correctly
  trips (it removes a corrupt pool copy — a genuine Ceph op).
- **offline (`inv/history_check.py`):** Elle/Knossos over the recorded op history post-run for deep ordering anomalies a
  per-scrape asserter can't see (keep histories bounded — linearizability checking is NP-complete).

### 4.9 Offline history checker
- **Purpose:** post-run deep-anomaly check for single-leader / no-data-loss ordering that polling missed.
- **Build:** record the op history (allocations, leader transitions, replication-status transitions, GET verifications)
  during a run; feed to Elle (for the SQL/DB history) / Knossos (for the leader register). Bounded history windows.

---

## 5. Criteria (the definition of PASS)

A run is GO only if **all** of:

### 5.1 Continuous invariants (via `inv-guard`, every dynamic run) — any breach aborts the run, NO-GO
G1 single-leader+epoch · G2 replication-gate-at-disk-full · G3 durability (byte-identical re-GET) · G4 sole-producer ·
G5 at-least-once-no-dup · G6 terminal monotonicity · G7 AEAD determinism · G8 reaper safety. **Durability (G3/S8) and
split-brain (G1/S10/S11) are non-overridable.**

### 5.2 SLO gates (S1–S14 — DRAFT, ratify against a real in-cluster baseline first)
S1 per-node throughput ≥ 200 MB/s at knee (RATIFY — set CI floor ~70% of measured single-node p50) · S2/S3 lag p99
< 15 s below knee / < 60 s at knee (GB objects own profile, uncensored) · S4 backlog < 20 GiB/node + runway + monotonic
alerts · S5/S6 503 < 1% at knee / 0% below, 5xx < 0.1% · S7 recovery < 120 s · **S8 durability 100% (plaintext md5)** ·
S9 DLQ growth 0 · **S10/S11 exactly 1 leader, 0 unforced churn** · S12 0 unreplicated deletions even ≥95% disk ·
S13 0 redis evictions under noeviction · S14 6h soak slope ≈ 0 incl. `replicated`-on-SSD count.

### 5.3 Risk gates (R1–R6, from the todo)
R1 SSD leak slope ≈ 0 + free-space alert · **R2 index scans (no seq scan/spill) — missing index = NO-GO** · **R3 T4-C
Tier-2 within bound OR fix landed** · **R4 live-object persistent-mismatch NOT silently reclaimed + alarm before unlink
(non-overridable)** · R5 stalled-drain liveness alert fires on a forced total stall · R6 adjacent P0s (A1,A2,A3,A4/A5,A6,
A7,A10,A12) fixed or operator-accepted with a bound.

### 5.4 The stall caveat (R5) — build the liveness gate explicitly
Because lag is computed from *completed* drains, a total stall (breaker open / agent dead) yields **zero lag samples** → the
lag SLO looks healthy while the SSD fills and every PUT 503s. Build a **zero-throughput-with-backlog>0** liveness alert
(SQL/Redis-derived) that is **independent of the lag histogram**; F4/agent-kill must trip it within a RATIFY bound. The lag
SLO alone is proven insufficient.

The full 17-item GO/NO-GO checklist and the 15 open decisions were tracked in the s3-2.1 production-readiness gate (shipped; see git history).

---

## 6. How to run

```bash
# 0. pre-flight (deterministic, no staging)
python stress-test/inv/inv_det.py            # cargo --include-ignored + pytest + trybuild + callsite audit

# 1. certify the knee (staging, in-cluster load)
python stress-test/load/driver.py --profile A1 --guard   # ramps rungs under inv-guard; prints the certified knee
python stress-test/load/driver.py --profile A2 --guard
python stress-test/load/driver.py --profile GB --guard

# 2. allocator-under-pressure (Rig A compose + Rig B cargo)
docker compose -f stress-test/compose/docker-compose.alloc-stress.yml up -d
bash stress-test/alloc-stress/run_scenario.sh A B C D E F   # C = the R3 gap
cargo test -p hippius-drain-core --test it alloc_stress -- --include-ignored   # Rig B deterministic R3

# 3. chaos matrix (staging, under inv-guard + ledger)
HIPPIUS_DRAIN_LIVE=1 bash stress-test/faults/run_chaos.sh F1 F2 F3 F4 F5 F6 F7 F8

# 4. soak + backpressure + R2
python stress-test/load/driver.py --profile Soak --hours 6 --guard
python stress-test/load/driver.py --profile Backpressure --guard
python stress-test/db/prod_scale_gate.py --dump prod-clean.sql

# every run writes run-<id>.jsonl (inv-guard events) + the ledger; gate.py compiles PASS/FAIL.
```

---

## 7. Build sequencing (what unblocks what)

1. **`inv-guard` + `guards.py` + `ledger.py`** — nothing dynamic is trustworthy without them; build first.
2. **Toxiproxy toxics + redis/PG proxies + mock fault modes + noeviction override** — the fault substrate; prereq for
   F3/F6/F8 and allocator D.
3. **Load driver (warp/elbencho) + profiles** — certifies the knee that everything downstream uses at 80%.
4. **Alloc-stress Rig A + Rig B** — the user-priority allocator cells + the deterministic R3 reproducer.
5. **Chaos executor + Chaos Mesh CRDs** — F1–F8 on top of 1–2.
6. **Rust proptest/loom/fault-injection tiers** — fold into `inv-det` (can proceed in parallel with 2–5).
7. **`db/prod_scale_gate.py`** — independent; run against a clean dump any time.
8. **`inv/history_check.py`** — last; consumes the recorded histories from 3–5.

**Reuse-first reminder:** the drain stack is already in the e2e compose and there are `tests/staging` live-drain tests +
`cargo test` coordinator tests (behind `--include-ignored` + `CEPHOR_TEST_REDIS_URL`). Build the *orchestration and the
missing fault substrate*, not new copies of what exists.

---

## 8. External toolchain (chosen picks)

Chaos Mesh (k8s pod/net/time/io/stress CRDs) · Toxiproxy (per-dependency net faults) · tc/netem + iptables DROP (real
IP-layer / asymmetric partitions) · Jepsen Elle/Knossos + `fail`/failpoint + turmoil/madsim (coordination, R3, fence
monotonicity) · dm-flakey/dm-dust/dm-delay + fio (disk faults) · CrashMonkey/dm-log-writes (fsync/power-loss, post-deploy)
· warp + elbencho (S3 load) · memtier_benchmark + `DEBUG SLEEP` (Redis stress/OOM) · EXPLAIN(ANALYZE,BUFFERS) + HypoPG/Dexter
(R2) · Prometheus trend + pprof/heaptrack (soak leaks).
