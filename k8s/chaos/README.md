# k8s/chaos — Chaos Mesh CRDs for the WI-19 F1–F8 fault matrix

These manifests are the in-cluster half of the WI-19 chaos matrix (`stress-test/plan.md` §4.5).
They are **applied on demand** by `stress-test/faults/run_chaos.sh` (via `stress-test/faults/inject.py`),
always **on top of `inv-guard`** + the durability ledger, never on their own. Each cell asserts a
recovery bound AND zero invariant break.

**Target:** namespace `hippius-s3-staging`; selectors use the app labels `drain-allocator`,
`drain-agent`, `api`, `gateway`, and the CNPG cluster `postgres`.

| File | Fault | Mechanism | Recovery bound |
|---|---|---|---|
| `f1-agent-kill.yaml` | F1 agent-node kill mid-replication | PodChaos pod-kill | resume ≤60s; stuck-claim ≤6min |
| `f2-allocator-failover.yaml` | F2 allocator failover + fence (headline) | PodChaos + NetworkChaos partition | ≤45s partition; ≤10s clean |
| `f3-redis-queues-netchaos.yaml` | F3 redis-queues blip/hang | NetworkChaos delay/loss (toxiproxy owns evict/noeviction) | coord ≤30s; backlog ≤2min |
| `f4-cephfs-degraded.yaml` | F4 CephFS slow/degraded | NetworkChaos toward OSD/MDS | breaker ≤20s; drain ≤10min |
| `f5-ssd-fill.yaml` | F5 SSD fill pressure/critical | fallocate Job on the ingest SSD | 503 ceases ≤2min |
| `f6-postgres-failover.yaml` | F6 postgres failover | PodChaos kill CNPG primary | query ≤20s; drain ≤10min |
| `f7-clock-skew.yaml` | F7 clock skew (disposable pods only) | TimeChaos per-container | stable ≤2min |
| `f8-corrupt-chunk.yaml` | F8 corrupt/partial chunk | **toxiproxy slicer** (primary) / scoped IOChaos | detect ≤5s; reclaim ≤1h |

## Safety rails (do not remove)

- **Never `IOChaos` the shared CephFS mount.** F4 degrades Ceph via **NetworkChaos toward the
  OSD/MDS pods** on a disposable pool — an IOChaos fault on the shared object-cache mount would hit
  every tenant. F8's real mechanism is the toxiproxy `slicer` toxic (truncated backend body); the
  IOChaos in `f8-*.yaml` is scoped to a single disposable agent container and is opt-in.
- **F7 clock skew is disposable-pods only** (`clockIds: [CLOCK_MONOTONIC]` if the lease deadlines
  are monotonic) — never the API/gateway, whose auth depends on wall-clock.
- Every CRD is a **one-shot** (has a `duration`, no `schedule`) so a forgotten object self-heals.
  Delete explicitly with `kubectl -n hippius-s3-staging delete -f <file>`.
- Chaos Mesh must be installed in the cluster; these are inert YAML until then.

Prereqs: Chaos Mesh controller installed; `kubectl` context `hippius`; run under `inv-guard`.
