# stress-test/ — production-readiness harness

A **runnable** suite that asserts production readiness of the hippius-s3 drain stack against a live S3
endpoint (default `s3-staging.hippius.com`), plus the drain internals when the staging cluster is reachable.

This is the first executable increment of the build spec in [`plan.md`](plan.md). It implements the
S3-facing + light invariant tiers (durability oracle, functional/adversarial correctness, a concurrency
ramp, single-leader/backlog/terminal invariants, and drain-replication convergence). The heavy tiers
(allocator fence-race rigs, full chaos matrix, GB-scale soak) are specified in `plan.md` and are the next
increments.

## Run

```bash
# from the repo root, with the venv active
source .venv/bin/activate
source .aws.cli.env            # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
python stress-test/run.py
```

- **With cluster access** (default): needs `kubectl` pointed at the `hippius` context. Runs the invariant +
  convergence probes against staging Postgres (`cephor_replication_status`) and Prometheus (`drain_*`).
- **S3-only**: `python stress-test/run.py --no-cluster` — durability + functional + load only.
- `--keep` leaves the test buckets; `--timeout N` sets the drain-convergence budget (default 300 s).

Exit code is `0` (GO) / `1` (NO-GO). A markdown + JSON report and the durability ledger are written to
`stress-test/results/`.

## Pre-flight (deterministic, no live S3/cluster) — `inv-det`

Before any live run, prove the invariants that can be checked deterministically (WI-19 GO/NO-GO #2):

```bash
export CEPHOR_TEST_REDIS_URL=redis://localhost:6379   # required — the split-brain fence tests need a real Redis
python stress-test/inv/inv_det.py                     # cargo drain-core (--include-ignored) + pytest unit + G4 audit
python stress-test/inv/inv_det.py --integration       # also tests/integration (needs DB/Redis)
```

This runs the **8 `#[ignore]`d coordinator epoch/lease tests** (0 executed coverage under a bare `cargo test`), the
Python unit suite, and a static **sole-producer (G4)** audit asserting the dead `enqueue_upload` producer stays
callerless. A skipped required check (e.g. no `CEPHOR_TEST_REDIS_URL`) is **NO-GO**, not a pass.

## What it asserts (scenario → criterion)

| Scenario | Asserts | Pass condition |
|---|---|---|
| `functional_adversarial` (T1) | correctness, non-happy-path | range GET exact bytes; overwrite→latest; **MPU wrong-ETag rejected**; MPU abort→absent; anon public 200 / private 403; ListObjectsV2 delimiter; zero-byte round-trip |
| `durability_corpus` + `durability_reverify` | **S8/G3 no-data-loss (non-overridable)** | every acked object (mixed sizes incl. forced-multipart) re-GETs **byte-identical** (plaintext md5 + size) |
| `concurrency_ramp` (T5-lite) | S5/S6 backpressure + throughput | non-503 5xx rate < 1%, no hangs; **503 SlowDown is correct backpressure** (counted, not failed) |
| `invariant_assert` (cluster) | G1 / G6 / S4 | `sum(drain_leader) ≤ 1`; our objects' repl rows never regress from terminal; backlog is a bounded exported gauge |
| `replication_convergence` (cluster) | drain actually replicates | 0 `pending`/`draining` rows for our objects within the drain window; `drain_parts_replicated_total` advances |

The durability oracle keys on **client-side plaintext md5**, never ETag (hippius objects are
envelope-encrypted and MPU ETags are not content hashes).

## Layout

```
stress-test/
├── plan.md              # full build spec (all tiers)
├── run.py               # entrypoint (live S3 + cluster run)
├── inv/
│   └── inv_det.py       # pre-flight: cargo (--include-ignored) + pytest + sole-producer (G4) audit
├── harness/
│   ├── config.py        # endpoint + creds + cluster access
│   ├── s3util.py        # boto3 client (path-style, SigV4) + ops + md5
│   ├── ledger.py        # the acked-object durability manifest (oracle)
│   ├── probes.py        # staging Postgres + Prometheus probes (optional)
│   ├── scenarios.py     # the scenario suite
│   └── report.py        # PASS/FAIL/OBSERVED report (md + json)
└── results/             # run reports + ledgers (gitignored)
```
