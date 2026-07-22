# hippius-drain — Remaining Work

> Status: the drain service was lifted into this repo as a root Cargo workspace on 2026-06-18.
> The durable core path is complete and verified; this doc lists what is left from the original
> design spec. Written for a fresh agent picking the work up in this repo.

## 1. What this is

`hippius-drain` is the SSD→CephFS drain service for the S3 ingest path (it replaces the Python
background replicator + janitor). It owns exactly one hop — node-local SSD → CephFS — plus garbage
collection of aborted/canceled/failed in-flight chunk debris on ingest nodes.

Three crates, one workspace rooted at the repo root (`Cargo.toml`, members under `crates/`):

| Crate | Kind | Role |
|-------|------|------|
| `hippius-drain-core` | library | all domain vocabulary, pure logic, the `Clock` trait, config types, traits, the Postgres layer (`pg` feature), and the `migrations/` set |
| `hippius-drain-agent` | binary (+lib for tests) | per-node drain daemon: trigger → claim → copy → verify → commit → unlink, GC, heartbeat, obey rate allocation |
| `hippius-drain-allocator` | binary (+lib for tests) | singleton leader-elected budget allocator: read fleet heartbeats + Ceph ceiling, run pure `allocate()`, write per-node allotments |

**Naming, deliberate:** the crates are `hippius-drain-*`, but the runtime contracts were left on the
original names by design — env vars are `CEPHOR_*` and Postgres tables are `cephor_*` (operator
configs and live DB schema, out of scope for a crate rename). Do not "fix" these without a
coordinated ops change.

**Full design docs** were NOT copied into this repo. They live in the `cephor` repo under
`docs/plans/`:
- `2026-06-17-cephor-drain-service-design.md` — validated architecture
- `2026-06-17-cephor-implementation-plan.md` — the M0–M10 milestone plan referenced below
- `2026-06-18-cephor-audit-remediation.md` — the 36-finding audit remediation (all closed)

## 2. Build & verify

```bash
# From the repo root. Toolchain is pinned (rust-toolchain.toml → 1.95.0).
cargo build --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings   # must be clean
cargo fmt --all -- --check

# Pure-logic tests run anywhere. The #[sqlx::test] cases need a live Postgres:
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5433/postgres"
cargo test --workspace --all-features
```

As of the lift: build/clippy/fmt clean; **193 tests pass** (115 core lib + 7 core integration +
60 agent + 11 allocator) against a real Postgres. Without `DATABASE_URL` the 14 `#[sqlx::test]`
cases fail with `DATABASE_URL must be set` — that is environmental, not a defect.

The workspace `forbid`s `unsafe_code` and runs a strict clippy block (panic-prevention:
`unwrap_used`/`panic`/`todo`/`print_*` all denied). Honor it — no `unwrap`/`expect` outside tests.

## 3. What is already built (do not redo)

The load-bearing path is complete: the crash-safe drain state machine with the `Verified` typestate
and streaming content hash, the Postgres layer (SKIP-LOCKED claim, lease-guarded commit, heartbeat,
allocation write with fencing epoch, GC claim), the pure `allocate()` math, the enforcement valve
(token bucket / circuit breaker / decay / drain+GC semaphores), the leader-elected allocator tick
loop, the agent runtime (drain workers, `ChunkLandedListener` Postgres `LISTEN/NOTIFY` trigger,
reconciler backstop, statvfs disk-pressure heartbeat), both binaries wired with tracing + config +
SIGTERM/SIGINT shutdown, and the agent observability snapshot (`SnapshotCell`/`AgentSnapshot`).

## 4. Remaining work, prioritized

### Tier 1 — spec'd components not built (functional)

1. **Live Ceph-mgr ceiling probe (design M7 `CephProbe`).** *Highest value.* Today the ceiling is
   `StaticCeiling(CephCeiling::Open(configured))` — `crates/hippius-drain-core/src/tick.rs`. The
   allocator backs off on fleet latency/error saturation but is **blind to real Ceph near-full**,
   which is the condition the whole service exists to manage. The seam is ready: implement a new
   `CephCeilingSource` (the trait in `tick.rs`) that probes the Ceph mgr REST API (OSD near-full +
   MDS load) and `classify()`s the result into `CephCeiling::{Open,NearFull,Critical}`; pass it to
   `run_allocator<C: CephCeilingSource>` (`crates/hippius-drain-allocator/src/run.rs`) instead of
   `StaticCeiling`. Fail-safe: on probe error, decay the last-known ceiling — never mint a fake
   `NearFull`. This is the natural integration point with this repo's existing Ceph/IPFS layer.
   **Blocked on:** the Ceph mgr REST flavor/version/DTO (see §5). Needs `reqwest` (not yet a dep);
   gate it behind an `http` feature so `--no-default-features` still builds.

2. **Redis `drain_requests` eager trigger (design M8 `RedisQueueConsumer`).** The fast-path
   per-chunk trigger. Only the Postgres `LISTEN` path + reconciler backstop exist today (no `redis`
   dep). **Correctness-neutral** — the reconciler is the source of truth, so drains still happen;
   this only lowers trigger latency. Mirror the `arion_upload_requests` queue; `brpop` on a
   dedicated connection.

3. **Reader-bell publish (design M5 `BellPublisher`).** The `publish bell` step after commit (redis
   publish, to wake readers). Missing (depends on redis). Durability does not depend on it — readers
   fall back to DB status. Confirm the DB-status fallback with the read-path team before relying on it.

4. **Observability / OTel (design M9).** No metrics today. The agent already exposes a snapshot seam
   (`SnapshotCell`/`AgentSnapshot` in `crates/hippius-drain-core/src/snapshot.rs`) to read from, but
   **the allocator-side `FleetWeights` snapshot seam was never built** — add it to
   `run_allocator`/the allocator runtime first. Then build typed metric registries (integer-coded
   enum gauges derived by exhaustive match over the canonical domain enums — no string-label escape
   hatch) and alert hysteresis over the `Clock` trait, exported via OTLP. Gate behind an `otel`
   feature.

5. **`verify_hash_contract` boot self-check (design M5→M10).** Fail-fast at agent startup if the
   hash algorithm/encoding diverges from the api's part-key derivation (the single failure mode:
   wrong hash ⇒ every verify fails ⇒ silent stall). Hash a known fixture, assert the encoded result
   equals the expected `PartKey`. Run it in `main` before the runtime starts.

### Tier 2 — test rigor the spec mandated

6. **loom test (design M4).** The apply-allocation-races-admit interleaving across the
   bucket/breaker/decay mutexes was spec'd as a *mandatory* (not optional) `loom` permutation test.
   Absent. Funnel the sync primitives through a `crate::sync` shim and run under `--cfg loom`.
7. **`trybuild` compile-fail tests.** M6: assert the covariant `GcClaim` variance is rejected
   (the invariant `PhantomData<&'tx mut ()>` compiles, a covariant `&'tx ()` must not). M9: assert
   the metric API has no string-label method. Absent.
8. **CI workflow + integration harness.** No `.github/workflows/ci.yml` for the Rust crates, and the
   `#[sqlx::test]` cases need an external Postgres. The spec called for a testcontainers strategy
   (pinned `postgres:17`, Docker-gated job) so `cargo test` without Docker still runs the pure tiers.
   Wire a Rust CI job here (this repo's existing CI is Python-only).

### Tier 3 — production wiring / spec divergences

9. **`clap` CLI + `secrecy`-wrapped secrets + `anyhow` boundary.** Config is currently plain env-var
   strings (`crates/hippius-drain-{agent,allocator}/src/config.rs`), DB URLs are not
   `SecretString`-wrapped, and the mains use `thiserror` rather than `anyhow` at the boundary. The
   spec called for `clap` + `secrecy` + `anyhow`-at-main. Deliberate simplification at lift time;
   revisit if the deployment needs CLI flags or secret-redaction in logs.
10. **CephFS GC enablement (design M6).** The pool-folder reclaim code is built but **feature-gated
    off** (`gc-cephfs`, see `crates/hippius-drain-core/src/gc.rs`). Only SSD-local GC ships today.
    **Blocked on** the api↔GC write-fence contract (§5). Flip the feature on once the fence is
    confirmed.

## 5. External owned gates (block several of the above)

These are confirmations/contracts owned outside the drain service — track them, they gate the work:

| Item | Gates | Needed for |
|------|-------|-----------|
| Ceph mgr REST flavor / version / DTO | the live probe | Tier 1 #1 |
| api ↔ GC write-fence (no writes after terminal commit, OR object lock, OR `terminal_at + δ` grace) | CephFS rm | Tier 3 #10 |
| api schema contract + terminal-state signal (column names, enum literal strings, PG-enum-vs-TEXT) | GC enablement | Tier 3 #10 |
| reader-bell DB-status fallback confirm | bell publish | Tier 1 #3 |
| CephFS parent-dir-fsync durability on the real mount (measure) | crash-safety tuning | staging |
| §7 tunables — watermarks, AIMD constants, lag-SLA, tick interval, decay half-life (measure on staging) | rollout | staging |

## 6. Suggested order

`#1 (live Ceph probe)` first — without it the allocator cannot see the condition it manages, and it
is the natural hippius-s3 integration point. Then `#4 (observability)` for operability, then
`#5 (hash self-check)` as a cheap safety win. `#2`/`#3` (redis trigger + bell) are latency-only and
can wait. `#6`–`#8` and `#9` are quality hardening. `#10` unblocks once the api write-fence lands.
