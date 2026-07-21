# hippius-drain — Live Ceph Ceiling Probe (Tier 1 #1 / design M7)

> Date: 2026-06-18. Status: validated against live cluster data, pre-implementation.
> Supersedes the design's literal "restful `osd dump` JSON DTO" for the first cut — see §2.

## 1. Goal

Replace `StaticCeiling` in the allocator with a live source that sees real Ceph
near-full, by implementing a new `CephCeilingSource` (the trait in
`crates/hippius-drain-core/src/tick.rs`) and passing it to `run_allocator` instead of
`StaticCeiling`. The allocator is otherwise unchanged — `run_tick`/`run_allocator`
signatures are already generic over `C: CephCeilingSource` and are NOT touched.

## 2. Frozen contract (from live discovery, 2026-06-18)

Cluster is Rook-Ceph / Ceph v19.2.3 (squid). Ratified flavor: **prometheus mgr
exporter, near-full only** (see [[ceph-mgr-probe-cluster-facts]] for why the
dashboard and restful alternatives were set aside for the first cut).

- **Endpoint:** `http://rook-ceph-mgr.rook-ceph.svc:9283/metrics` (cross-namespace
  from `hippius-s3-staging`; egress is open). Plain HTTP, **no auth, no TLS**.
  Configurable via `CEPHOR_CEPH_MGR_METRICS_URL`.
- **Format:** Prometheus text exposition. The probe reads three signals:
  - `ceph_health_detail{name="OSD_FULL",...} v` — `v >= 1` ⇒ at least one OSD at the
    full ratio ⇒ **Critical** (Ceph physically blocks writes).
  - `ceph_health_detail{name="OSD_NEARFULL",...} v` / `name="OSD_BACKFILLFULL"` —
    `v >= 1` ⇒ **NearFull** (clamp the budget).
  - `ceph_cluster_total_bytes` / `ceph_cluster_total_used_bytes` — used-ratio as a
    corroborating numeric signal vs configured thresholds (the health-check series
    are emitted only while a check is active; absence = healthy, so the always-present
    ratio is the floor that makes "healthy" positively observable rather than inferred
    from absence).
  - **MDS load is not exported on this cluster** (`exclude_perf_counters=true`) and is
    deferred to a follow-up (enable perf counters, or add the dashboard path).
- **Thresholds** (`CephThresholds`, all tunable via env, defaults from the live
  cluster): nearfull `0.85`, full `0.95`. These mirror Ceph's own
  `nearfull_ratio`/`full_ratio`.

## 3. Data structures & ownership (Rust)

Pure logic in `hippius-drain-core` (behind a new `http` feature for the deps); the
HTTP shell in `hippius-drain-allocator` (behind `http`), so `--no-default-features`
still builds and the agent binary never links the probe.

Core (`hippius-drain-core`, `feature = "http"`):
- `struct CephReport { osd_full: bool, osd_nearfull: bool, used_ratio: Option<f64> }`
  — the parsed, flavor-agnostic signal set. Flavor-specific parsing
  (`parse_prometheus_metrics(&str) -> Result<CephReport, ProbeParseError>`) is a pure
  function over the exposition text.
- `struct CephThresholds { nearfull: f64, full: f64 }` (validated `0.0..=1.0`,
  `nearfull <= full`).
- `fn classify(report: &CephReport, ceiling_rate: ByteRate, thresholds: &CephThresholds)
  -> CephCeiling` — pure. full ⇒ `Critical`; nearfull (flag OR ratio≥nearfull) ⇒
  `NearFull(clamped)`; else `Open(ceiling_rate)`.
- `enum DecayState` + `fn decay(last_known: CephCeiling, consecutive_failures: u32)
  -> CephCeiling` — pure fail-safe: step `Open → NearFull → Critical` as failures
  accrue, clamping the carried rate down each step. Never returns a *fresh* `NearFull`
  from thin air on the first failure unless `last_known` already warranted it.
- `enum ProbeParseError` (thiserror, `#[non_exhaustive]`): `MissingMetric`,
  `MalformedValue` — reachable because the cluster's exporter omits absent series.

Allocator (`hippius-drain-allocator`, `feature = "http"`):
- `struct CephProbe { client: reqwest::Client, url: String, ceiling_rate: ByteRate,
  thresholds: CephThresholds, state: Mutex<ProbeState> }` where
  `ProbeState { last_known: CephCeiling, consecutive_failures: u32 }`.
- `impl CephCeilingSource for CephProbe`: snapshot `(last_known, failures)` under the
  lock and **drop the guard**, `await` the HTTP GET + parse (the lint
  `await_holding_lock` is denied, and the future must be `Send`, so no guard and no
  `RefCell` may cross the await), then re-lock to store the new state. On `Ok` →
  `classify` + reset failures; on `Err` → `decay(last_known, failures+1)` + bump.
- `fn probe(&self) -> Result<CephReport, ProbeError>` — the reqwest GET with
  `ClientBuilder::timeout`; `ProbeError` wraps `reqwest::Error` (`#[from]`) and
  `ProbeParseError` (`#[from]`).

## 4. Dependency justification

`reqwest` with `default-features = false` (no TLS backend — the endpoint is plain
HTTP, so we avoid pulling OpenSSL/rustls), behind the `http` feature. `wiremock`
(dev-only) for the probe's HTTP edge tests.

## 5. TDD order (tests first, real captured bytes as fixtures)

1. `parse_prometheus_metrics`: fixtures from the live `/metrics` capture — healthy
   (no OSD_NEARFULL series, ratio 0.276), synthesized nearfull, synthesized full,
   missing-cluster-bytes (⇒ `MissingMetric`), malformed value. `proptest`: parser
   never panics on arbitrary text.
2. `classify`: boundary table (below/at/above nearfull and full; flag-set vs
   ratio-only) + `proptest` monotonicity (more-full input ⇒ never a *looser* ceiling).
3. `decay`: `proptest` monotone non-increasing in `consecutive_failures`, converges to
   `Critical`, identity at `0` failures.
4. `CephProbe` via `wiremock`: 200-healthy ⇒ `Open`; 200-nearfull ⇒ `NearFull`;
   200-full ⇒ `Critical`; 503 / timeout / connection-refused ⇒ decayed last-known,
   never panic; consecutive failures deepen the decay; a recovery resets it.
5. Wire `main.rs`: select `CephProbe` when `CEPHOR_CEPH_MGR_METRICS_URL` is set and the
   `http` feature is on, else `StaticCeiling`. Config parsing tests via the existing
   fixture-map pattern.

## 6. Out of scope (follow-ups)

MDS-load clamp (needs perf counters or the dashboard API); the dashboard/restful
flavors; OTel metrics for the probe (Tier 1 #4 builds the allocator snapshot seam).
