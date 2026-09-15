# 19 — Deployment, Infra & Observability (greenfield Rust S3 service)

**Date:** 2026-09-15 · **Status:** first cut · **Owner:** OPS
**Reads on:** [`09-ops-deploy-observability.md`](./09-ops-deploy-observability.md) (the Python/drain ops template — mirror it), [`10-write-path-decision.md`](./10-write-path-decision.md) (SSD-staging topology, ack semantics), [`13-hcfs-as-is-integration.md`](./13-hcfs-as-is-integration.md) (hcfs client + admin bearer), [`14-build-plan.md`](./14-build-plan.md) (crate DAG, what we do NOT build), [`12-schema-design.md`](./12-schema-design.md) (`staged_blobs`, refcount).

## Scope & standing assumptions

This is the deploy/infra/observability design for the **greenfield Rust S3 service** — a single `axum` app split into a scalable **API tier** and a per-SSD-node **ingest DaemonSet** (SSD-staging → forwarder → HCFS, doc 10's DECIDED path), with its **own Postgres** and **Postgres-backed workers**. HCFS is a **separate service reached over HTTP** with an admin bearer (doc 13).

Standing facts that shape every section:

- **Deploys SEPARATELY from `hippius-s3` main/staging.** Its own namespace, its own image, its own Postgres cluster, its own Secret. It does **not** reuse `hippius-s3-secrets`, the `postgres-nvme` cluster, or the Python fleet's pods. This doc uses the placeholder namespace `hippius-s3r` (r = rust rewrite) / images `ghcr.io/thenervelab/hippius-s3r/*`; adjust to the chosen names.
- **No Redis.** HCFS owns Arion/S3 dual-write, retry, and usage→chain reporting (doc 14 "What this plan deliberately does not build"). The write queue, staged-blob state, and refcount are all **Postgres-backed** (`staged_blobs`, doc 12), drained with `SELECT ... FOR UPDATE SKIP LOCKED`. This is the single biggest topology divergence from the Python system (which runs 5 Redis instances, `k8s/base/redis-statefulsets.yaml`).
- **The drain crate is the Rust-service template** for image/config/health/OTel (doc 09 §6, `Dockerfile.drain`, `crates/hippius-drain-{agent,allocator}/`). Where a pattern is "same as the drain," this doc points at the file rather than re-deriving it.
- **We inherit HCFS's caps as hard constraints** (doc 13 §Caps): 16 MiB part cap, mandatory `Content-Length` on the file part, no suffix ranges, non-idempotent single `DELETE`.

---

## 1. Pod / topology inventory

One image, one `axum` binary; **role is env-selected** (`S3_ROLE=api|ingest|forwarder|refcount-gc|mpu-reaper|migrator`) exactly as the drain image carries two binaries chosen by k8s `command:` (`Dockerfile.drain:8`, doc 09 §2.1). All workloads land in the **`hippius-s3r`** namespace (separate from `hippius-s3-prod`).

| # | Workload | Kind | Replicas / placement | Role / entrypoint | Resources (req → lim) | Health probe |
|---|---|---|---|---|---|---|
| 1 | **s3-api** | Deployment | 3+ with **HPA** (CPU + custom); anti-affinity across nodes; **no SSD, not on ingest nodes** | `S3_ROLE=api` — reads (ranged GET/HEAD straight to HCFS), metadata ops (bucket/list/MPU-init), **WORM sync-before-ack writes** | 1000m/2Gi → 4000m/8Gi (match api-local) | `httpGet /health :8000` (startup/live/ready), mirror `k8s/production/api-local-deployments-production.yaml:184-211` |
| 2 | **s3-ingest** | DaemonSet | one per SSD node (`s3r-ingest=true` label + required hostname allow-list); `priorityClassName: s3r-ingest-priority`; `maxUnavailable:1` | `S3_ROLE=ingest` — fast-ack PUT lands ciphertext chunks + `meta.json` on node NVMe, **serve-pending-from-SSD** reads for not-yet-forwarded objects (node-sticky) | 1000m/2Gi → 4000m/8Gi | `httpGet /health :8000` (startup/live/ready); readiness ALSO fails on SSD unwritable / NearFull |
| 3 | **forwarder** | container in the s3-ingest pod (co-located, shares the hostPath NVMe) | one per SSD node | `S3_ROLE=forwarder` — polls `staged_blobs` (`land→forwarding→replicated`), POSTs chunks to HCFS w/ admin bearer + tenant ss58, marks replicated, cleans SSD | 100m/128Mi → 1000m/1Gi (match drain-agent) | **file-freshness exec** (`.alive`/`.ready`), mirror `drain-agent-daemonset.yaml:278-302` |
| 4 | **refcount-gc** | Deployment | 1+ (Postgres `SKIP LOCKED`, safe to scale) | `S3_ROLE=refcount-gc` — scans blobs at refcount 0, issues HCFS `DELETE` at zero, treats 404 as success (doc 13 §3) | 250m/512Mi → 1000m/2Gi | **file-freshness exec** (drain pattern) |
| 5 | **mpu-reaper** | Deployment | **1 (singleton, `Recreate`)** | `S3_ROLE=mpu-reaper` — abort abandoned multipart uploads, reap orphan staged chunks | 50m/128Mi → 500m/512Mi | file-freshness exec |
| 6 | **postgres** | CNPG `Cluster` | own cluster (3 inst) on dedicated PG nodes; **separate from `postgres-nvme`** | app schema owned by sqlx migrations | see §1.3 | CNPG-managed |
| 7 | **db-migrations** | Job | one-shot, `backoffLimit:3`, `ttl:7200` | `S3_ROLE=migrator` (or `sqlx migrate run`) — applies the schema Job-first (recommended, see Open Q3) | — | — |
| 8 | **otel-collector** | Deployment (or reuse cluster collector) | 1 | `otel/opentelemetry-collector-contrib` OTLP :4317/:4318 → Prometheus :8889 | 250m/512Mi → 1/2Gi | `httpGet / :13133` |
| 9 | **s3-migration** (Phase 4 only) | Job / Deployment | as needed | `S3_ROLE=... backfill` — read old-fmt → re-encrypt → new service, idempotent/resumable (doc 14 Phase 4) | — | — |

**How the tiers split traffic (recommended default; see Open Q1).** One S3 endpoint fronts both via the same Service/Ingress:
- **Writes to default (fast-ack) buckets** must land on an ingest node → route to **s3-ingest** with **`trafficDistribution: PreferSameNode`** (the api Service already uses this, `k8s/base/services.yaml`, doc 09 §2.5) so a node's clients stay on its ingest pod, giving **read-after-write during the pending window** without cross-node coordination.
- **Reads / metadata / WORM writes** go to **s3-api** (HPA-scaled, stateless — it only round-trips HCFS). A GET of a not-yet-forwarded object either lands on the owning ingest node (node-sticky) or is proxied there (serve-pending). Doc 10 explicitly scopes this to a *minimal* serve-pending path, **not** the Python peer/hydrate tier — so do NOT port `HIPPIUS_PEER_*` (`api-local...:144-166`).

**Ingest-node co-location (mirrors the Python triple).** The Python ingest node runs **api-local + drain-agent + arion-uploader-local** on the same NVMe (doc 09 §2.4). The Rust equivalent collapses to **s3-ingest (writer) + forwarder (drains SSD → HCFS)** in one DaemonSet pod sharing one hostPath. `NODE_NAME` must match on both containers (identity for node-scoped `staged_blobs` claims), the same lockstep discipline as `CEPHOR_NODE_ID == NODE_NAME` (`drain-agent-daemonset.yaml:127-133`, doc 09 §2.4 "a mismatch announces to a queue nothing drains").

### 1.1 How every workload reaches HCFS

All data-path pods (**s3-api**, **forwarder**, **refcount-gc**) speak plain **HTTP(S)** to the HCFS service: `POST /upload` (first field `account_ss58`), `GET /download/{ss58}/{file_id}` (ranged), `DELETE /delete/{ss58}/{file_id}` (doc 13 §"routes"). Auth is `Authorization: Bearer <admin>` on every call (`HCFS_ADMIN_BEARER_TOKEN`, doc 13 §"Auth"). HCFS runs as its **own separate service** (its own repo/deploy at `/Users/camden/Source/hcfs`) — it is *not* a pod in this inventory; we reach it by URL (`HCFS_BASE_URL`). Egress is **locked to the HCFS endpoint by NetworkPolicy** (§2.3).

### 1.2 What we deliberately do NOT deploy

Per doc 14: **no** Arion/S3 dual-write, Arion retry-worker, usage→chain reporter, Ceph pool, drain-allocator/AIMD budget, multi-tier read cache, janitor/hydrate, peer-serving, and **no Redis**. The Python inventory rows 3–16 (doc 09 §2.4) mostly vanish. The allocator (AIMD write-budget) has **no analog** — there is no shared Ceph pool to protect; SSD pressure is handled locally by a readiness gate + watermark shed (§5), not a fleet budget.

### 1.3 Postgres (own cluster)

Mirror the CNPG pattern in `k8s/production/postgres-nvme-cluster.yaml` but as a **distinct cluster** (`postgres-s3r`), on its own storage/nodes, so it is isolated from `hippius-s3` main/staging. Carry over the hard-won settings: `autovacuum_vacuum_cost_limit: 3000` and `autovacuum_vacuum_max_threshold: 100000` (the big-table vacuum fix, `postgres-nvme-cluster.yaml:46-53`), no memory limit (page-cache OOM lesson, `:87-97`), barman-cloud WAL archiving, `failoverDelay: 15`.

**Connection budget must be RE-DERIVED (doc 09 Open Q1):** the Python `×4 uvicorn workers` multiplier is gone — the Rust API is **one Tokio process** with a single `sqlx`/`deadpool` pool (doc 09 §6.1). Budget = (s3-api replicas × pool_max) + (ingest nodes × pool_max) + (forwarder + refcount-gc + mpu-reaper pools) against `max_connections` (Python runs 1000, `postgres-nvme-cluster.yaml:66`). Size `max_connections` to the actual pod count, not the Python assumption.

---

## 2. Config surface & secrets

### 2.1 Env-var surface

Follow the drain config-loader pattern verbatim (`crates/hippius-drain-agent/src/config.rs`, doc 09 §1.1/§6.5): `Config::from_env()` delegating to `from_lookup(|k| env::var(k).ok())` (testable), typed `#[non_exhaustive] thiserror ConfigError` (`Missing`/`Invalid`/`NonPositive`/`OutOfRange`), **present-but-unparsable = loud crash, missing = documented `DEFAULT_*` const, zero rates rejected**, strict-bool (`_parse_bool` semantics, doc 09 §1.1), fail-closed SS58 allowlist validated against network format **42** (doc 09 §1.1). New prefix: `S3R_*` for service-owned knobs; reuse `HIPPIUS_OVH_KMS_*`, `OTEL_*`, `ENABLE_MONITORING` verbatim so KMS certs and the collector wire up unchanged.

| Group | Var | Meaning | Notes |
|---|---|---|---|
| **DB (own)** | `S3R_DATABASE_URL` | this service's Postgres DSN | required; secret |
| | `S3R_DB_POOL_MIN/MAX_SIZE`, `_ACQUIRE_TIMEOUT`, `_COMMAND_TIMEOUT` | sqlx pool bounds | budget vs `max_connections` (§1.3) |
| **HCFS** | `HCFS_BASE_URL` | HCFS service base URL | required (e.g. `https://hcfs.internal/`) |
| | `HCFS_ADMIN_BEARER_TOKEN` | admin bearer for cross-tenant claim | **secret, blast-radius §2.3**; `repr=False` in logs |
| | `HCFS_VERIFY_SSL`, `HCFS_TIMEOUT_SECONDS`, `HCFS_MAX_RETRIES`, `HCFS_BACKOFF_*_MS` | client tuning | mirror `HIPPIUS_ARION_*` / uploader backoff (doc 09 §1.2) |
| **KMS / OVH** | `HIPPIUS_KMS_MODE` (`required`\|`disabled`), `HIPPIUS_OVH_KMS_ENDPOINT`/`_OKMS_ID`/`_DEFAULT_KEY_ID`, `_CERT_PATH`/`_KEY_PATH`/`_CA_PATH`, `_TIMEOUT_SECONDS`/`_MAX_RETRIES`, `KEK_CACHE_TTL_SECONDS`, `HIPPIUS_AUTH_ENCRYPTION_KEY` | envelope-crypto KEK (S3-owned, doc 01/14) | reuse names; `required` ⇒ all 5 OVH vars present or crash (doc 09 §1.1) |
| **ss58 / signing** | `S3R_TENANT_SS58_*` (tenant→ss58 model, Open Q4) | per-tenant ss58 passed as `account_ss58` to HCFS (doc 13 §4) | fail-closed fmt-42 validation |
| | `API_SIGNING_KEY` | presigned-URL signing | random UUID if unset (doc 09 §1.2) |
| | (SigV4 access-key/secret credential store) | S3 auth (doc 14 Phase 1) | in DB or secret per cred model |
| **SSD / ingest** | `S3R_SSD_ROOT` | node-local staging dir | e.g. `/var/lib/hippius/local_object_cache` |
| | `S3R_SSD_NEARFULL_PERMILLE`, `_CRITICAL_PERMILLE` | readiness/shed watermarks (§5) | local analog of the Ceph pool gate |
| | `NODE_NAME`, `POD_IP`, `POD_NAME` | node/pod identity (downward API) | ingest + forwarder; identity for `staged_blobs` claims |
| | `S3R_ACK_POLICY_*` | per-bucket fast-ack vs sync-before-ack (WORM) | doc 10 ack semantics |
| **Role / health** | `S3_ROLE` | selects the binary's role (§1) | required |
| | `S3R_LIVENESS_FILE`, `S3R_READINESS_FILE` | file-freshness probe paths (workers) | mirror `CEPHOR_LIVENESS_FILE`/`_READINESS_FILE` |
| **Observability** | `ENABLE_MONITORING`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `RUST_LOG`, `SENTRY_DSN`/`SENTRY_TRACES_SAMPLE_RATE` | OTel + logs + Sentry | reuse verbatim (doc 09 §3); `service.instance.id` load-bearing (§3) |

**Config injection** follows Python's layering (doc 09 §1): a `configmap-defaults` (all `DEFAULT_*`) + `configmap-environment` + the `s3r-secrets` Secret, `envFrom` on the pod (as `api-local...:111-117`). Two constants stay **pinned in code**, not env: the HCFS backend contract and `HIPPIUS_SS58_FORMAT = 42` (doc 09 §1.1 "two secrets pinned in code").

### 2.2 Secrets inventory

Own Secret **`s3r-secrets`** (separate from `hippius-s3-secrets`), recreated **wholesale in the deploy workflow's "Update secrets" step** as `kubectl create secret ... --from-literal=... | kubectl apply -f -` (the pattern in `production-deploy.yaml:216-254`). Contents: `S3R_DATABASE_URL`, `HCFS_ADMIN_BEARER_TOKEN`, `API_SIGNING_KEY`, `HIPPIUS_OVH_KMS_*` ids, SigV4 credential material. KMS mTLS client cert/key live in a separate `ovh-kms-certs` Secret mounted `defaultMode: 0400` (as `api-local...:237-240`).

**Durability rule (doc 09 §5.4, load-bearing):** any secret/config change must be made **both** in the deploy-workflow literal (durable) **and** applied live (immediate), or a `kubectl patch` evaporates on the next deploy. New env vars for this service must be added to that step.

### 2.3 Admin-bearer blast radius (⚑ the top infra risk)

`HCFS_ADMIN_BEARER_TOKEN` **bypasses SS58 matching on every hcfs endpoint, not just the gateway** — it can read/write/delete *any* account (`gates.rs:290-296, :684-687, :851`; doc 13 §"Auth" + Open Q1). Every data-path pod (s3-api, forwarder, refcount-gc) holds it. Containment:

1. **NetworkPolicy egress allowlist** — restrict these pods' egress to the HCFS endpoint (and Postgres, OTLP, KMS) only, so a leaked token can't be exfiltrated to arbitrary hosts. Base off `k8s/base/networkpolicy.yaml` but **tighten the wide-open `egress: ipBlock 0.0.0.0/0`** (`networkpolicy.yaml:48-53`) to the HCFS service CIDR/host. This is a deliberate divergence from the Python policy, which is permissive.
2. **Secret only in data-path pods** — do not mount the bearer into mpu-reaper/migrator or any pod that never calls HCFS.
3. **Ingress lock** — S3 public entry scoped to an edge label at the CNI layer (the `allow-public-edge` / `hippius.io/edge` pattern, `networkpolicy.yaml:59-76`), so a pod mid-roll is unreachable from outside even if a Service selects it.
4. **Ask hcfs for a scoped service bearer** — the durable fix is a bearer that can act only for the S3 service's tenant set, not a god token (doc 13 Open Q1). That is an hcfs change; until then, admin + egress-lock + rotation is the posture. Rotation: rotate via the deploy-workflow literal + live apply.

---

## 3. Observability

**Telemetry-compatible with the existing collector** (doc 09 §3): everything is **pushed** over OTLP; the app exposes **no `/metrics`** endpoint — the otel-collector's Prometheus exporter (`:8889`) is the scrape surface (`servicemonitor.yaml`).

### 3.1 OTLP wiring (mirror the drain exactly, doc 09 §3.1/§6.2)

- Crates `=`-pinned: `opentelemetry = "0.31"` (metrics), `opentelemetry_sdk` (metrics, rt-tokio), `opentelemetry-otlp` (**grpc-tonic**) — OTLP/gRPC via tonic to `:4317`, NOT http-proto. Setup mirrors `crates/hippius-drain-agent/src/metrics.rs`.
- **`PeriodicReader` at 10 s** (`EXPORT_INTERVAL`, matches Python's `export_interval_millis=10000`).
- **`service.instance.id` unique per process** (`pod:pid`) — load-bearing; without it multi-instance counters collide in the collector (doc 09 §3.1 "260k resets/h"). Set via `OTEL_RESOURCE_ATTRIBUTES=...,service.instance.id=$(POD_NAME)` exactly as `drain-agent-daemonset.yaml:262-263` / `api-local...:138-139`.
- Gated on `ENABLE_MONITORING ∈ {true,1,yes}`; exporter-build failure logged non-fatal.
- **API tier ALSO exports traces + request-path histograms** (the drain is metrics-only) with the **exact bucket boundaries** from doc 09 §3.1 (`http_request_duration_seconds`, `http_request_ttfb_seconds`, `http_pre_handler_duration_seconds`). Sentry via `sentry` + `sentry-tracing`, same DSN/env/sample-rate contract (doc 09 §3.3).

### 3.2 Metric contract

**Cardinality rule (must hold, doc 09 §3.2):** every attribute becomes a Prometheus label — keep bounded, **never** account ids / bucket names / object keys / ss58 / error strings as labels; those go on **trace span attrs** (`aws.s3.bucket`, `aws.s3.key`, `hippius.account.main`). Aggregate any per-node gauge with `max()` not `sum()` (doc 09 §3.2 alert invariant); `noDataState: OK` on every rule.

Reuse the existing HTTP/S3 contract names so dashboards work unchanged:
- **HTTP:** `http_requests_total{method,handler,status_code}` (`handler` falls back to `"unknown"`, never the path), `http_request_duration_seconds`, `http_request_ttfb_seconds`, `http_request_bytes_total{direction="in"}`, `http_response_bytes_total{direction="out"}`.
- **S3:** `s3_operations_total{operation,success}`, `s3_errors_total{error_type,operation}`, `s3_bytes_uploaded_total{operation}`, `s3_bytes_downloaded_total{operation}`.

New service-specific series (closed-set labels only):
- **Ingest / SSD staging:** `s3r_ingest_land_total{outcome}`, `s3r_ingest_ssd_used_bytes` / `_free_bytes` (gauge), `s3r_ingest_pressure_mode{mode=ok|nearfull|critical}`, `s3r_ingest_staged_bytes` (backlog gauge), `s3r_ingest_serve_pending_total{outcome}`, `s3r_reads_by_tier_total{tier=ssd_pending|hcfs}`.
- **Forwarder → HCFS:** `s3r_forward_total{outcome=ok|retry|failed}`, `s3r_forward_duration_seconds`, `s3r_forward_inflight` (gauge), `s3r_forward_pending_oldest_age_seconds` (gauge — **the durability-window SLI**, doc 10), `s3r_staged_blobs_by_state{state=landed|forwarding|replicated}` (gauge), `s3r_hcfs_request_duration_seconds{op=put|get|delete}`, `s3r_hcfs_errors_total{op,status_class}`.
- **Refcount GC:** `s3r_refcount_gc_scanned_total`, `s3r_refcount_gc_deleted_total{outcome}`, `s3r_refcount_gc_hcfs_delete_total{outcome=ok|404_treated_ok|error}`, `s3r_blob_refcount_zero_pending` (gauge), `s3r_refcount_gc_errors_total`.
- **Crypto / integrity:** `s3r_chunk_aead_failures_total{outcome}` (key-committing AEAD verify failures, doc 14 crypto mandate), `s3r_kms_requests_total{outcome}`, `s3r_kek_cache_hits_total`/`_misses_total`.
- **Postgres queue health:** `s3r_staged_queue_depth` (gauge), `db_pool_size`/`_free_connections`/`_used_connections` (reuse Python names).

### 3.3 Health probes

- **s3-api + ingest-api container:** serve `GET /health → {"status":"healthy"}` (200), **bypassing all middleware** (Python precedent, doc 09 §3.4/§6.4). Probes are `httpGet /health :8000` startup/liveness/readiness, copied from `api-local...:184-211` (short `initialDelay` — DaemonSet rolls serially, every second is multiplied by node count, `:180-183`; `failureThreshold × periodSeconds` grants the startup budget). **Ingest readiness additionally fails when the SSD is unwritable or past `S3R_SSD_CRITICAL_PERMILLE`** so a full-disk node sheds writes instead of accepting un-stageable PUTs (§5).
- **forwarder / refcount-gc / mpu-reaper (no HTTP):** **file-freshness exec probes**, the drain pattern verbatim — touch `.alive` each tick (liveness restarts a *wedged* runtime, `drain-agent-daemonset.yaml:278-287`), touch `.ready` only while *progressing* so a `ReadinessTracker` trips NotReady on a stalled-but-alive loop without a crash-restart (`:293-302`, doc 09 §6.4).
- **Graceful shutdown / uvicorn-parity (doc 09 §2.2/§6.3):** `tini` PID 1 (from `Dockerfile.drain`); `CancellationToken` root + `JoinSet`, `select!` over SIGINT/SIGTERM; grace **< `terminationGracePeriodSeconds − preStop sleep`** (the api-local `25 < 45 − 10` rule, `api-local...:59-62,201-204`). Keep-alive/idle timeout **≥ the edge (ATS/HAProxy) keepalive (75 s)** or the server closes a pooled socket first and turns an idle conn into a failed request (doc 09 §6.1 — "the single most important uvicorn-parity knob").

### 3.4 Dashboards & alerts

Prod alert rules live in the **external `thenervelab/hippius-otel` repo** (doc 09 §3.2); this repo's `monitoring/grafana/provisioning/alerting/` is local-dev only. New rules to add there for this service (all `noDataState: OK`):
- **`S3rForwardBacklogAge`** (critical) — `max(s3r_forward_pending_oldest_age_seconds) > threshold`: the durability-window SLO; a fast-acked object un-forwarded too long is at data-loss risk (doc 10).
- **`S3rSsdNearFull` / `S3rSsdCritical`** — SSD pressure; Critical means writes are shedding.
- **`S3rHcfsErrorRate`** — `rate(s3r_hcfs_errors_total)` elevated: HCFS unhealthy; fast-ack still lands but backlog grows.
- **`S3rForwarderMissing`** (mirror `S3DrainAgentMissing`) — a node has no forwarder → its SSD never drains.
- **`S3rRefcountGcStalled`** — `s3r_blob_refcount_zero_pending` climbing with no `_deleted_total` progress: blob leak in HCFS/Arion.
- **`S3rChunkAeadFailures`** — nonzero `s3r_chunk_aead_failures_total`: key-commitment/integrity breach.
- **`S3rStagedBlobsStuck`** — `staged_blobs` wedged in `forwarding`.

---

## 4. Build & CI

### 4.1 Image (mirror `Dockerfile.drain`, 2-stage)

```dockerfile
FROM rust:1.95.0-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY rust-toolchain.toml ./            # pin toolchain before any cargo
COPY Cargo.toml Cargo.lock ./ ; COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p s3r-api \
    && cp target/release/s3r-api /usr/local/bin/
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/s3r-api /usr/local/bin/
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["/usr/local/bin/s3r-api"]        # role selected by S3_ROLE env / k8s command:
```

Same shape as `Dockerfile.drain`: BuildKit cache mounts, `--locked`, `tini` PID 1, `ca-certificates`, one image → role-selected. **One divergence from the drain:** the S3 service **needs TLS to HCFS** (HTTPS admin bearer) whereas the drain is TLS-less (`Dockerfile.drain:15-16`). Use **`rustls`** (pure-Rust) so the runtime image stays free of `libssl-dev` — don't reach for openssl unless a dep forces it. Sizing note: unlike the drain (which unlinks api-written files as root, `Dockerfile.drain:49-51`), the forwarder and ingest share one hostPath written by one uid, so run non-root with a matching `fsGroup`.

### 4.2 Toolchain / lint / deny pins (the template, doc 09 §4.2)

`rust-toolchain.toml` (`channel="1.95.0"`, minimal profile), `rustfmt.toml` (`max_width=150`, `edition="2024"`), `deny.toml` (advisories `yanked=deny`; license allowlist MIT/Apache-2.0/BSD/ISC/Unicode-3.0/Zlib; `bans: wildcards=deny`; `sources: unknown-registry/git=deny` — supply-chain gating is **cargo-deny**, not cargo-audit). Workspace lints **forbid `unsafe_code`** and **deny `unwrap_used`/`panic`/`todo`/`print_std*`/`await_holding_lock`/`exit`** — which is why the shutdown path returns `ExitCode` rather than `std::process::exit` (doc 09 §6.3).

### 4.3 CI jobs (mirror doc 09 §4.2)

- **`rust`** — services `postgres:17`; env `S3R_DATABASE_URL`; toolchain `dtolnay/rust-toolchain@master` pinned `1.95.0` (+clippy+rustfmt), `Swatinem/rust-cache@v2`:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace --all-features --locked -- --include-ignored` (`#[sqlx::test]` auto-creates per-test DBs)
- **`rust-deny`** — `cargo deny check`.
- Deploy workflows add **`rust-gate`** (fmt+clippy+deny, **blocks the image build**), **`build-image`** (`docker/build-push-action@v5`, `type=sha,format=short`+`latest`, `cache-from/to: type=gha`), and **`deploy-*`** (`kustomize edit set image .../s3r:${SHORT_SHA}`, `kubectl apply -k`, then rollout gating). Serialize staging vs prod with `concurrency.group` (`production-deploy.yaml:196`). **Deploy ORDER:** db-migrations Job → forwarder/refcount-gc → s3-ingest DaemonSet (≥1 Ready) → s3-api — the allocator-first discipline of `production-deploy.yaml:415-428` adapted (schema before consumers).

### 4.4 E2E harness — dockerized S3 conformance against a local HCFS

The acceptance oracle is a **client-driven S3 conformance suite** (doc 06 checklist; doc 14 Phase 0). Model the compose stack on `docker-compose.e2e.yml` (doc 09 §4.3) but **swap the Python/Arion/drain stack for a real HCFS**:

- **hcfs-server** built from `/Users/camden/Source/hcfs` + **its own postgres** + **minio** (S3 backend, `HCFS_STORAGE_BACKEND=s3` or `both`) + mock chain — this is exactly the `e2e-local` recipe HCFS already runs (postgres + minio + branch server, per HCFS CLAUDE.md "CI" §). HCFS is configured with a known `HCFS_ADMIN_BEARER_TOKEN` the S3 service holds.
- **s3-ingest + forwarder + refcount-gc + own postgres**, the Rust service under test, writing to a container-local SSD dir.
- **toxiproxy on the HCFS hop** (as e2e does for the Arion hop, doc 09 §4.3) to fault-inject: kill HCFS mid-run and assert the **write-availability decoupling** property (fast-ack PUT still returns 200 on SSD, and drains to HCFS on recovery — the whole reason SSD-staging was chosen, doc 10); assert forwarder retry/backoff and idempotent re-POST (HCFS content-dedup, doc 13 §"idempotent").
- Run an **AWS-SDK / `s3-tests`-style suite** as the conformance run, plus the durability/serve-pending assertions above.
- **`tests/smoke/`** (SigV4, endpoint-driven via `HIPPIUS_ENDPOINT`) is the **language-agnostic black-box harness** reusable against staging/prod (doc 09 §4.3 "the correct black-box conformance harness for the Rust rewrite") — chain a `smoke-staging` job on the deploy. `tests/e2e/` (hardcoded localhost, direct-DB asserts) is NOT portable.

---

## 5. SSD provisioning

**What the ingest DaemonSet needs from a node** (mirror `api-local...:219-227` + `drain-agent-daemonset.yaml:303-312`):

- **A dedicated local NVMe** mounted **out-of-band** (node prep, NOT CI) at a hostPath, e.g. `/s3r-data`, exposed to the pod at `S3R_SSD_ROOT` (`/var/lib/hippius/local_object_cache`).
- **`hostPath type: Directory` (NOT `DirectoryOrCreate`)** — the deliberate safety latch: if the NVMe isn't mounted the pod stays `ContainerCreating` instead of silently writing ingest churn onto the node root disk and filling it (`api-local...:222-227`, `drain-agent-daemonset.yaml:306-311`). Staging may use `DirectoryOrCreate` on a node-root path if it shares nodes.
- **Node label `s3r-ingest=true` + required `nodeAffinity` hostname allow-list** (belt-and-suspenders: a stray/mislabeled node can't place ingest on a Postgres/other node, `api-local...:66-88`). One label list, referenced by both the DaemonSet and its co-located forwarder, kept in lockstep.
- **`priorityClassName: s3r-ingest-priority`** (below `system-*-critical`, never preempts system, `s3-ingest-priorityclass.yaml`).
- **A root `initContainer` that `mkdir -p && chmod 0777`** the mount so the non-root binary can write (`api-local...:96-104`).

**Sizing the staging reservoir.** Because the read cache is gone (doc 10: SSD is **write-staging only**, no residency/hydrate retention), the reservoir is **far smaller than Python's ~930 GB/node replicated shard** (`drain-agent-daemonset.yaml:159`). Size it to absorb the *un-forwarded backlog*:

```
reservoir ≈ peak_ingest_rate_per_node × max_forward_lag_window
          + steady_state_in_flight (forward concurrency × part size)
          + serve-pending hot set (objects read before forward completes)
          + headroom for the NearFull/Critical shed watermarks
```

`max_forward_lag_window` = the longest HCFS outage/slowdown you want a node to ride through without failing PUTs (the doc 10 burst-absorption guarantee).

**Quantified from prod telemetry (2026-09-15).** Measured on the live 5-node ingest tier (Loki + Prometheus; see the number's provenance in the register resolution log):

| Input | Peak (size against this) | Sustained (5-min) |
|---|---|---|
| Per-node PUT ingest **bytes** | **~160–190 MB/s/node** (burst) | ~72 MB/s/node |
| Per-node PUT **ops** | ~4.3–4.8 PUT/s/node | — |
| HCFS store latency (one ~4 MiB blob POST) | p50 **630 ms** · p95 **1.37 s** · p99 **2.36 s** · max ~6.3 s | — |

Burst is **~2.3× sustained** — size the reservoir against the burst. So:

```
reservoir/node ≈ 190 MB/s × max_forward_lag_window  +  ~120 MB in-flight  +  serve-pending hot set  +  ~20% headroom
                 └─ 60 s → ~11 GB · 5 min → ~57 GB · 15 min → ~171 GB
```

**Recommendation:** a **~64 GB/node** reservoir rides out a ~5-minute HCFS stall at peak burst (57 GB + in-flight + headroom); **~96–128 GB/node** buys ~10–15 min of margin. Either is **≪ Python's ~930 GB/node** (7–14× smaller), because this is write-staging only. Pick `max_forward_lag_window` = your HCFS-blip SLO (5 min is a sane default given HCFS's own retry/dual-write underneath).

**Forwarder concurrency (derived).** To *drain* 190 MB/s/node at p95 1.37 s per 4 MiB POST, one connection moves ~2.9 MB/s → need **~65 concurrent POSTs/node at peak** (~25 to hold the 72 MB/s sustained). Set the forwarder's per-node concurrency knob in that ~32–64 range; below it the backlog grows and the reservoir fills. *(Caveat: the latency figure is the current `arion-uploader`'s wall-clock — FS read + POST + a `chunk_backend` insert + any concurrency-semaphore wait — so it's an upper bound on the pure HCFS POST; conservative for sizing. "PUT" here conflates `put_object` + `upload_part`, and bytes are dominated by large multipart parts (~29 MB/PUT), but bytes/s/node is what sizes the disk and that's measured directly.)*

**Durability SLO threshold (derived).** If the forwarder keeps up, oldest-un-drained-blob age tracks the POST latency (sub-second to ~2.4 s at p99). So `S3rForwardBacklogAge` should alert well above p99 — e.g. **warn at >30 s, page at >120 s** (the forwarder is falling behind or HCFS is stalled), not at single-digit seconds.

Enforce **local watermarks** analogous to the Ceph pool gate (`drain-allocator-deployment.yaml:129-150`), but SSD-local and simple: `S3R_SSD_NEARFULL_PERMILLE` warns + prioritizes forwarding, `S3R_SSD_CRITICAL_PERMILLE` **fails ingest readiness** so the node sheds writes (to the sync-to-HCFS API path or 503) rather than filling the disk. There is no fleet AIMD budget — pressure is handled per-node.

---

## ⚑ Flags & gotchas

1. **⚑ Admin-bearer blast radius** — the token reads/writes ANY hcfs account (doc 13 Open Q1). Egress-lock to HCFS, mount only in data-path pods, pursue a scoped bearer (§2.3). Highest-severity item here.
2. **⚑ Fast-ack durability window** — a `200` on a default bucket means "on one node's SSD," not durable in HCFS/Arion (doc 10). Node/disk loss before forward = data loss. Alert on `s3r_forward_pending_oldest_age_seconds`; use **sync-before-ack for WORM/sn85 buckets**.
3. **⚑ `hostPath type: Directory`** — never `DirectoryOrCreate` on the dedicated NVMe path, or a missing mount silently fills the node root (§5).
4. **⚑ Secret durability** — every secret/config change goes in the **deploy-workflow literal AND live**, or a live patch evaporates on next deploy (doc 09 §5.4).
5. **⚑ `service.instance.id` is load-bearing** — unique per process (`pod:pid`) or multi-instance counters collide in the collector (doc 09 §3.1).
6. **⚑ Postgres connection budget** — the Python `×4 uvicorn` multiplier is gone (single Tokio process); re-derive against `max_connections` before scaling (§1.3, doc 09 Open Q1).
7. **⚑ Node-sticky routing** — read-after-write during the pending window depends on `PreferSameNode` + serve-pending-from-SSD; a cross-node GET of an un-forwarded object 503s / must proxy (doc 10). Do NOT port the Python peer tier.
8. **⚑ HCFS caps** — 16 MiB part cap, **mandatory `Content-Length`** on the file part (else 400 `size_mismatch`), **no suffix ranges** (`bytes=-N` silently 200s), **non-idempotent single DELETE** (treat 404 as success) — all in the forwarder + read path (doc 13 §Caps/§3).
9. **⚑ Keep-alive ≥ edge timeout (75 s)** — else the server closes pooled sockets first → failed idle requests (doc 09 §6.1).
10. **⚑ No Redis** — the write queue / staged-blob state / refcount are Postgres (`SKIP LOCKED`), a deliberate divergence from the Python 5-Redis topology; don't reintroduce a Redis dependency by habit.
11. **⚑ Separate everything** — namespace, image, Postgres, Secret are all distinct from `hippius-s3` main/staging; never point at `hippius-s3-secrets` or `postgres-nvme`.

---

## Open questions

1. **Endpoint split between s3-api (Deployment) and s3-ingest (DaemonSet).** Recommended: one Service, `PreferSameNode`, writes→ingest / reads+metadata→api. But whether a single Service fronts both, or the ingress path-routes, or the ingest pod is the sole front door (Python's cutover model where api-local became the front door, doc 09 §2.5) needs a decision. Affects §1 routing and the NetworkPolicy.
2. **Is the forwarder a sidecar in the ingest pod or its own DaemonSet?** This doc assumes co-located sidecar (one hostPath, one `NODE_NAME`). A separate DaemonSet is cleaner for independent rollout but duplicates the node/label plumbing (the Python system ran them separate — api-local + drain-agent, doc 09 §2.4).
3. **Migrations ownership.** Job-first (recommended, `k8s/base/migration-job.yaml` precedent) vs the API running `sqlx migrate` at boot. Doc 09 Open Q3 recommends a Job stays authoritative and the app *gates*, not migrates. Decide before Phase 1.
4. **Tenant → ss58 model.** Per-tenant billing attribution requires passing each tenant's real ss58 as `account_ss58` and those tenants being **non-exempt** (⇒ also quota-gated) — the exempt/reported coupling (doc 13 §4/§5, Open Q2/Q4). Does the S3 service own its own credit gate + reporting, or inherit HCFS's? Drives whether we deploy any billing worker at all.
5. **Reservoir sizing is not quantitative yet** — needs measured HCFS PUT latency + peak per-node ingest to fix `max_forward_lag_window` and the NVMe size (§5; doc 10 deciding question).
6. **HPA signal for a single-Tokio-process API.** CPU alone under-reflects an IO-bound reverse-proxy-to-HCFS workload; a custom metric (in-flight requests, or `s3r_forward_inflight`/RPS via the collector→Prometheus adapter) is likely needed. Not derivable from the Python `UVICORN_WORKERS` model.
7. **Scoped hcfs service bearer vs admin** (doc 13 Open Q1) — an hcfs change; until it exists, admin + egress-lock is the posture. Track as an hcfs prerequisite.
8. **Chain-reporter fan-out budget** — if we rely on HCFS's `hcfs-chain-reporter` for usage→chain, the S3 service's tenant/blob churn shares the ≤250 updates/tick single-signer budget with native Drive (doc 13 Open Q3). Confirm it fits before go-live.
9. **Own otel-collector vs shared** — deploy a dedicated collector in `hippius-s3r` or point at the cluster collector? Affects the NetworkPolicy `:4317` egress and ServiceMonitor scope.
