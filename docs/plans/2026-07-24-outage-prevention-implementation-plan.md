# Outage-Prevention Implementation Plan (post 2026-07-24 5xx incident)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the 2026-07-24 outage class (silent drain/upload-pipeline stall → cross-node GET 503s → HAProxy outage alerts) impossible to sustain undetected, without changing any existing behavior by default.

**Architecture:** Three layers, in strict order of value-per-risk: (1) *Detect* — alerts on metrics that already exist plus three small additive metrics; (2) *Stall-proof* — progress-based liveness, I/O timeouts, and Redis client hardening so a wedged worker self-heals; (3) *Blast-radius* — reduce the read path's hard dependency on replication for fresh objects (design-gated, flag-gated). Every behavior change ships behind a flag that defaults to today's behavior; alerts ship in observe-only mode first.

**Tech Stack:** Python 3.10+ (hippius_s3, workers), Rust (crates/hippius-drain-*), Kustomize k8s manifests, Grafana alerting provisioned from the sibling `thenervelab/hippius-otel` repo, OTel → otel-collector:4317 → Prometheus (collector prometheus exporter :8889).

---

## Incident summary (verified, for context)

- 07:33 UTC first GET 503s (`SlowDown`, raised at [hippius_s3/services/object_reader.py:417](../../hippius_s3/services/object_reader.py) after the 25s first-chunk peek, `HIPPIUS_STREAM_FIRST_CHUNK_TIMEOUT_SECONDS`).
- ~07:40–09:00 UTC the drain→upload chain produced ~99.9% less (`chunk_backend` rows/10min: ~4,000 → 9) with **zero** abnormal logs. The 09:00 deploy restarted the workers and recovery began.
- Residue: 14,626 `pending` rows in `cephor_replication_status` (oldest 2 days), 975 terminal `failed` rows, `janitor_underreplicated_live_chunks` pegged at its 500 cap.
- Cascade: harbor-registry PVC = JuiceFS backed by s3.hippius.com; `us_ats_prod` fronts s3 — three HAProxy alerts, one incident.

## Hard constraints (apply to every task)

1. **No breaking changes.** New behavior is env-flag-gated, default = current behavior. New alerts start in the `S3 Alerts` folder as non-paging (Mattermost only) for ≥1 week of observation before severity is raised. Schema changes are additive only. Probe changes ship in two stages (code first, probe switch after soak).
2. **No assumptions.** Every task starts by re-verifying its cited facts on the current branch (files move). Tasks touching sibling repos (`thenervelab/s3-backup`, `thenervelab/hippius-otel`) begin with a clone-and-verify step.
3. Staging first, always: `staging` branch → staging deploy → soak → `k8s-production` per [.github/workflows](../../.github/workflows).
4. Repo conventions: ruff + mypy strict, line length 120, no defensive try/except, tests behavior-not-implementation ([CLAUDE.md §9.3](../../CLAUDE.md)).
5. **Single source of truth for alerting: `thenervelab/hippius-otel`.** No alert rules, contact points, or notification policies may be defined in this repo, in any other repo, or click-ops'd in a Grafana UI. Every alert this plan creates lands as provisioned code in `hippius-otel`; Task 1.0 migrates the strays that already exist. If a rule needs a capability `hippius-otel` provisioning doesn't support yet (e.g. Loki-datasource rules), extend `hippius-otel` — do not route around it.

## Known cross-repo dependencies

| Repo | What lives there | Tasks affected |
|---|---|---|
| `thenervelab/hippius-otel` | **The single source of truth for ALL alerting** (rules at `alerting/rules/`, contact points, policies, `sync-alerting.sh` — per [docs/grafana-alerting.md](../grafana-alerting.md)) | 1.0, 1.1–1.3, 1.4 (rule), 1.5 (rule), 1.6 (rule), 1.7, 3.4 |
| `thenervelab/s3-backup` | backup / hydrator / cleanup workers ("Missing backup chunk" originates there; confirmed NOT in this repo — see [hippius_s3/workers/errors.py:414](../../hippius_s3/workers/errors.py)) | 1.7, 2.6 |

**Known alerting strays to consolidate (verified):** (a) `monitoring/grafana/provisioning/alerting/{alert-rules.yml, contact-points.yml, notification-policies.yml, discord-template.yml}` in THIS repo — 16 rules in folder "Hippius S3 Alerts", mounted only by [docker-compose.monitoring.yml:107](../../docker-compose.monitoring.yml), a drift-prone copy of what `hippius-otel` owns; (b) the prod Grafana "HAProxy Alerts" folder (the alert that fired on 2026-07-24, rule uid `hapx5xxout07`) — its provenance (provisioned from `hippius-otel` vs UI-managed) is **unverified** and is inventoried in Task 1.0.

> Note: the hippius-mem team-memory server was unreachable during planning (writes/recalls timed out). The topology/gotcha notes from this incident still need to be persisted once the server is back — see "Post-plan follow-ups" at the end.

---

# Phase 0 — Operational repair (runbook, no code, operator-driven)

These are one-time operator actions. They are prerequisites for the alert thresholds in Phase 1 making sense (alerts tuned against a backlogged system will be wrong).

### Task 0.1: Diagnose the 14.6K `pending` backlog before re-driving anything

**Decision:** Diagnose first; do NOT blanket-reset. Rationale: `claim_part` is node-scoped ([crates/hippius-drain-core/src/store.rs:496-517](../../crates/hippius-drain-core/src/store.rs)) and the reconciler is SSD-scan-driven — a `pending` row whose SSD file no longer exists on its `node_id` can never drain and will churn forever. Blindly flipping rows to `pending` (they already are) or clearing `node_id` would violate the drain design.
**Alternatives rejected:** bulk `UPDATE ... SET status='pending'` (rows already pending; no-op); deleting rows (destroys durability accounting).

**Step 1 (read-only):** classify the backlog:
```sql
-- age × node distribution
SELECT node_id, status, count(*),
       min(landed_at) AS oldest, max(landed_at) AS newest
FROM cephor_replication_status
WHERE status IN ('pending','draining','failed')
GROUP BY node_id, status ORDER BY node_id, status;

-- are the pending rows' versions even servable? (aborted PUTs are expected noise)
SELECT (ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != '')) AS servable, count(*)
FROM cephor_replication_status crs
JOIN object_versions ov ON ov.object_id::text = crs.object_id AND ov.object_version = crs.version
WHERE crs.status = 'pending'
GROUP BY 1;
```
**Step 2:** for a sample of old `pending` rows, check on the named node whether `$CEPHOR_SSD_ROOT/<object_id>/v<version>/part_<n>/meta.json` exists (`kubectl exec` into that node's api-local pod, path is `/var/lib/hippius/local_object_cache/...`).
**Step 3:** disposition:
- SSD data present → the agent will drain it; investigate why it hasn't (allocator budget? `deferred_until`? agent logs on that node).
- SSD data gone + version not servable → these are aborted-PUT orphans; the existing MPU-cleanup path ([hippius_s3/services/mpu_cleanup.py:65](../../hippius_s3/services/mpu_cleanup.py) → `fail_replication_status_for_version.sql`) is the sanctioned way to mark them failed. If the cleanup isn't catching them, that's a bug to file — do not hand-edit rows without recording the query used.
- SSD data gone + version IS servable → **data-loss candidates**; escalate before touching anything.
**Step 4:** record findings + queries in `docs/audits/2026-07-24-drain-backlog-triage.md`.

### Task 0.2: Triage the 975 `failed` rows — do NOT blanket-retry

**Decision:** `failed` means "persistent chunk byte-mismatch AND version not servable" ([crates/hippius-drain-core/src/partdrain.rs:499-503](../../crates/hippius-drain-core/src/partdrain.rs)) — mostly aborted PUTs, correctly terminal. Verify that classification holds for the current 975 (join against `object_versions` servability as in Task 0.1); only escalate rows where the version IS servable.
**Alternatives rejected:** a retry loop for `failed` (would churn on genuinely-dead rows; `corrupt` already covers the retryable mismatch class via `redrive_corrupt_parts`, [store.rs:368-381](../../crates/hippius-drain-core/src/store.rs)).

### Task 0.3: Unwedge Ceph (separate change window, cluster-admin)

**Decision:** raise backfill-full temporarily so the 5 `backfill_toofull` PGs complete and the upmap balancer resumes; then capacity. Verified state: osd.4 88.1%, `ceph-filesystem-data0` 89.7%/2 TiB headroom, balancer refusing (misplaced 5.2% > 5%).
```bash
ceph osd set-backfillfull-ratio 0.92        # temporary; revert to 0.90 after backfill completes
ceph -s                                     # watch: backfill_toofull count → 0, misplaced % falling
# after misplaced < 5% the balancer auto-resumes; verify: ceph balancer status
```
**Guard:** never raise `full_ratio`; only `backfillfull`. Revert after convergence. Then: capacity plan for the pool (<80% target) and mon disk (22–24% free) — tracked as an infra ticket, out of scope here.
**Alternatives rejected:** `ceph osd reweight osd.4` (moves the problem, fights upmap balancer); deleting cache data (janitor already GCs replicated parts; manual deletion risks the replication gate).

### Task 0.4: Triage `unpin_requests:dlq` (1,519 entries)

Use the existing runbook script (read first, dry-run): [hippius_s3/scripts/dlq_requeue.py](../../hippius_s3/scripts/dlq_requeue.py). Unpins are deletions — verify a sample against `chunk_backend.deleted` expectations before requeue.

---

# Phase 1 — Detection (highest value ÷ risk; target: this week)

Ordering note: 1.0 establishes the single alerting home and unblocks everything after it. 1.1–1.3 need **no code in this repo** (metrics already exported). 1.4–1.6 add small additive metrics/workers.

### Task 1.0: Consolidate ALL alerting into `hippius-otel` (single source of truth)

**Files:**
- Sibling repo `hippius-otel`: `alerting/rules/*.yaml`, `alerting/{contact-points,policies}.yaml`
- This repo: **delete** `monitoring/grafana/provisioning/alerting/` (4 files), modify `docker-compose.monitoring.yml:107` mount note, add pointer in `monitoring/README` (create if absent)
- Create: `docs/runbooks/alerting.md` (one paragraph: "all alerting lives in hippius-otel; how to add a rule")

**Decision:** alert definitions currently exist in three places — `hippius-otel` (the declared source per [docs/grafana-alerting.md](../grafana-alerting.md)), a 16-rule copy inside this repo wired only to local compose Grafana, and whatever the prod Grafana UI holds directly (at minimum the "HAProxy Alerts" folder — the very alert that detected this incident — whose provenance is unknown). Divergent copies rot: the in-repo copy predates the 2026-07-17 move and nobody reconciles it. Consolidate to one provisioned-as-code home.
**Evaluated options for the local-dev copy:**
1. Keep it as a mirror synced from `hippius-otel` — rejected: adds a sync mechanism to maintain, and local dev doesn't need paging rules.
2. Mount a checked-out `hippius-otel` path into compose Grafana — rejected: couples local dev to a second repo checkout.
3. **Delete it (chosen)** — per the "replace, don't deprecate" convention; local compose Grafana keeps dashboards + datasources only. Consequence: local Grafana shows no alert rules — acceptable, and anyone iterating on a rule does it against `hippius-otel`'s own local flow.
**Breaking-change analysis:** prod is untouched (the deleted files were never provisioned to k8s — verified: no ConfigMap references them). Local-dev alert preview disappears; flagged in the PR description, not silent.

**Step 1 (read-only inventory, no assumptions):** export every Grafana-managed rule from prod Grafana via the API and diff against `hippius-otel`:
```bash
# read-only; needs a viewer/editor API token for monitoring.hippicode.com
curl -s -H "Authorization: Bearer $GRAFANA_TOKEN" \
  https://monitoring.hippicode.com/api/v1/provisioning/alert-rules | jq -r '.[] | [.folderUID // .folder, .title, .uid] | @tsv' | sort
```
Classify each rule: (a) already in `hippius-otel` → done; (b) UI-managed only (expect the "HAProxy Alerts" folder here, uid `hapx5xxout07`) → export full JSON, convert to the `hippius-otel` rule format, import; (c) obsolete → list for the owner to confirm deletion — never delete unilaterally.
**Step 2:** PR to `hippius-otel` importing category (b) rules verbatim (same thresholds — this task migrates, it does not retune; retuning is Tasks 1.1+). Verify with that repo's `sync-alerting.sh` flow against staging Grafana first.
**Step 3:** confirm the imported rules now show as provisioned (read-only in the UI) in prod Grafana; only then delete the UI originals (Grafana provisioning with the same uid replaces them in place — verify uid preservation in the export/convert step so silence windows are zero).
**Step 4:** PR to this repo deleting `monitoring/grafana/provisioning/alerting/` + the runbook pointer. Commit: `chore: remove local alerting copies; hippius-otel is the single alerting source`.
**Step 5:** add a guard so strays don't return: in `hippius-otel`, `sync-alerting.sh` (or its CI) becomes the only provisioning path; in this repo the runbook states the rule. (A CI lint that fails on `monitoring/**/alerting/` reappearing was considered and rejected as over-tooling for a two-repo org; revisit if it happens twice.)

### Task 1.1: Alert on the janitor replication sentinel (zero code here)

**Files:** sibling repo `hippius-otel` → `alerting/rules/hippius-s3.yaml`.
**Decision:** the gauge already exists and was pegged at cap during the outage with nobody watching: `janitor_underreplicated_live_chunks`, registered at [workers/run_janitor_in_loop.py:634-637](../../workers/run_janitor_in_loop.py), fed by [hippius_s3/sql/queries/find_underreplicated_live_chunks.sql](../../hippius_s3/sql/queries/find_underreplicated_live_chunks.sql) (SLA grace `HIPPIUS_REPLICATION_SLA_SECONDS`, default 900). Alerting on an existing metric is the cheapest possible detection.
**Breaking-change analysis:** none — pure alerting addition, non-paging for week 1.

**Step 1:** clone `hippius-otel`; read `alerting/rules/hippius-s3.yaml` and the existing rule format (16 rules); read `k8s/sync-alerting.sh` to confirm the provisioning flow.
**Step 2 (verify, no assumptions):** confirm the exact exported metric name in Prometheus — OTel names can be transformed by the collector's prometheus exporter:
```bash
kubectl -n <otel-ns> port-forward svc/otel-collector 8889:8889 &
curl -s localhost:8889/metrics | grep -i underreplicated
```
**Step 3:** add rule (adapt to the repo's schema):
- expr: `max_over_time(janitor_underreplicated_live_chunks[15m]) > 0` — sustained-nonzero, 15m `for:`.
- summary: "Live serveable chunks lack full backend coverage past replication SLA (janitor sentinel). N==500 means >=500 (scan cap)."
- severity: `high`, non-paging week 1 → `critical` after threshold review.
**Step 4:** provision to staging Grafana first (per that repo's flow), verify the rule evaluates, then prod. Commit in `hippius-otel`.

### Task 1.2: Alert on drain-pipeline stall (zero code here)

**Files:** `hippius-otel` → `alerting/rules/hippius-s3.yaml`.
**Decision:** the drain agent already exports `drain_parts_replicated_total` (counter) and `drain_ssd_backlog_bytes` (gauge, DB-sourced pending+draining bytes) — [crates/hippius-drain-agent/src/metrics.rs:86-165](../../crates/hippius-drain-agent/src/metrics.rs). "Work exists but nothing completes" is expressible today:
- expr: `sum(rate(drain_parts_replicated_total[10m])) == 0 and sum(drain_ssd_backlog_bytes) > 1e9` (threshold: ~1 GiB of undrained parts; tune against a week of normal data), `for: 10m`.
This exact rule would have fired ≈07:50 UTC on 2026-07-24.
**Alternatives considered:** DB-driven `chunk_backend` creation-rate alert (needs a new exporter — Task 1.5 covers age instead, which is more robust to traffic troughs); alerting on uploader `uploader_chunks_uploaded_total` alone (false-positives at night when there's genuinely no work — the backlog conjunct fixes that).
**Steps:** same verify-name → add rule → staging → prod flow as Task 1.1. Also add the symmetric uploader rule: `sum(rate(uploader_chunks_uploaded_total[10m])) == 0 and sum(drain_ssd_backlog_bytes) > 1e9` (uploader metric verified at [hippius_s3/monitoring.py:111](../../hippius_s3/monitoring.py)).

### Task 1.3: Loki-based alerts for the leading app-level indicators (zero code)

**Files:** `hippius-otel` only (Grafana alert rules can use the Loki datasource — verify that repo's provisioning supports the Loki datasource UID; if it doesn't, extend `hippius-otel`'s provisioning to carry Loki-datasource rules as part of this task. Per constraint 5, UI-provisioned rules are not a fallback).
**Decision:** two log-derived rates went 10–20× at onset, well before the HAProxy 20% threshold. Loki alerting needs no code and no new metrics:
- `sum(count_over_time({namespace="hippius-s3-prod", app="api-local"} |= "parts not ready" [10m])) > 200`
- `sum(count_over_time({namespace="hippius-s3-prod"} |= "Missing backup chunk" [10m])) > 500` (baseline was ~80/10m; verified from the incident timeline)
**Breaking-change analysis:** none. **Risk:** log-format drift silently breaks the alert — mitigated by Task 1.4 which adds a real metric for the first signal; the Loki rule is the fast bridge.
**Steps:** verify Loki datasource UID in `hippius-otel` provisioning → add both rules non-paging → staging → prod.

### Task 1.4: Add `download_not_ready_total` metric on the api (small additive code)

**Files:**
- Modify: `hippius_s3/monitoring.py` (MetricsCollector `__init__` + a `record_download_not_ready()` method, following the counter pattern at [monitoring.py:93-121](../../hippius_s3/monitoring.py))
- Modify: `hippius_s3/api/s3/objects/get_object_endpoint.py` (the `except DownloadNotReadyError` handler at :404-421)
- Test: `tests/unit/test_download_not_ready_metric.py`

**Decision:** replace reliance on log-grep (Task 1.3) with a first-class counter for the single most predictive user-facing signal. Labels: none beyond defaults (bucket/key would be unbounded cardinality — rejected).
**Breaking-change analysis:** additive metric + one method call inside an existing exception handler; no behavior change. Guard: the `record_*` call must not be able to raise into the handler — follow the existing `NullMetricsCollector` pattern ([monitoring.py:775-791](../../hippius_s3/monitoring.py)) which already makes recording a no-op when monitoring is off.

**Step 1: failing test**
```python
# tests/unit/test_download_not_ready_metric.py
from unittest.mock import MagicMock

def test_download_not_ready_error_records_metric(monkeypatch):
    collector = MagicMock()
    monkeypatch.setattr("hippius_s3.api.s3.objects.get_object_endpoint.get_metrics_collector", lambda: collector)
    # invoke the endpoint's DownloadNotReadyError path (reuse the existing endpoint-test fixture
    # from tests/unit that mocks object_reader.read_response to raise DownloadNotReadyError)
    ...
    collector.record_download_not_ready.assert_called_once()
```
(Adapt the invocation to the existing endpoint unit-test fixtures — read `tests/unit/` for the current GetObject test harness first; do not invent a new one.)
**Step 2:** run `pytest tests/unit/test_download_not_ready_metric.py -xvs` → expect FAIL (no such method).
**Step 3:** implement: counter `download_not_ready_total` in `MetricsCollector.__init__`, `record_download_not_ready(self) -> None`, no-op override in `NullMetricsCollector`; call it in the `except DownloadNotReadyError` branch of `get_object_endpoint.py`.
**Step 4:** `pytest tests/unit -x -q` (full unit suite — the handler is shared-path), `ruff check . && mypy hippius_s3` → all green.
**Step 5:** commit `feat: count DownloadNotReadyError as download_not_ready_total metric`.
**Step 6 (follow-through):** once deployed and the name is visible on :8889/metrics, swap the Task 1.3 Loki rule for `sum(rate(download_not_ready_total[10m])) > X` in `hippius-otel`.

### Task 1.5: Export drain-backlog age from the janitor (small additive code)

**Files:**
- Create: `hippius_s3/sql/queries/cephor_oldest_pending_age.sql`
- Modify: `workers/run_janitor_in_loop.py` (new observable gauge + observer, exactly mirroring `_obs_replication_sentinel` at :181-182 and its registration at :634-637)
- Test: `tests/integration/test_cephor_pending_age_sql.py` (mirror the existing sentinel SQL test `tests/integration/test_replication_sentinel_sql.py`)

**Decision:** rate-based alerts (1.2) miss slow leaks; an age gauge (`cephor_pending_oldest_age_seconds`) catches both the 2-day-old stuck rows found today and any future slow bleed. Janitor is the right host: it already owns cephor-adjacent read-only SQL and the observable-gauge pattern; the query is one indexed read.
**Alternatives rejected:** exporting from the drain agent (it's node-scoped — each agent only sees its own node; the janitor sees the global table); a p99 (needs a full scan; `MIN(landed_at)` is O(index)).
**Breaking-change analysis:** additive gauge + one read-only query per janitor cycle. Guard: wrap in the janitor's existing per-phase try/except pattern (the sentinel phase at :1432-1436 already isolates failures).

**Step 1: SQL + failing integration test**
```sql
-- hippius_s3/sql/queries/cephor_oldest_pending_age.sql
-- Oldest undrained part age in seconds; 0 when the backlog is empty.
SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(landed_at))), 0)::bigint AS age_seconds
FROM cephor_replication_status
WHERE status IN ('pending', 'draining');
```
Test seeds two rows (one old `pending`, one `replicated`) and asserts the age reflects only the pending row; plus the empty-table → 0 case. Copy the harness from `test_replication_sentinel_sql.py`.
**Step 2:** run it → FAIL (query file missing). **Step 3:** add file + janitor gauge `cephor_pending_oldest_age_seconds` + observer. **Step 4:** integration tests green; `ruff`/`mypy` clean. **Step 5:** commit. **Step 6:** `hippius-otel` rule: `cephor_pending_oldest_age_seconds > 4 * 900` (4× the replication SLA), non-paging first.

### Task 1.6: Cross-node write/read canary

**Files:**
- Create: `workers/run_s3_canary_in_loop.py`
- Modify: `k8s/base/workers-deployments.yaml` (new Deployment, cloned from the cachet-health-checker block at :839-963), `k8s/staging/` + `k8s/production/` env patches
- Modify: `hippius_s3/monitoring.py` (counters `s3_canary_checks_total{result}` — result ∈ {ok, fail}; bounded cardinality)
- Test: `tests/unit/test_s3_canary.py`

**Decision (evaluated three options):**
1. *Extend cachet-health-checker* — rejected: it has no S3 credentials/endpoint today ([k8s/base/workers-deployments.yaml:865-970](../../k8s/base/workers-deployments.yaml)) and conflates "status page updater" with "synthetic prober"; a failure in one shouldn't take out the other.
2. *Extend the hourly GH-Actions smoke suite* — rejected as the primary: hourly is too slow (today's outage would be caught at :00 only) and CI runners see the public edge, not per-node behavior. (It stays as the external check.)
3. **In-cluster canary worker (chosen):** every 60s, PUT a small unique object to a dedicated canary bucket via the internal `gateway:8080`, then GET it back N=8 times through the `api` ClusterIP service. The service round-robins across the 5 api-local pods, so 8 GETs statistically cover ≥2 nodes; a fresh-object cross-node read exercises exactly the pipeline that failed (local-NVMe → drain → shared cache / peer download). Failures increment `s3_canary_checks_total{result="fail"}`.
   **Consciously accepted limitation:** round-robin is probabilistic, not exhaustive per-node. Deterministic per-pod targeting needs endpoint discovery (k8s API + RBAC) — deferred; noted in the worker's header comment. Do NOT silently claim full coverage.
**Breaking-change analysis:** new isolated worker; zero shared-path changes. Bucket: `hippius-canary` under a dedicated account; objects deleted after each check; total write volume ≈ 1 object/min.

**Steps:** (1) failing unit test for the check function (mock httpx: PUT ok + one GET returning 503 → result "fail"); (2) implement the loop using the repo worker pattern (`run_worker` from [hippius_s3/workers/shutdown.py](../../hippius_s3/workers/shutdown.py), OTel via `get_metrics_collector`); (3) manifest with the standard exec liveness probe *plus* the heartbeat-file probe from Task 2.5 once that lands; (4) e2e: add the canary service to `docker-compose.e2e.yml` and one e2e test asserting the fail counter increments when toxiproxy blackholes the api; (5) `hippius-otel` rule: `sum(rate(s3_canary_checks_total{result="fail"}[10m])) > 0`, `for: 5m`; (6) staging soak 1 week → prod.

### Task 1.7: Backup/hydrator detection (cross-repo coordination)

**Files:** sibling repo `thenervelab/s3-backup` (+ `hippius-otel` for rules).
**Decision:** this repo cannot fix what it doesn't contain. Scope here = (a) the Loki bridge rule from Task 1.3 (works today), (b) an issue filed against `s3-backup` with the verified requirements: emit an OTel counter for `Missing backup chunk` events and a backup-lag gauge (time between `parts.uploaded_at` and backup completion), plus document what happens after a miss (retry/drop — currently unverifiable from here).
**Step 1:** clone `s3-backup`, verify the miss-handling behavior, write the issue with file:line citations from that repo. **Step 2:** if trivial (matching this repo's `errors.py` contract), implement there following that repo's conventions — separate PR, separate review.

---

# Phase 2 — Stall-proofing (1–2 sprints)

Staged rollout rule for every probe/timeout task: **code lands with the feature disabled → staging enables → 1 week soak → prod enables → only then consider changing defaults.**

### Task 2.1: Redis client hardening in hippius_s3 (Python)

**Files:**
- Modify: `hippius_s3/redis_utils.py:51` (standalone branch)
- Modify: call sites constructing bare queues clients: `hippius_s3/main.py:107-113`, `workers/run_arion_uploader_in_loop.py:114`, `workers/run_janitor_in_loop.py:1396`, `workers/run_orphan_checker_in_loop.py:140-141` (re-verify the full list with `rg -n "from_url" hippius_s3/ workers/ gateway/` first — scripts/*.py are out of scope)
- Test: `tests/unit/test_redis_client_construction.py`

**Decision:** the cluster branch of `create_redis_client` already sets `socket_connect_timeout=5, socket_timeout=5, health_check_interval=30, socket_keepalive=True, retry_on_error=[...]` ([redis_utils.py:41-49](../../hippius_s3/redis_utils.py)); the standalone branch and every direct `from_url` queues client set **nothing** — the verified enabler of infinite hangs on dead TCP. Fix = route all long-lived clients through `create_redis_client` (single choke point), giving the standalone branch the same kwargs.
**Evaluated risk — the ONE subtle consumer:** `ChunkNotifier` pub/sub deliberately runs without `socket_timeout` and compensates with a 1s FS re-poll ([hippius_s3/cache/notifier.py:35-38](../../hippius_s3/cache/notifier.py)); it already catches `RedisTimeoutError` and re-polls (:186-190, :276-282), so a 5s socket_timeout is *handled* — but this is the highest-risk consumer. Mitigation: flag `HIPPIUS_REDIS_STANDALONE_TIMEOUTS` (default `false` = today's bare behavior); flip on in staging, watch GET p99 + `notify:` pub/sub behavior for a week, then prod, then flip default in a follow-up PR.
**Breaking-change analysis:** with the flag off, byte-for-byte current behavior. With it on, blocking calls gain a 5s socket timeout — every long-lived consumer identified above already sits behind `with_redis_retry` ([workers/run_arion_uploader_in_loop.py:178-183](../../workers/run_arion_uploader_in_loop.py)) or the notifier's re-poll; re-verify each call site's retry wrapper as part of the PR checklist.

**Step 1:** failing test: `create_redis_client(url, cluster=False)` with flag on → client has `socket_timeout==5`, keepalive on; with flag off → constructed with no kwargs (assert via mock of `Redis.from_url`).
**Step 2–4:** implement, tests green, `ruff`/`mypy`.
**Step 5:** migrate the four call sites to `create_redis_client`. One commit per call site (bisectable).
**Step 6:** staging flag-on → soak → prod flag-on.

### Task 2.2: Progress-based heartbeat for Python workers (uploader first)

**Files:**
- Create: `hippius_s3/workers/heartbeat.py`
- Modify: `workers/run_arion_uploader_in_loop.py` (touch heartbeat each loop iteration)
- Modify (stage 2 only): `k8s/base/workers-deployments.yaml:173-182` (uploader livenessProbe)
- Test: `tests/unit/test_worker_heartbeat.py`

**Decision:** today's liveness is `grep -q python /proc/1/cmdline` — process-aliveness, blind to wedges. The drain agent's file-mtime pattern ([k8s/production/drain-agent-daemonset.yaml:226-235](../../k8s/production/drain-agent-daemonset.yaml)) is already proven in this stack; reuse it: the worker touches `/tmp/worker.alive` once per *completed loop iteration* (dequeue attempt counts — an idle-but-responsive worker is alive; a worker wedged inside brpop/upload does not touch and goes stale).
**Why uploader first:** it's the incident's silent victim, and its loop structure (`run_arion_uploader_in_loop.py:166-183`) has an obvious single touch point. Downloader/janitor/unpinner follow the identical recipe in follow-up PRs once the pattern soaks.
**Threshold evaluation:** loop iterates ≥ every ~2.5s (brpop 0.5 + retry wrapper); probe staleness 120s, failureThreshold 3, period 30 → restart after ≥ 6 min of true wedge. Deliberately lenient: a false restart of the uploader is cheap (queue is durable, `move_due_upload_retries` + claim CAS at [hippius_s3/queue.py:217-241](../../hippius_s3/queue.py) make requeue exactly-once), but we still stage it.
**Breaking-change analysis:** Stage A (heartbeat file only, probe untouched) is inert. Stage B (probe switch) changes restart semantics — ship as its own PR after ≥1 week of observing the file's mtime cadence in staging (`kubectl exec ... stat /tmp/worker.alive`).

**Step 1:** failing test: `Heartbeat(path).beat()` updates mtime; `is_fresh(max_age)` false for a stale file (tmp_path; no mocking of time — use real files).
**Step 2–4:** implement (~15 lines), wire one `hb.beat()` into the uploader loop, tests green.
**Step 5:** commit Stage A. **Step 6 (separate PR):** probe switch to `test $(($(date +%s) - $(stat -c %Y /tmp/worker.alive))) -lt 120`, staging first.

### Task 2.3: Drain-agent wedge self-recovery (Rust)

**Files:**
- Modify: `crates/hippius-drain-agent/src/runtime.rs` (supervisor-level watchdog), `crates/hippius-drain-agent/src/config.rs` (new env), `k8s/staging/drain-agent-daemonset.yaml` then production
- Test: `crates/hippius-drain-agent/src/runtime.rs` unit tests (tokio-test, mock store)

**Decision (evaluated three designs):**
1. *Tie the `.alive` heartbeat to drain progress* — rejected: `.alive` is shared by 6 workers via the heartbeat worker; making it drain-coupled restarts the whole agent when drains are legitimately idle (empty node) — false-positive machine.
2. *Wrap every FS op in `tokio::time::timeout`* — deferred to Task 2.4; on its own it can't catch every wedge class (e.g. a hung DB pool acquire).
3. **Watchdog task (chosen):** a 7th supervised task that every `watchdog_poll` (default 60s) reads two things it can already observe: (a) claimable-work-exists (`store` count of `pending` rows for this node — one indexed query) and (b) the last-progress timestamp (an `AtomicU64` bumped by `drain_next` on every completed part, success or handled failure). If work has existed continuously for `CEPHOR_WEDGE_EXIT_SECS` with zero progress → `tracing::error!` + `std::process::exit(1)`; k8s restarts the pod; claims self-recover via the existing lease re-claim ([store.rs:507](../../crates/hippius-drain-core/src/store.rs)). **Default `CEPHOR_WEDGE_EXIT_SECS=0` = disabled** — with the flag off the binary behaves exactly as today.
**Why exit(1) is safe here (verified):** every in-flight claim is fenced by `claim_seq` and recovered by the 5-min claim lease; a killed agent loses no data — the SSD copy is retained until `mark_replicated` commits.
**Breaking-change analysis:** flag-gated, default off. Rust changes go through the mandatory `rust-style` skill + `cargo clippy -D warnings` + `cargo test`.

**Steps (TDD):** (1) unit test for the watchdog decision function (pure: `(work_present_since, last_progress, now, threshold) -> Verdict`) — all edge cases: no work, fresh progress, threshold boundary; (2) implement pure function; (3) wire the supervised task + `AtomicU64` bump in `drain_next`; (4) `cargo test && cargo clippy --all-targets --all-features -- -D warnings`; (5) commit; (6) staging manifest sets `CEPHOR_WEDGE_EXIT_SECS=900`, soak, then prod.

### Task 2.4: Timeouts on drain FS and DB operations (Rust)

**Files:**
- Modify: `crates/hippius-drain-agent/src/localfs.rs` (`copy_into` :355-369, `stream_copy_hash` :299-315, `finalize_part` :579-594), `crates/hippius-drain-core/src/store.rs:209-217` (pool options), `config.rs`
- Test: existing localfs/partdrain unit tests + new timeout tests

**Decision:** verified: **zero** timeouts exist around FS or DB I/O in the drain path; a single hung CephFS `write`/`fsync` wedges the drain worker forever (exactly compensated-for today only by the readiness file, which doesn't restart anything). Add:
- FS: wrap each chunk copy + fsync + rename in `tokio::time::timeout(CEPHOR_FS_OP_TIMEOUT_SECS)`. On timeout → treat as the existing transient-failure path (`release_part`, part returns to `pending`, retried next poll — the "H1" semantics already in [worker.rs:227-230](../../crates/hippius-drain-agent/src/worker.rs)). **Default `0` = no timeout = today's behavior**; staging enables `600`.
- DB: `PgPoolOptions::acquire_timeout(30s)` + per-connection `SET statement_timeout` via `after_connect` — enabled by the same-style env, default off.
**Evaluated risk:** a copy that is slow-but-succeeding (giant part on a throttled CephFS) would now be cancelled and retried, re-copying from scratch — wasted work but *correct* (temp-file + atomic rename means no torn state; verified in `stream_copy_hash`/`finalize_part`). 600s default makes this pathological-only. **Important interaction:** timeout must fire *between* chunks or cancel the temp file cleanly — never leave the claim held; the `release_part`/`defer_part` paths already handle both.
**Breaking-change analysis:** flag-gated, default = today. Tokio cancellation of `tokio::fs` ops on a truly-hung kernel CephFS mount may not free the blocked spawn_blocking thread — document this limit in the code comment (the watchdog from 2.3 remains the backstop; the two tasks are complements, not alternatives).

**Steps:** failing test with a mock-slow source (tokio-test paused clock) asserting timeout → `Err(transient)`; implement; full `cargo test` + clippy; commit; staged enablement.

### Task 2.5: Continuous upload re-drive (close the "missed enqueue forever" hole)

**Files:**
- Create: `hippius_s3/sql/queries/list_underreplicated_for_redrive.sql` (derived from `find_underreplicated_live_chunks.sql` — reuse its join logic; add per-object grouping + a redrive-cooldown predicate)
- Modify: `workers/run_janitor_in_loop.py` (new phase after the sentinel phase; flag `HIPPIUS_JANITOR_REDRIVE_UPLOADS`, default `false`)
- Test: `tests/integration/test_janitor_upload_redrive.py`

**Decision:** verified gap: the drain `enqueue_sweep` only covers `status='replicated' AND upload_enqueued_at IS NULL` ([runtime.rs:401-425](../../crates/hippius-drain-agent/src/runtime.rs)); once `upload_enqueued_at` is stamped, a *lost* Redis message (crash after LPUSH-ack but before uploader processing, DLQ drop, or the incident's stall) is retried by **nothing** — `orphan_checker` only enqueues unpins (verified [workers/run_orphan_checker_in_loop.py:95-103](../../workers/run_orphan_checker_in_loop.py)). The janitor already computes exactly the right population (underreplicated live chunks past SLA) for the sentinel — extend it from *counting* to *re-enqueueing* `UploadChainRequest`s for those objects.
**Evaluated design constraints (no assumptions):**
- Build the `UploadChainRequest` the same way the drain enqueuer does (payload contract "KEEP-IN-SYNC" noted at [crates/hippius-drain-agent/src/enqueue.rs:57-73](../../crates/hippius-drain-agent/src/enqueue.rs) vs `hippius_s3/queue.py`) — reuse the existing Python `enqueue_upload_request` path, never hand-roll the payload.
- **Duplicate-enqueue safety must be verified, not assumed:** before writing code, confirm the uploader tolerates re-uploading an already-uploaded chunk (idempotent `chunk_backend` upsert). Read [hippius_s3/workers/uploader.py](../../hippius_s3/workers/uploader.py) and the insert SQL; if it double-inserts, add `ON CONFLICT DO NOTHING` first as its own additive migration/PR.
- Cooldown column: additive `ALTER TABLE ... ADD COLUMN last_redrive_at timestamptz` is owned by the **drain crate's migrations** (schema owner is the allocator, per manifest header) — coordinate: the migration lands in `crates/hippius-drain-core/migrations/`, Python only reads/writes the column. Cap per cycle (e.g. 200 objects) to avoid thundering herd.
**Breaking-change analysis:** flag default off; with flag on, worst case = duplicate uploads of already-stored chunks (verified-idempotent per the step above). Staging: enable, seed a synthetic underreplicated row, watch it heal.

**Steps:** (1) verify uploader idempotency (read code; if needed, sub-PR); (2) failing integration test: seed a live chunk with no `chunk_backend` rows older than SLA → run the janitor phase with flag on → assert an upload request appears on the queue and cooldown is stamped; flag off → nothing happens; (3) implement; (4) integration suite + `ruff`/`mypy`; (5) commit; (6) staged enablement.

### Task 2.6: File the s3-backup hardening issue (cross-repo)

Same treatment as this repo's workers, filed against `thenervelab/s3-backup` with requirements: progress heartbeat + probe, Redis/S3 client timeouts, retry-vs-drop documentation for hydrator misses. Verify current behavior in that repo first; implement there per its conventions. (Blocking dependency for closing the last silent-stall surface; not implementable from here.)

---

# Phase 3 — Blast-radius reduction (design-gated; do deliberately)

### Task 3.1: Design doc — peer-read from the ingest node ("serve fresh objects from the pod that has them")

**Files:** Create `docs/plans/2026-XX-XX-peer-read-design.md` (design only in this plan; implementation is its own reviewed plan).
**Decision:** this is the structural fix — it converts "replication stalled" from an outage into a latency bump — but it touches GET semantics, security, and service topology, so it gets the same rigor as the cutover: a design doc + review gate, NOT direct implementation. Verified constraints the design must solve (all confirmed in code):
1. The read path today has **zero** knowledge of the ingest node; `cephor_replication_status.node_id` exists and is indexed (`0013_undrained_by_node` index) — the natural lookup, but it's populated by the *reconciler scan*, so it lags a PUT by up to `reconcile_poll` (15s in prod): the design must handle "no row yet" (fallback: current pipeline wait).
2. No peer-addressing exists: the `api` Service is ClusterIP with selector `app: api-local` ([k8s/base/services.yaml:1-11](../../k8s/base/services.yaml), prod selector patch in kustomization) — the design needs a headless Service (additive, new manifest) + a node→pod resolution rule.
3. Security: peer requests bypass the gateway — must reuse the existing internal-trust mechanism (`parse_internal_headers` + ip_whitelist middleware) and be explicitly marked as internal reads.
4. Failure containment: peer timeout must be ≪ `stream_first_chunk_timeout_seconds` (25s) so the fallback chain (peer → pipeline wait → 503) never exceeds today's worst case. Flag: `HIPPIUS_PEER_READ_ENABLED`, default false.
**Deliverable:** design doc answering the four constraints with the alternatives table (peer-read vs Task 3.2 write-through vs "do nothing, rely on Phase 2"), a rollout plan, and an e2e test plan (the e2e compose stack already runs the full drain stack — verified `docker-compose.e2e.yml:251-303` — so "fresh object, cross-pod read, drain paused" is testable there).

### Task 3.2: Small-object write-through evaluation (measure before building)

**Files:** Create `docs/audits/2026-XX-XX-small-object-writethrough-eval.md`.
**Decision:** the incident's user-visible failures were tiny once-a-minute JSON files; writing objects ≤1 chunk synchronously to the shared CephFS would have prevented them entirely. But two verified blockers make this an *evaluation* first, not an implementation: (a) the api mounts CephFS **read-only** ([k8s/production/api-local-deployments-production.yaml:132](../../k8s/production/api-local-deployments-production.yaml)) — flipping to RW partially reverses the cutover's isolation rationale and re-exposes PUT latency to CephFS health (today's Ceph is degraded — Phase 0.3); (b) `DualFileSystemPartsStore` is read-fallback-only by design ([hippius_s3/cache/dual_fs_store.py:9-14](../../hippius_s3/cache/dual_fs_store.py)) — write-through needs a new store mode with its own failure semantics (CephFS write fails → PUT must still succeed on NVMe alone, else we've *added* an outage mode).
**Deliverable:** measure (from Loki + DB, read-only) what fraction of GET-503s during the incident were ≤1-chunk objects; if >80%, spec the write-through as: best-effort dual write behind `HIPPIUS_WRITE_THROUGH_MAX_BYTES` (default 0 = off), CephFS failure logged + counted but never failing the PUT. Compare against Task 3.1 (peer-read likely supersedes it) and pick **one** — building both is redundant risk. Present the recommendation for human review.

### Task 3.3: FS-cache version-dir hygiene (janitor)

**Files:**
- Modify: `workers/run_janitor_in_loop.py` (new GC predicate), possibly `hippius_s3/cache/fs_store.py`
- Test: `tests/unit/test_janitor_superseded_versions.py`

**Decision:** verified: janitor GC is mtime/replication-gated only and **not version-supersede-aware** — a hot object rewritten every minute keeps every `v<N>` dir alive via hot-retention `os.utime` touches (one object had 1,337 version dirs; the cache root holds 3.45M entries, a CephFS MDS stressor). Add a supersede rule: a part dir for version `v` is GC-eligible (in addition to existing rules) when `v < current_object_version - KEEP_N` **and** fully replicated **and** not DLQ-protected — i.e. only *tighten* the age gate, never bypass the replication/DLQ gates (those are the janitor's absolute safety invariants, [workers/run_janitor_in_loop.py:1-22](../../workers/run_janitor_in_loop.py)). `KEEP_N` default 2 (current + previous — the previous version is load-bearing: the envelope-race fallback serves `version - 1`, verified [hippius_s3/services/object_reader.py:270-325](../../hippius_s3/services/object_reader.py)).
**Alternatives rejected:** sharding the cache root layout (2-hex prefix dirs) — bigger win for MDS but it's a **breaking on-disk-layout change** touching every reader/writer/downloader/drain path simultaneously; explicitly out of scope under the no-breaking-changes constraint. Documented as a future candidate in todo.md instead.
**Breaking-change analysis:** flag `HIPPIUS_JANITOR_GC_SUPERSEDED` default false. Risk: deleting a version some reader still streams — mitigated because the rule only applies to *fully-replicated* parts (the downloader can re-fetch mid-stream via the existing pipeline) and `KEEP_N>=2` protects the fallback path.
**Steps:** failing unit test with a fake objects/versions fixture (versions v1..v5, current=5, all replicated → v1..v3 eligible, v4,v5 kept; plus not-replicated → kept regardless); implement; full janitor unit suite; commit; staged enablement.

### Task 3.4: Alert topology + cascade documentation

**Files:** `hippius-otel` (rule annotations), this repo `docs/runbooks/s3-cascade.md` (create).
**Decision:** three HAProxy alerts fired for one incident with no hint of the dependency. Add to the `harbor_auth_backend` and `us_ats_prod` alert annotations: "Likely downstream of s3.hippius.com — check the S3 Alerts folder first" (harbor-registry PVC = JuiceFS on bucket `hippius-juicefs-data`@s3.hippius.com; harbor-postgres on ceph-filesystem; ATS fronts s3). Write the cascade runbook capturing this incident's verified diagnosis chain (the Loki queries, the `chunk_backend` rate SQL, the `cephor_replication_status` triage SQL) so the next responder starts at minute 5, not minute 90.
**Breaking-change analysis:** none (docs + annotations).
**Longer-term (flagged for a human decision, not planned here):** whether harbor should sit on JuiceFS-backed-by-our-own-S3 at all — circular dependency between the registry and the platform it serves.

---

## Execution order & dependency graph

```
Phase 0 (ops)            0.1 → 0.2        0.3 (parallel)      0.4 (parallel)
Phase 1 (detect)         1.0 (alerting consolidation — FIRST) → 1.1, 1.2, 1.3 (no code) → 1.4 → 1.5 → 1.6      1.7 (parallel, cross-repo)
Phase 2 (stall-proof)    2.1 → 2.2        2.3 → 2.4 (Rust pair)        2.5 (after 1.5)      2.6 (parallel)
Phase 3 (design-gated)   3.1 and 3.2 (both docs; then pick ONE) → implementation as a NEW plan      3.3      3.4 (anytime)
```

Task 1.0 gates 1.1–1.3 only organizationally (they all land in the same `hippius-otel` PR stream); if 1.0's Grafana-inventory step is blocked on API access, 1.1–1.3 may proceed in `hippius-otel` in parallel — the consolidation of strays must still complete before Phase 1 is called done.

Every code task: staging branch → staging deploy → soak (≥1 week for probe/timeout flags) → prod. No task changes a default until its flag has soaked in prod.

## Post-plan follow-ups

- Persist the incident topology/gotchas to hippius-mem once the memory server is back (it timed out throughout planning — writes from two agents also failed with ATS 408s; the server itself needs attention).
- File the Ceph capacity ticket (Phase 0.3 hand-off).
- After Phase 1 has a week of data: revisit alert thresholds (they were set against post-incident estimates, marked in each rule's annotation).
