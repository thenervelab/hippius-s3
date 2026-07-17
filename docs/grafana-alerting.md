# Grafana Alerting (Mattermost)

**Status:** Live. 16 rules in Grafana folder `S3 Alerts`, delivered to the same two
Mattermost channels hcfs pages.

**The rules do not live in this repo.** They were moved to
[thenervelab/hippius-otel](https://github.com/thenervelab/hippius-otel) on
2026-07-17, which owns the shared Grafana:

| what | where |
|---|---|
| the 16 S3 alert rules | `hippius-otel/alerting/rules/hippius-s3.yaml` |
| contact points + routing | `hippius-otel/alerting/{contact-points,policies}.yaml` |
| provisioning | `hippius-otel/k8s/sync-alerting.sh` |
| CI guards | `hippius-otel/ci/`, `.github/workflows/pr-checks.yaml` |

To change an S3 alert, open a PR against hippius-otel. CI renders the change,
proves it deletes nothing live, and posts the cluster delta to the PR; merging to
`production` deploys and verifies it.

## Why they moved

Alert rules select on the `job` label, and `job` is set by Prometheus's
`job_name` — which is configured in hippius-otel. While the two lived in separate
repos, nothing could check them against each other. A tidy-looking rename of
`otel-collector-prod` → `otel-collector-s3-prod` sat in hippius-otel ready to
deploy; it would have orphaned the selectors in **7 of these 16 rules**. Because
they use `noDataState: OK`, they would not have errored — they would have gone
quietly inactive and stayed there, with alerting still reporting healthy.

`hippius-otel/ci/check_selectors.py` now fails CI on exactly that, which is only
possible because the rules sit next to `values/prometheus.yaml`.

## Rules

4 outage · 3 critical · 9 warning. `severity: critical` + `outage: "true"` routes
to `mattermost-critical`; everything else to `mattermost`. `outage` means
user-visible down/unreachable — a DLQ or a capacity warning is alarming but
nothing is broken for clients yet.

| rule | severity | for | condition |
|---|---|---|---|
| S3ServiceDown | critical/outage | 3m | gateway or api available replicas < 1 |
| S3FsCacheFullRejectingUploads | critical/outage | 15m | local-cache-pvc free < 8% |
| S3DownloadQueueStalled | critical/outage | 15m | download queue > 20 |
| RedisQueuesDown | critical/outage | 5m | redis-queues-0 not ready |
| S3ReadDownloadFailures | critical | 10m | cold-read failure ratio > 0.2 |
| S3UploadBacklogNotDraining | critical | 30m | upload drain ETA > 2h |
| PostgresInstancesCritical | critical | 3m | CNPG ready < 2 |
| S3FsCacheSpaceLow | warning | 30m | local-cache-pvc free < 15% |
| S3ReadDownloadSlow | warning | 30m | >25s download ratio > 0.05 |
| S3UploaderUnrecoverableDLQPushes | warning | 15m | non-transient DLQ > 10/h |
| S3UnpinBacklogDiverging | warning | 2h | unpin queue > 2M **and** rising over 6h |
| S3JanitorDown | warning | 30m | janitor available replicas < 1 |
| RedisQueuesMemoryFilling | warning | 30m | redis-queues mem/limit > 0.6 |
| PostgresInstanceDegraded | warning | 15m | CNPG ready < 3 |
| S3IngressPodRestarting | warning | 5m | gateway/api restarts > 0.5/h |
| OtelCollectorDown | warning | 15m | otel-collector up < 1 |

## Constraints that are load-bearing

Every threshold was replayed against 24h–30d of live prod data before shipping,
because hcfs's own rollout shipped a rule whose threshold was already breached —
it fired immediately and never stopped.

- **`noDataState: OK` on every otel-sourced rule.** The prod otel-collector had a
  158-minute metrics blackout on 2026-07-12 while k8s reported the pod healthy.
  With `NoData`, those rules would have emitted a `DatasourceNoData` storm for the
  whole window; `OK` collapses that into one `OtelCollectorDown` notification.
  Only kube-state/kubelet rules use `NoData`, since those gauges always exist.
- **`hippius_queue_length` must be wrapped in `max()`, never `sum()`.** All 5 api
  pods export the same Redis `LLEN` under identical labels, so `sum()` reads 5x
  high and silently rescales when replicas change.
- **Never `rate()` the `s3_*` / `http_*` / `gateway_*` / `auth_cache_*` counters** —
  see the gap below.
- **Annotations must use `{{ $values.A.Value | humanizePercentage }}`**, never
  `{{ $values.A | ... }}`. `$values.A` is a struct; the formatter fails with
  "can't convert template.Value to float" and the annotation never renders. This
  is invisible until the rule fires.

## Known gaps

1. **The user-facing request path is unalertable.** `s3_*`, `http_*`, `gateway_*`
   and `auth_cache_*` are exported by multiple uvicorn worker processes per pod
   under one label set, so the counters collide. Over 1h: `http_requests_total`
   logs 260,868 resets and a `rate()` of 394,818 req/s; `gateway_overhead_seconds_count`
   runs backwards. The single-process workers (`uploader_*`, `downloader_*`,
   `unpinner_*`) show 0 resets — the break follows the process model exactly.
   **No rule can currently detect a 5xx spike, an auth failure spike, or a latency
   regression**, and dashboards built on these counters report fiction.
   Root cause is deliberate: `hippius_s3/otel_setup.py:46` pins
   `service.instance.id` to the pod hostname with an explicit "never append
   os.getpid()", and `OTEL_RESOURCE_ATTRIBUTES` in the deployments reinforces it.
   Fix: give each process its own identity (the collector has
   `resource_to_telemetry_conversion: enabled`, so any per-process resource
   attribute becomes a distinguishing label).
2. **`RedisQueuesMemoryFilling` measures the wrong denominator** — container
   memory ÷ 4Gi limit, while redis evicts at its own 2gb maxmemory (~49% of that
   limit), below the 0.6 trigger. It cannot fire as written.
3. **The FS-cache 503 is inferred, not measured** (`fs_cache_pressure.py:64`
   rejects PUTs but emits no counter), so both FS-cache rules alert on the cause.
4. **`backup_last_success_timestamp` is hardcoded 0** on every pod — there is no
   backup-staleness alerting, and `time() - max(...)` ≈ 56 years is an inviting trap.
5. **Failed unpins are unobservable**: no DLQ depth gauge
   (`metrics_collector_task.py:66-75` polls only `arion_upload_requests:dlq`),
   `unpinner_dlq_total` stopped emitting, and `unpinner_requests_total{success="false"}`
   is unrepresentable (`monitoring.py:450`).
6. **`downloader_duration_seconds` uses millisecond buckets while recording
   seconds** (`downloader.py:317`), so `histogram_quantile` is pure interpolation.
