# Grafana Alerting (Mattermost)

**Status:** Live. 24 rules in Grafana folder `S3 Alerts`, delivered to the same two
Mattermost channels hcfs pages.

**The rules do not live in this repo.** They were moved to
[thenervelab/hippius-otel](https://github.com/thenervelab/hippius-otel) on
2026-07-17, which owns the shared Grafana:

| what | where |
|---|---|
| the 24 S3 alert rules | `hippius-otel/alerting/rules/hippius-s3.yaml` |
| contact points + routing | `hippius-otel/alerting/{contact-points,policies}.yaml` |
| provisioning | `hippius-otel/k8s/sync-alerting.sh` |
| CI guards | `hippius-otel/ci/`, `.github/workflows/pr-checks.yaml` |

To change an S3 alert, open a PR against hippius-otel. CI renders the change,
proves it deletes nothing live, and posts the cluster delta to the PR; merging to
`production` deploys and verifies it.

This repo still carries a **local-dev-only** alert set at
`monitoring/grafana/provisioning/alerting/alert-rules.yml` (provisioned into the
docker-compose Grafana). It is not the prod source of truth — the prod rules are the
hippius-otel set described above.

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

24 rules: 10 critical / 14 warning, of which 5 carry `outage: "true"`.
`severity: critical` + `outage: "true"` routes to `mattermost-critical`; everything
else to `mattermost`. `outage` means user-visible down/unreachable — a DLQ or a
capacity warning is alarming but nothing is broken for clients yet.

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
| S3CephPoolFull | critical/outage | — | drain Ceph pool at/over the full watermark — ingest cannot replicate |
| S3DrainAgentMissing | critical | — | an ingest node has no ready drain-agent — its parts never replicate/enqueue |
| S3DrainBreakerOpen | critical | — | drain enforcement breaker tripped open (sustained copy failure/saturation) |
| S3CephPoolNearFull | warning | — | drain Ceph pool over the near-full watermark |
| S3IngestCapacityDegraded | warning | — | fleet ingest capacity (drain throughput vs land rate) degraded |
| S3DrainAllocatorNotLeading | warning | — | no leader-elected drain-allocator writing budgets |
| S3CephMgrMetricsAbsent | warning | — | Ceph-mgr ceiling probe metrics missing (allocator flying blind) |
| S3DrainNodeStarved | warning | — | a drain node starved of budget/allocation |

The 8 `S3Drain*`/`S3Ceph*`/`S3Ingest*` rules above are the s3-2.1 drain-direct additions.
Exact `for` windows and thresholds live in `hippius-otel/alerting/rules/hippius-s3.yaml` (this
repo cannot verify them); severities above match the 10-critical / 14-warning / 5-outage split.

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

1. **FIXED (2026-07) — the multi-process counter collision is resolved.** Previously
   `s3_*`, `http_*`, `gateway_*` and `auth_cache_*` were exported by multiple uvicorn
   worker processes per pod under one label set, so the counters collided (over 1h:
   `http_requests_total` logged 260,868 resets, a `rate()` of 394,818 req/s;
   `gateway_overhead_seconds_count` ran backwards), making a 5xx / auth-failure / latency
   spike undetectable. Root cause was `otel_setup.py` pinning `service.instance.id` to the
   bare pod hostname with an explicit "never append os.getpid()". That is now reversed:
   `hippius_s3/otel_setup.py:74` sets `service.instance.id = f"{socket.gethostname()}:{os.getpid()}"`,
   so each process carries a distinct identity (the collector's
   `resource_to_telemetry_conversion: enabled` turns it into a distinguishing label) and the
   per-process counters no longer collide. Request-path rules on these counters are now viable.
2. **`RedisQueuesMemoryFilling` measures the wrong denominator** — container
   memory ÷ 4Gi limit, while redis evicts at its own 2gb maxmemory (~49% of that
   limit), below the 0.6 trigger. It cannot fire as written.
3. **The FS-cache 503 is inferred, not measured** (`fs_cache_pressure.py:64`
   rejects PUTs but emits no counter), so both FS-cache rules alert on the cause.
4. **`backup_last_success_timestamp` is hardcoded 0** on every pod — there is no
   backup-staleness alerting, and `time() - max(...)` ≈ 56 years is an inviting trap.
5. **FIXED — failed unpins are now observable.** `metrics_collector_task.py:96-102`
   (`_dlq_queues`) now gauges every backend upload DLQ (`{backend}_upload_requests:dlq`
   derived from `config.upload_backends`) **plus** `unpin_requests:dlq`, so a full ovh or
   unpin DLQ is no longer invisible. (Previously only `arion_upload_requests:dlq` was polled.)
6. **`downloader_duration_seconds` uses millisecond buckets while recording
   seconds**, so `histogram_quantile` is pure interpolation. (The prior `downloader.py:317`
   line cite has drifted — re-locate the histogram recording before quoting a line.)
