# Monitoring Architecture

This document describes the observability stack and how telemetry data flows through the system.

> **Scope:** this document covers the **local-dev docker-compose** monitoring stack only
> (`docker-compose.monitoring.yml`: OTEL collector → Prometheus/Loki/Tempo → Grafana → Discord).
> **Production is different.** Prod ships telemetry to the LGTM stack in the `monitoring`
> namespace, and prod **alerting rules live in the external
> [thenervelab/hippius-otel](https://github.com/thenervelab/hippius-otel) repo and page
> Mattermost** (see [grafana-alerting.md](grafana-alerting.md)) — not the docker-compose
> Grafana→Discord path shown below. Prod also adds the s3-2.1 drain-direct components
> (api-local SSD tier + Rust drain fleet) that this local stack does not model.

## Overview

The monitoring stack consists of:
- **OpenTelemetry (OTEL) Collector**: Central hub for metrics and traces
- **Prometheus**: Metrics storage and querying
- **Loki**: Log aggregation and querying
- **Tempo**: Distributed tracing storage
- **Grafana**: Unified visualization layer
- **Exporters**: Bridge legacy components to Prometheus

## Architecture Diagram

```mermaid
graph TB
    subgraph "Application Layer"
        API[FastAPI App<br/>:8000]
        Workers[Workers<br/>uploader, downloader,<br/>unpinner, etc.]
    end

    subgraph "Infrastructure"
        Redis[(Redis general cache<br/>:6379 — short-lived state only)]
        RedisAccounts[(Redis Accounts<br/>:6380)]
        RedisQueues[(Redis Queues<br/>:6382 — work queues + pub/sub)]
        RedisRateLimit[(Redis Rate-Limiting<br/>:6383)]
        RedisAcl[(Redis ACL<br/>:6384)]
        Postgres[(PostgreSQL<br/>:5432)]
        FsCache[FS chunk cache<br/>/var/lib/hippius/object_cache]
        Arion[Arion Service]
        DockerLogs[/var/lib/docker/containers]
    end

    subgraph "Metrics Exporters"
        RedisExp[Redis Exporter<br/>:9121]
        RedisAccExp[Redis Accounts Exporter<br/>:9122]
        PostgresExp[Postgres Exporter<br/>:9187]
    end

    subgraph "Observability Platform"
        OTEL[OTEL Collector<br/>:4317 gRPC, :4318 HTTP<br/>:8889 Prometheus exporter]
        Prom[Prometheus<br/>:9090]
        Loki[Loki<br/>:3100]
        Tempo[Tempo<br/>:3200]
        Promtail[Promtail]
        Grafana[Grafana<br/>:3000]
    end

    %% Metrics Flow (Push)
    API -->|OTLP gRPC<br/>metrics + traces| OTEL
    Workers -->|OTLP gRPC<br/>metrics + traces| OTEL

    %% Infrastructure to Exporters
    Redis -.->|queries| RedisExp
    RedisAccounts -.->|queries| RedisAccExp
    Postgres -.->|queries| PostgresExp

    %% Prometheus Scraping (Pull)
    Prom -->|scrape :9090| Prom
    Prom -->|scrape :8889| OTEL
    Prom -->|scrape :9121| RedisExp
    Prom -->|scrape :9122| RedisAccExp
    Prom -->|scrape :9187| PostgresExp

    %% Logs Flow
    DockerLogs -->|read logs| Promtail
    Promtail -->|HTTP push| Loki

    %% Traces Flow
    API -->|OTLP gRPC<br/>traces| Tempo
    Workers -->|OTLP gRPC<br/>traces| Tempo

    %% Visualization
    Grafana -->|query metrics| Prom
    Grafana -->|query logs| Loki
    Grafana -->|query traces| Tempo
    Grafana -->|alerting| Discord[Discord Webhook]

    %% Styling
    classDef appLayer fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef infra fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef exporter fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef observability fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px

    class API,Workers appLayer
    class Redis,RedisAccounts,RedisQueues,RedisRateLimit,RedisAcl,Postgres,FsCache,Arion,DockerLogs infra
    class RedisExp,RedisAccExp,PostgresExp exporter
    class OTEL,Prom,Loki,Tempo,Promtail,Grafana observability
```

## Data Flow Details

### Metrics Flow

#### 1. Application Metrics (Push Model)
```
FastAPI/Workers → OTEL Collector (OTLP :4317) → Prometheus Exporter (:8889) → Prometheus (scrape)
```

- Applications use **OpenTelemetry SDK** to create metrics
- Metrics are **pushed** via OTLP protocol to OTEL Collector
- OTEL Collector exposes metrics in Prometheus format on `:8889`
- Prometheus **scrapes** the OTEL Collector every 15 seconds

#### 2. Infrastructure Metrics (Pull Model)
```
Redis/Postgres → Exporter → Prometheus (scrape)
```

- Exporters query infrastructure components (Redis, Postgres)
- Each exporter exposes metrics in Prometheus format
- Prometheus scrapes exporters every 10-30 seconds

**Example Metrics:**
- `http_request_duration_seconds` (from OTEL)
- `fs_cache_disk_used_bytes`, `fs_cache_pressure_mode`, `fs_cache_hot_parts` (FS chunk cache, from the janitor/api)
- `fs_store_parts_on_disk`, `fs_store_oldest_age_seconds` (FS chunk store gauges)
- `redis_connected_clients` (from Redis Exporter)
- `pg_stat_database_tup_returned` (from Postgres Exporter)

> **Where object data lives:** actual object chunks are stored on the **filesystem chunk cache**
> at `/var/lib/hippius/object_cache`, not in Redis. Redis `:6379` is a short-lived **general
> cache** only; `redis-queues :6382` carries work queues + chunk-ready pub/sub. The `fs_cache_*`
> / `fs_store_*` gauges above are the ones to watch for object-storage capacity/pressure.

### Traces Flow (Push Model)

```
FastAPI/Workers → OTEL Collector (:4317) → OTEL Collector processes → Tempo (:3200)
```

**OR direct to Tempo:**

```
FastAPI/Workers → Tempo (:4319 OTLP)
```

- Applications auto-instrumented via `opentelemetry-instrument`
- Traces show request flow across services
- Tempo stores traces for distributed debugging

**Example Traces:**
- Full S3 PutObject request lifecycle
- Worker job processing spans
- Database query spans

### Logs Flow (Push Model)

```
Docker Containers → /var/lib/docker/containers/*.log → Promtail → Loki (:3100)
```

- Promtail reads Docker container logs from filesystem
- Logs are labeled with container metadata
- Pushed to Loki via HTTP
- Grafana queries Loki using LogQL

**Example Log Labels:**
- `container_name="hippius-s3-api-1"`
- `job="hippius-s3"`

### Alerting Flow

```
Prometheus → Grafana Alert Rules → Alertmanager → Discord Webhook
```

- **Local dev only:** alert rules in `monitoring/grafana/provisioning/alerting/` → Grafana
  evaluates against Prometheus → Discord webhook.
- **Production is different:** the prod alert rules live in the external
  [thenervelab/hippius-otel](https://github.com/thenervelab/hippius-otel) repo, are evaluated by
  the LGTM Grafana in the `monitoring` namespace, and page **Mattermost** (not Discord). See
  [grafana-alerting.md](grafana-alerting.md).

## Port Reference

| Service | Port | Purpose |
|---------|------|---------|
| API | 8000 | Main S3 API |
| Grafana | 3000 | Visualization UI |
| Loki | 3100 | Log ingestion/query |
| Tempo | 3200 | Trace query API |
| Tempo OTLP | 4319 | OTLP trace ingestion |
| OTEL Collector gRPC | 4317 | OTLP metrics/traces |
| OTEL Collector HTTP | 4318 | OTLP HTTP endpoint |
| OTEL Prometheus | 8889 | Metrics for Prometheus |
| Prometheus | 9090 | Metrics UI/API |
| Redis Exporter | 9121 | Redis metrics |
| Redis Accounts Exporter | 9122 | Redis accounts metrics |
| Postgres Exporter | 9187 | Postgres metrics |

## Configuration Files

- **OTEL Collector**: `monitoring/otel/otel-config.yml`
- **Prometheus**: `monitoring/prometheus/prometheus.yml`
- **Loki**: `monitoring/loki/loki-config.yml`
- **Promtail**: `monitoring/promtail/promtail-config.yml`
- **Tempo**: `monitoring/tempo/tempo.yaml`
- **Grafana Dashboards**: `monitoring/grafana/dashboards/`
- **Alert Rules**: `monitoring/grafana/provisioning/alerting/`

## Key Design Decisions

### Why OTEL Collector?

1. **Push vs Pull**: Applications push metrics, avoiding need to expose `/metrics` publicly
2. **Centralized Processing**: Single point for metric transformation, sampling, batching
3. **Multi-destination**: Can export to Prometheus, but also other backends if needed
4. **Protocol Translation**: Converts OTLP → Prometheus format

### Why Multiple Redis Exporters?

Each Redis instance (main, accounts) has separate concerns and is monitored independently to track:
- Cache hit rates per instance
- Memory usage per instance
- Connection counts per workload

### Why Loki Instead of Application Logs in OTEL?

- `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=false` in `start-api.sh:13`
- Loki provides better log aggregation and querying (LogQL)
- Promtail automatically labels logs with container metadata
- Avoids duplicate log streams

## Environment-Specific Configuration

### Development (`docker-compose.monitoring.yml`)

- Dummy Discord webhook: `DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/dummy`
- Default admin credentials: `admin/admin`
- All monitoring on shared `hippius_net` network

### Production

- Real Discord webhook set via environment variable
- Secure admin credentials
- Same configuration files work across environments
