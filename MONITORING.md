# Hippius S3 Monitoring Guide

## Overview

This guide provides comprehensive monitoring for the Hippius S3 service using OpenTelemetry, Prometheus, and Grafana. The monitoring stack tracks key metrics including:

- HTTP request rates and response times by account
- S3 operations (bucket/object creation, deletion)
- Redis queue lengths and multipart chunk counts
- Database performance metrics
- IPFS operation success rates
- Background process (pinner/unpinner) throughput

## Architecture

```
FastAPI App (OpenTelemetry) -> OTEL Collector -> Prometheus -> Grafana
                             \
Redis Exporter ----------------> Prometheus
PostgreSQL Exporter -----------> Prometheus
```

## Quick Start

1. **Install monitoring dependencies:**
   ```bash
   source .venv
   uv pip install -r monitoring-requirements.txt
   ```

2. **Set up environment variables:**
   ```bash
   export GRAFANA_ADMIN_USER=admin
   export GRAFANA_ADMIN_PASSWORD=your_secure_password
   ```

3. **Start the monitoring stack:**
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
   ```

4. **Access services:**
   - Grafana: http://localhost:3000 (admin/your_secure_password)
   - Prometheus: http://localhost:9090
   - API Metrics: http://localhost:8000/metrics

## Key Metrics

### HTTP API Metrics
- `http_requests_total` - Total HTTP requests with method, handler, account tags
- `http_request_duration_seconds` - Request latency percentiles
- `http_request_bytes_total` - Data transfer in (bytes)
- `http_response_bytes_total` - Data transfer out (bytes)

### S3 Operations
- `s3_objects_total` - Objects created/deleted by account
- `s3_buckets_total` - Buckets created/deleted by account

### Redis Metrics
- `queue_length` - Length of upload/unpin request queues
- `multipart_chunks_redis_total` - Count of multipart chunks in Redis
- `redis_memory_used_bytes` - Redis memory usage
- `redis_connected_clients` - Active Redis connections

### IPFS Operations
- `ipfs_operations_total` - IPFS upload/download operations
- `ipfs_operation_duration_seconds` - IPFS operation latency

### Database Metrics
- PostgreSQL connection pool metrics
- Query execution times
- Transaction rates

## Account-Based Filtering

All custom metrics include these labels for account-based filtering:
- `main_account` - Main account identifier from request.app.state.account.main_account
- `subaccount_id` - Subaccount identifier from request.app.state.account.id

## Grafana Dashboards

The pre-configured dashboard "Hippius S3 Overview" includes:

1. **HTTP Request Rate by Account** - Real-time request patterns
2. **Redis Connection & Key Count** - Redis health monitoring
3. **Redis Queue Lengths** - Background processing queue status
4. **Multipart Chunks Count** - Multipart upload activity
5. **Data Transfer Rate by Account** - Bandwidth usage patterns
6. **HTTP Response Times** - Latency monitoring with percentiles

### Dashboard Variables
- `$main_account` - Filter by main account (multi-select)
- `$subaccount_id` - Filter by subaccount (multi-select)

## Configuration

### Docker Compose Services

**Prometheus** (port 9090):
- Scrapes metrics from API, Redis exporter, PostgreSQL exporter
- 30-day retention
- Admin API enabled

**Grafana** (port 3000):
- Auto-provisioned Prometheus datasource
- Pre-loaded dashboards
- Admin user configured via environment variables

**OpenTelemetry Collector** (ports 4317/4318):
- Receives telemetry from FastAPI app
- Exports to Prometheus format
- Health check on port 13133

**Redis Exporter** (port 9121):
- Monitors Redis performance and queue lengths
- Checks multipart chunk keys automatically

**PostgreSQL Exporter** (port 9187):
- Database connection pool metrics
- Query performance monitoring

### Custom Metrics Collection

The `BackgroundMetricsCollector` runs every 10 seconds to collect:
- Redis queue lengths (`upload_requests`, `unpin_requests`)
- Multipart chunk counts (keys matching `multipart:*:part:*`)
- Redis memory statistics

## Monitoring Best Practices

### Alerting Rules (Recommended)

1. **High Error Rate**:
   ```promql
   rate(http_requests_total{status_code=~"5.."}[5m]) > 0.1
   ```

2. **High Queue Length**:
   ```promql
   queue_length > 1000
   ```

3. **Low Disk Space**:
   ```promql
   redis_memory_used_bytes / redis_memory_max_bytes > 0.9
   ```

4. **Slow Response Times**:
   ```promql
   histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2
   ```

### Performance Monitoring

- Monitor multipart upload patterns during peak hours
- Track account-specific usage patterns for capacity planning
- Watch Redis memory usage during large file uploads
- Monitor IPFS operation success rates for network health

## Troubleshooting

### Common Issues

1. **Metrics not appearing in Grafana**:
   - Check Prometheus targets at http://localhost:9090/targets
   - Verify OpenTelemetry collector logs: `docker logs hippius-otel-collector`

2. **High Redis memory usage**:
   - Check multipart chunk cleanup
   - Monitor `multipart_chunks_redis_total` metric

3. **Slow API responses**:
   - Check database connection pool metrics
   - Monitor IPFS operation latencies

### Log Locations

```bash
# API logs
docker logs api

# Monitoring stack logs
docker logs hippius-prometheus
docker logs hippius-grafana
docker logs hippius-otel-collector
```

## Production Deployment

### Security Considerations

1. **Change default passwords**:
   ```bash
   export GRAFANA_ADMIN_PASSWORD=$(openssl rand -base64 32)
   ```

2. **Restrict network access**:
   - Bind Grafana to internal network only
   - Use reverse proxy with authentication

3. **Enable TLS**:
   - Configure Grafana with SSL certificates
   - Use HTTPS for external access

### Scaling

- Prometheus can handle up to 1M samples/second
- For high-traffic deployments, consider Prometheus sharding
- Redis exporter scales with Redis performance
- PostgreSQL exporter minimal overhead

### Backup

- Prometheus data: `/prometheus` volume
- Grafana dashboards: `/var/lib/grafana` volume
- Configuration files: `./monitoring/` directory

## API Integration

To add custom metrics to new endpoints:

```python
from hippius_s3.monitoring import get_metrics_collector

# In your endpoint handler:
metrics_collector = get_metrics_collector()
if metrics_collector:
    metrics_collector.record_s3_operation(
        operation="your_operation",
        bucket_name=bucket_name,
        main_account=request.state.account.main_account,
        subaccount_id=request.state.account.id,
        success=True
    )
```

## Maintenance

### Regular Tasks

- Monitor Prometheus disk usage (30-day retention)
- Review Grafana dashboard performance
- Update monitoring dependencies monthly
- Check alert rule effectiveness

### Upgrades

1. Update monitoring-requirements.txt
2. Rebuild containers: `docker compose -f docker-compose.monitoring.yml build --no-cache`
3. Test metrics collection after upgrades
