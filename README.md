# Hippius S3

An S3-compatible gateway for Hippius decentralized storage. Data is stored on the Arion backend and published to the Hippius blockchain, accessible through standard S3 clients (AWS CLI, boto3, MinIO).

## Architecture

```
Client (AWS CLI / MinIO / boto3)
    | HTTPS + AWS SigV4
    v
Gateway (Auth + ACL + Rate Limiting + Audit)
    | HTTP + X-Hippius-* headers
    v
Hippius S3 API
    | Redis queues
    v
Arion Workers (upload / download / unpin)
    |
    v
Arion Storage Backend + Hippius Blockchain
```

## Features

- **S3 Operations**: Buckets, objects, multipart uploads, metadata, tagging, ACLs, lifecycle policies
- **S4 Extensions**: Atomic O(delta) appends with compare-and-swap semantics ([docs/s4.md](docs/s4.md))
- **Authentication**: 5 methods (presigned URL, bearer token, access key, seed phrase SigV4, anonymous)
- **Security**: Rate limiting, IP blocking (banhammer), input validation, credit verification
- **Encryption**: NaCl per-object keys with envelope encryption (OVH KMS in production)
- **Blockchain**: Automatic Arion storage and blockchain publishing with transaction tracking
- **Monitoring**: OpenTelemetry with LGTM stack (Loki, Grafana, Tempo, Mimir/Prometheus)
- **S3 Compatibility**: AWS CLI, MinIO Client, boto3, s3cmd ([docs/s3-compatibility.md](docs/s3-compatibility.md))

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development)

## Quick Start

```bash
# Create Docker network
docker network create hippius_net

# Copy and configure environment
cp .env.example .env
# Edit .env with required variables (see Environment Variables below)

# Start services
docker compose up -d

# Check logs
docker compose logs -f api
```

Database migrations run automatically on container startup via `hippius_s3.scripts.migrate`.

## Environment Variables

Create a `.env` file. Base defaults are in `.env.defaults`.

### Required

**Database**
| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `HIPPIUS_KEYSTORE_DATABASE_URL` | PostgreSQL for encryption keys (falls back to `DATABASE_URL`) |

**Redis (6 separate instances)**
| Variable | Port | Purpose | Persistence |
|----------|------|---------|-------------|
| `REDIS_URL` | 6379 | General cache / temp data | Ephemeral |
| `REDIS_ACCOUNTS_URL` | 6380 | Account credit cache | Persistent (AOF) |
| `REDIS_CHAIN_URL` | 6381 | Blockchain operation cache | Persistent (AOF) |
| `REDIS_QUEUES_URL` | 6382 | Work queues for async workers | Persistent, 2GB, LRU |
| `REDIS_RATE_LIMITING_URL` | 6383 | Rate limit counters | Ephemeral, 1GB |
| `REDIS_ACL_URL` | 6384 | ACL / permission cache | Ephemeral, 2GB, LRU |

**Arion Storage Backend**
| Variable | Description |
|----------|-------------|
| `HIPPIUS_ARION_BASE_URL` | Arion storage endpoint (default: `https://arion.hippius.com/`) |
| `ARION_SERVICE_KEY` | Arion service authentication key |
| `ARION_BEARER_TOKEN` | Arion bearer token |
| `HIPPIUS_ARION_VERIFY_SSL` | SSL verification (default: `true`) |

**Blockchain**
| Variable | Description |
|----------|-------------|
| `HIPPIUS_SUBSTRATE_URL` | Blockchain RPC URL (default: `wss://rpc.hippius.network`) |
| `HIPPIUS_VALIDATOR_REGION` | Validator region identifier (default: `decentralized`) |
| `HIPPIUS_API_BASE_URL` | Hippius blockchain API (default: `https://api.hippius.com/api`) |

**Authentication & Security**
| Variable | Description |
|----------|-------------|
| `HIPPIUS_SERVICE_KEY` | API key for Hippius service (64 char hex) |
| `HIPPIUS_AUTH_ENCRYPTION_KEY` | Encryption key for auth tokens (64 char hex) |
| `FRONTEND_HMAC_SECRET` | HMAC secret for frontend endpoints |

**Backend Routing**
| Variable | Default | Description |
|----------|---------|-------------|
| `HIPPIUS_UPLOAD_BACKENDS` | `arion` | Backends for uploads |
| `HIPPIUS_DOWNLOAD_BACKENDS` | `arion` | Backends for downloads (tried in order) |
| `HIPPIUS_DELETE_BACKENDS` | `arion` | Backends for deletions |

### Optional

**KMS / Encryption**
| Variable | Description |
|----------|-------------|
| `HIPPIUS_KMS_MODE` | `disabled` (dev) or `required` (production/staging) |
| `HIPPIUS_OVH_KMS_ENDPOINT` | OVH KMS server URL |
| `HIPPIUS_OVH_KMS_DEFAULT_KEY_ID` | Default KMS key ID |
| `HIPPIUS_OVH_KMS_OKMS_ID` | OVH KMS identifier |

**Feature Flags**
| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_AUDIT_LOGGING` | `true` | Operation audit trails |
| `ENABLE_BANHAMMER` | `true` | IP-based blocking |
| `HIPPIUS_BYPASS_CREDIT_CHECK` | `false` | Skip credit verification (testing only) |
| `ENABLE_REQUEST_PROFILING` | `false` | Request profiling |
| `ENABLE_API_DOCS` | `true` | Swagger UI |

**Monitoring**
| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_MONITORING` | `false` | OpenTelemetry instrumentation |
| `LOKI_ENABLED` | `false` | Loki log aggregation |
| `SENTRY_DSN` | (empty) | Sentry error tracking |

See `.env.defaults` for the full list of configurable values.

## Docker Compose Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Base config: API, gateway, PostgreSQL, 6 Redis instances, Arion workers |
| `docker-compose.prod.yml` | Production overrides: performance tuning, backup, health monitoring |
| `docker-compose.staging.yml` | Staging configuration |
| `docker-compose.e2e.yml` | E2E testing: mock services (mock-arion, mock-kms, mock-hippius-api, toxiproxy) |
| `docker-compose.monitoring.yml` | LGTM observability stack |

### Running the System

**Development**
```bash
docker compose up -d
docker compose logs -f api
```

**Production**
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

**Staging**
```bash
docker compose -f docker-compose.yml -f docker-compose.staging.yml up -d
```

**With Monitoring**
```bash
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

**E2E Testing**
```bash
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --wait
pytest tests/e2e -v
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml down -v
```

## Usage Examples

### MinIO Client (Python)

```python
from minio import Minio
import base64

seed_phrase = "your twelve word seed phrase here"
encoded_key = base64.b64encode(seed_phrase.encode('utf-8')).decode('utf-8')

client = Minio(
    "s3.hippius.com",
    access_key=encoded_key,
    secret_key=seed_phrase,
    secure=True,
    region="decentralized"
)

client.put_object("my-bucket", "file.txt", data, length)
```

### boto3

```python
import boto3
import base64

seed_phrase = "your twelve word seed phrase here"

client = boto3.client(
    "s3",
    endpoint_url="https://s3.hippius.com",
    aws_access_key_id=base64.b64encode(seed_phrase.encode()).decode(),
    aws_secret_access_key=seed_phrase,
    region_name="decentralized",
)

client.put_object(Bucket="my-bucket", Key="file.txt", Body=b"hello")
```

### AWS CLI

```bash
aws configure set aws_access_key_id "base64-encoded-seed-phrase"
aws configure set aws_secret_access_key "your-seed-phrase"
aws configure set default.region "decentralized"
aws configure set default.s3.signature_version "s3v4"

aws s3 mb s3://my-bucket/ --endpoint-url https://s3.hippius.com
aws s3 cp file.txt s3://my-bucket/ --endpoint-url https://s3.hippius.com
```

See [examples/py/](examples/py/) and [examples/js/](examples/js/) for more complete examples.

## Authentication

Five methods, evaluated in priority order:

1. **Presigned URL** - Query params (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Signature`)
2. **Bearer Token** - `Authorization: Bearer <token>` (`hip_*` prefixed tokens)
3. **Access Key** - `hip_*` credentials in AWS SigV4 Authorization header
4. **Seed Phrase SigV4** - Base64-encoded 12-word seed as access key, plain seed as secret key
5. **Anonymous** - GET/HEAD on public buckets (no Authorization header)

Get access keys at: https://console.hippius.com/dashboard/settings

## GitHub Actions Secrets

Secrets required for CI/CD deployment (configured in `.github/workflows/k8s-deploy.yaml`):

| Category | Secret | Description |
|----------|--------|-------------|
| **Infrastructure** | `KUBE_CONFIG` | Base64-encoded kubeconfig |
| **Database** | `DATABASE_PASSWORD` | PostgreSQL password |
| **Redis** | `REDIS_URL` | Main Redis cluster |
| | `REDIS_ACCOUNTS_URL` | Account cache Redis |
| | `REDIS_CHAIN_URL` | Blockchain cache Redis |
| | `REDIS_QUEUES_URL` | Work queue Redis |
| | `REDIS_RATE_LIMITING_URL` | Rate limit Redis |
| | `REDIS_ACL_URL` | ACL cache Redis |
| **Arion** | `HIPPIUS_ARION_BASE_URL` | Arion storage URL |
| | `ARION_SERVICE_KEY` | Arion service auth key |
| | `ARION_BEARER_TOKEN` | Arion bearer token |
| | `HIPPIUS_ARION_VERIFY_SSL` | SSL verification flag |
| **Auth** | `HIPPIUS_SERVICE_KEY` | Hippius API auth key |
| | `HIPPIUS_AUTH_ENCRYPTION_KEY` | Auth token encryption |
| | `FRONTEND_HMAC_SECRET` | Frontend request signing |
| **KMS** | `HIPPIUS_KMS_MODE` | KMS operation mode |
| | `HIPPIUS_OVH_KMS_ENDPOINT` | OVH KMS endpoint |
| | `HIPPIUS_OVH_KMS_DEFAULT_KEY_ID` | Default KMS key |
| | `HIPPIUS_OVH_KMS_OKMS_ID` | OVH KMS identifier |
| **Queues** | `HIPPIUS_UPLOAD_QUEUE_NAMES` | Upload queue names |
| | `HIPPIUS_DOWNLOAD_QUEUE_NAMES` | Download queue names |
| **Backend Routing** | `HIPPIUS_UPLOAD_BACKENDS` | Upload backend config |
| | `HIPPIUS_DOWNLOAD_BACKENDS` | Download backend config |
| | `HIPPIUS_DELETE_BACKENDS` | Delete backend config |
| **Backup** | `OVH_BACKUP_ACCESS_KEY_ID` | OVH S3 backup credentials |
| | `OVH_BACKUP_SECRET_ACCESS_KEY` | OVH S3 backup secret |
| | `OVH_BACKUP_BUCKET` | OVH backup bucket |
| | `OVH_BACKUP_ENDPOINT_URL` | OVH endpoint URL |
| **Monitoring** | `SENTRY_DSN` | Sentry error tracking |
| | `CACHET_API_KEY` | Status page API key |
| | `CACHET_COMPONENT_ID` | Status page component ID |
| | `DISCORD_WEBHOOK_URL` | Discord notifications |
| **Other** | `RESUBMISSION_SEED_PHRASE` | Retry failed operations |

## Project Structure

```
hippius_s3/            Main API application
  api/                 S3-compatible REST API (internal, port 8000)
    s3/                Bucket, object, multipart, tagging endpoints
    middlewares/        IP whitelist, profiler, input validation, metrics
  services/            Business logic (crypto, KMS, Arion client, copy, audit)
  workers/             Worker core logic (uploader, downloader, unpinner)
  writer/              Write pipeline (chunker, write-through, DB)
  reader/              Read pipeline (planner, fetcher, decrypter, streamer)
  repositories/        Database access layer
  cache/               Multi-layer cache (Redis + filesystem)
  dlq/                 Dead letter queues (upload, unpin)
  sql/                 Migrations and parameterized queries
  scripts/             Operational scripts (migrate, requeue, nuke, purge)
gateway/               Public-facing FastAPI gateway (port 8080)
  middlewares/         Auth, SigV4, ACL, rate limit, banhammer, audit, CORS
  services/            Auth orchestrator, ACL, account, forwarding
workers/               Worker entry points (run_*_in_loop.py)
cacher/                Substrate account data cacher
tests/                 Unit, integration, E2E, ACL test suites
benchmark/             Performance benchmarking tools
examples/              Python and JavaScript client examples
docs/                  Architecture and specification docs
k8s/                   Kubernetes manifests (base, staging, production, otel)
monitoring/            Grafana dashboards and observability config
```

## Development

### Local Setup

```bash
# Install dependencies
uv pip install -e ".[dev]"

# Run tests
pytest tests/unit -v
pytest tests/integration -v
pytest tests/e2e -v

# Code quality
ruff format .
ruff check . --fix
mypy hippius_s3

# Pre-commit hooks
pre-commit run --all-files
```

### Configuration

Layered environment files:
- `.env.defaults` - Base configuration with feature flags
- `.env` - Production/development overrides
- `.env.test-local` - Local testing (host-level pytest)
- `.env.test-docker` - Docker E2E testing

## Monitoring

Add `docker-compose.monitoring.yml` for the full LGTM observability stack:

```bash
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | http://localhost:3000 | Dashboards (admin/admin) |
| Prometheus | http://localhost:9090 | Metrics |
| Loki | http://localhost:3100 | Log aggregation |
| Tempo | http://localhost:3200 | Distributed tracing |
| OTel Collector | localhost:4317/4318 | OTLP receiver |
| App Metrics | http://localhost:8080/metrics | Prometheus endpoint |

Pre-built Grafana dashboards: Hippius S3 Overview (API performance, request rates, error rates) and S3 Workers (queue depths, processing rates, backend latency).

## Daily S3 Benchmark (last 30 days)

![Daily benchmark](https://s3.hippius.com/hippius-benchmarks/daily.svg)

Automated daily benchmark measuring PUT/GET throughput for 1MB, 100MB, and 1GB objects against production. Runs via GitHub Actions at 6 AM UTC ([`.github/workflows/daily-bench.yml`](.github/workflows/daily-bench.yml)).

## License

See [LICENSE](LICENSE) file.
