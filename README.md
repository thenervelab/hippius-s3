# Hippius S3

A high-performance S3-compatible gateway for Hippius' decentralized IPFS storage network. This service provides AWS S3 API compatibility while storing data on IPFS with built-in authentication, rate limiting, and audit logging.

## Overview

Hippius S3 is a production-ready S3-compatible API that stores data on IPFS while automatically publishing to the Hippius blockchain. It provides standard S3 operations with HMAC-based authentication and seamless integration with existing S3 clients (AWS CLI, boto3, MinIO).

## Features

**Core S3 Operations**: Bucket management, object operations, multipart uploads, metadata, tagging, ACLs, lifecycle policies

**Security**: HMAC authentication, rate limiting, IP-based protection, input validation, account credit verification

**Blockchain Integration**: Automatic IPFS storage and blockchain publishing with transaction tracking

**Production Ready**: Audit logging, health checks, multi-tenant support, CORS, performance profiling

**S3 Client Compatibility**: AWS CLI, MinIO Client, boto3, s3cmd

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development)

## Requirements

### Required Environment Variables

Create a `.env` file with these critical variables:

**Environment**
- `ENVIRONMENT` - Deployment environment (test/dev/staging/production)

**Database**
- `DATABASE_URL` - PostgreSQL connection string
- `HIPPIUS_KEYSTORE_DATABASE_URL` - PostgreSQL for encryption keys (falls back to DATABASE_URL)

**Redis (5 separate instances required)**
- `REDIS_URL` - General cache and temp data
- `REDIS_ACCOUNTS_URL` - Account credit cache (persistent)
- `REDIS_CHAIN_URL` - Blockchain operation cache (persistent)
- `REDIS_QUEUES_URL` - Work queues for async workers
- `REDIS_RATE_LIMITING_URL` - Rate limit counters

**IPFS**
- `HIPPIUS_IPFS_GET_URL` - IPFS gateway for reads (e.g., http://ipfs:8080)
- `HIPPIUS_IPFS_STORE_URL` - IPFS API for writes (e.g., http://ipfs:5001)

**Authentication & Security**
- `HIPPIUS_SERVICE_KEY` - API key for Hippius service (64 char hex string)
- `HIPPIUS_AUTH_ENCRYPTION_KEY` - Encryption key for auth (64 char hex string)
- `FRONTEND_HMAC_SECRET` - HMAC secret for frontend endpoints

**Blockchain**
- `HIPPIUS_SUBSTRATE_URL` - Blockchain RPC URL (wss://rpc.hippius.network)
- `HIPPIUS_VALIDATOR_REGION` - Validator region identifier
- `RESUBMISSION_SEED_PHRASE` - 12-word seed phrase for blockchain resubmissions

**Note**: See `.env.example` for all configuration options and `.env.defaults` for feature flags.

## Quick Start

### Setup

```bash
# Create Docker network
docker network create hippius_net

# Copy and configure environment
cp .env.example .env
# Edit .env with required variables (see Requirements section above)
```

### Docker Compose Files

The project includes multiple compose files for different environments:

**docker-compose.yml** - Base configuration
- Core services: API, gateway, PostgreSQL, IPFS, 5 Redis instances
- Workers: uploader, downloader, unpinner, janitor, account-cacher
- Use for local development

**docker-compose.prod.yml** - Production overrides
- Performance tuning (PostgreSQL, Redis optimizations)
- Includes utilities (backup, health monitoring)

**docker-compose.staging.yml** - Staging configuration
- Minimal staging setup with utilities

**docker-compose.e2e.yml** - E2E testing environment
- Test-specific services (mock APIs, toxiproxy)
- Bypass flags for faster tests

**docker-compose.monitoring.yml** - Observability stack
- Prometheus, Grafana, Loki, Tempo, OpenTelemetry
- Add to any environment for full monitoring

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

Database migrations run automatically on container startup via `hippius_s3.scripts.migrate`.

## Usage Examples

### Using MinIO Client

```python
from minio import Minio
import base64

seed_phrase = "your twelve word seed phrase here"
encoded_key = base64.b64encode(seed_phrase.encode('utf-8')).decode('utf-8')

client = Minio(
    "http://localhost:8080",
    access_key=encoded_key,
    secret_key=seed_phrase,
    secure=False,
    region="decentralized"
)

client.put_object(
    "my-bucket",
    "file.txt",
    data,
    length
)
```

### Using AWS CLI

```bash
aws configure set aws_access_key_id "base64-encoded-seed-phrase"
aws configure set aws_secret_access_key "your-seed-phrase"
aws configure set default.region "decentralized"
aws configure set default.s3.signature_version "s3v4"

aws s3 mb s3://my-bucket/ --endpoint-url http://localhost:8080
aws s3 cp file.txt s3://my-bucket/ --endpoint-url http://localhost:8080
```

## Architecture

```
Client (AWS CLI/MinIO/boto3)
    ↓ [HTTPS + AWS SigV4]
Gateway (Auth + ACL + Rate Limiting)
    ↓ [HTTP]
Hippius S3 API
    ↓ [Background Workers]
IPFS Network + Blockchain
```

### Project Structure

- **`hippius_s3/`** - Main API application with S3-compatible endpoints
- **`gateway/`** - Authentication and access control gateway
- **`workers/`** - Background workers for async operations
- **`tests/`** - Test suites (unit, integration, E2E, ACL)
- **`docs/`** - Documentation and specifications
- **`monitoring/`** - Observability stack configuration

See [CLAUDE.md](CLAUDE.md) for detailed architecture documentation.

## Development

### Local Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/unit -v
pytest tests/integration -v
pytest tests/e2e -v

# Format and type check
ruff format .
ruff check . --fix
mypy hippius_s3
```

### Configuration

The project uses layered environment configuration:
- `.env.defaults` - Base configuration with feature flags
- `.env` - Production/development overrides
- `.env.test-local` - Local testing configuration
- `.env.test-docker` - Docker E2E testing configuration

See [CLAUDE.md](CLAUDE.md) for detailed configuration guide.

### API Compatibility

See [docs/s3-compatibility.md](docs/s3-compatibility.md) for S3 compatibility matrix.

See [docs/s4.md](docs/s4.md) for S4 extensions (atomic append operations).

## Monitoring

### Enabling Monitoring

To enable the full observability stack, add `docker-compose.monitoring.yml`:

```bash
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

### What's Included

The monitoring stack provides comprehensive observability:

- **Prometheus** (localhost:9090) - Metrics collection with 30-day retention
- **Grafana** (localhost:3000) - Dashboards and visualization
- **Loki** (localhost:3100) - Log aggregation with 31-day retention
- **Tempo** (localhost:3200) - Distributed tracing with 1-hour retention
- **OpenTelemetry Collector** (localhost:4317/4318) - OTLP receiver and processor

### Accessing Monitoring Tools

**Grafana Dashboard** (Primary Interface)
- URL: http://localhost:3000
- Default credentials: `admin` / `admin`
- Pre-configured datasources: Prometheus, Loki, Tempo
- Pre-built dashboards:
  - Hippius S3 Overview (API performance, request rates, error rates)
  - S3 Workers (queue depths, processing rates, IPFS latency)

**Prometheus**
- URL: http://localhost:9090
- Direct metrics querying and exploration
- View scrape targets and health status

**Application Metrics**
- API: http://localhost:8080/metrics
- Direct Prometheus metrics endpoint


## License

See [LICENSE](LICENSE) file.
