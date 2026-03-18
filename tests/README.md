# Tests

## Test Categories

| Category | Directory | Description | External Deps |
|----------|-----------|-------------|---------------|
| Unit | `tests/unit/` | Isolated function testing, mocked services | None |
| Integration | `tests/integration/` | Worker logic with real DB/Redis, mocked externals | PostgreSQL, Redis |
| E2E | `tests/e2e/` | Full API testing with real S3 clients | Full Docker stack |
| ACL | `tests/e2e/acl/` | Access control and permission testing | Full Docker stack |

## Running Tests

```bash
# By directory
pytest tests/unit -v
pytest tests/integration -v
pytest tests/e2e -v

# By marker
pytest -m unit
pytest -m integration
pytest -m e2e

# Single test
pytest tests/e2e/test_GetObject.py::test_get_object_downloads_and_matches_headers -xvs

# E2E with Docker stack
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --wait
pytest tests/e2e -v
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml down -v

# Against real AWS S3
RUN_REAL_AWS=1 AWS_REGION=us-east-1 pytest tests/e2e -v -m 'not local'
```

## Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.unit` | Unit tests (no external deps) |
| `@pytest.mark.integration` | Integration tests (real DB/Redis) |
| `@pytest.mark.e2e` | End-to-end tests (full stack) |
| `@pytest.mark.s4` | S4 extension tests (skipped on real AWS) |
| `@pytest.mark.local` | Local-only tests (skipped on real AWS) |
| `@pytest.mark.hippius_cache` | Tests manipulating Hippius Redis cache (skipped on real AWS) |
| `@pytest.mark.hippius_headers` | Tests asserting Hippius-specific headers (skipped on real AWS) |

## Key Fixtures

**Session-scoped (E2E)**:
- `test_seed_phrase` - Fixed test seed phrase for auth
- `test_access_key` / `test_access_key_secret` - `hip_*` access key credentials
- `docker_services` - Docker Compose stack lifecycle

**Per-test (E2E)**:
- `boto3_client` - Boto3 S3 client (seed phrase auth)
- `boto3_access_key_client` - Boto3 S3 client (access key auth)
- `signed_http_get` - Helper for signed GET with Range headers
- `unique_bucket_name` - Generates unique bucket names
- `cleanup_buckets` - Cleans up test buckets after test

**Integration**:
- `gateway_app` / `gateway_client` - FastAPI test app and async client
- `gateway_db_pool` / `gateway_redis_clients` - Mock database and Redis
- `stopped_worker` - Helper to stop/restart workers during tests

## Environment

Tests load environment from layered files:
- Unit/Integration: `.env.defaults` + `.env.test-local`
- E2E: `.env.defaults` + `.env.test-docker`

All test environments set `HIPPIUS_BYPASS_CREDIT_CHECK=true`.

## Code Quality

```bash
ruff format .
ruff check . --fix
mypy hippius_s3
pre-commit run --all-files
```
