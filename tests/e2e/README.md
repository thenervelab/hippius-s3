# E2E Tests for Hippius S3

End-to-end tests that verify the full Hippius S3 pipeline from S3 client through the gateway and API to the Arion storage backend.

## Setup

The E2E tests run against a Docker Compose stack with mock services replacing external dependencies:

| Service | Port | Purpose |
|---------|------|---------|
| `mock-arion` | 8002 | Mock Arion storage backend |
| `mock-hippius-api` | 8001 | Mock Hippius blockchain API |
| `mock-kms` | 8443 | Mock KMS server (mTLS) |
| `toxiproxy` | 8474 | Network fault injection proxy |

### Starting the Stack

```bash
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --wait
```

### Running Tests

```bash
# All E2E tests
pytest tests/e2e/ -v

# Single test
pytest tests/e2e/test_GetObject.py::test_get_object_downloads_and_matches_headers -xvs

# Teardown
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml down -v
```

## Current Setup (with bypass flags)

Tests use environment variable bypasses to skip blockchain credit checks:

- `HIPPIUS_BYPASS_CREDIT_CHECK=true` - Skips credit verification for write operations

These bypasses allow tests to run without a live Substrate chain or funded accounts.

## Run Against Real AWS S3

You can run the same tests against AWS S3 to validate test expectations:

```bash
RUN_REAL_AWS=1 AWS_REGION=us-east-1 pytest tests/e2e -v -m 'not local'
```

Requirements:
- AWS credentials configured (env vars, default profile, or other supported methods)
- Tests marked `local`, `s4`, `hippius_cache`, or `hippius_headers` are automatically skipped
- Buckets are created with unique names and cleaned up, but AWS charges may apply

## Test Flow

1. **Auth**: Seed phrase base64-encoded as access key, plain seed as secret (SigV4)
2. **Bucket creation**: `PUT /{bucket}`
3. **Object upload**: `PUT /{bucket}/{key}` -> chunks written to FS cache + Redis queue -> Arion uploader processes
4. **Object read**: `GET /{bucket}/{key}` -> served from FS cache (write-through)
5. **Cleanup**: Delete objects and buckets

## Making Tests Truly E2E (Removing Bypasses)

To run against a real blockchain:

1. Remove `HIPPIUS_BYPASS_CREDIT_CHECK=true` from `docker-compose.e2e.yml`
2. Set up a local Hippius/Substrate devnet
3. Fund test accounts with credits
4. Add on-chain verification steps after uploads

### Migration Steps

1. Add bypass flags (current state)
2. Set up local devnet infrastructure
3. Fund test accounts
4. Remove bypass flags from code
5. Add on-chain verification
6. Update CI/CD to use real chain

## Security Notes

- Never commit funded seed phrases to version control
- Use CI secrets for funded accounts
- Consider rotating test seeds periodically
- Monitor chain costs for test accounts
