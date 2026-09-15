# tests/unit/

Pure unit tests. No external services — mocks for DB, Redis, Arion, KMS. Fast; runs on every save.

## Layout

```
tests/unit/
├── cache/                     # FS store + parts cache + notifier
├── gateway/                   # SigV4, auth, ACL scope
├── writer/                    # object_writer, chunker, write_through
├── services/                  # crypto, envelope, KEK, arion
├── test_janitor_hot_retention.py     # Absolute no-deletion under non-replication
└── conftest.py
```

## Running

```bash
pytest tests/unit -v
pytest tests/unit -xvs -k coalescing           # single test file by name
pytest tests/unit/cache -xvs
```

`-x` stops on first failure, `-v` verbose, `-s` shows stdout.

## Conventions

- **Mock external services**: `httpx`, asyncpg, Redis all mocked with `pytest-mock` fixtures or `unittest.mock`. DO NOT hit a real Redis or Postgres from here — use the integration tier.
- **Deterministic seed phrases / access keys**: [conftest.py](conftest.py) pins a fixed seed for repeatable SigV4 generation.
- **Bypass credit check**: `HIPPIUS_BYPASS_CREDIT_CHECK=true` is set in test env (enforced at [config.py:538](../../hippius_s3/config.py)).
- **ENABLE_BANHAMMER=false** in test env so the rate-limiter doesn't interfere.

## Tests worth knowing about

- [test_janitor_hot_retention.py](test_janitor_hot_retention.py) — exercises the absolute "no-deletion-of-non-replicated-data" invariant, including the critical-pressure ERROR-log-and-refuse branch.

## Running specific suites

```bash
pytest tests/unit/cache -xvs              # FS store, RedisObjectPartsCache, ChunkNotifier
pytest tests/unit/gateway -xvs            # SigV4, ACL scope, auth
pytest tests/unit -k "uploader or unpinner" -xvs   # Uploader/unpinner
pytest tests/unit/writer -xvs             # ObjectWriter + write-through
```

## New tests

Put them under the matching subdirectory. Naming: `test_<module>_<behavior>.py`. If the test needs a FS cache, use the `tmp_path` fixture from pytest; DO NOT rely on shared on-disk state across tests.

