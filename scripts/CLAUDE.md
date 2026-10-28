# scripts/ (top-level)

Top-level ops scripts. Smaller in scope than [hippius_s3/scripts/](../hippius_s3/scripts/) — these are usually one-off utilities that don't need full app wiring.

## Inventory

| Script | Purpose |
|---|---|
| [wait_for_migrations.py](wait_for_migrations.py) | initContainer gate used by every worker manifest: blocks until the schema is current. Load-bearing. |
| [retryable-mpu.py](retryable-mpu.py) | Multipart upload retry helper for manual use when a client lost an MPU mid-flight. Notes in [retryable-mpu.md](retryable-mpu.md). |
| [system-check.sh](system-check.sh) | Interactive cluster health sweep (redis-cluster, pods). |
| [drain_enqueue_sweep_backfill.sql](drain_enqueue_sweep_backfill.sql) | One-off psql backfill for replicated-but-unpublished drain rows. |
| [pool_decommission_audit.sql](pool_decommission_audit.sql) | Read-only audit: what still depends on the CephFS pool. |
| [retire_backend.sql](retire_backend.sql) | Batched retirement of a storage backend no longer in `STORAGE_BACKENDS`. |

## Invocation

These are plain scripts — no special setup beyond the venv:

```bash
source .venv/bin/activate
python scripts/retryable-mpu.py --help
```

If a script takes a DB URL, it reads from `DATABASE_URL` or a `--dsn` flag; if it takes an S3 endpoint, it uses `boto3` with standard AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`).

## Related

- Performance benchmarks live in the separate `hippius-benchmarks` repo.
- DB migrations are in [hippius_s3/scripts/](../hippius_s3/scripts/), not here.
