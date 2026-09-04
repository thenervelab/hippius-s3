# scripts/ (top-level)

Top-level ops scripts. Smaller in scope than [hippius_s3/scripts/](../hippius_s3/scripts/) — these are usually one-off utilities that don't need full app wiring.

## Inventory

| Script | Purpose |
|---|---|
| [gen_clean_dump.py](gen_clean_dump.py) | Produce a filtered Postgres dump where rows with `object_versions.size_bytes=0` or no Arion `chunk_backend` row are removed. Used to seed a clean local/staging DB from prod data without importing the broken-v5 detritus. |
| [deploy_smoke.py](deploy_smoke.py) | Post-deploy smoke-test harness invoked from CI. Exercises a small matrix of PUT/GET/LIST against the deploy target. |
| [locality_probe.py](locality_probe.py) | Proves the edge routes every request for one object key to the same api-local node by capturing `X-Hippius-Node` from every response (PUT/GET/HEAD agreement, multipart co-location, bucket-level spread, range/versioned/presigned reads, opt-in misplaced-object drill). Usage in [docs/locality-routing.md](../docs/locality-routing.md) under "Verification header"; reads `HIPPIUS_ROUTING_ENDPOINT` + AWS creds from env and exits 0 with a one-line notice when they are unset. Pure logic is unit-tested in `tests/unit/scripts/test_locality_probe.py`. |
| [retryable-mpu.py](retryable-mpu.py) | Multipart upload retry helper for manual use when a client lost an MPU mid-flight. Notes in [retryable-mpu.md](../retryable-mpu.md) (or wherever the doc landed — check git log). |

## Invocation

These are plain scripts — no special setup beyond the venv:

```bash
source .venv/bin/activate
python scripts/gen_clean_dump.py --help
```

If a script takes a DB URL, it reads from `DATABASE_URL` or a `--dsn` flag; if it takes an S3 endpoint, it uses `boto3` with standard AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`).

## Related

- Performance benchmarks now live in the separate `hippius-benchmarks` repo.
- DB migrations are in [hippius_s3/scripts/](../hippius_s3/scripts/), not here.

