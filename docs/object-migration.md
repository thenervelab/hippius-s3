Hippius S3 Object Migration

Purpose

- Migrate existing objects whose current version uses an old storage layout to the configured target storage version.
- Preserve part boundaries, stream per-part, and cut over atomically to the new version.

Key Concepts

- Versions: Each object has a `current_object_version` and a row in `object_versions` with per-version metadata.
- Migration versions: Internal versions created with `version_type='migration'`; not exposed via S3 APIs.
- Cutover: After all parts are uploaded and finalized, the pointer swaps to the new version.
- Cleanup: A separate job unpins and deletes non-current migration versions after a grace period.

What the migration does

1. Select objects where the current version’s `storage_version` < `HIPPIUS_TARGET_STORAGE_VERSION` (config).
2. Create a new migration version (`version_type='migration'`) using an advisory-locked, transactional insert.
3. Build a version-aware streaming plan via `build_stream_context`.
4. Stream each part, re-encrypt as needed, and upload that part into the new migration version.
5. If the source changes during migration (MD5/append_version/last_modified differ), mark the migration version as `failed` and stop.
6. When all parts succeed, finalize the migration version and CAS-swap `objects.current_object_version` to the new version.

What it does NOT do

- It does not delete old versions inline.
- It does not inline-unpin on abort; failed versions are marked and cleaned by the cleanup tool.

CLI Usage

- Migration (one-shot):

  - `uv run hippius_s3/scripts/migrate_objects.py --bucket <name> [--key <key>] --concurrency 4`
  - Flags:
    - `--bucket <name>`: scope to one bucket (required for `--key`)
    - `--key <key>`: migrate a single object
    - `--concurrency N`: max objects processed concurrently (default 4)
    - `--dry-run`: print plan without executing

- Cleanup (manual):
  - `uv run hippius_s3/scripts/cleanup_migration_versions.py [--bucket <name>] [--key <key>] [--min-age-minutes 30] [--limit 1000] [--dry-run]`
  - Behavior:
    - Selects non-current `version_type='migration'` versions older than `--min-age-minutes` (default 30)
    - Enqueues IPFS unpin (if a CID exists)
    - Deletes parts and the version row

Containerized runner (optional)

- Build: from repo root, `docker build -f migrator/Dockerfile -t hippius-migrator .`
- Run: set envs and run `hippius-migrator`. Supported envs:
  - `MIGRATE_BUCKET`, `MIGRATE_KEY`, `MIGRATE_DRY_RUN`
  - `MIGRATE_CONCURRENCY`

Operational Safety

- Async-safe aborts: On failure, the migrator only marks the migration version as `failed`. Background uploads will be refused by writer gates (status check).
- Writer gates: `mpu_upload_part` / `mpu_complete` only accept work when `status='publishing'`.
- Cutover: After finalize, a CAS update swaps the current pointer; readers remain consistent.
- Cleanup is the single source of truth for unpin+delete; it includes an age guard.

Monitoring & Reporting

- Migrator logs per-object start/done/failed, and prints an end-of-run summary (migrated, failed, planned).
- Cleanup logs total versions deleted; consider scheduling with metrics dashboards.

Notes & Limits

- Per-part buffering: the migrator accumulates one part in memory before upload; large parts can spike RAM.
- Concurrency: tune `--concurrency` to balance throughput and resource limits.
- Large selections: for very large datasets, add batching to the selection query to avoid materializing thousands of tasks.

Troubleshooting

- Failures due to source changes: rerun migration; the new baseline markers will reflect the updated source.
- Writer rejects on failed status: expected after abort; run cleanup to remove stale versions.
