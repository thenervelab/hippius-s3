Hippius S3 Object Migration Plan

Overview

- Migrate objects whose current version’s storage layout is older than the configured target storage version.
- Preserve original part boundaries and stream per part (≤ ~1 GB per part) to bound memory.
- Reuse existing reader/writer flows; add minimal helpers for creating a migration version row and atomically swapping the current version pointer.
- Ensure safety with soft md5 checks during migration and a final CAS swap on the pointer.
- Provide a manual cleanup process for non-current migration versions.

Targeting & Selection

- All objects: select where the current version’s storage_version < target.
- Bucket scope: restrict to a bucket.
- Single object: specify bucket and key.

Selection join (must target the current version):

```sql
SELECT o.object_id,
       o.bucket_id,
       o.object_key,
       ov.object_version,
       ov.storage_version,
       ov.content_type,
       ov.metadata
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = o.current_object_version
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE ov.storage_version < $target
  [AND b.bucket_name = $bucket]
  [AND o.object_key = $key]
```

Identity & Keys

- Use the bucket owner’s main account and standard seed handling (same as GetObject/Copy).
- Encryption keys from get_or_create_encryption_key_bytes; no changes required.

Per-Object Migration Flow

1. Snapshot source markers (CAS):
   - expected_old_version = objects.current_object_version
   - expected_md5 = md5_hash of the current version (from object_versions)
2. Remove any prior non-current migration version(s) for this object (restart policy): delete their parts and object_versions rows.
3. Create migration version row (minimal helper):
   - next_version = (SELECT COALESCE(MAX(object_version),0)+1 FROM object_versions WHERE object_id=$1)
   - INSERT INTO object_versions with:
     - object_version = next_version
     - version_type = 'migration'
     - storage_version = target_storage_version
     - content_type, metadata copied from the current version
     - status = 'publishing', append_version = 0
4. Stream and upload per-part:
   - Read manifest via read_parts_manifest(object_id)
   - Build plan via build_chunk_plan(...)
   - For each part in order:
     - Stream only that part via stream_plan(...)
     - Accumulate bytes for that part only and call ObjectWriter.mpu_upload_part(..., object_version=next_version)
     - Soft md5 check: re-fetch current version md5_hash; if changed from expected_md5, abort, delete migration version + parts, refresh markers, and restart
5. Finalize:
   - ObjectWriter.mpu_complete(..., object_version=next_version)
   - Do not modify status; workers will continue lifecycle
6. Atomic pointer swap (CAS):
   - UPDATE objects SET current_object_version=$new WHERE object_id=$id AND current_object_version=$expected_old
   - If no row updated, treat as conflict (object changed) → delete migration version + parts and restart

Notes

- For single-part objects, the same flow applies; finalize will reflect multipart behavior via existing logic.
- We do not touch status; lifecycle workers manage it.

Minimal Writer Additions

- create_version_for_migration(object_id, content_type, metadata, storage_version_target) -> int
  - Compute next_version and INSERT migration row in object_versions; return next_version
- swap_current_version_cas(object_id, expected_old_version, new_version) -> bool
  - Perform CAS update of objects.current_object_version

Existing Writer Methods (Reused)

- mpu_upload_part(..., object_version=new_version)
- mpu_complete(..., object_version=new_version)

Queries to Introduce

- list_objects_to_migrate.sql — selection query shown above
- create_migration_version.sql — insert migration object_versions row with computed next_version
- swap_current_version_cas.sql — CAS update for objects.current_object_version
- delete_version_and_parts.sql — delete parts for (object_id, object_version) then delete the version row

Cleanup (Manual for Now)

- Select versions: version_type='migration' AND object_version <> objects.current_object_version (optional bucket/key filters)
- For each:
  - Delete parts for (object_id, object_version)
  - Delete object_versions row
  - Optionally enqueue IPFS unpin (if applicable)

CLI & Container

- Migration CLI: scripts/migrate_objects.py
  - Flags: --all | --bucket <name> [--key <key>] | --key requires --bucket, plus --concurrency, --dry-run
  - Uses config target_storage_version
- Cleanup CLI: scripts/cleanup_migration_versions.py
  - Flags: --bucket, --key (age filters may be added later)
- Optional one-shot migrator service (compose) that runs the migration CLI and exits (env: MIGRATE_ALL, MIGRATE_BUCKET, MIGRATE_KEY, CONCURRENCY, DRY_RUN)

Concurrency & Safety

- Process a limited number of objects concurrently; per-object parts are uploaded sequentially to bound memory.
- Two safety layers:
  - Soft md5 check after each part
  - CAS swap on objects.current_object_version at cutover
- On mismatch or CAS failure, delete the in-progress migration version and restart against the latest state.

Telemetry (Optional)

- Track attempted, migrated, restarted, conflicts, failures; timings per object/part; include bucket/key context in logs.

Future Enhancements

- Configurable grace period and automatic cleanup for old migration versions
- Backpressure via downloader/uploader health signals
- Partial retries per part on transient errors
