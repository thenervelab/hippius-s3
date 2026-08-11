# Prod pre-steps for the SSD read-tier release

Run these **before** merging `staging → main`. Everything here is designed so the automatic
migration that follows is a no-op on prod, while staying the source of truth for staging, CI and
dev — where the tables are small and the in-transaction path is the safer one.

## Why by hand at all

Migration `0018_replication_reclaimed_marker` adds a column and then builds a partial index on
`cephor_replication_status` (~11.4M rows on prod). sqlx wraps a migration file without
`-- no-transaction` in **one transaction**, so the `ACCESS EXCLUSIVE` lock taken by
`ALTER TABLE ... ADD COLUMN` is held until COMMIT — through the whole index build, which is a
full heap scan. `ACCESS EXCLUSIVE` blocks readers, not just writers.

Blocked for that window: the entire drain fleet (`claim_part`, `release_part`, `mark_replicated`,
`record_landed_part`) and — on a **client** path — `CompleteMultipartUpload`, which runs
`wake_version_replication` against the same table. `lock_timeout` bounds acquisition, not the
build, so it does not bound this.

**Do not "fix" this by editing the migration.** `sqlx` checksums the entire file with SHA-384,
comments included, and `Migrator::DEFAULT` validates applied migrations — so changing so much as a
comment in an already-applied file gives `MigrateError::VersionMismatch(18)` and CrashLoops the
allocator on every environment that already ran it. The file is correct as written for a small
table; prod is the exception, and the exception is handled here.

## Steps

Run against the prod app DB (`postgres-nvme-rw`), before the deploy.

### 1. Add the column

Metadata-only (nullable, no default), so it is sub-millisecond once it holds the lock. Keep the
timeout short and just retry if it can't get in — that is the same trade the migration makes.

```sql
SET lock_timeout = '5s';
ALTER TABLE cephor_replication_status ADD COLUMN IF NOT EXISTS reclaimed_at TIMESTAMPTZ;
```

### 2. Build the index without blocking

`CONCURRENTLY` cannot run inside a transaction — run it on its own, not in a `psql -c` batch with
other statements.

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS cephor_replication_status_failed_reclaimable
    ON cephor_replication_status (node_id, updated_at)
    WHERE status = 'failed' AND reclaimed_at IS NULL;
```

### 3. Verify the index is VALID — do not skip this

`CREATE INDEX CONCURRENTLY` can fail and leave an **INVALID** index behind. If that happens, the
`IF NOT EXISTS` in migration 0018 will silently decline to rebuild it, and you are left with a
non-functional index while the failed-reclaim worklist quietly seq-scans an 11.4M-row table every
poll, per node.

```sql
SELECT indisvalid
FROM pg_index
WHERE indexrelid = 'cephor_replication_status_failed_reclaimable'::regclass;
```

Anything other than `t`: drop it and repeat step 2.

```sql
DROP INDEX CONCURRENTLY cephor_replication_status_failed_reclaimable;
```

### 4. Confirm the agent's schema gate now passes

This is the exact predicate `drain-agent`'s `wait-for-cephor-schema` initContainer runs. It must
return `t` before the DaemonSet rolls, or every pod sits in init.

```sql
SELECT to_regclass('public.cephor_ssd_residency') IS NOT NULL
   AND EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'cephor_replication_status'
                 AND column_name IN ('content_sha256', 'relanded_at', 'reclaimed_at')
               HAVING count(*) = 3);
```

Note this also requires migrations 0016 (`cephor_ssd_residency`) and 0019 (`content_sha256`,
`relanded_at`). Those are cheap — 0016 creates an empty table, 0019 is two metadata-only
`ADD COLUMN`s — so letting the allocator apply them normally is fine. Only 0018's index build
needs the treatment above.

## Deploy order

The manifests do not enforce this; `kubectl apply -k` starts everything rolling at once.

1. Steps 1–4 above, out of band.
2. **api-local** — picks up `NODE_NAME` and starts publishing `cephor:landed:<node>`. The old
   agent ignores the queue harmlessly, so this is safe to do first and wants to be: it means
   announcements are already flowing before anything depends on them.
3. **drain-allocator** — applies 0016/0019 (0018 is now a no-op) and starts publishing the
   per-node eviction reserve.
4. **drain-agent** — last. This is the binary that stops unlinking on commit, so retention begins
   here.

## After

Watch, in this order:

- `drain_landed_dropped_total` — **must stay 0.** Nonzero means the api and agent disagree about
  the announcement wire contract, every announcement is being discarded, and discovery has
  silently reverted to the walk.
- `drain_ssd_cache_bytes` — should rise and then plateau under the evictor's floor. Rising without
  plateau means eviction is not keeping up.
- `drain_ssd_evict_blocked_unreplicated_total` — **must stay 0.** This is the durability invariant:
  the evictor never removes a chunk that is not yet replicated.
- `drain_reland_vanished_total` — alert on any increase; it is the lost-race signature for a
  rewritten part.
- `fs_cache_pressure` 503s on PUT — retention deliberately runs the ingest SSD at 15–20% free
  against a 10%-free 503 threshold. That five-point margin is defended only by the evictor
  continuing to run.
