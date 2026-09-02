# Runbook — Object Lock migration pre-step (production)

Run this **before** merging the `staging → main` promotion PR that ships Object Lock. It performs
the whole schema change online, so the deploy-time `dbmate up` finds everything already in place
and every statement becomes a no-op.

The deploy is safe without the pre-step. The pre-step exists so the one genuinely expensive part —
scanning a 79 GB heap — happens at a moment you choose, rather than inside the deploy's critical
path where it stretches the window in which the migration job and the new pods are out of step.

## Why this is a pre-step and not just a migration

`object_versions` in production is **~152M rows / ~79 GB**. The original single-file migration added
both CHECK constraints in validated form and built the partial index non-concurrently. Both of those
take **ACCESS EXCLUSIVE** and both have to scan the whole heap — during which every read and write
to `object_versions` blocks. That is not a slow deploy, it is an outage of the object path.

Measured on PostgreSQL 18.6 against a 5M-row / 521 MB stand-in, with a writer inserting every 20 ms:

| | DDL wall time | max write stall |
|---|---|---|
| Original single-file migration | 0.80 s | **0.73 s** |
| Split online migration (3 files) | 1.12 s total | **0.14 s** |
| *baseline — writer running, no DDL* | — | *0.16 s* |

The split version's stall is **below the measurement noise floor**: it never blocks writes at all.
The original's 0.73 s is on a table **~150× smaller than production**, and the cost scales with heap
size, so the equivalent production stall is on the order of **a minute or two of fully blocked
writes**.

The split keeps each step in a lock mode that does not block traffic:

| Migration | Statement | Lock | Scans heap? |
|---|---|---|---|
| `…100000` | `ADD COLUMN` ×3 | ACCESS EXCLUSIVE, momentary | No — PG11+ stores the default in `pg_attribute.attmissingval` |
| `…100000` | `ADD CONSTRAINT … NOT VALID` ×2 | ACCESS EXCLUSIVE, momentary | No — `NOT VALID` skips verification |
| `…100001` | `CREATE INDEX CONCURRENTLY` | SHARE UPDATE EXCLUSIVE | Yes, but concurrent with reads **and writes** |
| `…100002` | `VALIDATE CONSTRAINT` ×2 | SHARE UPDATE EXCLUSIVE | Yes, but concurrent with reads **and writes** |

`SHARE UPDATE EXCLUSIVE` conflicts with other DDL and with autovacuum on this table — not with
traffic.

## Pre-step

Against the **production primary** (`postgres-nvme`, namespace `hippius-s3-prod`). Confirm you are
on the primary first — `CREATE INDEX CONCURRENTLY` on a replica is not a thing:

```bash
kubectl -n hippius-s3-prod get pods -l cnpg.io/cluster=postgres-nvme \
  -o custom-columns='NAME:.metadata.name,ROLE:.metadata.labels.cnpg\.io/instanceRole' --no-headers
```

Then, as four separate statements — **do not wrap these in a transaction**, `CREATE INDEX
CONCURRENTLY` is rejected inside one:

```sql
-- 1. buckets: O(1), the table is ~3.3k rows.
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS object_lock JSONB;

-- 2. object_versions columns + unvalidated constraints. All catalogue-only; expect milliseconds.
ALTER TABLE object_versions
    ADD COLUMN IF NOT EXISTS object_lock_mode TEXT,
    ADD COLUMN IF NOT EXISTS object_lock_retain_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS object_lock_legal_hold BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE object_versions
    ADD CONSTRAINT object_versions_object_lock_mode_check
    CHECK (object_lock_mode IS NULL OR object_lock_mode IN ('GOVERNANCE', 'COMPLIANCE')) NOT VALID;

ALTER TABLE object_versions
    ADD CONSTRAINT object_versions_object_lock_retention_pair_check
    CHECK ((object_lock_mode IS NULL) = (object_lock_retain_until IS NULL)) NOT VALID;

-- 3. The index. Minutes on 79 GB; reads and writes continue throughout.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_object_versions_locked
    ON object_versions (object_id, object_version)
    WHERE object_lock_retain_until IS NOT NULL OR object_lock_legal_hold;

-- 4. Validation. Also minutes, also online. Optional (see below) but keeps prod == staging.
ALTER TABLE object_versions VALIDATE CONSTRAINT object_versions_object_lock_mode_check;
ALTER TABLE object_versions VALIDATE CONSTRAINT object_versions_object_lock_retention_pair_check;
```

Step 4 is the one you can skip under time pressure. A `NOT VALID` CHECK still constrains every
INSERT and UPDATE from that moment on; all it leaves unverified are the pre-existing rows, which
carry NULL/false in all three columns by construction and therefore satisfy both predicates. The
deploy-time migration will validate them later if you skip it here.

Do **not** insert rows into `schema_migrations` by hand. Every statement above is written to be
idempotent, so let `dbmate up` run normally at deploy and record the versions itself.

## Verify before deploying

```sql
-- Expect 3 rows; legal_hold must be NOT NULL DEFAULT false.
SELECT column_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'object_versions' AND column_name LIKE 'object_lock%' ORDER BY 1;

-- Expect both present. convalidated=false is acceptable if you skipped step 4.
SELECT conname, convalidated FROM pg_constraint
WHERE conrelid = 'object_versions'::regclass AND conname LIKE '%object_lock%' ORDER BY 1;

-- MUST be indisvalid=true AND indisready=true. See recovery below if not.
SELECT indisvalid, indisready FROM pg_index
WHERE indexrelid = 'idx_object_versions_locked'::regclass;
```

## Recovery — failed concurrent index build

`CREATE INDEX CONCURRENTLY` can fail (deadlock, a long-running transaction, cancellation). It then
leaves behind an **invalid** index: never used to answer a query, but still maintained on every
write — pure cost. Worse, the `IF NOT EXISTS` in migration `…100001` will *skip* it on the next
run, so the failure is silent and permanent.

Always run the `indisvalid` check above. If it returns false:

```sql
DROP INDEX CONCURRENTLY idx_object_versions_locked;
```

then re-run step 3. A long-running transaction elsewhere is the usual cause — check
`pg_stat_activity` for old `xact_start` values before retrying.

## What the deploy then does

`dbmate up` applies four versions. After a clean pre-step every one is a no-op:

| Version | With pre-step done |
|---|---|
| `20260902090000_add_buckets_object_lock` | `ADD COLUMN IF NOT EXISTS` → skipped |
| `20260902100000_object_lock_versions` | columns skipped; both constraints guarded on `pg_constraint`, so already-validated ones are **not** demoted back to `NOT VALID` |
| `20260902100001_object_lock_versions_index` | `IF NOT EXISTS` → skipped |
| `20260902100002_object_lock_versions_validate` | Postgres checks `convalidated` first and skips the scan |

Measured re-run of all three `object_versions` migrations against an already-migrated 5M-row table:
**0.18 s**, versus 0.54 s for the first `VALIDATE` alone — confirming the no-op path.

## Rollback

The application tolerates the columns existing while the old code runs — nothing in the pre-deploy
code reads them. So a rollback of the *code* needs no schema change, and the columns should simply
be left in place.

Only if you need to remove the schema entirely (`dbmate down` ×3, or by hand), note that
`DROP INDEX` in the down migration is **not** concurrent and takes ACCESS EXCLUSIVE. On production
drop it by hand instead:

```sql
DROP INDEX CONCURRENTLY IF EXISTS idx_object_versions_locked;
```
