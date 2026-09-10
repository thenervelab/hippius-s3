# hippius_s3/sql/

All schema, migrations, and parameterized queries. Two subdirectories:

- [migrations/](migrations/) — versioned SQL migrations run by [../scripts/migrate.py](../scripts/migrate.py). Additive only; no destructive down-migrations.
- [queries/](queries/) — parameterized queries loaded via `hippius_s3.utils.get_query(name)`. One `.sql` file per query. 50+ queries today.

Root SQL files:

- [schema.sql](schema.sql) — canonical schema. Regenerated from migrations; treat as reference, not truth.

## Key tables

### `objects`

Logical objects. Points at `current_object_version` for the serve-able row. `object_key` is the primary S3 name.

### `object_names`

Extra S3 keys for the same `object_id` (same-bucket CopyObject). Ciphertext AAD binds `bucket_id`+`object_id`, so CopyObject cannot mint a new id. `resolve_object_id(bucket_id, key)` prefers `objects.object_key` then this table. Delete of the primary name promotes one alias (`promote_object_name` relocates a soft-deleted occupant of dest). Triggers reject a live primary and an alias sharing a key.

### `object_versions`

One row per PUT/overwrite/append. Core columns:

| Column | Notes |
|---|---|
| `object_id`, `object_version` | PK |
| `storage_version` | 1-5 supported. v5 is the target; older versions are decrypt-only. |
| `size_bytes`, `md5_hash` | **Empty/zero means "reserved but not complete"** — download query filters these out. |
| `content_type`, `metadata` | `metadata` is JSONB for `x-amz-meta-*` user metadata. |
| `kek_id` | UUID of the bucket KEK used to wrap the DEK. |
| `wrapped_dek` | `bytea`, AES-256-GCM wrapped. |
| `enc_suite_id` | `hip-enc/aes256gcm` for v5. |
| `enc_chunk_size_bytes` | Chunk size used when this version was written. |
| `append_version` | Monotonic counter for S4 append CAS. |
| `updated_at`, `created_at` | Timestamps. |

### `parts`

One row per part (simple PUT has one part; MPU/append has many). Columns include `size_bytes`, `etag`, `chunk_size_bytes` (per-part — supports legacy variable-chunk objects).

### `part_chunks`

One row per chunk within a part. Optional `cid` (used when the backend is content-addressed), `cipher_size_bytes`, `plain_size_bytes`, `checksum`.

### `chunk_backend`

The replication ledger:

```
chunk_backend(chunk_id, backend, backend_identifier, deleted, deleted_at, created_at)
```

- `backend` — `arion` (the only one in prod today).
- `backend_identifier` — Arion's returned identifier (aka `path_hash`).
- `deleted` + `deleted_at` — soft delete. Unpinner marks these; janitor hard-deletes once every required backend confirms.

The janitor's "fully replicated" check looks for a non-deleted row for every backend in `upload_backends ∪ backup_backends`.

### `buckets`, `bucket_acls`, `object_acls`

Standard S3 metadata + ACL rows.

### `multipart_uploads`

In-flight MPU state. Simple PUT also creates a row here for structural consistency (the `upload_id` is used in AEAD AAD — see [../writer/CLAUDE.md](../writer/CLAUDE.md)).

## Keystore DB

Separate DB via `HIPPIUS_KEYSTORE_DATABASE_URL` (falls back to `DATABASE_URL`). Stores bucket KEKs (wrapped by the KMS master key). See [../services/kek_service.py](../services/kek_service.py).

## Migrations

Run via `python -m hippius_s3.scripts.migrate`. Asyncpg-based; applies `.sql` files in [migrations/](migrations/) in filename order. No down-migrations.

On API startup, the container runs migrations first — see the Docker Compose service dependencies.

## Queries

All reads/writes go through `.sql` files loaded by [hippius_s3/utils/__init__.py `get_query`](../utils/). Some important ones:

- `get_object_for_download_with_permissions.sql` — the big query that GET/HEAD use. Filters out empty/reserved versions.
- `get_object_for_download_with_permissions_by_version.sql` — explicit-version variant for the envelope-race fallback.
- `upsert_object_basic.sql` — atomic reserve-a-new-version.
- `create_migration_version.sql`, `swap_current_version_cas.sql` — used by the v4→v5 migrator.
- `update_object_version_metadata.sql`, `update_object_version_envelope.sql` — set size/md5/envelope on completion.
- `get_chunk_backend_identifier.sql` — the downloader's per-chunk lookup.
- `count_chunk_backends.sql` — uses DB-driven `chunk_size_bytes` rather than hardcoded 4 MiB (recent fix; see commit `0a66a25` and the Apr 21 memory notes).

## Known issue: broken v5 rows

Historically, `object_versions` rows with `storage_version >= 5` but `kek_id IS NULL OR wrapped_dek IS NULL` have caused 500s on GET/HEAD/Copy. Root cause: a writer path that reserved the version, then died before the envelope UPDATE landed. The prod population is ~200k rows.

Now fixed on the write side ([../writer/object_writer.py:244-261](../writer/object_writer.py) writes the envelope immediately after reserve). The read-side filter (recommended in [../../todo.md](../../todo.md)) hasn't shipped yet; broken rows still 500 today.

Proposed CHECK constraint for when the backlog is drained:

```sql
ALTER TABLE object_versions
  ADD CONSTRAINT v5_requires_envelope
  CHECK (storage_version < 5 OR (kek_id IS NOT NULL AND wrapped_dek IS NOT NULL));
```

See [analysis.md](../../analysis.md) for the full fix plan.

## Storage-usage rollup: the only triggers in this schema that move a billed number

⚠️ **If you are reading `writer/object_writer.py` or `objects/delete_object_endpoint.py` and wondering
why an account's usage figure changed without any code doing it, this is why.** Five row triggers on
`objects` and `object_versions` maintain a per-bucket byte counter. Nothing in the application calls
them and nothing in the application can opt out of them.

Defined in [migrations/20260910120000_storage_usage_rollup.sql](migrations/20260910120000_storage_usage_rollup.sql),
which carries the full reasoning. The short version:

```
objects / object_versions  --5 row triggers, INSERT only-->  storage_delta_ledger
                                                                     |
                    usage-rollup worker: DELETE ... RETURNING, fold   v
                                                              bucket_storage_usage
                                                                     |
                            account total = SUM over live buckets  <--+
```

| Table | What it is |
|---|---|
| `storage_delta_ledger` | Insert-only queue of `(bucket_id, delta_bytes)`. Drained continuously. |
| `bucket_storage_usage` | The counter. One row per bucket. **Unclamped — CAN go negative.** |
| `storage_usage_rollup_state` | `backfilled_at`. NULL means the counter is deltas, not totals. |

**Why triggers, against the house style.** ~26 statements move billable bytes, and three of the
heaviest paths (`nuke_user.py`, `purge_buckets.py`, the janitor's `hard_delete_object`) move them
through `ON DELETE CASCADE` — no SQL in those paths names `object_versions` at all. A helper the
application must remember to call cannot see a cascade, and would be forgotten by the 27th path.

**The exact trigger set, and why not one more.** `objects` AFTER INSERT / AFTER UPDATE / **BEFORE**
DELETE, plus `object_versions` AFTER UPDATE / AFTER DELETE. Pinned by
`tests/integration/test_storage_usage_rollup.py::test_trigger_set_is_exactly_as_designed`.

- **There is deliberately NO INSERT trigger on `object_versions`.** `upsert_object_basic` is ONE
  statement whose CTEs both upsert `objects` and insert `object_versions`; the objects trigger
  already counts the new version, so adding the missing-looking one bills every PUT twice.
- The `objects` DELETE trigger is **BEFORE**, uniquely. `ON DELETE CASCADE` is an AFTER trigger named
  `RI_ConstraintTrigger_*`, AFTER ROW triggers fire in name order, and `RI_` beats any lowercase
  name — so an AFTER trigger would find every version row already gone and never decrement.
- **No trigger on `buckets`.** Liveness and ownership are applied at READ time by joining `buckets`,
  so soft-deleting or transferring a bucket needs no counter maintenance.

**Rules for anything that touches these tables in bulk:**

1. `ALTER TABLE ... DISABLE TRIGGER` before a bulk migration over `object_versions` or `objects`,
   then `ENABLE TRIGGER` and recompute the affected buckets with
   `SELECT recompute_bucket_storage_usage(bucket_id)`. Leaving them enabled writes one ledger row per
   changed row; disabling them without recomputing leaves the counter silently wrong.
2. **`TRUNCATE` does not fire row triggers at all.** A truncate of either table leaves every counter
   at its pre-truncate value with nothing to correct it. Recompute every bucket afterwards.
3. **Create an objects row and its first object_versions row in the SAME statement**, as all three
   `upsert_object_*` queries do. Split across statements, the objects INSERT trigger finds no version
   yet and there is no version INSERT trigger to catch up.
4. **Never repoint `objects.current_object_version` AND edit the outgoing version in one statement.**
   AFTER ROW triggers all fire at end-of-statement and would each see the other's finished work. Use
   two statements, as `soft_delete_object_version` + `repoint_current_version_after_delete` do.

Correctness is asserted, write path by write path, against `get_account_storage_bytes.sql` — the
canonical definition — in `tests/integration/test_storage_usage_rollup.py`. The reconciler in
`workers/run_usage_rollup_in_loop.py` re-checks it in production and exports drift, which is
expected to be exactly zero.

## Where to find things

| Want... | Look at... |
|---|---|
| Schema definition | [schema.sql](schema.sql) (regenerated; authoritative is `migrations/`) |
| Add a migration | [migrations/CLAUDE.md](migrations/CLAUDE.md) |
| Add a query | [queries/CLAUDE.md](queries/CLAUDE.md) |
| Query loader | `hippius_s3.utils.get_query` |
| Repository wrapper | [../repositories/](../repositories/) |
