# hippius_s3/sql/

All schema, migrations, and parameterized queries. Two subdirectories:

- [migrations/](migrations/) — versioned SQL migrations run by [../scripts/migrate.py](../scripts/migrate.py). Additive only; no destructive down-migrations.
- [queries/](queries/) — parameterized queries loaded via `hippius_s3.utils.get_query(name)`. One `.sql` file per query. 50+ queries today.

Root SQL files:

- [schema.sql](schema.sql) — canonical schema. Regenerated from migrations; treat as reference, not truth.

## Key tables

### `objects`

Logical objects. Points at `current_object_version` for the serve-able row.

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

## Where to find things

| Want... | Look at... |
|---|---|
| Schema definition | [schema.sql](schema.sql) (regenerated; authoritative is `migrations/`) |
| Add a migration | [migrations/CLAUDE.md](migrations/CLAUDE.md) |
| Add a query | [queries/CLAUDE.md](queries/CLAUDE.md) |
| Query loader | `hippius_s3.utils.get_query` |
| Repository wrapper | [../repositories/](../repositories/) |

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 13, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1669 | 10:14 AM | 🔵 | Comprehensive Authentication Architecture Analysis - No STS/OAuth Implementation Exists | ~806 |
| #1658 | 10:10 AM | 🔵 | Hippius S3 Database Schema Structure | ~659 |
</claude-mem-context>
