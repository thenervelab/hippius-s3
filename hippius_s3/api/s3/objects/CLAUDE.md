# hippius_s3/api/s3/objects/

S3 object operations. Each endpoint has its own file; router hookup at [../router.py](../router.py).

## Endpoints

| Method | Path | File | Handler |
|---|---|---|---|
| PUT | `/{bucket}/{key}` | [put_object_endpoint.py](put_object_endpoint.py) | `handle_put_object` |
| GET | `/{bucket}/{key}` | [get_object_endpoint.py](get_object_endpoint.py) | `handle_get_object` |
| HEAD | `/{bucket}/{key}` | [head_object_endpoint.py](head_object_endpoint.py) | `handle_head_object` |
| DELETE | `/{bucket}/{key}` | [delete_object_endpoint.py](delete_object_endpoint.py) | `handle_delete_object` |
| PUT | `/{bucket}/{key}` (Copy source) | [copy_object_endpoint.py](copy_object_endpoint.py) | `handle_copy_object` |

Router routes PUT to `handle_copy_object` if `x-amz-copy-source` header is present; otherwise to `handle_put_object`. Append detection (`x-amz-meta-append: true`) happens inside `handle_put_object` and routes to [../extensions/append.py](../extensions/append.py).

## PUT lifecycle (at a glance)

Full detail in [../../../CLAUDE.md section 3.1](../../../../CLAUDE.md). Summary for this dir:

1. Resolve bucket. 404 NoSuchBucket if missing ([put_object_endpoint.py:55-61](put_object_endpoint.py)).
2. Detect append ([put_object_endpoint.py:68-79](put_object_endpoint.py)).
3. Build metadata dict from `x-amz-meta-*` headers (stripping append control keys).
4. Pre-check existing object to reuse its `object_id` on overwrite ([put_object_endpoint.py:96-113](put_object_endpoint.py)).
5. Call [ObjectWriter.put_simple_stream_full](../../../writer/object_writer.py) with a streaming body iterator.
6. Enqueue upload request via [writer_enqueue_upload](../../../writer/queue.py).
7. Mark `multipart_uploads.is_completed = TRUE` so DELETE doesn't cascade the chunk_backend rows before the worker has had a chance to upload.
8. Return 200 with `ETag` and `x-amz-meta-append-version: 0`.

## GET lifecycle

1. Resolve object via `get_object_for_download_with_permissions` query.
2. Parse optional Range header → `RangeRequest`.
3. Call [build_stream_context](../../../services/object_reader.py) — this does the cache-vs-pipeline decision, enqueues download if needed, unwraps the DEK.
4. Call [stream_plan](../../../reader/streamer.py) — yields decrypted bytes with optional Range slicing.

## HEAD lifecycle

Like GET but without body streaming. Uses the same `build_stream_context` to resolve size/metadata, then returns headers only.

## DELETE lifecycle

Soft delete on `object_versions` / `chunk_backend`. Enqueues `unpin_requests` entries for each backend. See [../../../workers/unpinner.py](../../../workers/unpinner.py).

## Gotchas

### Pre-check overwrite race

[put_object_endpoint.py:105-108](put_object_endpoint.py):

```python
# TODO: Make object identity/version allocation fully DB-atomic by removing this
#       pre-check and always passing a generated candidate UUID. The writer already
#       treats the DB-returned object_id/object_version as authoritative.
```

The endpoint does a `SELECT` to find an existing `object_id` for the (bucket, key) pair, then the writer does an `INSERT ... ON CONFLICT` that's itself atomic. Two concurrent PUTs on the same key both see the same `prev`, both reuse the same candidate_object_id, and the writer deterministically resolves the race via DB. Works today, but could be simplified.

### Master-token object_id reuse

Since the writer trusts the DB-returned object_id ([object_writer.py:222-227](../../../writer/object_writer.py)), passing a candidate UUID that collides with an existing object is safe — the DB will override.

### Envelope-race fix (recent)

[object_writer.py:244-261](../../../writer/object_writer.py) now writes `kek_id`/`wrapped_dek` immediately after `upsert_object_basic` to prevent a concurrent GET seeing NULL envelope columns during an overwrite. Before this fix, `v5_missing_envelope_metadata` 500s appeared in prod whenever a concurrent GET raced a new PUT.

### Streaming copy vs fast-path

[copy_object_endpoint.py](copy_object_endpoint.py) branches:

- v5 single-part source + v5 destination, same-KMS, same-suite → **fast path** ([../../../services/copy_service_v5.py `execute_v5_fast_path_copy`](../../../services/copy_service_v5.py)). Re-wraps the DEK under the destination AAD, reuses CIDs via `chunk_backend` duplication. No byte copy.
- Multipart source OR cross-KMS → **streaming fallback** via `handle_streaming_copy` in [../copy_helpers.py](../copy_helpers.py). Full decrypt + re-encrypt round trip.

See [todo.md](../../../../todo.md) P1 for the latent risk if fast-path is re-enabled for MPU without FS backfill.

### Append contract

[../extensions/append.py](../extensions/append.py) implements S4 append. Contract summary:

- `x-amz-meta-append: true` on PutObject triggers append mode.
- `x-amz-meta-append-if-version: N` compare-and-swap against the append version counter — 412 PreconditionFailed if mismatch.
- `x-amz-meta-append-id: <id>` idempotency key; repeated appends with the same id are no-ops.
- Appends reserve a new part with `part_number = max_existing + 1`, stream-encrypt, and update the object's composite MD5 + append version on success.
- No byte rewrites of existing data. Size cap by S3 multipart limit (10000 parts × 512 MiB).

Full spec: [docs/s4.md](../../../../docs/s4.md).

<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

### Feb 13, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #1764 | 10:58 AM | 🔴 | Suppressed Ty Type Checker Unresolved Import Errors for lxml | ~511 |

### Feb 16, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #2459 | 11:03 PM | 🔴 | Complete diagnosis: Arion backend 404 failures cause objects to be filtered from ListObjects | ~569 |
| #2423 | 10:55 PM | 🔵 | PutObject endpoint uses ObjectWriter.put_simple_stream_full for uploads | ~350 |
| #2411 | 10:53 PM | 🔵 | GetObject endpoint retrieves objects via get_object_for_download_with_permissions queries | ~343 |
| #2410 | " | 🔵 | HeadObject endpoint implementation and database queries | ~331 |
| #2409 | " | 🔵 | S3 object router implementation using FastAPI | ~304 |

### Apr 23, 2026

| ID | Time | T | Title | Read |
|----|------|---|-------|------|
| #6914 | 12:20 PM | 🔄 | Migrated put_object and delete_object routes to scoped connection acquisition | ~571 |
| #6912 | 12:13 PM | 🔄 | HEAD object endpoint refactored to use connection pool with scoped acquisition | ~549 |
| #6911 | 12:11 PM | 🔄 | Migrated head_object and get_object routes to pool-based connection lifecycle | ~503 |
| #6902 | 11:55 AM | 🔵 | S3 object router shows all operations including MPU parts acquire database connections | ~555 |
</claude-mem-context>
