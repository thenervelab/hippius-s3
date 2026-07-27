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
4. Always allocate a fresh `candidate_object_id = uuid4()` and trust the DB-returned id — the pre-check `SELECT` was removed (WU-3). `upsert_object_basic`'s `ON CONFLICT (bucket_id, object_key) ... RETURNING object_id` resolves the authoritative id, so overwrites are handled by the upsert; nothing keys off the previous row ([put_object_endpoint.py:137-142](put_object_endpoint.py)).
5. Call [ObjectWriter.put_simple_stream_full](../../../writer/object_writer.py) with a streaming body iterator.
6. Persist the SSD address only — **no upload enqueue on the write path** (drain-direct cutover: the Rust drain is the sole producer, enqueuing after it replicates the part to the pool). The old `writer.queue.enqueue_upload` producer was removed.
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

### Object identity is DB-atomic (pre-check dropped)

The old `get_object_by_path` pre-check `SELECT` was deliberately removed (WU-3, [put_object_endpoint.py:137-142](put_object_endpoint.py)). The endpoint now always passes a fresh `candidate_object_id = uuid4()`; `upsert_object_basic`'s `INSERT ... ON CONFLICT (bucket_id, object_key) ... RETURNING object_id` resolves the authoritative id atomically, and everything downstream keys off `put_res.object_id`. This removed a DB round trip per PUT and closed a TOCTOU window — overwrites and concurrent PUTs on the same key are resolved by the upsert, not by reading the previous row.

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

*No recent activity*
</claude-mem-context>
