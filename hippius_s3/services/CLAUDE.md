# hippius_s3/services/

Business-logic layer. Crypto, KMS, backend clients, copy helpers, audit. The API layer calls into these; the services don't know about HTTP.

## Files

| File | Purpose |
|---|---|
| [object_reader.py](object_reader.py) | `build_stream_context` — cache-vs-pipeline decision, download coalescing, envelope unwrap for GETs. |
| [crypto_service.py](crypto_service.py) | `CryptoService` — AEAD adapter registry (`hip-enc/aes256gcm`, `hip-enc/legacy`), per-chunk encrypt/decrypt. |
| [key_service.py](key_service.py) | Per-object key derivation (legacy v≤4 SecretBox path). |
| [envelope_service.py](envelope_service.py) | `generate_dek`, `wrap_dek(kek, dek, aad)`, `unwrap_dek(kek, wrapped, aad)` — AES-256-GCM envelope. |
| [kek_service.py](kek_service.py) | Per-bucket KEK management: `get_or_create_active_bucket_kek`, `get_bucket_kek_bytes`, `init_kms_client`, `close_kek_pool`. |
| [ovh_kms_client.py](ovh_kms_client.py) | mTLS client to OVH KMS. Retries with exponential backoff; wraps/unwraps KEKs. |
| [local_kek_wrapper.py](local_kek_wrapper.py) | Local dev fallback when `HIPPIUS_KMS_MODE=disabled`. Uses `HIPPIUS_AUTH_ENCRYPTION_KEY` as the pseudo-master. |
| [arion_service.py](arion_service.py) | `ArionClient` — upload/download/unpin via HTTPS + Bearer token. |
| [hippius_api_service.py](hippius_api_service.py) | Blockchain-publishing API client. Also hosts token auth response models (used by the gateway). |
| [copy_service_v5.py](copy_service_v5.py) | `execute_v5_fast_path_copy` — DEK rewrap + CID reuse, no byte copy. |
| [parts_service.py](parts_service.py) | `upsert_part_placeholder` — idempotent `parts` row insert + optional `part_chunks` bulk. |
| [parts_catalog.py](parts_catalog.py) | `PartsCatalog.build_initial_download_chunks`, `wait_for_cids` — part→CID resolution. |
| [audit_service.py](audit_service.py) | Shared audit logging (used by gateway's audit middleware). |
| [ray_id_service.py](ray_id_service.py) | Ray-ID generation + context vars for structured logging. |
| [acl_helper.py](acl_helper.py) | `canned_acl_to_acl` + utilities for ACL conversion. |

## Crypto stack

```
HIPPIUS_KMS_MODE=required:                    HIPPIUS_KMS_MODE=disabled:
    OVH KMS (HSM, mTLS)                           HIPPIUS_AUTH_ENCRYPTION_KEY
        │                                             │
     master key                                    master key (local)
        │ wraps (via ovh_kms_client)                  │ wraps (via local_kek_wrapper)
        ▼                                             ▼
     Bucket KEK (stored wrapped in keystore DB)
        │ wraps (via envelope_service.wrap_dek)
        ▼
     Object DEK (per object_version; persisted as `wrapped_dek` on object_versions)
        │ encrypts (via crypto_service per-chunk adapter)
        ▼
     Chunk ciphertext (AES-256-GCM, AAD = "hippius-dek:{bucket_id}:{object_id}:{version}")
```

Suite IDs:
- `hip-enc/aes256gcm` — current default (v5+).
- `hip-enc/legacy` — NaCl SecretBox for legacy objects (v≤4). Still decrypt-supported; writers always use v5.

## `build_stream_context`

[object_reader.py:47](object_reader.py). Called by every GET/HEAD/streaming-copy. Responsibilities:

1. Validate storage version ([line 57](object_reader.py)) via `require_supported_storage_version`.
2. Read parts list, build chunk plan (respects Range).
3. **Batch check FS cache** ([line 67](object_reader.py)) via `obj_cache.chunks_exist_batch` — one pass.
4. If any chunk missing → enter pipeline mode:
   - For each missing part, attempt `SET NX EX {lock_ttl}` on `download_in_progress:{object_id}:v:{ov}:part:{pn}` ([line 87-94](object_reader.py)). Redis hiccups fail-open (still enqueue).
   - If you acquired the lock, resolve per-chunk CIDs from `part_chunks` ([line 117-143](object_reader.py)), build a `DownloadChainRequest`, enqueue.
   - If lock held by another streamer, skip enqueue — we'll wait on pub/sub.
5. Unwrap DEK:
   - Call `get_bucket_kek_bytes(bucket_id, kek_id)` — asks the KMS client to unwrap the bucket KEK.
   - Call `unwrap_dek(kek_bytes, wrapped_dek, aad)` → plaintext DEK.
6. **Envelope-race fallback**: if `kek_id`/`wrapped_dek` is NULL on the current version (a concurrent overwrite is in flight), re-read via `get_object_for_download_with_permissions_by_version` for `prev_version` ([line 177-220](object_reader.py)). This avoids 500s during overwrite windows. Doesn't help genuinely-orphaned v5 rows (see [todo.md](../../todo.md) P0).

Returns a `StreamContext(plan, object_version, storage_version, source, key_bytes, suite_id, bucket_id, upload_id)` for the streamer.

## `ArionClient`

[arion_service.py](arion_service.py). HTTP client:

- Shared `httpx.AsyncClient` per process.
- Bearer token + service key auth.
- Retries via `@retry_on_error(retries=3, backoff=5.0)` decorator. **No circuit breaker today** — listed in [todo.md](../../todo.md) as a P0 item pairing with the PUT-degradation postmortem.
- `upload_file_and_get_cid`, `download_file`, `delete_file` implement the `BackendClient` ABC from [../workers/uploader.py](../workers/uploader.py).
- Error classification distinguishes 507 (permanent, disk full upstream), 429/503/timeout (transient), `"pin"`/`"unpin"` errors (permanent).
- `X-Hippius-Bypass-Rate-Limiting` header sent on all requests.

## `copy_service_v5`

[copy_service_v5.py:24 `execute_v5_fast_path_copy`](copy_service_v5.py). Fast copy that skips bytes:

1. Create destination object_version with same storage_version as source.
2. Duplicate `chunk_backend` rows pointing at the same Arion identifiers.
3. Rewrap the DEK under the destination AAD via `rewrap_encryption_envelope`.
4. Update `object_versions` with the new `kek_id`/`wrapped_dek`.

**Latent risk if re-enabled for MPU**: the new object_id has `chunk_backend` rows pointing at the backend, but no FS cache entries. First GET on the destination will trigger the downloader to fetch from Arion (correct), but every read is cold until the FS cache fills. Acceptable but worth benchmarking. More critically, **streamer reads would hang** on `wait_for_chunk` if someone manually truncates `chunk_backend` or if the source's backend identifier has been unpinned. Guard this path carefully if scope is widened.

See [todo.md](../../todo.md) P1.

## `audit_service` / `ray_id_service`

- Audit: log shape is bucket/object/op/account/status/latency/ray_id. Shared between gateway audit middleware and API endpoint decorators.
- Ray-ID: uses a contextvar so structured logs across async boundaries carry the correlation id.
