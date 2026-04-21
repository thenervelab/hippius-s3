# PR #146 — Consolidated Review Findings

## P1 (blocks merge)

| ID | Area | Finding | Fix |
|---|---|---|---|
| **P1-A** | Janitor | `cleanup_old_parts_by_mtime` deletes if `old_enough OR fully_replicated`. The OR branch deletes chunks >7d old even if never replicated. **Direct violation of user rule.** | Change to `fully_replicated AND (old_enough OR hot_evicted)`. Replication is a hard gate. |
| **P1-B** | Janitor | Critical-pressure branch deletes non-replicated parts past half max-age as a "last resort". Docstring admits it. **Direct violation.** | Remove the override entirely. Under pressure: delete fully-replicated hot parts first (halve/zero hot retention), never non-replicated. Log ERROR instead. |
| **P1-C** | Janitor | DLQ key list is hardcoded to `["arion_upload_requests:dlq", "unpin_requests:dlq"]`; a future IPFS uploader would bypass protection. | Enumerate `[f"{b}_upload_requests:dlq" for b in config.upload_backends] + ["unpin_requests:dlq"]`. |
| **P1-D / P1-I** | Janitor + s3-backup | `is_replicated_on_all_backends` only checks `upload_backends` (arion/ipfs). OVH is recorded by s3-backup but never in `expected`. Janitor deletes FS chunks before OVH backup finishes. | Add `HIPPIUS_BACKUP_BACKENDS` config (default empty, prod `"ovh"`); union into `expected` for janitor check. |
| **P1-E** | Coalescing | `build_stream_context` enqueues a download on every cache miss. The `download_in_progress` flag is only CLEARED, never SET. Duplicate requests. | `SET download_in_progress:{oid}:{pn} <ray_id> NX EX 120` before enqueue; skip enqueue if already held. |
| **P1-F** | Redis decomm | E2E helpers (`tests/e2e/support/cache.py`, `test_GetObject_Range.py`) still connect to `redis://localhost:6385/0`. Every E2E that clears cache now fails. | Rewrite helpers to clear FS cache via `FileSystemPartsStore.delete_part`. |
| **P1-G** | Redis decomm | `hippius_s3/main.py` still imports `RedisDownloadChunksCache` and wires `app.state.dl_cache`. Zero consumers. Dead code footgun. | Delete the import, the assignment, the `download_chunks.py` module, and its exports. |
| **P1-H** | s3-backup | Hydrator base deployment pins `HIPPIUS_OBJECT_CACHE_DIR=/var/lib/hippius/object_cache` (CephFS, RO in prod). Production patch mounts `local-cache` RW but env doesn't repoint. Hydrator writes to RO mount → fails. | Production patch must override env to `local_object_cache` and set fallback. |

## P2 (important)

| ID | Area | Finding | Fix |
|---|---|---|---|
| P2-A | Janitor SQL | `count_chunk_backends.sql` hardcodes `4194304` chunk size. Future variable-size parts could mis-count. | Read `chunk_size_bytes` from `parts`. |
| P2-B | Janitor SQL | `find_objects_ready_for_hard_delete` treats "no chunk_backend rows" as "all deleted". | Require positive existence. |
| P2-C | Redis decomm | Dead: `download_chunks.py`, `set_download_chunk` shim, `download_cache_client` ctor param. | Delete. |
| P2-D | Redis decomm | Downloader docstring still says "stores it in the Redis chunk cache". | Update text. |
| P2-E–H | Tests | Missing coverage for pitfalls: P5.1 mid-stream delete, P7.1 crash/resume, P2.2 meta-only, P5.3 range-only fetch, P10.1 download-only replication. | Add five tests. |
| P2-I | s3-backup | `FileSystemPartsStore` duplicated between repos. Drift risk. | Add cross-reference comment in both; flag in both CLAUDE.md files. |

## P3 (nice to have)

- P3-A: test downloader meta-before-chunks ordering (currently asserts "eventually exists")
- P3-B: simulated `relatime` atime test
- P3-C: `cleanup_stale_parts` gains replication check (low risk because DB cross-check catches most)
- P3-D: hydrator DB pool should be required (not optional) to enforce eager meta

## P4 (informational)

- P4-A: `version_type=='migration' → expected=["ipfs"]` path (intentional, migration artifact)
- P4-B: hydrator doesn't touch atime on chunk_exists-skip (minor)
