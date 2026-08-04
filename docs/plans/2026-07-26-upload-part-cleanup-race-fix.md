# UploadPart Cleanup Race Fix — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or subagent-driven-development) task-by-task.
> Call `mcp__hippius-mem__recall` with "UploadPart cleanup race data loss" before starting — full forensics in
> `mem_01KYFXNWZYMHN2X0D299KTCJDX` (supersedes the PR #359 narrative).

**Goal:** A failed or client-cancelled UploadPart/append stream must never destroy data another attempt of the
same part has already published — closing the race that corrupted `beam-dev/100gbdestination1` v5 (2026-07-26)
and destroyed 44 `tora-m365` objects (2026-07-22).

**Architecture:** Duplicate PUTs for one `(object_id, object_version, part_number)` share one SSD directory.
Chunk writes are already safe (atomic tmp+rename; deterministic AES-GCM nonce ⇒ byte-identical files across
same-upload duplicates). The only destructive ops are directory-level deletes: `_cleanup_partial` (MPU),
`_cleanup_part` (append), and any delete-old-on-reupload path. The fix removes FS deletion from all
failure-path cleanups (Redis key cleanup stays), adds a publish-time trim for the shrinking-reupload edge, and
adds a page-worthy signal when the drain writes off a part of a *servable* version (the detection gap that let
both incidents run silently).

**Tech Stack:** Python (`hippius_s3/writer/object_writer.py`, `hippius_s3/cache/fs_store.py`), one small Rust
change (`crates/hippius-drain-agent/src/worker.rs` + core store), pytest + sqlx tests.

---

## Verified facts (do NOT re-derive; re-verify only if the code moved)

- `_cleanup_partial` at [object_writer.py:740](../../hippius_s3/writer/object_writer.py) deletes the whole part
  dir via `fs_store.delete_part`; append's `_cleanup_part` at ~:1121 does the same for `(object, cov, next_part)`.
- `fs_store.delete_part` logs "FS: failed to delete part" at [fs_store.py:466](../../hippius_s3/cache/fs_store.py).
- Nonce derivation is deterministic per (key, bucket, object, part, chunk_index, upload_id)
  ([crypto_service.py:107-141](../../hippius_s3/services/crypto_service.py)) — same-upload duplicates produce
  byte-identical ciphertext. Meta.json is written last and is the readiness signal; the drain reconciler only
  registers meta-complete parts.
- Simple PUT is NOT affected (each PUT gets a fresh version → no shared dir).
- Incident signature: `parts.uploaded_at` AFTER `cephor_replication_status.landed_at` + gateway showing
  duplicate `partNumber` PUTs + api-local "failed to delete part" WARNs milliseconds after the final 200.

## Open questions Task 1 must answer before any edit

1. **Every** `fs_store.delete_part` caller in the upload path (rg it): is there a delete-old-before-write on
   part re-upload in `mpu_upload_part_stream`? Each caller gets an explicit keep/remove/guard decision.
2. Drain-side chunk enumeration: does the Rust drain copy **all** `chunk_*.bin` present or exactly
   `meta.num_chunks`? (crates/hippius-drain-agent localfs `list_chunks` + copy path.) This decides whether the
   publish-time trim (Task 3) is required for correctness or only hygiene.
3. Which crypto suite serves storage_version=5 writes in prod — confirm the deterministic-nonce AESGCM variant
   is the active one (the file also documents a legacy random-nonce suite).

---

### Task 1: Fact verification + failing regression test (the race, reproduced)

**Files:** `tests/unit/test_upload_part_cleanup_race.py` (new)

- Answer the three open questions; record answers in the task report (they gate Tasks 2-3 design details).
- Write the failing test first: drive `mpu_upload_part_stream` twice concurrently for the same
  (object, version, part) against a real `FileSystemPartsStore` (tmpdir) with mocked DB/Redis — winner
  completes (meta + parts row), loser's stream raises mid-write (client disconnect) AFTER the winner published.
  Assert: meta.json + all chunk files still present, and a follow-up `get_chunk` serves. This MUST fail on
  current code (loser's `_cleanup_partial` deletes the dir). Mirror the fixture style of the existing writer
  unit tests (`ls tests/unit | rg -i writer|mpu`).
- Same-shape test for the append path (`_cleanup_part` after a CAS loser).
- Commit: `test(writer): reproduce the duplicate-UploadPart cleanup race` (failing tests marked xfail-strict
  or committed together with Task 2 — follow TDD: see them fail, then fix in Task 2 and flip).

### Task 2: Remove FS deletion from failure-path cleanups

**Files:** `hippius_s3/writer/object_writer.py` (`_cleanup_partial`, `_cleanup_part`, any Task-1-found callers)

- Failure cleanups no longer call `fs_store.delete_part`. They keep: Redis chunk-key cleanup (best-effort,
  keyed per chunk) and the parts-row delete where the flow already does it (`_delete_part_row` — DB rows are
  per-attempt-upserted and safe).
- Why-comment on each site: chunk files are atomic-rename, byte-identical across same-upload duplicates, and
  invisible to readers/reconciler without meta.json — deleting them is pure hazard (2026-07-26 incident: a
  cancelled duplicate's cleanup destroyed a completed part's data after the 200). A never-published dir leaks
  until the SSD GC path handles it — leak beats loss (same doctrine as the drain reclaim).
- If Task 1 found a delete-old-on-reupload call: replace with the Task 3 publish-time trim (or drop it if the
  drain is meta-driven; decide from Task 1's answer and document).
- Run Task 1's tests → green. Full gates: `ruff check`, `ruff format --check`, `pytest tests/unit -q`
  (~2150 tests), `ty check hippius_s3` (CI's type checker — CLAUDE.md's mypy line is stale).
- Commit: `fix(writer): failed upload streams no longer delete shared part data`

### Task 3: Publish-time trim (only if Task 1 says the drain enumerates all files)

**Files:** `hippius_s3/writer/object_writer.py` (meta-write site), `hippius_s3/cache/fs_store.py` if a helper
is needed.

- After writing meta with `num_chunks=N`, delete chunk files with index >= N (the shrinking different-content
  reupload edge). Skip entirely (with a doc note) if the drain/read path is meta-driven — YAGNI.
- Tests: reupload-with-fewer-chunks leaves exactly N chunks + meta; concurrent identical attempt racing the
  trim loses nothing (indices < N never trimmed).
- Commit: `fix(writer): trim stale chunk tail at publish instead of pre-delete`

### Task 4: Page-worthy signal for servable write-offs (Rust, small)

**Files:** `crates/hippius-drain-core/src/store.rs`, `crates/hippius-drain-agent/src/worker.rs`,
`crates/hippius-drain-agent/src/metrics.rs`

- At the missing-source write-off site (`MissingSourceOutcome::Failed` arm): one extra store query —
  `is_version_servable(object, version)` (reuse the existing servability predicate helper if present from the
  mark path; check what #359 left — `is_version_servable` was explicitly kept). Servable → log ERROR (not WARN)
  with a distinct marker (`DRAIN_WRITEOFF_SERVABLE`) + new counter `drain_parts_written_off_servable_total`.
  Unservable → current WARN path unchanged. One query per write-off (rare) — no hot-path cost.
- Tests: sqlx test for the servability query; worker test asserting ERROR-path counter on a servable version
  and the plain path on an unservable one.
- Commit: `feat(drain): servable write-offs log ERROR + dedicated counter`
- Follow-up note (do NOT do here): hippius-otel rule `increase(drain_parts_written_off_servable_total[1h]) > 0`
  — would have caught both incidents within ~2.5h.

### Task 5: Full gates + wrap-up

- Full workspace: cargo fmt/clippy/test (drain crates), ruff + pytest tests/unit, the Task 1 race tests.
- Verify no interaction with PR #359's reclaim change (it holds failed row-present parts; our leaked meta-less
  dirs have NO cephor row + present ov row → held post-#359: confirm the leak lifecycle is documented in both
  places and the ov-row-cleanup follow-up is filed in todo.md or an issue).
- Update `mem_01KYFXNWZYMHN2X0D299KTCJDX` with the fix landing; rollout: staging first per repo rules.
