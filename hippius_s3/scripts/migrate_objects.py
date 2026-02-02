from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from typing import AsyncGenerator

import asyncpg  # type: ignore[import-untyped]
import redis.asyncio as async_redis  # type: ignore[import-untyped]

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.reader.streamer import stream_plan
from hippius_s3.services.object_reader import build_stream_context
from hippius_s3.utils import get_query
from hippius_s3.writer.db import ensure_upload_row
from hippius_s3.writer.object_writer import ObjectWriter


def _now_ts() -> float:
    return float(time.time())


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.write("\n")
        f.flush()
        try:
            import os

            os.fsync(f.fileno())
        except Exception:
            # Best-effort; not all environments/filesystems allow fsync.
            pass
    tmp.replace(path)


def _atomic_write_json(path: Path, obj: Any) -> None:
    _atomic_write_text(path, json.dumps(obj, indent=2, sort_keys=True))


def _format_progress_dashboard(work_items: list[dict[str, Any]]) -> str:
    now = _now_ts()
    total = len(work_items)
    counts: dict[str, int] = {}
    for it in work_items:
        s = str(it.get("status") or "pending")
        counts[s] = counts.get(s, 0) + 1

    def c(name: str) -> int:
        return int(counts.get(name, 0))

    lines: list[str] = []
    lines.append("Hippius migration progress")
    lines.append("")
    lines.append(f"Total: {total}")
    lines.append(
        "Status: "
        f"pending={c('pending')} planned={c('planned')} running={c('running')} "
        f"succeeded={c('succeeded')} failed={c('failed')} skipped={c('skipped')}"
    )
    lines.append("")

    # Show a rough "liveness" indicator: how long since the last terminal event.
    last_finished = 0.0
    for it in work_items:
        finished_at = it.get("finished_at")
        if finished_at is None:
            continue
        try:
            last_finished = max(last_finished, float(finished_at))
        except Exception:
            continue
    if last_finished > 0:
        lines.append(f"Last completion: {now - last_finished:.0f}s ago")
    else:
        lines.append("Last completion: (none yet)")
    lines.append("")

    # Show a few most recent non-success items for quick debugging.
    # We sort by finished_at/started_at if present.
    def ts(it: dict[str, Any]) -> float:
        return float(it.get("finished_at") or it.get("started_at") or 0.0)

    interesting = [it for it in work_items if str(it.get("status") or "") in {"failed", "skipped", "running"}]
    interesting.sort(key=ts, reverse=True)
    if interesting:
        lines.append("Recent:")
        for it in interesting[:10]:
            status = str(it.get("status") or "")
            bucket = str(it.get("bucket") or "")
            key = str(it.get("key") or "")
            err = str(it.get("last_error") or it.get("skip_reason") or "")
            started_at = it.get("started_at")
            elapsed = ""
            if status == "running" and started_at is not None:
                try:
                    elapsed = f" ({now - float(started_at):.0f}s)"
                except Exception:
                    elapsed = ""
            if err:
                lines.append(f"- {status:9s} {bucket}/{key}{elapsed} — {err}")
            else:
                lines.append(f"- {status:9s} {bucket}/{key}{elapsed}")
    return "\n".join(lines)


def _clear_screen() -> str:
    # ANSI clear + cursor home
    return "\x1b[2J\x1b[H"


def _normalize_json_work_items(raw: Any, log: logging.Logger) -> list[dict[str, Any]]:
    """
    Accepts either:
      - list[{"bucket": "...", "key": "...", ...}]
      - list["bucket|key"]
    Returns a normalized list of dicts.
    """
    if not isinstance(raw, list):
        raise ValueError("Expected JSON array worklist")

    items: list[dict[str, Any]] = []
    for i, it in enumerate(raw):
        if isinstance(it, str):
            line = it.strip()
            if not line or line.startswith("#"):
                continue
            if "|" not in line:
                log.warning("Skipping malformed work item string (expected 'bucket|key'): %r", it)
                continue
            bucket, key = (p.strip() for p in line.split("|", 1))
            if not bucket or not key:
                log.warning("Skipping malformed work item string (empty bucket/key): %r", it)
                continue
            items.append({"bucket": bucket, "key": key, "status": "pending", "attempts": 0})
            continue

        if not isinstance(it, dict):
            log.warning(
                "Skipping malformed work item (expected object or string): index=%d type=%s", i, type(it).__name__
            )
            continue

        bucket = str(it.get("bucket") or it.get("bucket_name") or "").strip()
        key = str(it.get("key") or it.get("object_key") or "").strip()
        if not bucket or not key:
            log.warning("Skipping malformed work item (missing bucket/key): index=%d value=%r", i, it)
            continue

        out = dict(it)
        out["bucket"] = bucket
        out["key"] = key
        out.setdefault("status", "pending")
        out.setdefault("attempts", 0)
        items.append(out)

    return items


async def _fetch_current_md5(db: Any, object_id: str) -> str:
    row = await db.fetchrow(
        """
        SELECT ov.md5_hash
        FROM objects o
        JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
        WHERE o.object_id = $1
        """,
        object_id,
    )
    return (row and (row[0] or "")) or ""


async def _fetch_current_markers(db: Any, object_id: str) -> tuple[str, int, float]:
    row = await db.fetchrow(
        """
        SELECT ov.md5_hash, ov.append_version, EXTRACT(EPOCH FROM COALESCE(ov.last_modified, NOW()))
        FROM objects o
        JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
        WHERE o.object_id = $1
        """,
        object_id,
    )
    md5 = (row and (row[0] or "")) or ""
    av = int(row[1] or 0) if row else 0
    lm = float(row[2] or 0.0) if row else 0.0
    return md5, av, lm


async def migrate_one(
    *,
    db: Any,
    redis_client: Any,
    object_id: str,
    bucket_id: str,
    bucket_name: str,
    object_key: str,
    content_type: str,
    metadata: dict[str, Any],
    expected_old_version: int,
    is_public: bool,
    source_storage_version: int,
    address: str,
) -> bool:
    config = get_config()
    log = logging.getLogger("migrator")

    # Do not delete prior migration versions here; cleanup tool handles unpin + delete safely

    from hippius_s3.cache import FileSystemPartsStore  # local import

    fs_store = FileSystemPartsStore(config.object_cache_dir)
    writer = ObjectWriter(db=db, redis_client=redis_client, fs_store=fs_store)
    new_version = await writer.create_version_for_migration(
        object_id=object_id,
        content_type=content_type,
        metadata=metadata,
        storage_version_target=config.target_storage_version,
        upload_backends=config.upload_backends,
    )

    obj_cache = RedisObjectPartsCache(redis_client)
    # Build common context via reader (version-aware and cache-aware)
    info = {
        "object_id": object_id,
        "bucket_name": bucket_name,
        "object_key": object_key,
        "storage_version": int(source_storage_version),
        "object_version": int(expected_old_version),
        "is_public": bool(is_public),
        "multipart": True,  # safe default; unused by stream
        "metadata": metadata,
    }
    ctx = await build_stream_context(
        db,
        redis_client,
        obj_cache,
        info,
        rng=None,
        address=address,
    )
    plan = ctx.plan

    # Skip objects without any chunks/parts in plan (mark version failed)
    if not plan:
        with suppress(Exception):
            await db.execute(
                "UPDATE object_versions SET status='failed', last_modified=NOW() WHERE object_id = $1 AND object_version = $2",
                object_id,
                int(new_version),
            )
        log.error(f"Skipping {bucket_name}/{object_key} ({object_id}) — no parts/plan")
        return False

    # Ensure single upload row for this migration version
    upload_id = await ensure_upload_row(
        db,
        object_id=object_id,
        bucket_id=bucket_id,
        object_key=object_key,
        content_type=content_type,
        metadata=metadata,
    )

    # Group plan by part order
    by_part: dict[int, list] = {}
    for it in plan:
        by_part.setdefault(int(it.part_number), []).append(it)

    # Baseline markers before first part
    base_md5, base_av, base_lm = await _fetch_current_markers(db, object_id)

    # Upload parts sequentially
    for part_number in sorted(by_part.keys()):
        part_plan = by_part[int(part_number)]
        try:
            gen = stream_plan(
                obj_cache=obj_cache,
                object_id=object_id,
                object_version=ctx.object_version,
                plan=part_plan,
                sleep_seconds=config.http_download_sleep_loop,
                storage_version=ctx.storage_version,
                key_bytes=ctx.key_bytes,
                suite_id=ctx.suite_id,
                bucket_id=ctx.bucket_id,
                upload_id=ctx.upload_id,
                address=address,
                bucket_name=bucket_name,
            )
            # Accumulate bytes for this part
            buf = bytearray()
            async for chunk in gen:
                buf.extend(chunk)

            # Upload part into new version
            await writer.mpu_upload_part(
                upload_id=str(upload_id),
                object_id=object_id,
                object_version=int(new_version),
                bucket_name=bucket_name,
                account_address=address,
                seed_phrase="",
                part_number=int(part_number),
                body_bytes=bytes(buf),
            )

            # Soft change detection: md5 + append_version + last_modified
            cur_md5, cur_av, cur_lm = await _fetch_current_markers(db, object_id)
            if (cur_md5 or "") != (base_md5 or "") or int(cur_av) != int(base_av) or float(cur_lm) != float(base_lm):
                # Mark failed; allow cleanup to unpin + delete later
                with suppress(Exception):
                    await db.execute(
                        "UPDATE object_versions SET status='failed', last_modified=NOW() WHERE object_id = $1 AND object_version = $2",
                        object_id,
                        int(new_version),
                    )
                log.warning(
                    f"Source changed during migrate {bucket_name}/{object_key} ({object_id}); aborting and cleaned up"
                )
                return False
        except Exception:
            # Clean up on any part failure
            with suppress(Exception):
                await db.execute(
                    "UPDATE object_versions SET status='failed', last_modified=NOW() WHERE object_id = $1 AND object_version = $2",
                    object_id,
                    int(new_version),
                )
            log.exception(
                f"Error migrating part {part_number} for {bucket_name}/{object_key} ({object_id}); cleaned up new version"
            )
            return False

    # Finalize + CAS swap
    await writer.mpu_complete(
        bucket_name=bucket_name,
        object_id=object_id,
        object_key=object_key,
        upload_id=str(upload_id),
        object_version=int(new_version),
        address=address,
        seed_phrase="",
    )
    # Enqueue background publish for the migrated version
    try:
        parts = await db.fetch(
            get_query("list_parts_for_version"),
            object_id,
            int(new_version),
        )
        if parts:
            req = UploadChainRequest(
                address=address,
                bucket_name=bucket_name,
                object_key=object_key,
                object_id=str(object_id),
                object_version=int(new_version),
                chunks=[Chunk(id=int(p["part_number"])) for p in parts],
                upload_id=str(upload_id),
            )
            await enqueue_upload_request(req)
    except Exception:
        log.exception(
            f"Failed to enqueue background publish for migrated {bucket_name}/{object_key} ({object_id}) v={int(new_version)}"
        )
    swapped = await writer.swap_current_version_cas(
        object_id=object_id,
        expected_old_version=int(expected_old_version),
        new_version=int(new_version),
    )
    if not swapped:
        with suppress(Exception):
            await db.execute(
                "UPDATE object_versions SET status='failed', last_modified=NOW() WHERE object_id = $1 AND object_version = $2",
                object_id,
                int(new_version),
            )
        return False

    return True


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("migrator")
    work_items: list[dict[str, Any]] | None = None
    state_lock = asyncio.Lock()
    state_dirty = asyncio.Event()
    state_file: Path | None = None
    state_flush_seconds = float(getattr(args, "state_flush_seconds", 0.0) or 0.0)
    state_writer_task: asyncio.Task[None] | None = None
    progress_task: asyncio.Task[None] | None = None
    progress_done = asyncio.Event()
    progress_enabled = bool(getattr(args, "progress", False))
    progress_interval = float(getattr(args, "progress_interval_seconds", 2.0) or 2.0)
    progress_stream_name = str(getattr(args, "progress_output", "stderr") or "stderr")
    progress_stream = sys.stdout

    if bool(getattr(args, "objects_json_stdin", False)):
        raw = json.load(sys.stdin)
        work_items = _normalize_json_work_items(raw, log)
        state_file_arg = str(getattr(args, "state_file", "") or "").strip()
        if state_file_arg:
            state_file = Path(state_file_arg)

            async def _state_writer() -> None:
                """
                Single writer coroutine that checkpoints JSON state safely under concurrency.
                We write full snapshots using atomic rename to avoid partial/corrupted files.
                """
                assert work_items is not None
                try:
                    while True:
                        await state_dirty.wait()
                        state_dirty.clear()
                        # Coalesce bursts of updates.
                        if state_flush_seconds > 0:
                            await asyncio.sleep(state_flush_seconds)
                        async with state_lock:
                            try:
                                _atomic_write_json(state_file, work_items)
                            except Exception:
                                log.exception("Failed to write state file: %s", state_file)
                except asyncio.CancelledError:
                    return

            state_writer_task = asyncio.create_task(_state_writer())

        if progress_enabled:
            progress_stream = sys.stderr if progress_stream_name == "stderr" else sys.stdout
            if not progress_stream.isatty():
                log.info("Progress dashboard disabled (output is not a TTY).")
                progress_enabled = False

            if progress_enabled:

                async def _progress_loop() -> None:
                    assert work_items is not None
                    # Initial draw
                    try:
                        while not progress_done.is_set():
                            async with state_lock:
                                dash = _format_progress_dashboard(work_items)
                            progress_stream.write(_clear_screen())
                            progress_stream.write(dash + "\n")
                            progress_stream.flush()
                            try:
                                await asyncio.wait_for(progress_done.wait(), timeout=max(0.2, progress_interval))
                            except asyncio.TimeoutError:
                                continue
                    finally:
                        # Final draw
                        with suppress(asyncio.CancelledError):
                            async with state_lock:
                                dash = _format_progress_dashboard(work_items)
                            progress_stream.write(_clear_screen())
                            progress_stream.write(dash + "\n")
                            progress_stream.flush()

                progress_task = asyncio.create_task(_progress_loop())

    config = get_config()
    db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.services.kek_service import init_kms_client

    initialize_queue_client(redis_queues_client)
    await init_kms_client(config)
    try:
        target = config.target_storage_version

        async def _iter_targets() -> AsyncGenerator[dict[str, Any], None]:
            if work_items is not None:
                resume = bool(getattr(args, "resume", False))
                seen: set[tuple[str, str]] = set()
                for it in work_items:
                    bucket = str(it.get("bucket") or "")
                    key = str(it.get("key") or "")
                    status = str(it.get("status") or "pending")
                    work_id = (bucket, key)
                    if work_id in seen:
                        # Don't silently ignore duplicates. Mark them skipped unless they already have a terminal status.
                        if status not in {"succeeded", "skipped"}:
                            async with state_lock:
                                it["status"] = "skipped"
                                it["skip_reason"] = "duplicate"
                                it["finished_at"] = _now_ts()
                                state_dirty.set()
                        continue
                    seen.add(work_id)
                    if resume and status in {"succeeded", "skipped"}:
                        continue

                    # Mark planned as soon as we accept the work item.
                    async with state_lock:
                        it["status"] = "planned"
                        it["last_error"] = ""
                        # Clear stale fields from prior runs (if any)
                        it.pop("skip_reason", None)
                        it.pop("finished_at", None)
                        it.pop("started_at", None)
                        it["planned_at"] = _now_ts()
                        state_dirty.set()

                    rows = await db.fetch(get_query("list_objects_to_migrate"), target, bucket, key)
                    if not rows:
                        # Not necessarily an error: it may already be migrated or not found.
                        async with state_lock:
                            it["status"] = "skipped"
                            it["skip_reason"] = "not eligible / not found"
                            it["finished_at"] = _now_ts()
                            state_dirty.set()
                        log.info("Skipping (not eligible / not found): %s/%s", bucket, key)
                        continue

                    if len(rows) != 1:
                        # Correctness guard: a single bucket/key should map to exactly one eligible current row.
                        async with state_lock:
                            it["status"] = "failed"
                            it["last_error"] = f"expected 1 row from list_objects_to_migrate, got {len(rows)}"
                            it["finished_at"] = _now_ts()
                            state_dirty.set()
                        log.error(
                            "Work item %s/%s returned %d eligible rows; expected 1. Marked failed.",
                            bucket,
                            key,
                            len(rows),
                        )
                        continue

                    r = rows[0]
                    yield {
                        "object_id": str(r["object_id"]),
                        "bucket_id": str(r["bucket_id"]),
                        "bucket_name": str(r["bucket_name"]),
                        "object_key": str(r["object_key"]),
                        "is_public": bool(r["is_public"]),
                        "main_account_id": str(r["main_account_id"]),
                        "storage_version": int(r["storage_version"]),
                        "object_version": int(r["object_version"]),
                        "content_type": str(r["content_type"]),
                        "metadata": json.loads(r["metadata"])
                        if isinstance(r["metadata"], str)
                        else (r["metadata"] or {}),
                        "_work_item": it,
                    }
                return

            row_filter_bucket = args.bucket or None
            row_filter_key = args.key or None
            rows = await db.fetch(get_query("list_objects_to_migrate"), target, row_filter_bucket, row_filter_key)
            for r in rows:
                yield {
                    "object_id": str(r["object_id"]),
                    "bucket_id": str(r["bucket_id"]),
                    "bucket_name": str(r["bucket_name"]),
                    "object_key": str(r["object_key"]),
                    "is_public": bool(r["is_public"]),
                    "main_account_id": str(r["main_account_id"]),
                    "storage_version": int(r["storage_version"]),
                    "object_version": int(r["object_version"]),
                    "content_type": str(r["content_type"]),
                    "metadata": json.loads(r["metadata"]) if isinstance(r["metadata"], str) else (r["metadata"] or {}),
                }

        concurrency = max(1, int(getattr(args, "concurrency", 4)))
        total_done = 0
        total_ok = 0
        total_fail = 0
        total_planned = 0
        failed_samples: list[str] = []

        async def _process_one(o: dict[str, Any]) -> None:
            nonlocal total_done, total_ok, total_fail, total_planned

            work_item = o.get("_work_item")
            if not isinstance(work_item, dict):
                work_item = None

            obj_display = f"{o['bucket_name']}/{o['object_key']} ({o['object_id']})"

            # Mark running right before doing work.
            if work_item is not None:
                async with state_lock:
                    work_item["status"] = "running"
                    work_item["started_at"] = _now_ts()
                    work_item.pop("skip_reason", None)
                    work_item.pop("finished_at", None)
                    work_item["attempts"] = int(work_item.get("attempts") or 0) + 1
                    state_dirty.set()

            task_db: Any | None = None
            try:
                task_db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
                address = str(o.get("main_account_id", ""))

                if args.dry_run:
                    log.info("DRY-RUN migrate %s from ov=%s", obj_display, o["object_version"])
                    total_planned += 1
                    total_done += 1
                    if work_item is not None:
                        async with state_lock:
                            work_item["status"] = "planned"
                            work_item["finished_at"] = _now_ts()
                            state_dirty.set()
                    return

                log.info("START migrate %s", obj_display)
                timeout_s = float(getattr(args, "timeout_seconds", 0.0) or 0.0)
                ok = False
                try:
                    coro = migrate_one(
                        db=task_db,
                        redis_client=redis_client,
                        object_id=o["object_id"],
                        bucket_id=o["bucket_id"],
                        bucket_name=o["bucket_name"],
                        object_key=o["object_key"],
                        content_type=o["content_type"],
                        metadata=o["metadata"] or {},
                        expected_old_version=int(o["object_version"]),
                        is_public=bool(o.get("is_public", False)),
                        source_storage_version=int(o.get("storage_version", 2)),
                        address=address,
                    )
                    ok = await (asyncio.wait_for(coro, timeout=timeout_s) if timeout_s > 0 else coro)
                except asyncio.TimeoutError:
                    ok = False
                    log.error("TIMEOUT migrate %s (%.1fs)", obj_display, timeout_s)
                    if work_item is not None:
                        async with state_lock:
                            work_item["last_error"] = f"timeout after {timeout_s:.1f}s"
                            state_dirty.set()
                except Exception as e:
                    ok = False
                    log.exception("Unhandled error migrating %s", obj_display)
                    if work_item is not None:
                        async with state_lock:
                            work_item["last_error"] = f"{type(e).__name__}: {e}"
                            state_dirty.set()

                total_done += 1
                if ok:
                    total_ok += 1
                    log.info("DONE migrate %s", obj_display)
                    if work_item is not None:
                        async with state_lock:
                            work_item["status"] = "succeeded"
                            work_item["finished_at"] = _now_ts()
                            state_dirty.set()
                else:
                    total_fail += 1
                    log.error("FAILED migrate %s", obj_display)
                    if len(failed_samples) < 100:
                        failed_samples.append(obj_display)
                    if work_item is not None:
                        async with state_lock:
                            work_item["status"] = "failed"
                            work_item["finished_at"] = _now_ts()
                            state_dirty.set()
            except Exception as e:
                # Catch-all to avoid leaving work items stuck in "running".
                total_done += 1
                total_fail += 1
                log.exception("Worker failure for %s", obj_display)
                if len(failed_samples) < 100:
                    failed_samples.append(obj_display)
                if work_item is not None:
                    async with state_lock:
                        work_item["status"] = "failed"
                        work_item["last_error"] = f"{type(e).__name__}: {e}"
                        work_item["finished_at"] = _now_ts()
                        state_dirty.set()
            finally:
                if task_db is not None:
                    with suppress(Exception):
                        await task_db.close()

        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=max(1, concurrency * 2))

        async def _producer() -> None:
            async for o in _iter_targets():
                await queue.put(o)
            for _ in range(concurrency):
                await queue.put(None)

        async def _worker() -> None:
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return
                    await _process_one(item)
                finally:
                    queue.task_done()

        producer_task = asyncio.create_task(_producer())
        worker_tasks = [asyncio.create_task(_worker()) for _ in range(concurrency)]

        await producer_task
        await queue.join()
        # Workers exit naturally after consuming the sentinel `None`s.
        await asyncio.gather(*worker_tasks)

        # End-of-run report
        total = total_done
        ok_count = total_ok
        fail_count = total_fail
        log.info(
            "Migration report: total=%d migrated=%d failed=%d planned=%d",
            total,
            ok_count,
            fail_count,
            total_planned,
        )
        if failed_samples:
            log.error("Failed objects (showing up to %d): %s", len(failed_samples), ", ".join(failed_samples))
        rc = 0 if fail_count == 0 else 1

        # Final checkpoint to state file (if enabled)
        if state_file is not None and work_items is not None:
            async with state_lock:
                try:
                    _atomic_write_json(state_file, work_items)
                except Exception:
                    log.exception("Failed to write final state file: %s", state_file)

        return rc
    finally:
        progress_done.set()
        if progress_task is not None:
            with suppress(asyncio.CancelledError):
                await progress_task
        if state_writer_task is not None:
            state_writer_task.cancel()
            with suppress(asyncio.CancelledError):
                await state_writer_task
        try:
            # redis.asyncio client exposes aclose(); if unavailable, fall back
            close = getattr(redis_client, "aclose", None)
            if callable(close):
                await close()
            else:
                close2 = getattr(redis_client, "close", None)
                if callable(close2):
                    close2()
            close_q = getattr(redis_queues_client, "aclose", None)
            if callable(close_q):
                await close_q()
        finally:
            from hippius_s3.services.kek_service import close_kek_pool

            await close_kek_pool()
            await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Migrate objects to target storage version")
    ap.add_argument("--bucket", default="", help="Bucket name (optional; with --key for single object)")
    ap.add_argument("--key", default="", help="Object key (optional; requires --bucket)")
    ap.add_argument(
        "--objects-json-stdin",
        action="store_true",
        help="Read a JSON array worklist from stdin and update per-item statuses.",
    )
    ap.add_argument(
        "--progress",
        action="store_true",
        help="Render a live progress dashboard that refreshes periodically (recommended with --objects-json-stdin).",
    )
    ap.add_argument(
        "--progress-interval-seconds",
        type=float,
        default=2.0,
        help="Progress dashboard refresh interval (default 2.0s).",
    )
    ap.add_argument(
        "--progress-output",
        choices=["stdout", "stderr"],
        default="stderr",
        help="Where to write progress output.",
    )
    ap.add_argument(
        "--state-file",
        default="",
        help="When used with --objects-json-stdin, periodically checkpoint the updated worklist to this path (atomic writes).",
    )
    ap.add_argument(
        "--state-flush-seconds",
        type=float,
        default=2.0,
        help="Checkpoint coalescing delay in seconds (default 2.0). Set 0 to write on every update.",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="When used with --objects-json-stdin, skip items already marked succeeded/skipped.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print planned migrations without executing")
    ap.add_argument("--concurrency", type=int, default=4, help="Max objects to migrate in parallel")
    ap.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="Per-object timeout in seconds (0 disables). Default: 30s.",
    )
    args = ap.parse_args()

    if args.key and not args.bucket:
        raise SystemExit("--key requires --bucket")
    if args.objects_json_stdin and (args.bucket or args.key):
        raise SystemExit("--objects-json-stdin cannot be combined with --bucket/--key")
    if args.state_file and not args.objects_json_stdin:
        raise SystemExit("--state-file requires --objects-json-stdin")
    if args.progress and not args.objects_json_stdin:
        raise SystemExit("--progress currently requires --objects-json-stdin")

    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
