from __future__ import annotations

import argparse
import asyncio
import json
import logging
from contextlib import suppress
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
        storage_version_target=int(getattr(config, "target_storage_version", 3)),
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
                should_decrypt=ctx.should_decrypt,
                sleep_seconds=float(config.http_download_sleep_loop),
                address=address,
                bucket_name=bucket_name,
                storage_version=ctx.storage_version,
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
    config = get_config()
    db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)
    try:
        target = int(getattr(config, "target_storage_version", 3))

        async def _iter_targets() -> AsyncGenerator[dict[str, Any], None]:
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

        sem = asyncio.Semaphore(max(1, int(getattr(args, "concurrency", 4))))
        results: list[bool] = []
        migrated: list[str] = []
        failed: list[str] = []
        planned: list[str] = []

        async def _process(o: dict[str, Any]) -> None:
            async with sem:
                address = str(o.get("main_account_id", ""))
                obj_display = f"{o['bucket_name']}/{o['object_key']} ({o['object_id']})"
                if args.dry_run:
                    log.info(f"DRY-RUN migrate {obj_display} from ov={o['object_version']}")
                    results.append(True)
                    planned.append(obj_display)
                    return
                log.info(f"START migrate {obj_display}")
                ok = await migrate_one(
                    db=db,
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
                results.append(ok)
                if ok:
                    log.info(f"DONE migrate {obj_display}")
                    migrated.append(obj_display)
                else:
                    log.error(f"FAILED migrate {obj_display}")
                    failed.append(obj_display)

        tasks = [asyncio.create_task(_process(o)) async for o in _iter_targets()]
        if tasks:
            await asyncio.gather(*tasks)

        # End-of-run report
        total = len(results)
        ok_count = sum(1 for r in results if r)
        fail_count = total - ok_count
        log.info(
            "Migration report: total=%d migrated=%d failed=%d planned=%d",
            total,
            len(migrated),
            len(failed),
            len(planned),
        )
        if failed:
            log.error("Failed objects (%d): %s", len(failed), ", ".join(failed))
        return 0 if fail_count == 0 else 1
    finally:
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
            await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Migrate objects to target storage version")
    ap.add_argument("--bucket", default="", help="Bucket name (optional; with --key for single object)")
    ap.add_argument("--key", default="", help="Object key (optional; requires --bucket)")
    ap.add_argument("--dry-run", action="store_true", help="Print planned migrations without executing")
    ap.add_argument("--concurrency", type=int, default=4, help="Max objects to migrate in parallel")
    args = ap.parse_args()

    if args.key and not args.bucket:
        raise SystemExit("--key requires --bucket")

    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
