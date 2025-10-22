from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any
from typing import AsyncGenerator

import asyncpg  # type: ignore[import-untyped]
import redis.asyncio as async_redis  # type: ignore[import-untyped]

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
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
    expected_md5: str,
    is_public: bool,
    source_storage_version: int,
    address: str,
    seed_phrase: str,
) -> bool:
    config = get_config()

    # Restart policy: delete any non-current migration versions for this object
    rows_old = await db.fetch(
        """
        SELECT ov.object_version
        FROM object_versions ov
        JOIN objects o ON o.object_id = ov.object_id
        WHERE ov.object_id = $1
          AND ov.version_type = 'migration'
          AND ov.object_version <> o.current_object_version
        """,
        object_id,
    )
    for r in rows_old:
        await db.execute(get_query("delete_version_and_parts"), object_id, int(r["object_version"]))

    writer = ObjectWriter(db=db, redis_client=redis_client)
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
        seed_phrase=seed_phrase,
    )
    plan = ctx.plan

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

    # Upload parts sequentially
    for part_number in sorted(by_part.keys()):
        # Stream this part only
        part_plan = by_part[int(part_number)]
        gen = stream_plan(
            obj_cache=obj_cache,
            object_id=object_id,
            object_version=ctx.object_version,
            plan=part_plan,
            should_decrypt=ctx.should_decrypt,
            seed_phrase=seed_phrase,
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
            seed_phrase=seed_phrase,
            part_number=int(part_number),
            body_bytes=bytes(buf),
        )

        # Soft md5 check: abort if changed
        cur_md5 = await _fetch_current_md5(db, object_id)
        if (cur_md5 or "") != (expected_md5 or ""):
            # Delete in-progress new version and its parts
            await db.execute(get_query("delete_version_and_parts"), object_id, int(new_version))
            return False

    # Finalize + CAS swap
    await writer.mpu_complete(
        bucket_name=bucket_name,
        object_id=object_id,
        object_key=object_key,
        upload_id=str(upload_id),
        object_version=int(new_version),
        address=address,
        seed_phrase=seed_phrase,
    )
    swapped = await writer.swap_current_version_cas(
        object_id=object_id,
        expected_old_version=int(expected_old_version),
        new_version=int(new_version),
    )
    if not swapped:
        await db.execute(get_query("delete_version_and_parts"), object_id, int(new_version))
        return False

    return True


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
    redis_client = async_redis.from_url(config.redis_url)
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
                    "object_version": int(r["object_version"]),
                    "content_type": str(r["content_type"]),
                    "metadata": json.loads(r["metadata"]) if isinstance(r["metadata"], str) else (r["metadata"] or {}),
                }

        # Owner identity: for now, use main account equal to bucket owner; resolve per object
        # We fetch owner/main account via get_object_for_download_with_permissions in callers normally;
        # For migration we assume address/seed in env or external context.
        address = args.address or ""
        seed_phrase = args.seed or ""

        rc = 0
        async for o in _iter_targets():
            md5_row = await db.fetchrow(
                """
                SELECT ov.md5_hash
                FROM objects o
                JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
                WHERE o.object_id = $1
                """,
                o["object_id"],
            )
            md5 = (md5_row and (md5_row[0] or "")) or ""

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
                expected_md5=md5,
                is_public=bool(o.get("is_public", False)),
                source_storage_version=int(o.get("storage_version", 2)),
                address=address,
                seed_phrase=seed_phrase,
            )
            if not ok:
                rc = 1
        return rc
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
        finally:
            await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Migrate objects to target storage version")
    ap.add_argument("--bucket", default="", help="Bucket name (optional; with --key for single object)")
    ap.add_argument("--key", default="", help="Object key (optional; requires --bucket)")
    ap.add_argument("--address", default="", help="Account address to use for decrypt/encrypt context")
    ap.add_argument("--seed", default="", help="Seed phrase for decrypt/encrypt context")
    args = ap.parse_args()

    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
