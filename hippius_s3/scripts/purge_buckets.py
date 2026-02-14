from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from datetime import timezone
from typing import Any

import aiofiles
import asyncpg
import redis.asyncio as async_redis


async def _resolve_buckets(db: Any, *, address: str, bucket_names: list[str]) -> list[dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT bucket_id, bucket_name
        FROM buckets
        WHERE main_account_id = $1
          AND bucket_name = ANY($2::text[])
        ORDER BY bucket_name
        """,
        address,
        bucket_names,
    )
    return [dict(r) for r in rows]


async def collect_bucket_cids(db: Any, *, address: str, bucket_names: list[str]) -> list[str]:
    rows = await db.fetch(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        ), target_objects AS (
            SELECT o.object_id
            FROM objects o
            JOIN target_buckets tb ON tb.bucket_id = o.bucket_id
        ), all_cids AS (
            SELECT DISTINCT COALESCE(c.cid, ov.ipfs_cid) AS cid
            FROM target_objects tobj
            JOIN object_versions ov ON ov.object_id = tobj.object_id
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, ov.ipfs_cid) != ''
              AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT COALESCE(c.cid, p.ipfs_cid) AS cid
            FROM target_objects tobj
            JOIN parts p ON p.object_id = tobj.object_id
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, p.ipfs_cid) != ''
              AND COALESCE(c.cid, p.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT pc.cid
            FROM target_objects tobj
            JOIN parts p ON p.object_id = tobj.object_id
            JOIN part_chunks pc ON pc.part_id = p.part_id
            WHERE pc.cid IS NOT NULL
              AND pc.cid != ''
              AND pc.cid != 'pending'
        )
        SELECT cid FROM all_cids ORDER BY cid
        """,
        address,
        bucket_names,
    )
    return [str(row["cid"]) for row in rows]


async def get_bucket_deletion_stats(db: Any, *, address: str, bucket_names: list[str]) -> dict[str, int]:
    buckets_count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM buckets
        WHERE main_account_id = $1
          AND bucket_name = ANY($2::text[])
        """,
        address,
        bucket_names,
    )

    objects_count = await db.fetchval(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        )
        SELECT COUNT(*)
        FROM objects o
        JOIN target_buckets tb ON tb.bucket_id = o.bucket_id
        """,
        address,
        bucket_names,
    )

    object_versions_count = await db.fetchval(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        )
        SELECT COUNT(*)
        FROM object_versions ov
        JOIN objects o ON o.object_id = ov.object_id
        JOIN target_buckets tb ON tb.bucket_id = o.bucket_id
        """,
        address,
        bucket_names,
    )

    parts_count = await db.fetchval(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        )
        SELECT COUNT(*)
        FROM parts p
        JOIN objects o ON o.object_id = p.object_id
        JOIN target_buckets tb ON tb.bucket_id = o.bucket_id
        """,
        address,
        bucket_names,
    )

    part_chunks_count = await db.fetchval(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        )
        SELECT COUNT(*)
        FROM part_chunks pc
        JOIN parts p ON p.part_id = pc.part_id
        JOIN objects o ON o.object_id = p.object_id
        JOIN target_buckets tb ON tb.bucket_id = o.bucket_id
        """,
        address,
        bucket_names,
    )

    multipart_uploads_count = await db.fetchval(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        )
        SELECT COUNT(*)
        FROM multipart_uploads mu
        JOIN target_buckets tb ON tb.bucket_id = mu.bucket_id
        """,
        address,
        bucket_names,
    )

    cids_count = await db.fetchval(
        """
        WITH target_buckets AS (
            SELECT bucket_id
            FROM buckets
            WHERE main_account_id = $1
              AND bucket_name = ANY($2::text[])
        ), target_objects AS (
            SELECT o.object_id
            FROM objects o
            JOIN target_buckets tb ON tb.bucket_id = o.bucket_id
        ), all_cids AS (
            SELECT DISTINCT COALESCE(c.cid, ov.ipfs_cid) AS cid
            FROM target_objects tobj
            JOIN object_versions ov ON ov.object_id = tobj.object_id
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, ov.ipfs_cid) != ''
              AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT COALESCE(c.cid, p.ipfs_cid) AS cid
            FROM target_objects tobj
            JOIN parts p ON p.object_id = tobj.object_id
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, p.ipfs_cid) != ''
              AND COALESCE(c.cid, p.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT pc.cid
            FROM target_objects tobj
            JOIN parts p ON p.object_id = tobj.object_id
            JOIN part_chunks pc ON pc.part_id = p.part_id
            WHERE pc.cid IS NOT NULL
              AND pc.cid != ''
              AND pc.cid != 'pending'
        )
        SELECT COUNT(*) FROM all_cids
        """,
        address,
        bucket_names,
    )

    return {
        "buckets": int(buckets_count or 0),
        "objects": int(objects_count or 0),
        "object_versions": int(object_versions_count or 0),
        "parts": int(parts_count or 0),
        "part_chunks": int(part_chunks_count or 0),
        "multipart_uploads": int(multipart_uploads_count or 0),
        "cids": int(cids_count or 0),
    }


async def enqueue_unpins(*, cids: list[str], address: str) -> int:
    from hippius_s3.queue import UnpinChainRequest
    from hippius_s3.queue import enqueue_unpin_request

    log = logging.getLogger("purge_buckets")
    enqueued = 0
    for idx, cid in enumerate(cids, 1):
        try:
            await enqueue_unpin_request(
                payload=UnpinChainRequest(
                    address=address,
                    object_id="purge_buckets_cleanup",
                    object_version=1,
                    cid=cid,
                )
            )
            enqueued += 1
        except Exception as e:
            log.warning(f"[{idx}/{len(cids)}] Failed to enqueue unpin request: {cid} - {e}")
    return enqueued


async def delete_buckets(db: Any, *, address: str, bucket_names: list[str]) -> list[str]:
    rows = await db.fetch(
        """
        DELETE FROM buckets
        WHERE main_account_id = $1
          AND bucket_name = ANY($2::text[])
        RETURNING bucket_name
        """,
        address,
        bucket_names,
    )
    return [str(r["bucket_name"]) for r in rows]


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("purge_buckets")

    from hippius_s3.config import get_config

    config = get_config()
    redis_queues_client: async_redis.Redis | None = None

    db = await asyncpg.connect(config.database_url)
    try:
        # Basic validation
        user_exists = await db.fetchval("SELECT COUNT(*) FROM users WHERE main_account_id = $1", args.address)
        if not user_exists:
            log.error(f"User not found: {args.address}")
            return 1

        bucket_names = sorted({b.strip() for b in args.bucket if b.strip()})
        if not bucket_names:
            log.error("No buckets specified")
            return 1

        buckets = await _resolve_buckets(db, address=args.address, bucket_names=bucket_names)
        if len(buckets) != len(bucket_names):
            found = {b["bucket_name"] for b in buckets}
            missing = [b for b in bucket_names if b not in found]
            log.error(f"Some buckets were not found for this user: {missing}")
            return 1

        stats = await get_bucket_deletion_stats(db, address=args.address, bucket_names=bucket_names)
        log.info(f"Target user: {args.address}")
        log.info(f"Target buckets ({len(bucket_names)}): {bucket_names}")
        log.info("This operation will cascade delete:")
        log.info(f"  - {stats['buckets']} buckets")
        log.info(f"  - {stats['objects']} objects")
        log.info(f"  - {stats['object_versions']} object versions")
        log.info(f"  - {stats['parts']} parts")
        log.info(f"  - {stats['part_chunks']} part chunks")
        log.info(f"  - {stats['multipart_uploads']} multipart uploads")

        log.info("Collecting CIDs...")
        cids = await collect_bucket_cids(db, address=args.address, bucket_names=bucket_names)
        log.info(f"Collected {len(cids)} unique CIDs")

        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = args.output_json or f"purge_buckets_{args.address}_{timestamp}.json"

        async with aiofiles.open(output_file, "w") as f:
            await f.write(
                json.dumps(
                    {"address": args.address, "buckets": bucket_names, "cids": cids},
                    indent=2,
                )
            )
        log.info(f"CID list written to: {output_file}")

        if args.dry_run:
            log.info("DRY-RUN: no deletions performed, no unpins enqueued")
            return 0

        if not args.yes:
            log.error("Refusing to delete without --yes")
            return 2

        # Delete buckets first; only enqueue unpins after successful DB delete.
        deleted_bucket_names = await delete_buckets(db, address=args.address, bucket_names=bucket_names)
        deleted_bucket_names_sorted = sorted(deleted_bucket_names)
        if deleted_bucket_names_sorted != bucket_names:
            log.error(
                "Deletion mismatch: expected to delete %s but deleted %s",
                bucket_names,
                deleted_bucket_names_sorted,
            )
            return 3

        log.info(f"Successfully deleted buckets: {deleted_bucket_names_sorted}")

        if args.unpin:
            redis_queues_client = async_redis.from_url(config.redis_queues_url)
            from hippius_s3.queue import initialize_queue_client

            initialize_queue_client(redis_queues_client)
            enqueued = await enqueue_unpins(cids=cids, address=args.address)
            log.info(f"Enqueued {enqueued}/{len(cids)} unpin requests")

        return 0
    finally:
        await db.close()
        if redis_queues_client:
            await redis_queues_client.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Admin tool: delete specific buckets for a user and optionally enqueue unpin requests"
    )
    ap.add_argument("--address", required=True, help="User main_account_id (SS58) that owns the buckets")
    ap.add_argument(
        "--bucket",
        action="append",
        default=[],
        help="Bucket name to delete (repeatable)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Preview and write CID list JSON, without deleting/unpinning"
    )
    ap.add_argument("--unpin", action="store_true", help="Enqueue unpin requests after DB deletion")
    ap.add_argument("--output-json", help="Where to write the CID list JSON (default: purge_buckets_<...>.json)")
    ap.add_argument("--yes", action="store_true", help="Required to actually delete")
    args = ap.parse_args()

    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
