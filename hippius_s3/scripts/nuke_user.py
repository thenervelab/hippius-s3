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


async def collect_user_cids(db: Any, address: str) -> list[str]:
    rows = await db.fetch(
        """
        WITH user_objects AS (
            SELECT o.object_id, o.current_object_version
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.main_account_id = $1
        ), all_cids AS (
            SELECT DISTINCT COALESCE(c.cid, ov.ipfs_cid) AS cid
            FROM user_objects uo
            JOIN object_versions ov ON ov.object_id = uo.object_id
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, ov.ipfs_cid) != ''
              AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT COALESCE(c.cid, p.ipfs_cid) AS cid
            FROM user_objects uo
            JOIN parts p ON p.object_id = uo.object_id
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, p.ipfs_cid) != ''
              AND COALESCE(c.cid, p.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT pc.cid
            FROM user_objects uo
            JOIN parts p ON p.object_id = uo.object_id
            JOIN part_chunks pc ON pc.part_id = p.part_id
            WHERE pc.cid IS NOT NULL
              AND pc.cid != ''
              AND pc.cid != 'pending'
        )
        SELECT cid FROM all_cids ORDER BY cid
        """,
        address,
    )
    return [str(row["cid"]) for row in rows]


async def unpin_cids(cids: list[str], config: Any, address: str) -> dict[str, int]:
    from hippius_s3.queue import UnpinChainRequest
    from hippius_s3.queue import enqueue_unpin_request

    log = logging.getLogger("nuke_user")
    success_count = 0
    failed_count = 0
    enqueued_count = 0

    log.info(f"Starting unpin operations for {len(cids)} CIDs")

    for idx, cid in enumerate(cids, 1):
        try:
            await enqueue_unpin_request(
                payload=UnpinChainRequest(
                    address=address,
                    object_id="nuke_user_cleanup",
                    object_version=1,
                    cid=cid,
                ),
            )
            enqueued_count += 1
        except Exception as e:
            log.warning(f"[{idx}/{len(cids)}] Failed to enqueue unpin request: {cid} - {e}")

    log.info(f"Unpin summary: {success_count} IPFS unpins, {failed_count} IPFS failures, {enqueued_count} enqueued")
    return {
        "ipfs_success": success_count,
        "ipfs_failed": failed_count,
        "enqueued": enqueued_count,
    }


async def get_deletion_stats(db: Any, address: str) -> dict[str, int]:
    buckets_count = await db.fetchval(
        "SELECT COUNT(*) FROM buckets WHERE main_account_id = $1",
        address,
    )

    objects_count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM objects o
        JOIN buckets b ON b.bucket_id = o.bucket_id
        WHERE b.main_account_id = $1
        """,
        address,
    )

    object_versions_count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM object_versions ov
        JOIN objects o ON o.object_id = ov.object_id
        JOIN buckets b ON b.bucket_id = o.bucket_id
        WHERE b.main_account_id = $1
        """,
        address,
    )

    parts_count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM parts p
        JOIN objects o ON o.object_id = p.object_id
        JOIN buckets b ON b.bucket_id = o.bucket_id
        WHERE b.main_account_id = $1
        """,
        address,
    )

    part_chunks_count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM part_chunks pc
        JOIN parts p ON p.part_id = pc.part_id
        JOIN objects o ON o.object_id = p.object_id
        JOIN buckets b ON b.bucket_id = o.bucket_id
        WHERE b.main_account_id = $1
        """,
        address,
    )

    multipart_uploads_count = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM multipart_uploads mu
        JOIN buckets b ON b.bucket_id = mu.bucket_id
        WHERE b.main_account_id = $1
        """,
        address,
    )

    cids_count = await db.fetchval(
        """
        WITH user_objects AS (
            SELECT o.object_id
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.main_account_id = $1
        ), all_cids AS (
            SELECT DISTINCT COALESCE(c.cid, ov.ipfs_cid) AS cid
            FROM user_objects uo
            JOIN object_versions ov ON ov.object_id = uo.object_id
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, ov.ipfs_cid) != ''
              AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT COALESCE(c.cid, p.ipfs_cid) AS cid
            FROM user_objects uo
            JOIN parts p ON p.object_id = uo.object_id
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
              AND COALESCE(c.cid, p.ipfs_cid) != ''
              AND COALESCE(c.cid, p.ipfs_cid) != 'pending'

            UNION

            SELECT DISTINCT pc.cid
            FROM user_objects uo
            JOIN parts p ON p.object_id = uo.object_id
            JOIN part_chunks pc ON pc.part_id = p.part_id
            WHERE pc.cid IS NOT NULL
              AND pc.cid != ''
              AND pc.cid != 'pending'
        )
        SELECT COUNT(*) FROM all_cids
        """,
        address,
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


async def nuke_user(db: Any, address: str) -> bool:
    result = await db.execute("DELETE FROM users WHERE main_account_id = $1", address)
    return "DELETE" in result


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("nuke_user")

    from hippius_s3.config import get_config

    config = get_config()
    db = await asyncpg.connect(config.database_url)

    redis_queues_client = None
    if args.unpin and not args.dry_run:
        redis_queues_client = async_redis.from_url(config.redis_queues_url)
        from hippius_s3.queue import initialize_queue_client

        initialize_queue_client(redis_queues_client)

    try:
        user_exists = await db.fetchval(
            "SELECT COUNT(*) FROM users WHERE main_account_id = $1",
            args.address,
        )

        if not user_exists:
            log.error(f"User not found: {args.address}")
            return 1

        stats = await get_deletion_stats(db, args.address)

        log.info(f"Collecting CIDs for user: {args.address}")
        cids = await collect_user_cids(db, args.address)
        log.info(f"Collected {len(cids)} unique CIDs")

        if args.dry_run:
            log.info(f"DRY-RUN: Would delete user: {args.address}")
            log.info(f"  Buckets: {stats['buckets']}")
            log.info(f"  Objects: {stats['objects']}")
            log.info(f"  Object Versions: {stats['object_versions']}")
            log.info(f"  Parts: {stats['parts']}")
            log.info(f"  Part Chunks: {stats['part_chunks']}")
            log.info(f"  Multipart Uploads: {stats['multipart_uploads']}")
            log.info(f"  Unique CIDs: {stats['cids']}")

            output_file = f"{args.address}_cids.json"
            output_data = {args.address: cids}

            async with aiofiles.open(output_file, "w") as f:
                await f.write(json.dumps(output_data, indent=2))

            log.info(f"CID list written to: {output_file}")
            return 0

        log.info(f"Deleting user: {args.address}")
        log.info("  Will cascade delete:")
        log.info(f"    - {stats['buckets']} buckets")
        log.info(f"    - {stats['objects']} objects")
        log.info(f"    - {stats['object_versions']} object versions")
        log.info(f"    - {stats['parts']} parts")
        log.info(f"    - {stats['part_chunks']} part chunks")
        log.info(f"    - {stats['multipart_uploads']} multipart uploads")

        deleted = await nuke_user(db, args.address)

        if not deleted:
            log.error(f"Failed to delete user: {args.address}")
            return 1

        log.info(f"Successfully deleted user: {args.address}")

        if args.unpin:
            log.info(f"Starting unpin operations for {len(cids)} CIDs...")
            unpin_results = await unpin_cids(cids, config, args.address)
            log.info(
                f"Unpin complete: IPFS={unpin_results['ipfs_success']}/{len(cids)} success, {unpin_results['enqueued']} enqueued"
            )

        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = f"nuke_user_{args.address}_{timestamp}.json"

        output_data = {args.address: cids}

        async with aiofiles.open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)

        log.info(f"CID list written to: {output_file}")
        log.info(f"Total CIDs: {len(cids)}")

        return 0
    finally:
        await db.close()
        if redis_queues_client:
            await redis_queues_client.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Delete all data for a user from the database")
    ap.add_argument("--address", required=True, help="User address (main_account_id) to delete")
    ap.add_argument("--dry-run", action="store_true", help="Preview deletions without executing")
    ap.add_argument(
        "--unpin", action="store_true", help="Unpin CIDs from IPFS and enqueue unpin requests (ignored in dry-run)"
    )
    args = ap.parse_args()

    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
