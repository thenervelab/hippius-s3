from __future__ import annotations

import argparse
import asyncio

import asyncpg  # type: ignore[import-untyped]
import redis.asyncio as async_redis  # type: ignore[import-untyped]

from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)
    try:
        rows = await db.fetch(
            """
            SELECT ov.object_id, ov.object_version, COALESCE(c.cid, ov.ipfs_cid) AS ipfs_cid, b.bucket_name, o.object_key, b.main_account_id
            FROM object_versions ov
            JOIN objects o ON o.object_id = ov.object_id
            JOIN buckets b ON b.bucket_id = o.bucket_id
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE ov.version_type = 'migration'
              AND ov.object_version <> o.current_object_version
              AND (NOW() - ov.last_modified) >= ($1::int * INTERVAL '1 minute')
              AND ($2::text IS NULL OR b.bucket_name = $2)
              AND ($3::text IS NULL OR o.object_key = $3)
            ORDER BY ov.object_id, ov.object_version
            LIMIT COALESCE($4::int, 1000)
            """,
            int(args.min_age_minutes),
            (args.bucket or None),
            (args.key or None),
            (args.limit if args.limit and args.limit > 0 else None),
        )
        if args.dry_run:
            print(f"Would delete {len(rows)} migration versions")
            return 0

        deleted = 0
        for r in rows:
            cid = r["ipfs_cid"]
            if cid:
                from contextlib import suppress

                with suppress(Exception):
                    await enqueue_unpin_request(
                        payload=UnpinChainRequest(
                            address=r["main_account_id"],
                            object_id=str(r["object_id"]),
                            object_version=int(r["object_version"]),
                            cid=str(cid),
                        ),
                    )
            await db.execute(
                """
                WITH del_parts AS (
                    DELETE FROM parts WHERE object_id = $1 AND object_version = $2 RETURNING 1
                ), del_ver AS (
                    DELETE FROM object_versions WHERE object_id = $1 AND object_version = $2 RETURNING 1
                )
                SELECT 1
                """,
                r["object_id"],
                r["object_version"],
            )
            deleted += 1
        print(f"Deleted {deleted} migration versions")
        return 0
    finally:
        await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Cleanup non-current migration versions")
    ap.add_argument("--bucket", default="", help="Bucket name filter")
    ap.add_argument("--key", default="", help="Object key filter (requires --bucket)")
    ap.add_argument("--min-age-minutes", type=int, default=30, help="Minimum age in minutes to consider for deletion")
    ap.add_argument("--limit", type=int, default=1000, help="Max versions to process in this run")
    ap.add_argument("--dry-run", action="store_true", help="Preview deletions without executing")
    args = ap.parse_args()
    if args.key and not args.bucket:
        raise SystemExit("--key requires --bucket")
    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
