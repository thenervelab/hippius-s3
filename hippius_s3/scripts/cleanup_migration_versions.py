from __future__ import annotations

import argparse
import asyncio

import asyncpg  # type: ignore[import-untyped]

from hippius_s3.config import get_config


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
    try:
        rows = await db.fetch(
            """
            SELECT ov.object_id, ov.object_version
            FROM object_versions ov
            JOIN objects o ON o.object_id = ov.object_id
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE ov.version_type = 'migration'
              AND ov.object_version <> o.current_object_version
              AND ($1::text IS NULL OR b.bucket_name = $1)
              AND ($2::text IS NULL OR o.object_key = $2)
            ORDER BY ov.object_id, ov.object_version
            """,
            (args.bucket or None),
            (args.key or None),
        )
        deleted = 0
        for r in rows:
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
    args = ap.parse_args()
    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
