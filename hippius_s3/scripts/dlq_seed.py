#!/usr/bin/env python3
"""
DLQ Seeder script

Seeds the DLQ with on-disk chunks for an object by copying parts from Redis cache
and pushing a DLQ entry. Intended for test/e2e use from within a container.

Usage:
  python -m hippius_s3.scripts.dlq_seed seed --object-id <uuid>
"""

import argparse
import asyncio
import json
import logging
from typing import Any
from typing import List

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


async def _list_part_numbers_by_object_id(conn: asyncpg.Connection, object_id: str) -> List[int]:
    rows = await conn.fetch(
        """
        SELECT part_number
        FROM parts
        WHERE object_id = $1
        ORDER BY part_number
        """,
        object_id,
    )
    return [int(r[0]) for r in rows]


async def seed_object_to_dlq(object_id: str) -> bool:
    config = get_config()
    redis: Any = async_redis.from_url(config.redis_url)

    try:
        # Determine parts from DB
        conn = await asyncpg.connect(config.database_url)
        try:
            part_numbers = await _list_part_numbers_by_object_id(conn, object_id)
        finally:
            await conn.close()

        if not part_numbers:
            logger.error(f"No parts found for object_id={object_id}")
            return False

        # No longer persist bytes to DLQ filesystem; just create a DLQ entry for testing

        # Push a minimal DLQ entry
        dlq_entry = {
            "payload": {
                "object_id": object_id,
                "object_key": "",
                "bucket_name": "",
                "upload_id": None,
                "chunks": [{"id": pn} for pn in part_numbers],
            },
            "object_id": object_id,
            "attempts": 0,
            "last_error": "seeded for test",
            "error_type": "transient",
        }
        await redis.lpush("ipfs_upload_requests:dlq", json.dumps(dlq_entry))
        logger.info(f"Seeded DLQ for object_id={object_id} with parts={part_numbers}")
        return True
    finally:
        await redis.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ Seeder")
    sub = parser.add_subparsers(dest="cmd")
    sp = sub.add_parser("seed", help="Seed DLQ for an object id from Redis cache")
    sp.add_argument("--object-id", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if args.cmd == "seed":
        ok = await seed_object_to_dlq(args.object_id)
        if not ok:
            raise SystemExit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
