#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.config import get_config
from hippius_s3.dlq.pinner_dlq import PinnerDLQManager


logger = logging.getLogger(__name__)


async def reset_pin_attempts_for_cid(db: asyncpg.Connection, cid: str) -> None:
    """Reset pin tracking for a specific CID."""
    await db.execute(
        """
        UPDATE part_chunks
        SET pin_attempts = 0,
            last_pinned_at = NULL
        WHERE cid = $1
        """,
        cid,
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Pinner DLQ Requeue CLI for Hippius S3")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    peek_parser = subparsers.add_parser("peek", help="Peek at DLQ entries")
    peek_parser.add_argument("--limit", type=int, default=10, help="Number of entries to show")

    subparsers.add_parser("stats", help="Show DLQ statistics")

    requeue_parser = subparsers.add_parser("requeue", help="Requeue entries")
    requeue_parser.add_argument("--cid", help="Specific CID to requeue (omit to requeue all)")
    requeue_parser.add_argument(
        "--force",
        action="store_true",
        help="Force immediate enqueue to substrate queue (default: reset attempts and wait for pin checker)",
    )

    purge_parser = subparsers.add_parser("purge", help="Purge entries from DLQ")
    purge_parser.add_argument("--cid", help="Specific CID to purge (omit to purge all)")

    export_parser = subparsers.add_parser("export", help="Export DLQ to JSON file")
    export_parser.add_argument("--file", required=True, help="Output file path")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    config = get_config()

    redis_client = async_redis.from_url(config.redis_url)
    db = await asyncpg.connect(config.database_url)

    try:
        dlq_manager = PinnerDLQManager(redis_client)

        if args.command == "peek":
            entries = await dlq_manager.peek(args.limit)
            if not entries:
                print("Pinner DLQ is empty")
                return

            print(f"Pinner DLQ entries (showing {len(entries)}):")
            for i, entry in enumerate(entries, 1):
                print(f"\n--- Entry {i} ---")
                print(f"CID: {entry.cid}")
                print(f"User: {entry.user}")
                print(f"Object ID: {entry.object_id}")
                print(f"Object Version: {entry.object_version}")
                print(f"Reason: {entry.reason}")
                print(f"Pin Attempts: {entry.pin_attempts}")
                print(f"Last Pinned At: {entry.last_pinned_at}")
                print(f"DLQ Timestamp: {entry.dlq_timestamp}")

        elif args.command == "stats":
            stats = await dlq_manager.stats()
            print("Pinner DLQ Statistics:")
            print(f"  Total entries: {stats['total_entries']}")
            print(f"  Queue key: {stats['queue_key']}")

        elif args.command == "requeue":
            if args.cid:
                entry_opt = await dlq_manager.find_and_remove(args.cid)
                if entry_opt is None:
                    print(f"No DLQ entry found for CID: {args.cid}")
                    sys.exit(1)
                entry = entry_opt

                await reset_pin_attempts_for_cid(db, args.cid)
                print(f"Reset pin attempts for CID: {args.cid}")

                if args.force:
                    from hippius_s3.queue import SubstratePinningRequest
                    from hippius_s3.queue import enqueue_substrate_request
                    from hippius_s3.queue import initialize_queue_client

                    redis_queues_client = async_redis.from_url(config.redis_queues_url)
                    initialize_queue_client(redis_queues_client)

                    request = SubstratePinningRequest(
                        cids=[entry.cid],
                        address=entry.user,
                        object_id=entry.object_id,
                        object_version=entry.object_version,
                    )
                    await enqueue_substrate_request(request)
                    print(f"Enqueued CID to substrate queue: {args.cid}")
                    await redis_queues_client.close()
                else:
                    print(f"CID will be re-enqueued on next pin checker cycle: {args.cid}")

            else:
                entries = await dlq_manager.export_all()
                if not entries:
                    print("Pinner DLQ is empty, nothing to requeue")
                    return

                count = 0
                for entry in entries:
                    removed_entry = await dlq_manager.find_and_remove(entry.cid)
                    if removed_entry:
                        await reset_pin_attempts_for_cid(db, entry.cid)
                        count += 1

                print(f"Reset pin attempts for {count} CIDs")

                if args.force:
                    from hippius_s3.queue import SubstratePinningRequest
                    from hippius_s3.queue import enqueue_substrate_request
                    from hippius_s3.queue import initialize_queue_client

                    redis_queues_client = async_redis.from_url(config.redis_queues_url)
                    initialize_queue_client(redis_queues_client)

                    for entry in entries:
                        request = SubstratePinningRequest(
                            cids=[entry.cid],
                            address=entry.user,
                            object_id=entry.object_id,
                            object_version=entry.object_version,
                        )
                        await enqueue_substrate_request(request)

                    print(f"Enqueued {count} CIDs to substrate queue")
                    await redis_queues_client.close()
                else:
                    print("CIDs will be re-enqueued on next pin checker cycle")

        elif args.command == "purge":
            count = await dlq_manager.purge(args.cid)
            if args.cid:
                if count > 0:
                    print(f"Purged 1 entry for CID: {args.cid}")
                else:
                    print(f"No entry found for CID: {args.cid}")
            else:
                print(f"Purged {count} entries from pinner DLQ")

        elif args.command == "export":
            entries = await dlq_manager.export_all()
            if not entries:
                print("Pinner DLQ is empty, nothing to export")
                return

            def _write_json() -> None:
                with open(args.file, "w") as f:
                    json.dump([e.to_dict() for e in entries], f, indent=2, default=str)

            await asyncio.to_thread(_write_json)
            print(f"Exported {len(entries)} entries to {args.file}")

    finally:
        if redis_client:
            await redis_client.close()
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
