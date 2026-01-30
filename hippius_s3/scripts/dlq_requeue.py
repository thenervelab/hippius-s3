#!/usr/bin/env python3
"""
DLQ Requeue CLI for Hippius S3

Command-line tool for managing the Dead-Letter Queue of failed upload requests.
Provides safe requeue operations with distributed locking and admin controls.

Usage:
    python -m hippius_s3.scripts.dlq_requeue peek --limit 10
    python -m hippius_s3.scripts.dlq_requeue requeue --object-id abc123
    python -m hippius_s3.scripts.dlq_requeue purge --object-id abc123
    python -m hippius_s3.scripts.dlq_requeue export --file dlq_backup.json
"""

import argparse
import asyncio
import contextlib
import json
import logging
import sys
import uuid
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


# Add the parent directory to sys.path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import redis.asyncio as async_redis

from hippius_s3.config import get_config
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_request


logger = logging.getLogger(__name__)


class DLQManager:
    """Manages Dead-Letter Queue operations."""

    def __init__(self, redis_client: Any):
        self.redis_client = redis_client
        self.dlq_key = "upload_requests:dlq"
        self.lock_prefix = "dlq:requeue:lock:"

    def _lock_key(self, object_id: str) -> str:
        return f"{self.lock_prefix}{object_id}"

    async def _acquire_lock(self, object_id: str, ttl_ms: int = 60000) -> Optional[str]:
        """Acquire a per-object lock using Redis SET NX PX. Returns token or None."""
        token = uuid.uuid4().hex
        ok = await self.redis_client.set(self._lock_key(object_id), token, nx=True, px=ttl_ms)
        return token if ok else None

    async def _release_lock(self, object_id: str, token: str) -> None:
        """Release lock only if token matches (atomic compare-and-delete)."""
        script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
        with contextlib.suppress(Exception):
            await self.redis_client.eval(script, 1, self._lock_key(object_id), token)

    async def peek(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Peek at DLQ entries without removing them."""
        raw = await self.redis_client.lrange(self.dlq_key, -limit, -1)
        out: List[Dict[str, Any]] = []
        for entry_json in raw:
            try:
                out.append(json.loads(entry_json))
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in DLQ: %r", entry_json)
        return out

    async def stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        count = await self.redis_client.llen(self.dlq_key)
        error_types = {"transient": 0, "permanent": 0, "unknown": 0}

        if count > 0:
            all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
            for entry_json in all_entries:
                try:
                    entry = json.loads(entry_json)
                    error_type = entry.get("error_type", "unknown")
                    error_types[error_type] = error_types.get(error_type, 0) + 1
                except json.JSONDecodeError:
                    error_types["unknown"] += 1

        return {"total_entries": count, "error_types": error_types}

    async def _find_and_remove_entry(self, object_id: str) -> Optional[Dict[str, Any]]:
        """Find and remove a specific entry by object_id."""
        # Get all entries (this is inefficient but DLQ should be small)
        all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
        target_entry: Optional[Dict[str, Any]] = None

        # Try to remove by exact string match (atomic per element)
        for entry_json in all_entries:
            try:
                entry = json.loads(entry_json)
            except json.JSONDecodeError:
                continue
            if entry.get("object_id") == object_id:
                removed = await self.redis_client.lrem(self.dlq_key, 1, entry_json)
                if removed:
                    target_entry = entry
                    break

        return target_entry

    async def requeue(self, object_id: str, force: bool = False) -> bool:
        """Requeue a specific entry by object_id with DLQ hydration."""
        # Acquire per-object lock to serialize requeues
        token = await self._acquire_lock(object_id)
        if not token:
            logger.error(f"Requeue already in progress for object_id: {object_id}")
            return False

        try:
            entry = await self._find_and_remove_entry(object_id)
            if not entry:
                logger.error(f"No DLQ entry found for object_id: {object_id}")
                return False

            # Check if it's a permanent error and force is not set
            if entry.get("error_type") == "permanent" and not force:
                logger.error(
                    f"Refusing to requeue permanent error for object_id: {object_id}. Use --force to override."
                )
                # Put it back
                await self.redis_client.lpush(self.dlq_key, json.dumps(entry))
                return False

            # Reconstruct the payload
            payload_data = entry["payload"]
            payload = UploadChainRequest.model_validate(payload_data)

            # Reset attempts if not forcing
            if not force:
                payload.attempts = 0

            # Directly requeue the payload (no DLQ filesystem hydration)
            await enqueue_upload_request(payload)
            logger.info(f"Successfully requeued object_id: {object_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to requeue object_id: {object_id}, error: {e}")
            # Put it back on failure (best-effort)
            with contextlib.suppress(Exception):
                if "entry" in locals() and entry:
                    await self.redis_client.lpush(self.dlq_key, json.dumps(entry))
            return False
        finally:
            # Always release the lock
            with contextlib.suppress(Exception):
                await self._release_lock(object_id, token)

    async def requeue_all(self, force: bool = False) -> int:
        """Requeue all DLQ entries (best-effort). Returns count successfully requeued."""
        all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
        count = 0
        for entry_json in all_entries:
            try:
                entry = json.loads(entry_json)
                oid = entry.get("object_id")
                if not oid:
                    continue
                ok = await self.requeue(str(oid), force)
                if ok:
                    count += 1
            except Exception:
                continue
        return count

    async def purge(self, object_id: Optional[str] = None) -> int:
        """Purge entries from DLQ. If object_id is specified, only that entry."""
        if object_id:
            entry = await self._find_and_remove_entry(object_id)
            return 1 if entry else 0
        # Purge all
        count = int(await self.redis_client.llen(self.dlq_key))
        await self.redis_client.delete(self.dlq_key)
        return count

    async def export(self, file_path: str) -> bool:
        """Export DLQ contents to a JSON file."""
        try:
            all_entries = await self.redis_client.lrange(self.dlq_key, 0, -1)
            entries = []
            for entry_json in all_entries:
                try:
                    entries.append(json.loads(entry_json))
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON: {entry_json}")

            def _write_json() -> None:
                with open(file_path, "w") as f:
                    json.dump(entries, f, indent=2, default=str)

            await asyncio.to_thread(_write_json)

            logger.info(f"Exported {len(entries)} entries to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to export DLQ: {e}")
            return False

    async def import_dlq(self, file_path: str) -> bool:
        """Import DLQ contents from a JSON file."""
        try:

            def _read_json() -> Any:
                with open(file_path, "r") as f:
                    return json.load(f)

            entries = await asyncio.to_thread(_read_json)

            for entry in entries:
                await self.redis_client.lpush(self.dlq_key, json.dumps(entry))

            logger.info(f"Imported {len(entries)} entries from {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to import DLQ: {e}")
            return False


async def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ Requeue CLI for Hippius S3")
    parser.add_argument(
        "--worker",
        choices=["upload", "unpin"],
        default="upload",
        help="Worker type: upload or unpin (default: upload)",
    )
    parser.add_argument(
        "--backend",
        choices=["ipfs", "arion"],
        default="ipfs",
        help="Backend name for upload worker (default: ipfs)",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # peek command
    peek_parser = subparsers.add_parser("peek", help="Peek at DLQ entries")
    peek_parser.add_argument("--limit", type=int, default=10, help="Number of entries to show")

    # requeue command
    requeue_parser = subparsers.add_parser("requeue", help="Requeue entries")
    requeue_parser.add_argument("--object-id", help="Specific object ID to requeue")
    requeue_parser.add_argument("--cid", help="Specific CID to requeue (for unpin worker)")
    requeue_parser.add_argument("--force", action="store_true", help="Force requeue of permanent errors")

    # purge command
    purge_parser = subparsers.add_parser("purge", help="Purge entries from DLQ")
    purge_parser.add_argument("--object-id", help="Specific object ID to purge")
    purge_parser.add_argument("--cid", help="Specific CID to purge (for unpin worker)")

    # export command
    export_parser = subparsers.add_parser("export", help="Export DLQ to JSON file")
    export_parser.add_argument("--file", required=True, help="Output file path")

    import_parser = subparsers.add_parser("import", help="Import DLQ from JSON file")
    import_parser.add_argument("--file", required=True, help="Input file path")

    # remove DLQ filesystem-related commands (no longer applicable)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Get Redis clients
    config = get_config()

    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)

    try:
        from hippius_s3.dlq.base import BaseDLQManager

        dlq_manager: BaseDLQManager[Any]
        if args.worker == "upload":
            from hippius_s3.dlq.upload_dlq import UploadDLQManager

            dlq_manager = UploadDLQManager(redis_queues_client, backend_name=args.backend)
        else:
            from hippius_s3.dlq.unpin_dlq import UnpinDLQManager

            dlq_manager = UnpinDLQManager(redis_queues_client)

        if args.command == "peek":
            entries = await dlq_manager.peek(args.limit)
            if not entries:
                print("DLQ is empty")
                return

            worker_desc = f"{args.worker} worker"
            if args.worker == "upload":
                worker_desc = f"{args.backend} backend {args.worker} worker"
            print(f"DLQ entries for {worker_desc} (showing {len(entries)}):")
            for i, entry in enumerate(entries, 1):
                print(f"\n--- Entry {i} ---")
                print(f"Object ID: {entry.get('object_id')}")
                if args.worker == "upload":
                    print(f"Backend: {args.backend}")
                    print(f"Upload ID: {entry.get('upload_id')}")
                    print(f"Bucket: {entry.get('bucket_name')}")
                    print(f"Key: {entry.get('object_key')}")
                else:
                    print(f"CID: {entry.get('cid')}")
                    print(f"Address: {entry.get('address')}")
                    print(f"Object Version: {entry.get('object_version')}")
                print(f"Attempts: {entry.get('attempts')}")
                print(f"Error Type: {entry.get('error_type')}")
                print(f"Last Error: {entry.get('last_error', '')[:100]}...")
                print(f"First Enqueued: {entry.get('first_enqueued_at')}")
                print(f"Last Attempt: {entry.get('last_attempt_at')}")

        elif args.command == "stats":
            stats = await dlq_manager.stats()
            print("DLQ Statistics:")
            print(f"  Total entries: {stats['total_entries']}")
            print("  Error types:")
            for error_type, count in stats["error_types"].items():
                print(f"    {error_type}: {count}")

        elif args.command == "requeue":
            identifier = args.cid if args.cid else args.object_id
            if identifier:
                success = await dlq_manager.requeue(identifier, args.force)
                if success:
                    id_type = "CID" if args.cid else "object_id"
                    print(f"Successfully requeued {id_type}: {identifier}")
                else:
                    id_type = "CID" if args.cid else "object_id"
                    print(f"Failed to requeue {id_type}: {identifier}")
                    sys.exit(1)
            else:
                count = await dlq_manager.requeue_all(args.force)
                print(f"Requeued {count} DLQ entries")

        elif args.command == "purge":
            identifier = args.cid if args.cid else args.object_id
            count = await dlq_manager.purge(identifier)
            if identifier:
                if count > 0:
                    id_type = "CID" if args.cid else "object_id"
                    print(f"Purged 1 entry for {id_type}: {identifier}")
                else:
                    id_type = "CID" if args.cid else "object_id"
                    print(f"No entry found for {id_type}: {identifier}")
            else:
                print(f"Purged {count} entries from DLQ")

        elif args.command == "export":
            success = await dlq_manager.export(args.file)
            if not success:
                sys.exit(1)

        elif args.command == "import":
            success = await dlq_manager.import_dlq(args.file)
            if not success:
                sys.exit(1)

        # removed DLQ filesystem-related command handlers

    finally:
        if redis_queues_client:
            await redis_queues_client.close()


if __name__ == "__main__":
    asyncio.run(main())
