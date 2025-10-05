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
from hippius_s3.dlq.logic import DLQLogic
from hippius_s3.dlq.storage import DLQStorage
from hippius_s3.queue import UploadChainRequest


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

            # Use DLQLogic for hydration and requeue
            dlq_logic = DLQLogic(redis_client=self.redis_client)
            success = await dlq_logic.requeue_with_hydration(payload, force=force, redis_client=self.redis_client)

            if success:
                logger.info(f"Successfully requeued object_id: {object_id}")
                return True
            logger.error(f"Failed to requeue object_id: {object_id}")
            # Put it back on failure
            await self.redis_client.lpush(self.dlq_key, json.dumps(entry))
            return False

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
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # peek command
    peek_parser = subparsers.add_parser("peek", help="Peek at DLQ entries")
    peek_parser.add_argument("--limit", type=int, default=10, help="Number of entries to show")

    # requeue command
    requeue_parser = subparsers.add_parser("requeue", help="Requeue a specific entry")
    requeue_parser.add_argument("--object-id", required=True, help="Object ID to requeue")
    requeue_parser.add_argument("--force", action="store_true", help="Force requeue of permanent errors")

    # purge command
    purge_parser = subparsers.add_parser("purge", help="Purge entries from DLQ")
    purge_parser.add_argument("--object-id", help="Specific object ID to purge (omit to purge all)")

    # export command
    export_parser = subparsers.add_parser("export", help="Export DLQ to JSON file")
    export_parser.add_argument("--file", required=True, help="Output file path")

    import_parser = subparsers.add_parser("import", help="Import DLQ from JSON file")
    import_parser.add_argument("--file", required=True, help="Input file path")

    # cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up archived DLQ data")
    cleanup_parser.add_argument("--object-id", help="Specific object ID to cleanup (omit to cleanup all archived)")

    # dlq-parts: list available DLQ part numbers for an object (for testing)
    dlq_parts_parser = subparsers.add_parser("dlq-parts", help="List DLQ parts for an object")
    dlq_parts_parser.add_argument("--object-id", required=True, help="Object ID to inspect")

    # dlq-part-size: get size of a DLQ part (for testing)
    dlq_part_size_parser = subparsers.add_parser("dlq-part-size", help="Get DLQ part size for an object")
    dlq_part_size_parser.add_argument("--object-id", required=True, help="Object ID")
    dlq_part_size_parser.add_argument("--part", type=int, required=True, help="Part number")

    # archived-exists: check if archived directory exists for object (for testing)
    archived_exists_parser = subparsers.add_parser(
        "archived-exists", help="Check if archived directory exists for object"
    )
    archived_exists_parser.add_argument("--object-id", required=True, help="Object ID")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Get Redis client
    config = get_config()
    redis_client = async_redis.from_url(config.redis_url)

    try:
        dlq_manager = DLQManager(redis_client)

        if args.command == "peek":
            entries = await dlq_manager.peek(args.limit)
            if not entries:
                print("DLQ is empty")
                return

            print(f"DLQ entries (showing {len(entries)}):")
            for i, entry in enumerate(entries, 1):
                print(f"\n--- Entry {i} ---")
                print(f"Object ID: {entry.get('object_id')}")
                print(f"Upload ID: {entry.get('upload_id')}")
                print(f"Bucket: {entry.get('bucket_name')}")
                print(f"Key: {entry.get('object_key')}")
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
            success = await dlq_manager.requeue(args.object_id, args.force)
            if success:
                print(f"Successfully requeued object_id: {args.object_id}")
            else:
                print(f"Failed to requeue object_id: {args.object_id}")
                sys.exit(1)

        elif args.command == "purge":
            count = await dlq_manager.purge(args.object_id)
            if args.object_id:
                if count > 0:
                    print(f"Purged 1 entry for object_id: {args.object_id}")
                else:
                    print(f"No entry found for object_id: {args.object_id}")
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

        elif args.command == "cleanup":
            storage = DLQStorage()

            if args.object_id:
                try:
                    storage.delete_archived_object(args.object_id)
                    print(f"Cleaned up archived DLQ data for object_id: {args.object_id}")
                except Exception as e:
                    print(f"Failed to cleanup archived data for object_id: {args.object_id}: {e}")
                    sys.exit(1)
            else:
                # This would be dangerous - require explicit confirmation
                print("Cleanup of all archived data requires manual intervention.")
                print("Please specify --object-id or delete manually from DLQ_ARCHIVE_DIR.")
                sys.exit(1)

        elif args.command == "dlq-parts":
            storage = DLQStorage()
            # Prefer DLQ meta first (stored sidecars)
            parts = storage.list_chunks(args.object_id)
            if not parts:
                # Fallback: derive parts from Redis cache keys when DLQ files not yet persisted
                try:
                    keys_pattern = f"obj:{args.object_id}:part:*:meta"
                    keys = [key async for key in redis_client.scan_iter(keys_pattern, count=1000)]
                    found_parts: list[int] = []
                    for k in keys:
                        try:
                            key_str = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
                            # Expect ...:part:{n}:meta
                            idx = key_str.rfind(":part:")
                            if idx != -1:
                                tail = key_str[idx + len(":part:") :]
                                num_str = tail.split(":", 1)[0]
                                pn = int(num_str)
                                found_parts.append(pn)
                        except Exception:
                            continue
                    parts = sorted(set(found_parts))
                except Exception:
                    parts = []
            print(json.dumps(parts))

        elif args.command == "dlq-part-size":
            storage = DLQStorage()
            try:
                size = storage.part_size(args.object_id, args.part)
                print(size)
            except Exception:
                # Consistent with previous behavior; don't crash the CLI
                print("0")

        elif args.command == "archived-exists":
            storage = DLQStorage()
            target = storage.archive_dir / args.object_id
            if target.exists():
                print("FOUND")
                return
            print("MISSING")
            sys.exit(1)

    finally:
        if redis_client:
            await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
