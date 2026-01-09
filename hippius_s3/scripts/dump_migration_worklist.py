from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from hippius_s3.config import get_config
from hippius_s3.utils import get_query


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.write("\n")
        f.flush()
        try:
            import os

            os.fsync(f.fileno())
        except Exception:
            pass
    tmp.replace(path)


async def dump_worklist_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("dump-migration-worklist")
    config = get_config()
    db = await asyncpg.connect(config.database_url)  # type: ignore[arg-type]
    try:
        target = args.target_storage_version or config.target_storage_version
        rows = await db.fetch(get_query("list_objects_to_migrate"), target, args.bucket or None, args.key or None)
        items: list[dict[str, Any]] = [
            {
                "bucket": str(r["bucket_name"]),
                "key": str(r["object_key"]),
                "status": "pending",
                "attempts": 0,
                # helpful context for humans/debugging (migrator will ignore extra fields)
                "object_id": str(r["object_id"]),
                "object_version": int(r["object_version"]),
                "storage_version": int(r["storage_version"]),
            }
            for r in rows
        ]

        out = json.dumps(items, indent=2, sort_keys=True)
        if args.output == "-":
            print(out)
        else:
            _atomic_write_text(Path(args.output), out)
            log.info("Wrote %d work items to %s", len(items), args.output)
        return 0
    finally:
        await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Dump a JSON migration worklist for migrate_objects.py")
    ap.add_argument("--output", default="-", help="Output path (default: '-', stdout)")
    ap.add_argument("--bucket", default="", help="Optional bucket filter")
    ap.add_argument("--key", default="", help="Optional key filter (requires --bucket)")
    ap.add_argument(
        "--target-storage-version",
        type=int,
        default=0,
        help="Override target storage version (default: HIPPIUS_TARGET_STORAGE_VERSION from config)",
    )
    args = ap.parse_args()
    if args.key and not args.bucket:
        raise SystemExit("--key requires --bucket")

    raise SystemExit(asyncio.run(dump_worklist_async(args)))


if __name__ == "__main__":
    main()
