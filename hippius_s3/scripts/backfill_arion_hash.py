#!/usr/bin/env python3
"""
One-shot backfill: fill chunk_backend.arion_hash (and the object_versions.arion_hash rollup) for
chunks uploaded before the uploader started storing it.

The uploader kept only HCFS's file_id in chunk_backend.backend_identifier. HCFS still has the real
Arion hash: file_records.arion_hash, keyed by (user_id, path_hash), where path_hash is exactly the
bytes that file_id hex-encodes. user_id is the account the chunk was uploaded under, which for a
normal upload is the bucket owner; that is the key used here so each lookup is a primary-key probe.

Rows the backfill cannot fill are counted, never guessed at:
  * missing_in_hcfs  — no (bucket owner, path_hash) row. Chunks written into someone else's bucket
                       under an ACL grant were uploaded under the writer's account, not the owner's.
  * empty_in_hcfs    — HCFS has the row but no Arion copy (arion_hash = '').

Reads HCFS read-only. Idempotent: only touches rows whose arion_hash IS NULL, so it can be stopped
and re-run at any point.

Usage:
    DATABASE_URL=... HCFS_DATABASE_URL=... python -m hippius_s3.scripts.backfill_arion_hash --dry-run
    DATABASE_URL=... HCFS_DATABASE_URL=... python -m hippius_s3.scripts.backfill_arion_hash
    # canary first:  --max-batches 1 --batch-size 100
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import asyncpg

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

BACKEND = "arion"

# Keyset scan over chunk_backend's (chunk_id, backend) primary key. Filled rows are filtered, so a
# re-run walks past them without rewriting anything.
SCAN_QUERY = """
SELECT cb.chunk_id, cb.backend_identifier, b.main_account_id, p.object_id, p.object_version
FROM chunk_backend cb
JOIN part_chunks pc ON pc.id = cb.chunk_id
JOIN parts p ON p.part_id = pc.part_id
JOIN objects o ON o.object_id = p.object_id
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE cb.backend = $1
  AND cb.chunk_id > $2
  AND NOT cb.deleted
  AND cb.arion_hash IS NULL
  AND cb.backend_identifier ~ '^[0-9a-f]{64}$'
ORDER BY cb.chunk_id
LIMIT $3
"""

# Runs against the HCFS database. One primary-key probe per (user_id, path_hash) pair.
HCFS_LOOKUP_QUERY = """
SELECT f.user_id, encode(f.path_hash, 'hex') AS file_id, f.arion_hash
FROM unnest($1::text[], $2::bytea[]) AS k(user_id, path_hash)
JOIN file_records f ON f.user_id = k.user_id AND f.path_hash = k.path_hash
"""

UPDATE_QUERY = """
UPDATE chunk_backend cb
SET arion_hash = v.arion_hash
FROM unnest($2::bigint[], $3::text[]) AS v(chunk_id, arion_hash)
WHERE cb.chunk_id = v.chunk_id
  AND cb.backend = $1
  AND cb.arion_hash IS NULL
"""


@dataclass
class BatchResult:
    scanned: int = 0
    matched: int = 0
    updated: int = 0
    missing_in_hcfs: int = 0
    empty_in_hcfs: int = 0
    versions_rolled_up: int = 0
    last_chunk_id: int | None = None
    missing_samples: list[tuple[int, str, str]] = field(default_factory=list)


async def backfill_batch(
    s3: Any,
    hcfs: Any,
    *,
    after_chunk_id: int,
    batch_size: int,
    dry_run: bool,
) -> BatchResult:
    """Fill one keyset page. ``s3`` and ``hcfs`` are asyncpg connections (the same one in tests)."""
    result = BatchResult()
    rows = await s3.fetch(SCAN_QUERY, BACKEND, after_chunk_id, batch_size)
    if not rows:
        return result
    result.scanned = len(rows)
    result.last_chunk_id = int(rows[-1]["chunk_id"])

    users = [r["main_account_id"] for r in rows]
    path_hashes = [bytes.fromhex(r["backend_identifier"]) for r in rows]
    found = {
        (h["user_id"], h["file_id"]): h["arion_hash"] for h in await hcfs.fetch(HCFS_LOOKUP_QUERY, users, path_hashes)
    }

    chunk_ids: list[int] = []
    hashes: list[str] = []
    versions: set[tuple[Any, int]] = set()
    for r in rows:
        arion_hash = found.get((r["main_account_id"], r["backend_identifier"]))
        if arion_hash is None:
            result.missing_in_hcfs += 1
            if len(result.missing_samples) < 5:
                result.missing_samples.append((int(r["chunk_id"]), r["main_account_id"], r["backend_identifier"]))
        elif arion_hash == "":
            result.empty_in_hcfs += 1
        else:
            chunk_ids.append(int(r["chunk_id"]))
            hashes.append(arion_hash)
            versions.add((r["object_id"], int(r["object_version"])))
    result.matched = len(chunk_ids)

    if dry_run or not chunk_ids:
        return result

    status = await s3.execute(UPDATE_QUERY, BACKEND, chunk_ids, hashes)
    result.updated = int(status.split()[-1])
    rollup = get_query("update_object_version_arion_hash")
    await s3.executemany(rollup, [(object_id, version, BACKEND) for object_id, version in versions])
    result.versions_rolled_up = len(versions)
    return result


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill chunk_backend.arion_hash from HCFS file_records")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), help="S3 DB (default: $DATABASE_URL)")
    parser.add_argument(
        "--hcfs-database-url",
        default=os.environ.get("HCFS_DATABASE_URL"),
        help="HCFS DB, read-only is enough (default: $HCFS_DATABASE_URL)",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-batches", type=int, default=0, help="Stop after N batches (0 = run to completion)")
    parser.add_argument("--start-after", type=int, default=0, help="Resume from this chunk_id")
    parser.add_argument("--sleep", type=float, default=0.1, help="Seconds to pause between batches")
    parser.add_argument("--dry-run", action="store_true", help="Look everything up, write nothing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not args.database_url or not args.hcfs_database_url:
        parser.error("both DATABASE_URL and HCFS_DATABASE_URL are required")

    s3 = await asyncpg.connect(args.database_url)
    hcfs = await asyncpg.connect(args.hcfs_database_url)
    totals = BatchResult()
    cursor = args.start_after
    batches = 0
    started = time.monotonic()
    try:
        while True:
            batch = await backfill_batch(
                s3, hcfs, after_chunk_id=cursor, batch_size=args.batch_size, dry_run=args.dry_run
            )
            if batch.last_chunk_id is None:
                break
            batches += 1
            cursor = batch.last_chunk_id
            for name in ("scanned", "matched", "updated", "missing_in_hcfs", "empty_in_hcfs", "versions_rolled_up"):
                setattr(totals, name, getattr(totals, name) + getattr(batch, name))
            if len(totals.missing_samples) < 5:
                totals.missing_samples.extend(batch.missing_samples[: 5 - len(totals.missing_samples)])
            logger.info(
                f"batch={batches} cursor={cursor} scanned={batch.scanned} matched={batch.matched} "
                f"updated={batch.updated} missing={batch.missing_in_hcfs} empty={batch.empty_in_hcfs} "
                f"versions={batch.versions_rolled_up}"
            )
            if args.max_batches and batches >= args.max_batches:
                logger.info(f"stopping after --max-batches={args.max_batches}; resume with --start-after {cursor}")
                break
            if args.sleep:
                await asyncio.sleep(args.sleep)
    finally:
        await s3.close()
        await hcfs.close()

    logger.info(
        f"DONE dry_run={args.dry_run} batches={batches} elapsed={time.monotonic() - started:.1f}s "
        f"scanned={totals.scanned} matched={totals.matched} updated={totals.updated} "
        f"missing_in_hcfs={totals.missing_in_hcfs} empty_in_hcfs={totals.empty_in_hcfs} "
        f"versions_rolled_up={totals.versions_rolled_up} last_chunk_id={cursor}"
    )
    for chunk_id, user, file_id in totals.missing_samples:
        logger.info(f"missing sample chunk_id={chunk_id} bucket_owner={user} file_id={file_id}")


if __name__ == "__main__":
    asyncio.run(main())
