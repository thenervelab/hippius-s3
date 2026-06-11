#!/usr/bin/env python3
"""Reconcile uploads lost with the NVMe cache after an evacuation.

When the cache node dies, chunks staged on its node-local NVMe that were never uploaded to any
backend die with it. The affected object_versions sit in 'publishing'/'pinning' forever and their
GETs hang waiting on chunks that will never arrive. This script finds versions whose part_chunks
have ZERO live chunk_backend rows (nothing ever reached a backend) and marks them 'failed' — the
same transition the uploader applies on permanent failure — so clients get a clean 404/error
instead of a hang.

Notes:
- Queued upload requests for these versions also self-resolve via the uploader's meta-wait
  deadline -> permanent -> DLQ path; this script is the accelerator + covers versions no longer
  in any queue.
- Versions with SOME chunk_backend rows (partially uploaded) are intentionally skipped: the
  uploader/backup retry machinery may still complete them — triage those via the DLQ.

Usage:
    python -m hippius_s3.scripts.reconcile_lost_uploads                # dry-run (default)
    python -m hippius_s3.scripts.reconcile_lost_uploads --apply
    python -m hippius_s3.scripts.reconcile_lost_uploads --min-age-minutes 30 --apply
"""

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import asyncpg

from hippius_s3.config import get_config


CANDIDATES_SQL = """
SELECT
    ov.object_id,
    ov.object_version,
    ov.status,
    ov.created_at,
    COUNT(pc.id) AS chunk_count
FROM object_versions ov
JOIN parts p
  ON p.object_id = ov.object_id AND p.object_version = ov.object_version
JOIN part_chunks pc
  ON pc.part_id = p.part_id
WHERE ov.status IN ('publishing', 'pinning')
  AND ov.created_at < now() - make_interval(mins => $1)
  AND NOT EXISTS (
      SELECT 1
      FROM parts p2
      JOIN part_chunks pc2 ON pc2.part_id = p2.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc2.id AND NOT cb.deleted
      WHERE p2.object_id = ov.object_id AND p2.object_version = ov.object_version
  )
GROUP BY ov.object_id, ov.object_version, ov.status, ov.created_at
ORDER BY ov.created_at
"""

MARK_FAILED_SQL = """
UPDATE object_versions SET status = 'failed'
WHERE object_id = $1 AND object_version = $2 AND status IN ('publishing', 'pinning')
"""


async def find_candidates(conn: Any, min_age_minutes: int) -> list[Any]:
    return await conn.fetch(CANDIDATES_SQL, min_age_minutes)


async def mark_failed(conn: Any, candidates: list[Any]) -> int:
    marked = 0
    for row in candidates:
        result = await conn.execute(MARK_FAILED_SQL, row["object_id"], row["object_version"])
        # asyncpg returns e.g. "UPDATE 1"; the status re-check guards against races with a
        # concurrently-recovering uploader.
        if result.endswith("1"):
            marked += 1
    return marked


async def main() -> int:
    parser = argparse.ArgumentParser(description="Mark never-uploaded object_versions failed after NVMe cache loss")
    parser.add_argument("--apply", action="store_true", help="actually mark candidates failed (default: dry-run)")
    parser.add_argument(
        "--min-age-minutes",
        type=int,
        default=60,
        help="only consider versions older than this (avoid racing in-flight PUTs; default 60)",
    )
    args = parser.parse_args()

    config = get_config()
    conn = await asyncpg.connect(dsn=config.database_url)
    try:
        candidates = await find_candidates(conn, args.min_age_minutes)
        if not candidates:
            print(
                f"No lost uploads found (status publishing/pinning, older than {args.min_age_minutes}m, "
                f"zero live chunk_backend rows)."
            )
            return 0

        print(f"{len(candidates)} candidate object_versions (never reached any backend):")
        for row in candidates[:50]:
            print(
                f"  object_id={row['object_id']} v={row['object_version']} status={row['status']} "
                f"created={row['created_at']} chunks={row['chunk_count']}"
            )
        if len(candidates) > 50:
            print(f"  ... and {len(candidates) - 50} more")

        if not args.apply:
            print("\nDRY RUN — nothing changed. Re-run with --apply to mark these failed.")
            return 0

        marked = await mark_failed(conn, candidates)
        print(f"\nMarked {marked}/{len(candidates)} object_versions failed.")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
