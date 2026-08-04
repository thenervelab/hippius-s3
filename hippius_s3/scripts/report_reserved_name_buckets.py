from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any

import asyncpg

from hippius_s3.config import get_config


logger = logging.getLogger("report-reserved-name-buckets")

EXIT_CLEAN = 0
EXIT_FINDINGS = 1
EXIT_OPERATIONAL_FAILURE = 2

# First path segments the gateway never forwards as a bucket: either a real route swallows the
# request, or auth_router exempts it from SigV4 so no identity is stamped. A bucket sitting on one
# of these is unreachable by its owner yet permanently holds the globally-unique name (prod
# incident 2026-08-03, buckets "docs" and "docs2").
#
# TODO: import gateway.middlewares.input_validation.RESERVED_BUCKET_SEGMENTS once PR #388 lands —
# it defines the same set for the CreateBucket rejection. Duplicated here so this script works
# against staging today; the sync obligation is the whole reason it exists.
RESERVED_SEGMENTS = (
    "docs",
    "health",
    "metrics",
    "openapi.json",
    "redoc",
    "robots.txt",
    "user",
)

# acl_middleware returns before any permission check for these, so objects under a bucket of this
# name are readable and writable with no auth AND no ACL — strictly worse than merely stranded.
# See gateway/middlewares/acl.py: `path == "/health" or path.startswith("/user/")`.
ACL_BYPASSED_SEGMENTS = frozenset({"user", "health"})

_QUERY = """
SELECT b.bucket_name,
       b.bucket_id::text AS bucket_id,
       b.main_account_id,
       b.created_at,
       b.deleted_at,
       (SELECT count(*) FROM objects o WHERE o.bucket_id = b.bucket_id AND o.deleted_at IS NULL)
           AS live_objects
FROM buckets b
WHERE b.bucket_name = ANY($1::text[])
ORDER BY b.deleted_at NULLS FIRST, b.bucket_name
"""

# The name collision is one way to end up ownerless; a gateway bug is another. Catching the
# symptom directly means the report stays useful even if a new exempt path is added and this
# script's segment list goes stale.
_OWNERLESS_QUERY = """
SELECT b.bucket_name,
       b.bucket_id::text AS bucket_id,
       b.main_account_id,
       b.created_at,
       b.deleted_at
FROM buckets b
WHERE b.deleted_at IS NULL
  AND (b.main_account_id IS NULL OR b.main_account_id = '' OR b.main_account_id = 'anonymous')
ORDER BY b.created_at
"""


def _severity(row: dict[str, Any]) -> str:
    if row["deleted_at"] is not None:
        # Soft-deleted rows don't hold the name (the unique index is partial over live rows), so
        # they're history, not a problem to fix.
        return "resolved"
    if row["bucket_name"] in ACL_BYPASSED_SEGMENTS:
        return "acl-bypass"
    return "stranded"


async def _collect(conn: asyncpg.Connection, statement_timeout_ms: int) -> dict[str, Any]:
    await conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")

    reserved = [dict(r) for r in await conn.fetch(_QUERY, list(RESERVED_SEGMENTS))]
    for row in reserved:
        row["severity"] = _severity(row)

    ownerless = [dict(r) for r in await conn.fetch(_OWNERLESS_QUERY)]

    return {"reserved_name_buckets": reserved, "ownerless_buckets": ownerless}


def _render(report: dict[str, Any]) -> None:
    reserved = report["reserved_name_buckets"]
    ownerless = report["ownerless_buckets"]

    live = [r for r in reserved if r["severity"] != "resolved"]
    if not live:
        logger.info("No live bucket holds a reserved gateway name.")
    for row in live:
        logger.warning(
            "%s: bucket=%r owner=%r id=%s created=%s live_objects=%d",
            row["severity"].upper(),
            row["bucket_name"],
            row["main_account_id"],
            row["bucket_id"],
            row["created_at"],
            row["live_objects"],
        )
        if row["severity"] == "acl-bypass":
            logger.warning(
                "  ^ acl_middleware skips the permission check for /%s/* entirely, so every object "
                "under this bucket is anonymously readable AND writable. Remediate before anything else.",
                row["bucket_name"],
            )

    if ownerless:
        logger.warning("%d live bucket(s) with an empty/anonymous owner:", len(ownerless))
        for row in ownerless:
            logger.warning(
                "  ORPHAN: bucket=%r owner=%r id=%s created=%s",
                row["bucket_name"],
                row["main_account_id"],
                row["bucket_id"],
                row["created_at"],
            )
    else:
        logger.info("No live bucket has an empty/anonymous owner.")


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    conn = await asyncpg.connect(args.database_url or config.database_url)
    try:
        report = await _collect(conn, args.statement_timeout_ms)
    finally:
        await conn.close()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _render(report)

    findings = [r for r in report["reserved_name_buckets"] if r["severity"] != "resolved"]
    return EXIT_FINDINGS if findings or report["ownerless_buckets"] else EXIT_CLEAN


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Report buckets whose name collides with a reserved gateway route segment, "
        "or that have no owner. Read-only.",
    )
    ap.add_argument("--database-url", default=None, help="Override DATABASE_URL from config")
    ap.add_argument("--json", action="store_true", help="Emit the raw report as JSON")
    ap.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=30000,
        help="Server-side statement timeout (default: 30000)",
    )
    return ap


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # An operational failure must not read as "clean" — a probe that can't tell a connection error
    # from a genuine all-clear gets trusted once and then silently stops meaning anything.
    try:
        raise SystemExit(asyncio.run(main_async(build_parser().parse_args())))
    except (asyncpg.PostgresError, OSError) as exc:
        logger.error("report did not complete: %s", exc)
        raise SystemExit(EXIT_OPERATIONAL_FAILURE) from exc


if __name__ == "__main__":
    main()
