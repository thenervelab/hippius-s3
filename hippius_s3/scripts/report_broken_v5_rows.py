from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any
from typing import Protocol

import asyncpg

from hippius_s3.config import get_config


# A row claims envelope encryption (storage_version >= 5) but has no usable envelope. Both halves
# matter: object_reader.py fails on a missing wrapped_dek even when kek_id is present, which the
# delete_v4.sql runbook query misses because it keys on kek_id alone.
_BROKEN_V5 = """ov.storage_version >= 5
    AND (ov.kek_id IS NULL OR ov.wrapped_dek IS NULL)"""

_SINCE_CLAUSE = "\n    AND ov.created_at >= NOW() - ($1::int * INTERVAL '1 day')"


class ReadOnlyConnection(Protocol):
    async def execute(self, sql: str, *params: Any) -> Any: ...

    async def fetch(self, sql: str, *params: Any) -> Any: ...


def _where(since_days: int | None) -> str:
    return _BROKEN_V5 + (_SINCE_CLAUSE if since_days is not None else "")


def build_cohort_query(*, since_days: int | None) -> str:
    return f"""SELECT
    ov.multipart AS multipart,
    ov.status AS status,
    (ov.object_version = o.current_object_version) AS live_current,
    (ov.size_bytes = 0) AS empty_placeholder,
    COUNT(*)::bigint AS row_count,
    MIN(ov.created_at) AS oldest,
    MAX(ov.created_at) AS newest
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
WHERE {_where(since_days)}
GROUP BY 1, 2, 3, 4
ORDER BY row_count DESC"""


def build_sample_query(*, since_days: int | None) -> str:
    limit_param = "$2" if since_days is not None else "$1"
    return f"""SELECT
    b.bucket_name AS bucket_name,
    o.object_key AS object_key,
    ov.object_id::text AS object_id,
    ov.object_version AS object_version,
    ov.storage_version AS storage_version,
    ov.multipart AS multipart,
    ov.status AS status,
    ov.size_bytes AS size_bytes,
    ov.created_at AS created_at,
    (ov.object_version = o.current_object_version) AS live_current,
    (ov.kek_id IS NULL) AS kek_id_missing,
    (ov.wrapped_dek IS NULL) AS wrapped_dek_missing
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE {_where(since_days)}
ORDER BY (ov.object_version = o.current_object_version) DESC, ov.created_at DESC
LIMIT {limit_param}"""


def cohort_params(*, since_days: int | None) -> tuple[Any, ...]:
    return () if since_days is None else (since_days,)


def sample_params(*, since_days: int | None, limit: int) -> tuple[Any, ...]:
    return cohort_params(since_days=since_days) + (limit,)


def _scope(since_days: int | None) -> str:
    # --since-days bounds the RESULT, never the WORK. Migrations 20260528120100/120200 dropped
    # idx_object_versions_kek_id and idx_object_versions_status, so the broken-v5 predicate has no
    # usable access path: every invocation seq-scans object_versions (~130M rows in prod) and hash
    # joins to `objects`. Say so, or an operator reads "90 days" as "cheap" and runs it on the primary.
    cost = "full sequential scan of object_versions regardless of window — run against a replica"
    if since_days is None:
        return f"all time — complete total; {cost}"
    return f"created_at within the last {since_days} days — PARTIAL count, not a total; {cost}"


def build_report(
    cohort_rows: list[Any],
    sample_rows: list[Any],
    *,
    since_days: int | None,
    limit: int,
) -> dict[str, Any]:
    cohorts = [
        {
            "multipart": r["multipart"],
            "status": r["status"],
            "live_current": bool(r["live_current"]),
            "empty_placeholder": bool(r["empty_placeholder"]),
            "row_count": int(r["row_count"]),
            "oldest": r["oldest"],
            "newest": r["newest"],
        }
        for r in cohort_rows
    ]
    return {
        "mode": "report-only (read-only transaction; this script has no write path)",
        "bound": {"since_days": since_days, "scope": _scope(since_days)},
        "total_broken_rows": sum(c["row_count"] for c in cohorts),
        "live_current_rows": sum(c["row_count"] for c in cohorts if c["live_current"]),
        "empty_placeholder_rows": sum(c["row_count"] for c in cohorts if c["empty_placeholder"]),
        "cohorts": cohorts,
        "sample_limit": limit,
        "samples": sample_rows,
    }


EXIT_OK = 0
EXIT_BROKEN_ROWS_FOUND = 1
EXIT_OPERATIONAL_FAILURE = 2


def exit_code(report: dict[str, Any]) -> int:
    # Live current versions are the subset a GET actually 500s on, so a monitoring probe alerts on it.
    return EXIT_BROKEN_ROWS_FOUND if int(report["live_current_rows"]) > 0 else EXIT_OK


async def run_report(
    conn: ReadOnlyConnection,
    *,
    since_days: int | None,
    limit: int,
    statement_timeout_ms: int,
) -> dict[str, Any]:
    await conn.execute("SET default_transaction_read_only = on")
    await conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")

    cohort_rows = await conn.fetch(build_cohort_query(since_days=since_days), *cohort_params(since_days=since_days))
    sample_rows = await conn.fetch(
        build_sample_query(since_days=since_days),
        *sample_params(since_days=since_days, limit=limit),
    )
    return build_report(list(cohort_rows), [dict(r) for r in sample_rows], since_days=since_days, limit=limit)


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("report-broken-v5-rows")
    since_days = resolve_since_days(args)
    conn = await asyncpg.connect(get_config().database_url)
    try:
        report = await run_report(
            conn,
            since_days=since_days,
            limit=int(args.limit),
            statement_timeout_ms=int(args.statement_timeout_ms),
        )
    finally:
        await conn.close()

    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    rc = exit_code(report)
    if rc:
        log.warning("%d broken-v5 rows are the live current version and will 500 on GET", report["live_current_rows"])
    return rc


def resolve_since_days(args: argparse.Namespace) -> int | None:
    return None if args.all_time else int(args.since_days)


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description=(
            "Report object_versions rows with storage_version >= 5 but a missing encryption envelope "
            "(kek_id or wrapped_dek NULL). READ-ONLY: it opens a read-only transaction and runs SELECTs "
            "only. There is deliberately no delete/repair mode — a human decides what to do with the "
            "backlog, and any mutation is a separate, reviewed change."
        )
    )
    ap.add_argument(
        "--since-days",
        type=_positive_int,
        default=90,
        help="Only count rows created within this many days (default: 90). Yields a PARTIAL count. "
        "Note this bounds the RESULT, not the cost — see --all-time.",
    )
    ap.add_argument(
        "--all-time",
        action="store_true",
        help="Drop the time bound for a complete total. The broken-v5 predicate has no usable index "
        "(idx_object_versions_kek_id and _status were dropped by migration), so EVERY invocation — with "
        "or without this flag — seq-scans object_versions. Point this at a replica, never the primary.",
    )
    ap.add_argument("--limit", type=_positive_int, default=20, help="Max sample rows to list (default: 20)")
    ap.add_argument(
        "--statement-timeout-ms",
        type=_positive_int,
        default=60000,
        help="Server-side statement timeout so a runaway scan cannot pin a backend (default: 60000)",
    )
    return ap


def main() -> None:
    # A statement timeout is the LIKELY outcome against prod (the predicate seq-scans ~130M rows), and
    # it must not be reported as "backlog found" — a probe that cannot tell a timeout from a real
    # finding flaps and gets muted. Operational failures get their own code.
    try:
        raise SystemExit(asyncio.run(main_async(build_parser().parse_args())))
    except (asyncpg.PostgresError, OSError) as exc:
        logging.getLogger("report-broken-v5-rows").error("report did not complete: %s", exc)
        raise SystemExit(EXIT_OPERATIONAL_FAILURE) from exc


if __name__ == "__main__":
    main()
