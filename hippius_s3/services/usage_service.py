"""How many bytes an account stores.

Computed outright, per bucket, in keyset pages. There is no rollup table, no counter and no
triggers: the accounts that need this number are the ones on a billing plan, and there are a few
tens of them.

That "few tens of ACCOUNTS" is not the same claim as "cheap per account", and the difference is what
this module is shaped around. Account cardinality was never the problem -- objects-per-account is.
One prod account owns a JuiceFS bucket holding 7.83M live objects, where the single-statement
aggregate takes ~64s cold. See get_bucket_storage_bytes_page.sql.

A maintained counter (a delta ledger folded into a per-bucket rollup) is the real answer and is
written up in todo.md; it is a schema change and a backfill, and this module is what makes the
current design work until then.

The only caller is the plans-cacher, in the background. Nothing on the request path runs this.
"""

from __future__ import annotations

from typing import Any

from hippius_s3.utils import get_query


# Safety valve on the keyset walk. A cursor that fails to advance would otherwise spin forever
# holding a pool connection; at the default page size this bounds one bucket at 5 billion objects,
# which is far past anything real and far short of infinite.
MAX_PAGES_PER_BUCKET = 10_000


async def get_account_storage_bytes(
    db: Any,
    main_account_id: str,
    timeout: float,  # noqa: ASYNC109
    page_size: int,
) -> int:
    """Bytes stored by this account: current versions of live objects in live buckets.

    THE definition of "storage used" in this codebase. Must stay in step with
    get_admin_account_stats.sql and console_list_buckets.sql -- those are the numbers an operator and
    a customer respectively see, and enforcing a third one is how you get a support ticket nobody can
    resolve.

    `timeout` bounds each STATEMENT, not the whole account: chunking exists precisely so no single
    statement approaches it. It is asyncpg's own `timeout=` (hence the ASYNC109 waiver), which
    cancels server-side; an asyncio timeout would abandon the coroutine and leave the aggregate
    running on the backend -- exactly the thing being bounded.
    """
    buckets = await db.fetch(get_query("list_account_bucket_ids"), main_account_id, timeout=timeout)

    total = 0
    for row in buckets:
        total += await _bucket_storage_bytes(db, row["bucket_id"], timeout, page_size)
    return total


async def _bucket_storage_bytes(db: Any, bucket_id: Any, timeout: float, page_size: int) -> int:  # noqa: ASYNC109
    """Walk one bucket in keyset pages, summing as we go."""
    total = 0
    cursor = ""

    for _ in range(MAX_PAGES_PER_BUCKET):
        row = await db.fetchrow(
            get_query("get_bucket_storage_bytes_page"),
            bucket_id,
            cursor,
            page_size,
            timeout=timeout,
        )
        if row is None:
            return total

        total += int(row["bytes_used"])
        # Short page means we reached the end. Compared against PAGE rows, not summed rows -- see
        # the query header for why those differ.
        if int(row["rows_seen"]) < page_size:
            return total

        cursor = row["last_key"]

    raise RuntimeError(
        f"storage count for bucket {bucket_id} exceeded {MAX_PAGES_PER_BUCKET} pages; "
        f"refusing to keep walking a cursor that may not be advancing"
    )
