"""How many bytes an account stores.

One query, deliberately. There is no rollup table, no counter and no triggers: the accounts that
need this number are the ones on a billing plan, and there are a few tens of them, so computing it
outright for each is a few seconds of background work per cycle. A maintained counter would buy
nothing here except a second source of truth to keep in step.

The only caller is the plans-cacher, in the background. Nothing on the request path runs this -- see
the query header for why that matters.
"""

from __future__ import annotations

from typing import Any

from hippius_s3.utils import get_query


async def get_account_storage_bytes(db: Any, main_account_id: str, timeout: float) -> int:  # noqa: ASYNC109
    """Bytes stored by this account, computed now. Expensive; see the query header.

    `timeout` is not optional. api/admin.py bounds the identical aggregate for the same reason: a
    10+ TB account can push it into tens of seconds, and here that would hold one of only a few
    pool connections and pin the xmin horizon for the duration. A timeout fails the cycle, which is
    already the all-or-nothing behaviour the caller wants.

    It is asyncpg's own `timeout=`, not `asyncio.timeout` (hence the ASYNC109 waiver): asyncpg
    cancels the query server-side, whereas an asyncio timeout would abandon the coroutine and leave
    the aggregate running on the backend -- exactly the thing being bounded.
    """
    row = await db.fetchrow(get_query("get_account_storage_bytes"), main_account_id, timeout=timeout)
    return int(row["bytes_used"]) if row else 0
