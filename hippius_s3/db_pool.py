from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator


class PoolAcquireTimeout(Exception):
    """Acquiring a pooled DB connection timed out — the pool is saturated.

    Distinct from a query/command timeout: asyncpg raises a bare ``asyncio.TimeoutError``
    for BOTH pool-acquire timeouts and per-statement ``command_timeout``s, so matching on the
    exception type alone would relabel a slow/lock-blocked query as "pool saturated" and tell the
    client to retry into the same contention. This exception is raised ONLY when the acquire itself
    times out, so callers can map exactly that to a 503 SlowDown.
    """


@asynccontextmanager
async def acquire_with_timeout(pool: Any, timeout: float) -> AsyncIterator[Any]:  # noqa: ASYNC109 (forwarded to asyncpg pool.acquire)
    """Acquire a pooled connection with a bounded wait, releasing it on exit.

    Only the acquire is guarded: a timeout while waiting for a free connection raises
    ``PoolAcquireTimeout``. Timeouts raised by work done inside the ``async with`` body
    (e.g. a query hitting ``command_timeout``) propagate unchanged.
    """
    try:
        conn = await pool.acquire(timeout=timeout)
    except asyncio.TimeoutError as e:
        raise PoolAcquireTimeout() from e
    try:
        yield conn
    finally:
        await pool.release(conn)
