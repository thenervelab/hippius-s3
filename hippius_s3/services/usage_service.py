"""How many bytes an account stores.

One query, deliberately. There is no rollup table, no counter and no triggers: the accounts that
need this number are the ones on a billing plan, and there are a few tens of them, so computing it
outright for each is a few seconds of background work per cycle. A maintained counter would buy
nothing here except a second source of truth to keep in step.

The only caller is the plans-cacher, in the background. Nothing on the request path runs this -- see
the query header for why that matters.
"""

from __future__ import annotations

import logging
from typing import Any

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def get_account_storage_bytes(db: Any, main_account_id: str) -> int:
    """Bytes stored by this account, computed now. Expensive; see the query header."""
    row = await db.fetchrow(get_query("get_account_storage_bytes"), main_account_id)
    return int(row["bytes_used"]) if row else 0
