from __future__ import annotations

import asyncio
import logging
from typing import Any
from typing import Awaitable
from typing import Callable


logger = logging.getLogger(__name__)

# How long a replaced client's close() may take before it is abandoned. A wedged pool may not
# close promptly; nothing waits on the old client once it is swapped out.
OLD_CLIENT_CLOSE_TIMEOUT_SECONDS = 5.0


class ReplaceableHttpClient:
    """Process-wide httpx (or lookalike) client that a PoolTimeout streak can throw away.

    `reset` builds the replacement FIRST and swaps it in synchronously — generation gates in
    the Arion fetcher and the peer fetcher rely on that. Only the close of the old client is
    deferred. Per-request `async with httpx.AsyncClient()` callers do not need this.
    """

    def __init__(
        self,
        make: Callable[[], Any],
        *,
        drain_seconds: float,
        name: str,
    ) -> None:
        self._make = make
        self._client = make()
        self._drain_seconds = float(drain_seconds)
        self._name = name

    @property
    def client(self) -> Any:
        return self._client

    def reset(self) -> Awaitable[None]:
        """Swap in a fresh client now; return the deferred close of the old one.

        A failing `make()` raises here and leaves the previous client installed. The old pool's
        state is logged before the swap: it is the only evidence of what was holding the
        connections, and it goes away with the client.
        """
        old = self._client
        take_snapshot = getattr(old, "pool_snapshot", None)
        snapshot = take_snapshot() if take_snapshot is not None else "unavailable"
        self._client = self._make()
        logger.error("replacing the %s; old pool %s", self._name, snapshot)
        return self._close_old(old)

    async def _close_old(self, old: Any) -> None:
        # The old client still carries healthy in-flight fetches (only the pool is wedged, not
        # every connection). A slow but healthy body can outlive the drain, which is acceptable:
        # that fetch fails as a transient error and retries on the new client.
        await asyncio.sleep(self._drain_seconds)
        close = getattr(old, "aclose", None) or getattr(old, "close", None)
        if close is None:
            logger.warning("old %s has neither aclose nor close; dropped without closing", self._name)
            return
        try:
            await asyncio.wait_for(close(), timeout=OLD_CLIENT_CLOSE_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - the old client is already unreferenced
            logger.warning("old %s did not close cleanly (%s); dropped", self._name, exc)


def live_client(holder: Any) -> Any:
    """The object that actually speaks HTTP.

    `ReplaceableHttpClient` wraps that object as `.client`. Fakes used in tests are the
    object itself. Shutdown and `_http()` must use this, not `getattr(holder, "client")`.
    """
    if isinstance(holder, ReplaceableHttpClient):
        return holder.client
    return holder
