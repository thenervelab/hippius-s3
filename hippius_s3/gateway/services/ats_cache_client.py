from __future__ import annotations

import asyncio
import logging

import httpx

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(2.0))
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def _purge(endpoint: str, host: str, key: str) -> None:
    url = f"{endpoint.rstrip('/')}/{key.lstrip('/')}"
    try:
        response = await _get_client().request("PURGE", url, headers={"Host": host})
    except httpx.HTTPError as e:
        logger.warning("ATS PURGE request failed endpoint=%s host=%s key=%s: %s", endpoint, host, key, e)
        return
    if response.status_code >= 400:
        logger.warning("ATS PURGE endpoint=%s host=%s key=%s status=%d", endpoint, host, key, response.status_code)


async def _purge_all(endpoints: list[str], host: str, key: str) -> None:
    await asyncio.gather(*(_purge(ep, host, key) for ep in endpoints), return_exceptions=True)


# The event loop holds tasks weakly; without a strong reference here a scheduled purge can be
# garbage-collected mid-flight and silently never sent (same inflight-set idiom as the unpinner
# and downloader workers).
_inflight: set[asyncio.Task] = set()


def schedule_purge(host: str, key: str) -> None:
    """Fire-and-forget PURGE against every configured ATS endpoint in parallel. No-op when empty."""
    endpoints = get_config().ats_cache_endpoints
    if not endpoints:
        return
    task = asyncio.create_task(_purge_all(endpoints, host, key))
    _inflight.add(task)
    task.add_done_callback(_inflight.discard)
