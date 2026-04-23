"""Tests for ats_cache_client lifecycle — close() and lazy client creation."""

import pytest

from gateway.services import ats_cache_client


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _reset_state() -> None:
    ats_cache_client._client = None
    yield
    ats_cache_client._client = None


def test_get_client_is_lazy() -> None:
    assert ats_cache_client._client is None
    client = ats_cache_client._get_client()
    assert ats_cache_client._client is client


def test_get_client_reuses_instance() -> None:
    a = ats_cache_client._get_client()
    b = ats_cache_client._get_client()
    assert a is b


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    await ats_cache_client.close()  # no client yet — no-op
    await ats_cache_client.close()  # still no-op
    assert ats_cache_client._client is None


@pytest.mark.asyncio
async def test_close_resets_client() -> None:
    ats_cache_client._get_client()
    assert ats_cache_client._client is not None
    await ats_cache_client.close()
    assert ats_cache_client._client is None
