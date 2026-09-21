from __future__ import annotations

import pytest

from hippius_s3.cache.peers import ReplaceablePeerClient
from hippius_s3.http_client import ReplaceableHttpClient
from hippius_s3.http_client import live_client
from hippius_s3.reader.backend_fetch import _ReplaceableArionClient


def test_the_peer_and_arion_aliases_are_the_shared_holder() -> None:
    assert ReplaceablePeerClient is ReplaceableHttpClient
    assert _ReplaceableArionClient is ReplaceableHttpClient


def test_live_client_unwraps_the_holder_and_passes_fakes_through() -> None:
    inner = object()
    holder = ReplaceableHttpClient(lambda: inner, drain_seconds=0.0, name="t")
    assert live_client(holder) is inner
    assert live_client(inner) is inner
    assert live_client(None) is None


@pytest.mark.asyncio
async def test_a_client_with_neither_close_logs_instead_of_leaking_silently(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    class _Bare:
        pass

    n = {"i": 0}

    def make() -> _Bare:
        n["i"] += 1
        return _Bare()

    holder = ReplaceableHttpClient(make, drain_seconds=0.0, name="t")
    with caplog.at_level(logging.WARNING, logger="hippius_s3.http_client"):
        await holder.reset()
    assert any("neither aclose nor close" in r.getMessage() for r in caplog.records)
