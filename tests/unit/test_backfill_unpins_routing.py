"""Routing tests for the backfill-unpins --backends override.

The override exists to reach a backend OUTSIDE config.delete_backends (prod: rows live
on ovh while delete_backends is ['arion']). The generic enqueue path intersects the
request with that allowlist, so it can never widen routing — the script must enqueue
directly to each named queue. These tests pin that behavior against the exact prod
config the flag was written for.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import initialize_queue_client
from hippius_s3.scripts.backfill_soft_delete_unpins import _enqueue_unpin


def _payload(backends: list[str] | None) -> UnpinChainRequest:
    return UnpinChainRequest(
        address="user1",
        object_id="obj-1",
        object_version=1,
        delete_backends=backends,
    )


def _config(delete: list[str]) -> AsyncMock:
    cfg = AsyncMock()
    cfg.delete_backends = delete
    return cfg


class TestBackendsOverrideRouting:
    @pytest.mark.asyncio
    async def test_off_allowlist_override_reaches_its_queue(self) -> None:
        # Prod scenario: delete_backends=['arion'], operator passes --backends ovh.
        # The allowlist intersection would be empty; direct routing must still land it.
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(["ovh"]), ["ovh"])
        assert await redis.llen("ovh_unpin_requests") == 1
        assert await redis.llen("arion_unpin_requests") == 0

    @pytest.mark.asyncio
    async def test_multi_backend_override_fans_to_every_named_queue(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(["arion", "ovh"]), ["arion", "ovh"])
        assert await redis.llen("arion_unpin_requests") == 1
        assert await redis.llen("ovh_unpin_requests") == 1

    @pytest.mark.asyncio
    async def test_no_override_falls_back_to_config_fan_out(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(None), [])
        assert await redis.llen("arion_unpin_requests") == 1
        assert await redis.llen("ovh_unpin_requests") == 0
