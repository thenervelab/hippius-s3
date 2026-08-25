"""RQ-4: on a first-chunk timeout the streamer compare-and-deletes its own coalesce locks so the
next GET re-enqueues immediately, instead of the retry cadence being bounded by the 600s lock TTL.

The CAD (delete only when the lock still holds our ray token) must match the key format and token
semantics of _enqueue_missing_downloads (the SET NX) and the downloader's release exactly.
"""

from __future__ import annotations

from typing import Any

import pytest

from hippius_s3.services.object_reader import _release_coalesce_locks


class _RecordingRedis:
    def __init__(self) -> None:
        self.evals: list[tuple[str, int, str, str]] = []

    async def eval(self, script: str, numkeys: int, key: str, token: str) -> int:
        self.evals.append((script, numkeys, key, token))
        return 1


@pytest.mark.asyncio
async def test_release_cads_each_part_on_the_ray_token() -> None:
    redis = _RecordingRedis()
    await _release_coalesce_locks(redis, object_id="obj-1", object_version=3, part_numbers={1, 2}, ray_token="ray-9")

    assert len(redis.evals) == 2
    keys = {e[2] for e in redis.evals}
    assert keys == {
        "download_in_progress:obj-1:v:3:part:1",
        "download_in_progress:obj-1:v:3:part:2",
    }
    assert all(e[3] == "ray-9" for e in redis.evals), "must compare-and-delete on our ray token"
    # Compare-and-delete Lua: only DEL when GET still equals our token.
    assert all("GET" in e[0] and "DEL" in e[0] for e in redis.evals)
    assert all(e[1] == 1 for e in redis.evals), "one key per eval"


@pytest.mark.asyncio
async def test_release_is_best_effort_on_redis_error() -> None:
    class _ErrRedis:
        async def eval(self, *_a: Any, **_k: Any) -> int:
            raise RuntimeError("redis down")

    # Must not raise — a failed release just lets the TTL expire.
    await _release_coalesce_locks(_ErrRedis(), object_id="obj", object_version=1, part_numbers={1}, ray_token="t")
