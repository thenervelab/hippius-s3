"""The api's half of the promote-floor wire contract.

The drain agent resolves the free-space floor promotion must stay above — it is the only side
that knows the reserve the allocator handed this node — and publishes it as an integer under
`cephor:promote_floor:{node}`. These tests pin what this side does with that key, including
every way it can be unusable, because each of those has to degrade to the static fallback
rather than to an exception on the read path or to a disabled read tier.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from hippius_s3.promote_floor import PROMOTE_FLOOR_TTL_SECONDS
from hippius_s3.promote_floor import PublishedPromoteFloor
from hippius_s3.promote_floor import create_published_floor_source
from hippius_s3.promote_floor import promote_floor_key


class FakeRedis:
    """A GET-only Redis: scripted value, optional failure, and a call log."""

    def __init__(self, value: object = None, raises: BaseException | None = None) -> None:
        self.value = value
        self.raises = raises
        self.keys: list[str] = []

    async def get(self, key: str) -> object:
        self.keys.append(key)
        if self.raises is not None:
            raise self.raises
        return self.value


def published(floor_permille: object) -> str:
    return json.dumps({"floor_permille": floor_permille, "source": "drain-agent", "ts": 1_800_000_000})


@pytest.mark.asyncio
async def test_a_published_floor_reads_back_as_a_free_space_ratio() -> None:
    """Permille on the wire (it shares units with the allocator's reserve), ratio in here (it
    is compared against `shutil.disk_usage`)."""
    source = PublishedPromoteFloor(FakeRedis(published(425)), "node-7")
    assert await source.ratio() == pytest.approx(0.425)


@pytest.mark.asyncio
async def test_the_key_is_scoped_to_this_node() -> None:
    """Each node's evictor holds its own reserve, so reading a peer's floor would gate this
    node's promotion on a disk it does not write to."""
    redis = FakeRedis(published(175))
    await PublishedPromoteFloor(redis, "node-7").ratio()
    assert redis.keys == ["cephor:promote_floor:node-7"]
    assert promote_floor_key("node-7") == "cephor:promote_floor:node-7"


@pytest.mark.asyncio
async def test_an_absent_key_reads_as_signal_unavailable() -> None:
    """The very first read, before any agent has ever published — and the steady state on a
    node whose evictor forms no band. Both must hand the caller back to its static floor."""
    assert await PublishedPromoteFloor(FakeRedis(None), "node-7").ratio() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [
        "not json at all",
        json.dumps({"source": "drain-agent"}),
        published("four hundred"),
        published(0),
        published(1001),
        published(-5),
    ],
    ids=["garbage", "missing-field", "not-a-number", "zero", "over-full-disk", "negative"],
)
async def test_a_malformed_or_out_of_range_value_reads_as_unavailable_without_raising(raw: str) -> None:
    """A corrupt value must not be trusted AND must not propagate. `0` and `1001` are in here
    because both parse fine as integers and neither is a floor: zero would disable the gate
    outright, and a floor above the whole disk would disable promotion forever."""
    assert await PublishedPromoteFloor(FakeRedis(raw), "node-7").ratio() is None


@pytest.mark.asyncio
async def test_a_redis_error_keeps_the_last_good_floor_inside_the_publish_ttl() -> None:
    """A blip must not re-open promotion on a node the agent just told us is stressed.

    Absence is the publisher's honest "no signal"; a failed READ is not, so the last good value
    stands — but only for as long as the publisher's own TTL, so a dead Redis cannot pin a
    stale floor indefinitely.
    """
    redis = FakeRedis(published(425))
    source = PublishedPromoteFloor(redis, "node-7")
    assert await source.ratio(now=1_000.0) == pytest.approx(0.425)

    redis.raises = RuntimeError("connection reset")
    assert await source.ratio(now=1_010.0) == pytest.approx(0.425), "held the last good floor through the blip"
    assert await source.ratio(now=1_000.0 + PROMOTE_FLOOR_TTL_SECONDS + 1) is None, (
        "past the publish TTL the signal is unavailable, not stale"
    )


@pytest.mark.asyncio
async def test_a_hung_redis_is_bounded_rather_than_stalling_the_read_path() -> None:
    """The api's queues client is built with no socket timeout, so a reachable-but-blocked
    Redis would hang the GET — and this GET is awaited inline before a chunk is returned."""

    class HangingRedis:
        async def get(self, _key: str) -> object:
            await asyncio.sleep(3600)

    source = PublishedPromoteFloor(HangingRedis(), "node-7", timeout_seconds=0.01)
    assert await source.ratio() is None


def test_no_node_identity_means_no_source_at_all() -> None:
    """Without NODE_NAME there is no way to know WHICH node's floor to read, and reading the
    wrong one is worse than reading none. Same rule the residency recorder already follows."""
    assert create_published_floor_source(FakeRedis(), "") is None
    assert create_published_floor_source(None, "node-7") is None
    assert create_published_floor_source(FakeRedis(), "node-7") is not None
