"""The api tells its node's drain agent about each part it writes.

Discovery used to mean the agent walking the whole SSD cache every 15s and asking the database
which parts it had never seen — fine when the disk held only the undrained backlog, ruinous once
retention made it hold the node's entire replicated shard (2.28M parts on prod).

Two properties carry the whole design and are pinned here: the announcement lands strictly after
`meta.json` (the readiness gate), and it can never fail a PUT.
"""

from __future__ import annotations

import json

import pytest

from hippius_s3.cache import read_recency
from hippius_s3.writer import landed
from hippius_s3.writer.landed import LandedPartPublisher
from hippius_s3.writer.landed import get_landed_publisher
from hippius_s3.writer.landed import initialize_landed_publisher
from hippius_s3.writer.landed import landed_queue_key
from hippius_s3.writer.write_through_writer import WriteThroughPartsWriter


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


class FakePipeline:
    def __init__(self, sink: list[tuple], fail: bool) -> None:
        self._sink = sink
        self._fail = fail
        self.ops: list[tuple] = []

    def lpush(self, key: str, value: str) -> None:
        self.ops.append(("lpush", key, value))

    def ltrim(self, key: str, start: int, stop: int) -> None:
        self.ops.append(("ltrim", key, start, stop))

    async def execute(self) -> None:
        if self._fail:
            raise ConnectionError("redis-queues down")
        self._sink.extend(self.ops)


class FakeQueues:
    def __init__(self, fail: bool = False) -> None:
        self.executed: list[tuple] = []
        self.fail = fail

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self.executed, self.fail)


@pytest.fixture(autouse=True)
def _reset_singleton():
    """The publisher and the recency recorder are process-wide singletons, so leaking one across
    tests would let an earlier test's queue/pool receive a later test's writes."""
    yield
    landed._publisher = None
    read_recency._recorder = None


def test_the_queue_key_matches_the_agents(monkeypatch) -> None:
    """Cross-language contract. The agent's `landed_queue_key` builds the same string; if either
    side drifts the api publishes into a key nobody drains and discovery silently reverts to the
    disk walk, with no error anywhere."""
    assert landed_queue_key("k8s-v3-node1") == "cephor:landed:k8s-v3-node1"


@pytest.mark.asyncio
async def test_the_published_message_matches_the_agents_wire_contract() -> None:
    """These three field names ARE the protocol.

    The agent parses into a struct with exactly `object_id`, `version`, `part_number` and
    discards anything it cannot read — so renaming one side alone disables the fast path
    silently rather than erroring. Asserted on the exact key set, not just the values.
    """
    queues = FakeQueues()
    await LandedPartPublisher(queues, "node-a").publish(OBJ, 7, 3)

    op, key, raw = queues.executed[0]
    assert (op, key) == ("lpush", "cephor:landed:node-a")
    assert json.loads(raw) == {"object_id": OBJ, "version": 7, "part_number": 3}


@pytest.mark.asyncio
async def test_the_queue_is_bounded_in_the_same_round_trip() -> None:
    """An agent that is down must not let this list grow without limit on a 1 GB Redis.

    The trim rides in the same pipeline as the push: paying a second round-trip per part on the
    write path to bound a queue that is normally empty would be a poor trade.
    """
    queues = FakeQueues()
    await LandedPartPublisher(queues, "node-a", max_depth=1000).publish(OBJ, 1, 1)

    assert [op[0] for op in queues.executed] == ["lpush", "ltrim"]
    assert queues.executed[1] == ("ltrim", "cephor:landed:node-a", 0, 999)


@pytest.mark.asyncio
async def test_a_redis_outage_never_reaches_the_caller() -> None:
    """The bytes and meta are already durable when this runs, so raising here would fail a PUT
    that fully succeeded. The reconciler still finds the part on disk."""
    await LandedPartPublisher(FakeQueues(fail=True), "node-a").publish(OBJ, 1, 1)


def test_no_node_identity_means_no_publisher() -> None:
    """Without NODE_NAME there is no way to say WHOSE agent should drain the part, and an
    announcement on the wrong node's queue would record a row against a node that does not hold
    the data — where the node-scoped `claim_part` would never drain it. Publishing nothing is
    strictly better: the reconciler on the node that does hold it still finds it."""
    assert initialize_landed_publisher(FakeQueues(), "") is None
    assert get_landed_publisher() is None


def test_no_queue_client_means_no_publisher() -> None:
    assert initialize_landed_publisher(None, "node-a") is None


@pytest.mark.asyncio
async def test_write_meta_announces_strictly_after_the_readiness_gate() -> None:
    """THE ordering invariant.

    `meta.json` is what makes a part readable and claimable. Announcing before it lands would
    let the drain claim a part whose meta is not yet on disk. The order is asserted directly
    rather than inferred from the source.
    """
    order: list[str] = []

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            order.append("meta")

    class RecordingPublisher:
        async def publish(self, *_a) -> None:
            order.append("announce")

    landed._publisher = RecordingPublisher()
    writer = WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60)

    await writer.write_meta(OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4)

    assert order == ["meta", "announce"], "the announcement must follow the readiness gate"


@pytest.mark.asyncio
async def test_write_meta_works_with_no_publisher_installed() -> None:
    """Workers, scripts and tests never initialize the singleton; they must still write meta."""
    calls: list[str] = []

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            calls.append("meta")

    landed._publisher = None
    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).write_meta(
        OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4
    )

    assert calls == ["meta"]


@pytest.mark.asyncio
async def test_a_failing_announcement_does_not_fail_the_write(monkeypatch) -> None:
    """End to end through the writer: a dead redis-queues costs a fast discovery path, not a PUT."""

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            return None

    initialize_landed_publisher(FakeQueues(fail=True), "node-a")
    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).write_meta(
        OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4
    )


@pytest.mark.asyncio
async def test_write_meta_stamps_recency_before_the_announcement() -> None:
    """The evict-vs-reland shield's ordering half.

    A rewrite of an already-replicated part touches nothing the drain evictor sorts on, so
    until the agent pops the announcement and checks the content, the only copy of the new
    bytes ranks as the LRU's coldest candidate. The recency stamp is what makes it hottest
    instead — and it must land BEFORE the announcement, so by the time the agent (and its
    evictor, in the same process) can react to the message the shield is already down.
    """
    order: list[str] = []

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            order.append("meta")

    class RecordingRecorder:
        async def __call__(self, *_a) -> None:
            order.append("stamp")

    class RecordingPublisher:
        async def publish(self, *_a) -> None:
            order.append("announce")

    read_recency._recorder = RecordingRecorder()
    landed._publisher = RecordingPublisher()

    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).write_meta(
        OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4
    )

    assert order == ["meta", "stamp", "announce"]


@pytest.mark.asyncio
async def test_write_meta_works_with_no_recency_recorder_installed() -> None:
    """Workers and scripts never initialize the recorder; their meta writes must not care."""
    calls: list[str] = []

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            calls.append("meta")

    read_recency._recorder = None
    landed._publisher = None
    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).write_meta(
        OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4
    )

    assert calls == ["meta"]
