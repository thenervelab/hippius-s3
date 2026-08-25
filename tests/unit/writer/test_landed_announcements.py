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

from hippius_s3.cache import peers
from hippius_s3.cache import read_recency
from hippius_s3.cache.peers import PeerRegistry
from hippius_s3.cache.peers import fresh_part_key
from hippius_s3.cache.peers import set_active_registry
from hippius_s3.writer import landed
from hippius_s3.writer.landed import LandedPartPublisher
from hippius_s3.writer.landed import get_landed_publisher
from hippius_s3.writer.landed import initialize_landed_publisher
from hippius_s3.writer.landed import landed_queue_key
from hippius_s3.writer.write_through_writer import WriteThroughPartsWriter


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


class _HangingPipe:
    def lpush(self, *_a: object) -> None:
        return None

    def ltrim(self, *_a: object) -> None:
        return None

    async def execute(self) -> None:
        import asyncio as _asyncio

        await _asyncio.sleep(300)


class _HangingRedis:
    """A queues client that accepts the pipeline and then never answers."""

    def pipeline(self) -> _HangingPipe:
        return _HangingPipe()


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
    peers._active_registry = None


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
async def test_write_meta_stamps_the_ingest_node_after_meta_before_announce() -> None:
    """Wrong-node GETs in the pre-claim window need this hint; it must not precede meta.json."""
    order: list[str] = []

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            order.append("meta")

    class RecordingPublisher:
        async def publish(self, *_a) -> None:
            order.append("announce")

    class RecordingRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        async def set(self, key: str, value: str, ex: int | None = None) -> None:
            order.append("remember")
            self.store[key] = value

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

    redis = RecordingRedis()
    set_active_registry(PeerRegistry(redis, "node-b", "http://10.42.2.9:8000", 90))
    landed._publisher = RecordingPublisher()
    writer = WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60)

    await writer.write_meta(OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4)

    assert order == ["meta", "remember", "announce"]
    assert redis.store[fresh_part_key(OBJ, 1, 1)] == "node-b"


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
async def test_publish_part_announces_strictly_after_the_store_publish() -> None:
    """The SECOND choke point, and the one with no `set_meta` call to hang an announcement on.

    MPU parts and append deltas stage their chunks, so for them the meta write happens INSIDE
    `fs_store.publish_part`. An announcement missing here does not error anywhere: those parts
    just stop reaching the agent directly and wait for the reconciler's disk walk.
    """
    order: list[str] = []

    class RecordingStore:
        async def publish_part(self, *_a, **_kw) -> None:
            order.append("publish")

    class RecordingPublisher:
        async def publish(self, *_a) -> None:
            order.append("announce")

    landed._publisher = RecordingPublisher()
    writer = WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60)

    await writer.publish_part(OBJ, 1, 1, attempt_id="0123456789abcdef", chunk_size=4, num_chunks=1, plain_size=4)

    assert order == ["publish", "announce"], "the announcement must follow the promotion"


@pytest.mark.asyncio
async def test_a_failed_publish_announces_nothing() -> None:
    """Announcing a part whose promotion raised would have the agent claim a part that is not
    published — its meta belongs to whatever attempt was there before, if any."""
    announced: list[tuple] = []

    class FailingStore:
        async def publish_part(self, *_a, **_kw) -> None:
            raise OSError("rename failed mid-swap")

    class RecordingPublisher:
        async def publish(self, *args) -> None:
            announced.append(args)

    landed._publisher = RecordingPublisher()
    writer = WriteThroughPartsWriter(FailingStore(), None, ttl_seconds=60)

    with pytest.raises(OSError):
        await writer.publish_part(OBJ, 1, 1, attempt_id="0123456789abcdef", chunk_size=4, num_chunks=1, plain_size=4)

    assert announced == []


@pytest.mark.asyncio
async def test_publish_part_works_with_no_publisher_installed() -> None:
    """Same as `write_meta`: workers, scripts and tests never initialize the singleton."""
    calls: list[str] = []

    class RecordingStore:
        async def publish_part(self, *_a, **_kw) -> None:
            calls.append("publish")

    landed._publisher = None
    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).publish_part(
        OBJ, 1, 1, attempt_id="0123456789abcdef", chunk_size=4, num_chunks=1, plain_size=4
    )

    assert calls == ["publish"]


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
async def test_a_failing_recency_stamp_does_not_fail_the_write() -> None:
    """The stamp's other never-fail half, through the REAL recorder.

    asyncpg.InterfaceError (a closing/uninitialised pool) is neither PostgresError nor
    OSError, so a narrowly-guarded recorder would let it escape write_meta — failing the
    client PUT and, worse, skipping the announcement below it, the very signal the B-2
    re-drive depends on. The write must complete and the announcement must still fire.
    """
    import asyncpg

    from hippius_s3.cache.read_recency import ReadRecencyRecorder

    order: list[str] = []

    class RecordingStore:
        async def set_meta(self, *_a, **_kw) -> None:
            order.append("meta")

    class ClosingPool:
        def acquire(self) -> None:
            raise asyncpg.InterfaceError("pool is closing")

    class RecordingPublisher:
        async def publish(self, *_a) -> None:
            order.append("announce")

    read_recency._recorder = ReadRecencyRecorder(ClosingPool(), "node-a")
    landed._publisher = RecordingPublisher()

    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).write_meta(
        OBJ, 1, 1, chunk_size=4, num_chunks=1, plain_size=4
    )

    assert order == ["meta", "announce"]


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


@pytest.mark.asyncio
async def test_publish_part_stamps_recency_before_the_announcement() -> None:
    """The staging path needs the same shield, and needs it MORE than write_meta does.

    `write_meta` covers the simple-PUT path, where a rewrite lands in a per-object-version dir
    and the evict-vs-reland race does not arise. The staging path here is MPU parts and append
    deltas — a re-uploaded MPU part is exactly the B-2 shape the shield exists for, so a stamp
    on write_meta alone protects the case that cannot happen and leaves the one that does
    uncovered.
    """
    order: list[str] = []

    class RecordingStore:
        async def publish_part(self, *_a, **_kw) -> None:
            order.append("publish")

    class RecordingRecorder:
        async def __call__(self, *_a) -> None:
            order.append("stamp")

    class RecordingPublisher:
        async def publish(self, *_a) -> None:
            order.append("announce")

    read_recency._recorder = RecordingRecorder()
    landed._publisher = RecordingPublisher()

    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).publish_part(
        OBJ, 1, 1, attempt_id="attemptaa", chunk_size=4, num_chunks=1, plain_size=4
    )

    assert order == ["publish", "stamp", "announce"]


@pytest.mark.asyncio
async def test_publish_part_works_with_no_recency_recorder_installed() -> None:
    """Same degradation as write_meta: no recorder outside the api, and that must not fail a PUT."""
    calls: list[str] = []

    class RecordingStore:
        async def publish_part(self, *_a, **_kw) -> None:
            calls.append("publish")

    read_recency._recorder = None
    landed._publisher = None
    await WriteThroughPartsWriter(RecordingStore(), None, ttl_seconds=60).publish_part(
        OBJ, 1, 1, attempt_id="attemptaa", chunk_size=4, num_chunks=1, plain_size=4
    )
    assert calls == ["publish"]


@pytest.mark.asyncio
async def test_a_failed_publish_stamps_no_recency() -> None:
    """A publish that raised produced no new bytes, so there is nothing to keep hot.

    Stamping anyway would mark a part the evictor may legitimately want as freshly-read, on the
    strength of a write that did not happen.
    """
    stamped: list[str] = []

    class FailingStore:
        async def publish_part(self, *_a, **_kw) -> None:
            raise FileNotFoundError("staged set vanished")

    class RecordingRecorder:
        async def __call__(self, *_a) -> None:
            stamped.append("stamp")

    read_recency._recorder = RecordingRecorder()
    landed._publisher = None

    with pytest.raises(FileNotFoundError):
        await WriteThroughPartsWriter(FailingStore(), None, ttl_seconds=60).publish_part(
            OBJ, 1, 1, attempt_id="attemptaa", chunk_size=4, num_chunks=1, plain_size=4
        )

    assert stamped == []


@pytest.mark.asyncio
async def test_a_hanging_redis_does_not_hang_the_put() -> None:
    """The swallow catches a redis-queues that ERRORS. It does nothing for a slow one.

    This await is on the client PUT path and the api's queues client is built with no
    socket_timeout, so without a bound a slow redis-queues stalls every PUT — the shape of a real
    prod incident, where a 1.29M-entry list on this same instance surfaced as GET IncompleteRead.
    """
    import asyncio as _asyncio

    publisher = landed.LandedPartPublisher(_HangingRedis(), "node-a")

    started = _asyncio.get_running_loop().time()
    await _asyncio.wait_for(publisher.publish(OBJ, 1, 1), timeout=landed._PUBLISH_TIMEOUT_SECONDS + 5)
    elapsed = _asyncio.get_running_loop().time() - started

    assert elapsed < landed._PUBLISH_TIMEOUT_SECONDS + 2, (
        f"publish waited {elapsed:.1f}s on a hanging redis; it must give up at its own bound"
    )


@pytest.mark.asyncio
async def test_a_timed_out_announcement_never_reaches_the_caller_and_is_bounded() -> None:
    """Timing out is the same contract as erroring — but it must also actually STOP.

    The elapsed assertion is the point: without it this test passes against an unbounded await,
    just slowly, so it would pin nothing.
    """
    import asyncio as _asyncio

    started = _asyncio.get_running_loop().time()
    await landed.LandedPartPublisher(_HangingRedis(), "node-a").publish(OBJ, 1, 1)
    elapsed = _asyncio.get_running_loop().time() - started

    assert elapsed < landed._PUBLISH_TIMEOUT_SECONDS + 2


@pytest.mark.asyncio
async def test_a_dropped_announcement_is_counted_not_just_logged(monkeypatch) -> None:
    """A drop has to be alertable.

    The announcement is the only trigger for the B-2 divergence check — the reconciler tallies an
    already-`replicated` part as an orphan and does not content-check it — so losing one for a
    RE-uploaded part leaves the pool serving the previous attempt's bytes under the new ETag.
    The agent's `drain_landed_dropped_total` counts only messages it could not PARSE, so it
    cannot see one that never arrived. If this is only a log line, nothing can page on it.
    """
    recorded: list[str] = []
    monkeypatch.setattr(landed, "_record_announce_failure", lambda outcome: recorded.append(outcome))

    await landed.LandedPartPublisher(_HangingRedis(), "node-a").publish(OBJ, 1, 1)
    assert recorded == ["timeout"]

    recorded.clear()

    class _BrokenRedis:
        def pipeline(self) -> object:
            raise ConnectionError("redis-queues is down")

    await landed.LandedPartPublisher(_BrokenRedis(), "node-a").publish(OBJ, 1, 1)
    assert recorded == ["error"], "an outright failure must be distinguishable from a timeout"
