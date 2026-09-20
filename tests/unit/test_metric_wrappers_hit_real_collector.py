"""Every swallow-all metric wrapper must actually move its counter.

The `_record_*` helpers on the read path all share one shape: lazily import
`get_metrics_collector`, call one `record_*` method, and swallow `Exception` so observability can
never fail a GET. That last part is also what makes them fragile — rename or mistype the collector
method and the wrapper becomes a permanent no-op. The read path keeps working, nothing logs, and
the metric simply stops existing. A dashboard panel going flat is the only symptom, and a flat
panel is indistinguishable from "this never happens".

Nothing else can catch that. Every behavioural test of these modules injects a `MagicMock`
collector, and a Mock answers to any attribute name, so a typo'd method still "records". And the
process-wide default is `NullMetricsCollector`, whose methods are `*args, **kwargs` no-ops — so
under pytest, with no MeterProvider configured, calling a wrapper is unobservable by construction.

So this wires a REAL `MetricsCollector` to a real in-memory OTel reader and asserts the counter
moved by one, with the labels the wrapper claims to attach.
"""

from __future__ import annotations

import ast
import pathlib
from typing import Any
from typing import Callable
from typing import Iterator
from typing import Mapping
from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from hippius_s3 import fs_pressure
from hippius_s3 import monitoring
from hippius_s3.cache import dual_fs_store
from hippius_s3.cache import peers
from hippius_s3.cache import read_recency
from hippius_s3.cache import residency
from hippius_s3.monitoring import MetricsCollector
from hippius_s3.reader import backend_fetch
from hippius_s3.reader import streamer
from hippius_s3.writer import landed


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    """A real collector whose instruments write into an in-memory reader.

    The instruments are created in `MetricsCollector.__init__` from `metrics.get_meter(...)`, so
    the meter has to be swapped BEFORE construction — patching the collector afterwards would
    leave every counter as the no-op instrument the global provider hands out under pytest.
    `set_meter_provider` is refused after the first call in a process, which is why this patches
    `get_meter` instead of installing the provider globally.
    """
    in_memory = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[in_memory])
    monkeypatch.setattr(monitoring.metrics, "get_meter", provider.get_meter)

    collector = MetricsCollector(redis_client=MagicMock())
    # The wrappers resolve the collector through this module global on every call.
    monkeypatch.setattr(monitoring, "_metrics_collector", collector)

    yield in_memory

    provider.shutdown()


def _counter_total(reader: InMemoryMetricReader, name: str, attributes: Mapping[str, str]) -> int:
    """Sum the data points of `name` whose attributes include `attributes`."""
    data = reader.get_metrics_data()
    if data is None:
        return 0
    return sum(
        point.value
        for resource_metric in data.resource_metrics
        for scope_metric in resource_metric.scope_metrics
        for metric in scope_metric.metrics
        if metric.name == name
        for point in metric.data.data_points
        if attributes.items() <= dict(point.attributes or {}).items()
    )


def _gauge_value(reader: InMemoryMetricReader, name: str) -> int | None:
    """Last observed value of gauge `name`, or None if it was never exported."""
    data = reader.get_metrics_data()
    if data is None:
        return None
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == name:
                    return metric.data.data_points[-1].value
    return None


# (wrapper, args, exported counter name, labels the wrapper must attach).
#
# Held as a table because the completeness check below compares it against every wrapper in the
# tree: a new `_record_*` helper that forgets to add a row here fails that test rather than
# shipping uncovered.
WRAPPERS: list[tuple[str, Callable[..., None], tuple[Any, ...], str, dict[str, str]]] = [
    (
        "hippius_s3/cache/dual_fs_store.py::_record_tier",
        dual_fs_store._record_tier,
        ("local",),
        "chunk_reads_by_tier_total",
        {"tier": "local"},
    ),
    (
        "hippius_s3/cache/dual_fs_store.py::_record_promotion_skipped",
        dual_fs_store._record_promotion_skipped,
        ("disk_pressure",),
        "promotion_skipped_total",
        {"reason": "disk_pressure"},
    ),
    (
        "hippius_s3/cache/residency.py::_record_release_failure",
        residency._record_release_failure,
        (),
        "residency_release_failures_total",
        {},
    ),
    (
        "hippius_s3/writer/landed.py::_record_announce_failure",
        landed._record_announce_failure,
        ("timeout",),
        "landed_announce_failures_total",
        {"outcome": "timeout"},
    ),
    (
        "hippius_s3/cache/peers.py::_record_shed",
        peers._record_shed,
        ("peer_miss",),
        "peer_fetch_shed_total",
        {"reason": "peer_miss"},
    ),
    (
        "hippius_s3/cache/read_recency.py::_record_write",
        read_recency._record_write,
        ("written",),
        "read_recency_writes_total",
        {"outcome": "written"},
    ),
    (
        "hippius_s3/fs_pressure.py::_record_floor_divergence",
        fs_pressure._record_floor_divergence,
        ("stricter",),
        "promote_floor_divergence_total",
        {"direction": "stricter"},
    ),
    (
        "hippius_s3/reader/streamer.py::_record_aead_failure",
        streamer._record_aead_failure,
        ("local", "recovered"),
        "chunk_aead_failures_total",
        {"tier": "local", "outcome": "recovered"},
    ),
    (
        "hippius_s3/reader/backend_fetch.py::_record_backend_read",
        backend_fetch._record_backend_read,
        (),
        "chunk_reads_by_tier_total",
        {"tier": "backend"},
    ),
    (
        "hippius_s3/reader/backend_fetch.py::_record_backend_fetch_outcome",
        backend_fetch._record_backend_fetch_outcome,
        ("pool_timeout",),
        "backend_fetch_outcomes_total",
        {"outcome": "pool_timeout"},
    ),
]


@pytest.mark.parametrize(
    ("wrapper", "args", "counter", "labels"),
    [pytest.param(w, a, c, lab, id=site.split("::")[-1]) for site, w, a, c, lab in WRAPPERS],
)
def test_wrapper_moves_its_counter(
    reader: InMemoryMetricReader,
    wrapper: Callable[..., None],
    args: tuple[Any, ...],
    counter: str,
    labels: dict[str, str],
) -> None:
    before = _counter_total(reader, counter, labels)

    wrapper(*args)

    assert _counter_total(reader, counter, labels) == before + 1, (
        f"{counter}{labels} did not move: the wrapper swallowed whatever went wrong, so a drifted "
        f"collector method name would look exactly like this"
    )


def test_a_drifted_collector_method_is_what_this_catches(
    reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure mode stated as a test: delete the method, the counter stops, nothing raises.

    Without this, the tests above could pass for the wrong reason — e.g. if the wrappers were
    reached through a Mock. Here the wrapper is called with the collector method genuinely
    missing: it must stay silent (a metrics failure must never fail a read) AND record nothing.
    """
    monkeypatch.delattr(type(monitoring.get_metrics_collector()), "record_chunk_read_tier")
    before = _counter_total(reader, "chunk_reads_by_tier_total", {"tier": "pool"})

    dual_fs_store._record_tier("pool")  # must not raise

    assert _counter_total(reader, "chunk_reads_by_tier_total", {"tier": "pool"}) == before


def test_publish_slots_moves_both_gauges(reader: InMemoryMetricReader) -> None:
    # A gauge setter has the same silent-drift failure mode as the counter wrappers: rename
    # the collector method and the gauges go quiet with nothing failing. Prove the chain moves.
    backend_fetch._publish_slots(3, 1)
    assert _gauge_value(reader, "backend_fetch_inflight") == 3
    assert _gauge_value(reader, "backend_fetch_waiting") == 1


# Gauge setters are covered by their own real-collector test above, not by the counter table.
GAUGE_SETTERS: set[str] = {"hippius_s3/reader/backend_fetch.py::_publish_slots"}


def _swallows_exceptions(fn: ast.FunctionDef) -> bool:
    return any(
        isinstance(handler.type, ast.Name) and handler.type.id in {"Exception", "BaseException"}
        for node in ast.walk(fn)
        if isinstance(node, ast.Try)
        for handler in node.handlers
    )


def _calls_collector_method(fn: ast.FunctionDef) -> bool:
    return any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr.startswith(("record", "update", "set"))
        for node in ast.walk(fn)
    )


def _wrappers_in_tree() -> set[str]:
    """Every module-level swallow-all wrapper, by structure rather than by name."""
    # This is the module docstring's definition of a wrapper: it resolves the collector, calls one
    # record_/update_/set_ method on it, and swallows Exception. Unwrapped call sites
    # (`get_metrics_collector().record_x(...)` with no try) raise on drift, so they are out of scope.
    found: set[str] = set()
    for path in sorted(pathlib.Path("hippius_s3").rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef) or "get_metrics_collector" not in ast.dump(node):
                continue
            if _swallows_exceptions(node) and _calls_collector_method(node):
                found.add(f"{path.as_posix()}::{node.name}")
    return found


def test_every_wrapper_in_the_tree_is_covered() -> None:
    """A new swallow-all wrapper must not ship without a row above.

    The gap this file closes was not one wrapper being wrong — it was six landing at once with
    nothing able to observe any of them. Enumerating them by hand only stays honest if adding the
    seventh is what breaks.
    """
    assert _wrappers_in_tree() == {site for site, *_ in WRAPPERS} | GAUGE_SETTERS
