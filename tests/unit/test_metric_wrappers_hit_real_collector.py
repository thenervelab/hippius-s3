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
from hippius_s3.reader import streamer


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


def _wrappers_in_tree() -> set[str]:
    """Every module-level `_record*` function that resolves a metrics collector."""
    found: set[str] = set()
    for path in sorted(pathlib.Path("hippius_s3").rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("_record"):
                continue
            if "get_metrics_collector" in ast.dump(node):
                found.add(f"{path.as_posix()}::{node.name}")
    return found


def test_every_wrapper_in_the_tree_is_covered() -> None:
    """A new swallow-all wrapper must not ship without a row above.

    The gap this file closes was not one wrapper being wrong — it was six landing at once with
    nothing able to observe any of them. Enumerating them by hand only stays honest if adding the
    seventh is what breaks.
    """
    assert _wrappers_in_tree() == {site for site, *_ in WRAPPERS}
