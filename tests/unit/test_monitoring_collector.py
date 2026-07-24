"""Smoke tests for the new worker-loop metric surface on MetricsCollector.

The real collector uses the default (no-op) OTel meter when no provider is configured
(as in unit tests), so these assert the new `record_*` methods construct + fire without
raising, that a redis-less collector is constructable (for the cachet worker), and that
NullMetricsCollector no-ops them all.
"""

from __future__ import annotations

import pytest

from hippius_s3.metrics_collector_task import _redis_memory
from hippius_s3.monitoring import MetricsCollector
from hippius_s3.monitoring import NullMetricsCollector


def _exercise(collector: MetricsCollector | NullMetricsCollector) -> None:
    collector.record_mpu_reaper_cycle(success=True, reaped=3, duration=0.5, oldest_reaped_age=1200.0)
    collector.record_mpu_reaper_cycle(success=False, reaped=0, duration=0.1)
    collector.record_orphan_checker_cycle(success=True, files_scanned=100, orphans_found=2, duration=1.0)
    collector.record_account_cacher_cycle(success=True, accounts_cached=42, duration=0.3)
    collector.record_cachet_check(status="operational", update_success=True)
    collector.record_dlq_push(queue="arion_upload_requests:dlq", error_type="transient")
    collector.record_dlq_requeue(queue="arion_upload_requests:dlq", count=5)
    collector.record_dlq_requeue(queue="arion_upload_requests:dlq")  # default count=1


def test_real_collector_is_constructable_without_redis_and_records() -> None:
    collector = MetricsCollector()  # redis_client optional — the cachet worker path
    _exercise(collector)


def test_null_collector_no_ops_every_new_method() -> None:
    _exercise(NullMetricsCollector())


@pytest.mark.parametrize("count", [0, -1])
def test_dlq_requeue_ignores_nonpositive_counts(count: int) -> None:
    # A batch that requeued nothing must not emit a spurious increment.
    MetricsCollector().record_dlq_requeue(queue="q", count=count)


# ------------------------------------------------------- redis memory gauge (cluster-aware)


def test_redis_memory_reads_a_single_node_info() -> None:
    used, cap = _redis_memory({"used_memory": 15354056, "maxmemory": 4294967296})
    assert (used, cap) == (15354056, 4294967296)


def test_redis_memory_sums_across_cluster_nodes() -> None:
    """A RedisCluster fans INFO out to every primary and returns one mapping PER NODE, so the flat
    `info["used_memory"]` lookup returned 0 and the gauge read empty for cluster-backed pods."""
    info = {
        "10.0.0.1:6379": {"used_memory": 100, "maxmemory": 1000},
        "10.0.0.2:6379": {"used_memory": 250, "maxmemory": 1000},
        "10.0.0.3:6379": {"used_memory": 50, "maxmemory": 1000},
    }
    assert _redis_memory(info) == (400, 3000)


@pytest.mark.parametrize("info", [{}, {"n1": {}}, {"n1": {"used_memory": 5}, "n2": {}}])
def test_redis_memory_tolerates_missing_fields(info: dict) -> None:
    used, cap = _redis_memory(info)
    assert used >= 0 and cap >= 0
