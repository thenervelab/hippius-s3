"""The request histograms must export second-scale bucket boundaries, not the SDK defaults.

The OTel SDK's default histogram boundaries are (0, 5, 10, 25, ..., 10000) — millisecond-scale.
Our histograms record SECONDS, so before `build_metric_views` every sample landed in the first two
buckets and `histogram_quantile()` in Grafana was interpolating almost blind. The failure mode is
silent: dashboards keep rendering plausible-looking percentiles with no hint that the resolution is
gone, which is why these tests assert the boundaries that actually reach the exporter.
"""

from __future__ import annotations

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from hippius_s3.otel_setup import HTTP_DURATION_BUCKETS
from hippius_s3.otel_setup import HTTP_TTFB_BUCKETS
from hippius_s3.otel_setup import build_metric_views


def _exported_bounds(instrument_name: str) -> tuple[float, ...]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader], views=build_metric_views())
    histogram = provider.get_meter("test").create_histogram(name=instrument_name, unit="s")
    histogram.record(0.123, attributes={"method": "GET"})

    metrics_data = reader.get_metrics_data()
    assert metrics_data is not None
    for resource_metrics in metrics_data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == instrument_name:
                    return tuple(metric.data.data_points[0].explicit_bounds)
    raise AssertionError(f"{instrument_name} was not exported")


def test_ttfb_histogram_gets_subsecond_resolution() -> None:
    assert _exported_bounds("http_request_ttfb_seconds") == HTTP_TTFB_BUCKETS


def test_pre_handler_histogram_gets_subsecond_resolution() -> None:
    assert _exported_bounds("http_pre_handler_duration_seconds") == HTTP_TTFB_BUCKETS


def test_duration_histogram_gets_second_scale_buckets_with_a_transfer_tail() -> None:
    bounds = _exported_bounds("http_request_duration_seconds")
    assert bounds == HTTP_DURATION_BUCKETS
    assert bounds[0] <= 0.01 and bounds[-1] >= 600, "must resolve fast requests AND multi-minute uploads"


def test_unrelated_histograms_keep_their_default_aggregation() -> None:
    """The views must match by exact instrument name — a glob that swallowed e.g. the uploader's
    worker histograms would silently rebucket series this change never audited."""
    bounds = _exported_bounds("uploader_duration_seconds")
    assert bounds != HTTP_TTFB_BUCKETS and bounds != HTTP_DURATION_BUCKETS
