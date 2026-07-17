"""Locks in the two invariants behind the 2026-07-17 metrics incident.

Both regressed silently for months because nothing here was covered, and the existing
worker tests mock the collector — a Mock accepts any keyword, so it would happily let an
account label back in without failing.
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hippius_s3.monitoring import MetricsCollector
from hippius_s3.otel_setup import build_resource


def _request(method: str = "GET", path: str = "/my-bucket/some/object/key.bin") -> SimpleNamespace:
    return SimpleNamespace(
        method=method,
        url=SimpleNamespace(path=path),
        headers={},
    )


def _response(status_code: int = 200) -> SimpleNamespace:
    return SimpleNamespace(status_code=status_code, headers={})


@pytest.fixture
def collector() -> MetricsCollector:
    # No MeterProvider is configured under pytest, so create_counter/create_histogram
    # hand back no-op instruments; swap in mocks to capture the attributes.
    c = MetricsCollector(redis_client=MagicMock())
    c.http_requests_total = MagicMock()
    c.http_request_duration = MagicMock()
    c.http_request_bytes = MagicMock()
    c.http_response_bytes = MagicMock()
    return c


class TestInstanceIdIsPerProcess:
    """A pod runs UVICORN_WORKERS processes; each MUST be its own metric writer."""

    def test_instance_id_includes_the_pid(self) -> None:
        attrs = build_resource("hippius-s3-api").attributes
        assert attrs["service.instance.id"].endswith(f":{os.getpid()}")
        assert attrs["service.name"] == "hippius-s3-api"

    def test_two_processes_on_one_host_get_distinct_identities(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("hippius_s3.otel_setup.socket.gethostname", lambda: "api-abc123")

        monkeypatch.setattr("hippius_s3.otel_setup.os.getpid", lambda: 11)
        worker_a = build_resource("hippius-s3-api").attributes["service.instance.id"]
        monkeypatch.setattr("hippius_s3.otel_setup.os.getpid", lambda: 22)
        worker_b = build_resource("hippius-s3-api").attributes["service.instance.id"]

        # Sharing an identity is what collapsed 4 workers into 1 accumulator slot and
        # made every scrape look like a counter reset (~105x inflated rates).
        assert worker_a != worker_b
        assert worker_a == "api-abc123:11"

    def test_env_cannot_override_the_per_process_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The deployments set service.instance.id=$(POD_NAME) here. If env won, the fix
        # would be a silent no-op and the 105x would come straight back.
        monkeypatch.setenv(
            "OTEL_RESOURCE_ATTRIBUTES",
            "deployment.environment=production,service.namespace=hippius-s3-prod,service.instance.id=api-pod-xyz",
        )
        attrs = build_resource("hippius-s3-api").attributes

        assert attrs["service.instance.id"] != "api-pod-xyz"
        assert attrs["service.instance.id"].endswith(f":{os.getpid()}")
        # ...while the rest of that env var must still merge through: the dashboards
        # select on service_namespace.
        assert attrs["service.namespace"] == "hippius-s3-prod"
        assert attrs["deployment.environment"] == "production"


class TestNoUnboundedLabels:
    """Every attribute becomes a Prometheus label, so each must have a closed value set."""

    def test_handler_falls_back_to_unknown_not_the_url_path(self, collector: MetricsCollector) -> None:
        collector.record_http_request(request=_request(), response=_response(), duration=0.1, handler=None)

        attrs = collector.http_requests_total.add.call_args.kwargs["attributes"]
        # The path holds the object key: falling back to it mints one series per object.
        assert attrs["handler"] == "unknown"
        assert "some/object/key.bin" not in str(attrs)

    def test_no_account_labels_are_emitted(self, collector: MetricsCollector) -> None:
        collector.record_http_request(request=_request(), response=_response(), duration=0.1, handler="get_object")

        for instrument in (collector.http_requests_total.add, collector.http_request_duration.record):
            attrs = instrument.call_args.kwargs["attributes"]
            assert set(attrs) == {"method", "handler", "status_code"}

    @pytest.mark.parametrize("kwarg", ["main_account", "subaccount_id"])
    def test_passing_an_account_is_a_hard_error(self, collector: MetricsCollector, kwarg: str) -> None:
        # Account attribution belongs in the audit log and on spans, which index
        # high-cardinality keys by design. Keep the door shut rather than ignoring it
        # silently: main_account was 87% of the namespace's series and nothing read it.
        with pytest.raises(TypeError):
            collector.record_http_request(
                request=_request(),
                response=_response(),
                duration=0.1,
                **{kwarg: "5E71kYuDbwhMbnK7JVtH1xLMguogmvozTrNJHYD9KnEcuWgZ"},
            )
