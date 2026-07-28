"""Locks in the two invariants behind the 2026-07-17 metrics incident.

Both regressed silently for months because nothing here was covered, and the existing
worker tests mock the collector — a Mock accepts any keyword, so it would happily let an
account label back in without failing.
"""

import ast
import inspect
import os
import pathlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from hippius_s3.monitoring import MetricsCollector
from hippius_s3.otel_setup import build_resource


# Every label any record_* method is allowed to emit. Each has a closed value set:
# HTTP verbs, status codes, named handlers/operations, configured backends, retry
# attempt numbers, in/out, and the fixed database names we back up. Anything outside
# this set is either unbounded or caller-controlled and must not become a label.
BOUNDED_LABELS = {
    "method",
    "handler",
    "status_code",
    "operation",
    "success",
    "error_type",
    "backend",
    "attempt",
    "direction",
    "database",
    # DLQ key: "unpin_requests:dlq" or "<backend>_upload_requests:dlq", so it is the
    # configured backend set — same closed axis as `backend` above.
    "queue",
    # Cachet component status, mapped through _STATUS_LABELS from {1, 3, 4} with an
    # "unknown" fallback — four values, fixed in code.
    "status",
    # FS-cache shed attribution. `reason` is returned by should_reject_fs_cache_write as one of
    # "threshold" | "pool" ("ok" never reaches the counter — it is the not-rejected case), and
    # `pressure_mode` is the janitor's published signal stringified: "0" | "1" | "2" | "None".
    # Both are closed sets fixed in code, not caller- or object-derived.
    "reason",
    "pressure_mode",
}


def _mock_all_instruments(collector: MetricsCollector) -> dict[str, MagicMock]:
    """Swap every counter/histogram on the collector for a mock that records calls."""
    mocks: dict[str, MagicMock] = {}
    for name, value in list(vars(collector).items()):
        if name.startswith("_") or name == "redis_client" or name == "meter":
            continue
        if hasattr(value, "add") or hasattr(value, "record"):
            mocks[name] = MagicMock()
            setattr(collector, name, mocks[name])
    return mocks


def _dummy_for(param: inspect.Parameter) -> Any:
    if param.name == "request":
        return _request()
    if param.name == "response":
        return _response()
    ann = param.annotation
    if ann is bool:
        return True
    if ann is int:
        return 1
    if ann is float:
        return 0.1
    return "x"


def _call_with_dummies(method: Any) -> None:
    """Invoke a record_* method supplying only its required parameters."""
    kwargs = {
        name: _dummy_for(p)
        for name, p in inspect.signature(method).parameters.items()
        if p.default is inspect.Parameter.empty
    }
    method(**kwargs)


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

    def test_build_resource_is_the_only_way_a_resource_is_built(self) -> None:
        """No production code may call Resource.create() outside build_resource.

        The tests above prove build_resource is correct, not that anything uses it —
        reverting a call site to an inline Resource.create({... gethostname()}) would
        leave them all green while restoring the collision. Every such call site is an
        independent chance to hand two processes the same identity, so there must be
        exactly one.
        """
        offenders = []
        for path in (*pathlib.Path("hippius_s3").rglob("*.py"), *pathlib.Path("workers").rglob("*.py")):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                    continue
                if node.func.attr != "create" or getattr(node.func.value, "id", None) != "Resource":
                    continue
                enclosing = [
                    n.name
                    for n in ast.walk(tree)
                    if isinstance(n, ast.FunctionDef) and n.lineno <= node.lineno <= (n.end_lineno or n.lineno)
                ]
                if "build_resource" not in enclosing:
                    offenders.append(f"{path}:{node.lineno} (in {enclosing or ['<module>']})")

        assert offenders == [], "Resource.create() outside build_resource: " + ", ".join(offenders)

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

    def test_no_record_method_anywhere_accepts_an_account(self) -> None:
        # Guarding record_http_request alone is not enough: main_account was threaded
        # through fifteen record_* methods, and a reviewer put it back on
        # record_s3_operation with the rest of this file passing. Cover all of them.
        offenders = {
            name: sorted(set(inspect.signature(fn).parameters) & {"main_account", "subaccount_id"})
            for name, fn in inspect.getmembers(MetricsCollector, inspect.isfunction)
            if name.startswith("record_") and set(inspect.signature(fn).parameters) & {"main_account", "subaccount_id"}
        }
        assert offenders == {}, f"account params are back on: {offenders}"

    def test_every_record_method_emits_only_bounded_labels(self, collector: MetricsCollector) -> None:
        """Call every record_* method and assert nothing open-ended reaches a label.

        Stronger than checking parameter names: it catches a label added from any
        source — a caller-supplied value, a bucket name, an error string — not just an
        account threaded back through the signature.
        """
        instruments = _mock_all_instruments(collector)

        called = [name for name, _ in inspect.getmembers(collector, inspect.ismethod) if name.startswith("record_")]
        for name in called:
            _call_with_dummies(getattr(collector, name))

        emitted: set[str] = set()
        for mock in instruments.values():
            for call in mock.add.call_args_list + mock.record.call_args_list:
                emitted |= set(call.kwargs.get("attributes") or {})

        assert called, "no record_* methods were exercised — the reflection broke"
        assert emitted, "no attributes captured — the instrument mocking broke"
        assert emitted <= BOUNDED_LABELS, f"unbounded label(s) emitted: {sorted(emitted - BOUNDED_LABELS)}"


class TestOperationCountedExactlyOnce:
    """s3_operations_total is owned by record_s3_operation ALONE.

    Every data op (put_object, get_object, upload_part, CompleteMPU) calls BOTH
    record_s3_operation and record_data_transfer. When record_data_transfer also bumped
    s3_operations_total, each such op counted twice — split across two label shapes
    ({operation} vs {operation, success}) — inflating the counter 2x. Lock the split of
    responsibilities: record_data_transfer records BYTES ONLY.
    """

    def _counting_collector(self) -> tuple[MetricsCollector, MagicMock, MagicMock, MagicMock]:
        c = MetricsCollector(redis_client=MagicMock())
        c.s3_operations_total = MagicMock()
        c.s3_bytes_uploaded = MagicMock()
        c.s3_bytes_downloaded = MagicMock()
        return c, c.s3_operations_total, c.s3_bytes_uploaded, c.s3_bytes_downloaded

    def test_record_data_transfer_records_bytes_only_not_the_op_count(self) -> None:
        c, ops, up, _down = self._counting_collector()
        c.record_data_transfer(operation="put_object", bytes_transferred=1234, bucket_name="b")

        ops.add.assert_not_called()  # the double-count that inflated s3_operations_total 2x
        up.add.assert_called_once()
        assert up.add.call_args.args[0] == 1234

    def test_record_s3_operation_is_the_sole_counter(self) -> None:
        c, ops, _up, _down = self._counting_collector()
        c.record_s3_operation(operation="put_object", bucket_name="b", success=True)

        ops.add.assert_called_once_with(1, attributes={"operation": "put_object", "success": "true"})

    def test_a_data_op_calling_both_increments_the_count_exactly_once(self) -> None:
        # Mirrors the put_object/get_object/upload_part/CompleteMPU call pattern.
        c, ops, _up, _down = self._counting_collector()
        c.record_s3_operation(operation="get_object", bucket_name="b", success=True)
        c.record_data_transfer(operation="get_object", bytes_transferred=10, bucket_name="b")

        assert ops.add.call_count == 1, "a data op must count once, not twice"
