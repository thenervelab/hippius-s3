"""Pure-logic guards for the locality probe: header capture, agreement/spread verdicts, exit code,
and the no-config early exit. No network — the boto3 client is a stub that fires the after-call
hook the same way botocore does."""

from __future__ import annotations

import importlib.util
import sys
from email.message import Message
from pathlib import Path
from typing import Any

import pytest


SCRIPT_PATH = Path(__file__).parents[3] / "scripts" / "locality_probe.py"
spec = importlib.util.spec_from_file_location("locality_probe", SCRIPT_PATH)
assert spec and spec.loader
probe = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = probe
spec.loader.exec_module(probe)


def parsed(node: str | None, status: int = 200, **extra: Any) -> dict[str, Any]:
    headers = {"x-hippius-node": node} if node is not None else {}
    return {"ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": headers}, **extra}


class FakeEvents:
    def __init__(self) -> None:
        self.handlers: list[Any] = []

    def register(self, _name: str, handler: Any) -> None:
        self.handlers.append(handler)


class FakeClient:
    """Answers each op with the next queued node, emitting after-call like botocore does."""

    def __init__(self, nodes: list[str | None], extra: dict[str, dict[str, Any]] | None = None) -> None:
        self.nodes = nodes
        self.extra = extra or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.meta = type("Meta", (), {"events": FakeEvents()})()

    def __getattr__(self, op: str) -> Any:
        def _call(**kwargs: Any) -> dict[str, Any]:
            self.calls.append((op, kwargs))
            response = parsed(self.nodes.pop(0), **self.extra.get(op, {}))
            for handler in self.meta.events.handlers:
                handler(parsed=response, http_response=None, model=None, context={})
            return response

        return _call


def config(**overrides: Any) -> Any:
    env = {
        "HIPPIUS_ROUTING_ENDPOINT": "https://example.invalid",
        "AWS_ACCESS_KEY_ID": "k",
        "AWS_SECRET_ACCESS_KEY": "s",
    }
    env.update(overrides)
    return probe.load_config(env)


def test_node_from_parsed_reads_lowercased_header() -> None:
    assert probe.node_from_parsed(parsed("node-a")) == "node-a"
    assert probe.node_from_parsed(parsed(None)) is None
    assert probe.node_from_parsed({}) is None
    assert probe.node_from_parsed(parsed("")) is None


def test_node_from_headers_is_case_insensitive() -> None:
    headers = Message()
    headers["x-hippius-node"] = "node-b"
    assert probe.node_from_headers(headers) == "node-b"
    assert probe.node_from_headers(Message()) is None


def test_hook_capture_returns_node_with_response() -> None:
    client = FakeClient(["node-a", None])
    p = probe.Probe(client, config(), "bkt")
    node, response = p.call("head_object", Key="k")
    assert node == "node-a"
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert client.calls == [("head_object", {"Bucket": "bkt", "Key": "k"})]
    assert p.call("head_object", Key="k")[0] is None


def test_load_config_defaults_and_missing() -> None:
    assert probe.load_config({}) is None
    assert probe.load_config({"HIPPIUS_ROUTING_ENDPOINT": "https://example.invalid"}) is None
    cfg = config()
    assert (cfg.region, cfg.keys, cfg.gets, cfg.lists, cfg.keep_bucket, cfg.drill) == (
        "decentralized",
        20,
        5,
        50,
        False,
        False,
    )
    cfg = config(
        HIPPIUS_PROBE_KEYS="3", HIPPIUS_PROBE_KEEP_BUCKET="true", HIPPIUS_PROBE_DRILL="1", AWS_DEFAULT_REGION="r"
    )
    assert (cfg.region, cfg.keys, cfg.keep_bucket, cfg.drill) == ("r", 3, True, True)


def test_main_without_config_exits_zero(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    for name in ("HIPPIUS_ROUTING_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name, raising=False)
    assert probe.main() == 0
    assert "skipped" in capsys.readouterr().out


def test_single_part_agreement_passes_and_reports_distribution() -> None:
    obs = [
        probe.KeyObservation("a", "n1", ("n1", "n1"), "n1"),
        probe.KeyObservation("b", "n2", ("n2", "n2"), "n2"),
    ]
    result = probe.evaluate_single_part(obs)
    assert result.passed
    assert "put nodes: n1=1, n2=1" in result.details
    assert result.line().startswith("PASS single-part agreement:")


def test_single_part_agreement_flags_mismatch_missing_header_and_body() -> None:
    obs = [
        probe.KeyObservation("ok", "n1", ("n1",), "n1"),
        probe.KeyObservation("moved", "n1", ("n2", "n1"), "n1"),
        probe.KeyObservation("nohdr", None, ("n1",), "n1"),
        probe.KeyObservation("corrupt", "n1", ("n1",), "n1", body_ok=False),
    ]
    result = probe.evaluate_single_part(obs)
    assert not result.passed
    text = result.line()
    assert "moved put=n1 gets=[n2, n1] head=n1" in text
    assert f"nohdr put={probe.NO_NODE}" in text
    assert "corrupt" in text and "body-mismatch" in text
    assert "ok put=" not in text


def test_colocation_requires_one_non_missing_node() -> None:
    assert probe.evaluate_colocation("mpu", {"create": "n1", "part1": "n1", "complete": "n1"}).passed
    split = probe.evaluate_colocation("mpu", {"create": "n1", "part1": "n2"})
    assert not split.passed and "part1=n2" in split.line()
    missing = probe.evaluate_colocation("mpu", {"create": "n1", "abort": None})
    assert not missing.passed and f"abort -> {probe.NO_NODE}" in missing.line()
    assert not probe.evaluate_colocation("mpu", {}).passed


def test_spread_needs_more_than_one_node_and_no_missing_header() -> None:
    assert probe.evaluate_spread("list", ["n1", "n2", "n1"]).passed
    single = probe.evaluate_spread("list", ["n1", "n1"])
    assert not single.passed and "expected more than one node" in single.line()
    missing = probe.evaluate_spread("list", ["n1", "n2", None])
    assert not missing.passed and f"1 responses with {probe.NO_NODE}" in missing.line()
    assert "n1=1, n2=1, no X-Hippius-Node header=1" in missing.line()


def test_drill_passes_when_all_reads_hit_owner_fast() -> None:
    samples = [probe.DrillSample(200, 0.1, "n2") for _ in range(3)]
    result = probe.evaluate_drill(samples, "n1")
    assert result.passed
    assert "owner=n2, put node=n1 (differs: expected" in result.line()


def test_drill_fails_on_status_ttfb_or_stray_node() -> None:
    samples = [
        probe.DrillSample(200, 0.1, "n2"),
        probe.DrillSample(200, 0.1, "n2"),
        probe.DrillSample(503, 0.1, "n2", "ClientError"),
        probe.DrillSample(200, 6.0, "n2"),
        probe.DrillSample(200, 0.1, "n1"),
        probe.DrillSample(200, 0.1, None),
    ]
    result = probe.evaluate_drill(samples, "n2")
    assert not result.passed
    text = result.line()
    assert "get#2 status=503 ClientError" in text
    assert "get#3 ttfb=6.00s" in text
    assert "get#4 node=n1" in text
    assert f"get#5 node={probe.NO_NODE}" in text
    assert not probe.evaluate_drill([], "n1").passed


def test_sample_from_error_uses_client_error_response() -> None:
    class ClientError(Exception):
        response = parsed("n3", status=503)

    sample = probe.sample_from_error(ClientError("boom"), 1.5)
    assert (sample.status, sample.node, sample.error, sample.ttfb_seconds) == (503, "n3", "ClientError", 1.5)
    sample = probe.sample_from_error(TimeoutError("read timed out"), 9.0)
    assert (sample.status, sample.node) == (0, None)
    assert "TimeoutError: read timed out" in sample.error


def test_exit_code_is_one_if_any_check_failed() -> None:
    ok = probe.CheckResult("a", True)
    bad = probe.CheckResult("b", False)
    assert probe.exit_code([ok, ok]) == 0
    assert probe.exit_code([ok, bad]) == 1
    assert probe.exit_code([]) == 0


def test_cleanup_deletes_by_key_even_when_put_reported_version_id() -> None:
    # The server returns a VersionId on PUT but rejects DeleteObject with it on an unversioned bucket.
    client = FakeClient(["n1"] * 4, {"list_objects_v2": {"Contents": [{"Key": "k"}, {"Key": "j"}]}})
    p = probe.Probe(client, config(), "bkt")
    p.cleanup()
    assert [(op, kw.get("Key"), kw.get("VersionId")) for op, kw in client.calls] == [
        ("list_objects_v2", None, None),
        ("delete_object", "k", None),
        ("delete_object", "j", None),
        ("delete_bucket", None, None),
    ]


def test_cleanup_falls_back_to_plain_keys_without_version_ids() -> None:
    client = FakeClient(["n1"] * 4, {"list_objects_v2": {"Contents": [{"Key": "k"}]}})
    probe.Probe(client, config(), "bkt").cleanup()
    assert [(op, kw.get("Key")) for op, kw in client.calls] == [
        ("list_objects_v2", None),
        ("delete_object", "k"),
        ("delete_bucket", None),
    ]
