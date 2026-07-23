import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "extract_failure_details.py"
spec = importlib.util.spec_from_file_location("extract_failure_details", SCRIPT_PATH)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

HTTPX_LONGREPR = (
    "[gw0] linux -- Python 3.12.13 /opt/hostedtoolcache/Python/3.12.13/x64/bin/python\n"
    "tests/smoke/test_smoke_regional.py:142: in test_regional_presigned_url_roundtrip\n"
    "    resp = httpx.get(presigned_url, timeout=30)\n"
    "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/httpx/_api.py:198: in get\n"
    "    return request(\n"
    "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/httpx/_transports/default.py:101: "
    "in map_httpcore_exceptions\n"
    "    yield\n"
    "E   httpx.ReadTimeout: The read operation timed out"
)


def make_test(nodeid: str, longrepr, message: str | None, crash_path: str) -> dict:
    call: dict = {"outcome": "failed", "longrepr": longrepr}
    if message is not None:
        call["crash"] = {"path": crash_path, "lineno": 118, "message": message}
    return {"nodeid": nodeid, "outcome": "failed", "call": call}


def make_report(*tests: dict, exitcode: int = 1, collectors: list | None = None) -> dict:
    report = {"exitcode": exitcode, "tests": list(tests)}
    if collectors is not None:
        report["collectors"] = collectors
    return report


TIMEOUT_TEST = make_test(
    "smoke/test_smoke_regional.py::test_regional_presigned_url_roundtrip[eu-central-1]",
    HTTPX_LONGREPR,
    "httpx.ReadTimeout: The read operation timed out",
    "/opt/hostedtoolcache/Python/3.12.13/x64/lib/python3.12/site-packages/httpx/_transports/default.py",
)


def write_report(tmp_path: Path, name: str, report: dict) -> Path:
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps(report))
    return path


def test_read_timeout_is_explained_in_plain_language():
    failures = mod.extract_failures(make_report(TIMEOUT_TEST), "smoke-regional")
    message = mod.format_failures_for_alert(failures)

    assert "did not respond before the client timeout" in message
    assert "regional cache suite" in message
    assert "test_regional_presigned_url_roundtrip[eu-central-1]" in message
    assert "while running the test" in message


def test_location_points_at_test_code_not_library_internals():
    failures = mod.extract_failures(make_report(TIMEOUT_TEST), "smoke-regional")
    message = mod.format_failures_for_alert(failures)

    assert "tests/smoke/test_smoke_regional.py:142" in message
    assert "site-packages" not in message
    assert "[gw0]" not in message


def test_assertion_error_keeps_the_failing_comparison():
    test = make_test(
        "smoke/test_smoke_production.py::test_put_get_roundtrip",
        (
            "tests/smoke/test_smoke_production.py:88: in test_put_get_roundtrip\n"
            "    assert body == payload\n"
            "E   AssertionError: assert b'abc' == b'abd'"
        ),
        "AssertionError: assert b'abc' == b'abd'",
        "tests/smoke/test_smoke_production.py",
    )
    failures = mod.extract_failures(make_report(test), "smoke-production")
    message = mod.format_failures_for_alert(failures)

    assert "did not match what the test expected" in message
    assert "assert b'abc' == b'abd'" in message


def test_missing_crash_info_falls_back_to_traceback_error_line():
    test = make_test("smoke/test_x.py::test_y", "tests/smoke/test_x.py:10: in test_y\nE   RuntimeError: boom", None, "")
    failures = mod.extract_failures(make_report(test), "smoke-production")

    assert "RuntimeError: boom" in failures[0]["explanation"]


def test_non_string_longrepr_does_not_crash():
    test = make_test("smoke/test_x.py::test_y", {"weird": "shape"}, "SomeError: x", "tests/smoke/test_x.py")
    failures = mod.extract_failures(make_report(test), "smoke-production")

    assert failures[0]["traceback"] == ""


def test_unknown_error_falls_back_to_raw_message():
    assert "SomeWeirdError: boom" in mod.explain_error("SomeWeirdError: boom")


def test_missing_report_file_is_reported_not_silently_ignored(tmp_path):
    message = mod.build_alert_message([tmp_path / "smoke-regional.json"])

    assert "no report file was written" in message
    assert "regional cache suite" in message
    assert "No failures found" not in message


def test_unparseable_report_file_is_reported(tmp_path):
    path = tmp_path / "smoke-production.json"
    path.write_text("{truncated")
    message = mod.build_alert_message([path])

    assert "could not be parsed" in message
    assert "killed mid-run" in message


def test_collection_error_is_surfaced(tmp_path):
    report = make_report(
        exitcode=2,
        collectors=[
            {
                "nodeid": "smoke/test_smoke_production.py",
                "outcome": "error",
                "longrepr": "ImportError: cannot import name 'boto3'",
            }
        ],
    )
    message = mod.build_alert_message([write_report(tmp_path, "smoke-production", report)])

    assert "could not even collect" in message
    assert "ImportError" in message


def test_nonzero_exit_without_failures_is_surfaced(tmp_path):
    report = make_report(exitcode=5)
    message = mod.build_alert_message([write_report(tmp_path, "smoke-subtoken-scope", report)])

    assert "no tests were collected" in message
    assert "sub-token scope suite" in message


def test_interrupted_run_with_recorded_failures_shows_only_the_failures(tmp_path):
    report = make_report(TIMEOUT_TEST, exitcode=2)
    message = mod.build_alert_message([write_report(tmp_path, "smoke-regional", report)])

    assert "did not respond before the client timeout" in message
    assert "Suite-level problems" not in message


def test_all_reports_clean_still_explains_the_red_workflow(tmp_path):
    message = mod.build_alert_message([write_report(tmp_path, "smoke-production", make_report(exitcode=0))])

    assert "no test failures were captured" in message
    assert "check the run logs" in message


def test_webhook_payload_has_no_gif():
    payload = mod.build_payload("2 test(s) failed", "https://github.com/x/y/actions/runs/1")
    assert "giphy" not in payload["text"]
    assert "![" not in payload["text"]
    assert "View run" in payload["text"]
    assert "s3.hippius.com smoke tests failed" in payload["text"]
