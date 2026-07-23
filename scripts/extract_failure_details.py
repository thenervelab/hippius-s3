#!/usr/bin/env python3
"""
Extract detailed failure information from pytest JSON reports.

This script parses pytest-json-report output files and builds a human-readable
CI alert: which test failed, against which suite/region, what the error means
in plain language, and where in *our* code it happened. Suite-level breakage
(missing report, collection error, pytest crash) is reported explicitly so the
alert never claims "no failures" while the workflow is red.
"""

import json
import re
import sys
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


# Report-file stem -> what a reader should understand failed, without opening the run.
SUITE_LABELS = {
    "smoke-production": "core S3 suite (s3.hippius.com)",
    "smoke-regional": "regional cache suite",
    "smoke-subtoken-scope": "sub-token scope suite",
}

STAGE_LABELS = {
    "call": "while running the test",
    "setup": "while preparing the test (setup)",
    "teardown": "during cleanup (teardown)",
}

# Plain-language explanations for error classes that show up in smoke runs.
# Matched against the exception type in the crash message ("httpx.ReadTimeout: ...").
ERROR_EXPLANATIONS = [
    (
        "ReadTimeout",
        "The endpoint accepted the connection but did not respond before the client "
        "timeout — the service is likely slow, overloaded, or hung.",
    ),
    (
        "ConnectTimeout",
        "Could not establish a connection to the endpoint in time — it may be down or unreachable.",
    ),
    (
        "ConnectError",
        "The connection to the endpoint failed or was refused — the service may be down.",
    ),
    (
        "ConnectionError",
        "The connection to the endpoint failed or was refused — the service may be down.",
    ),
    (
        "RemoteProtocolError",
        "The server closed the connection mid-response — a crash or restart on the server side.",
    ),
    (
        "SSLError",
        "TLS handshake with the endpoint failed — check certificates on the target.",
    ),
    (
        "AssertionError",
        "The response did not match what the test expected — see the traceback below.",
    ),
]

PYTEST_EXIT_MEANINGS = {
    1: "some tests failed",
    2: "the test run was interrupted",
    3: "pytest itself crashed (internal error)",
    4: "pytest was invoked incorrectly (usage error)",
    5: "no tests were collected",
}


def parse_report(report_path: Path) -> Dict[str, Any]:
    """Parse a single pytest JSON report file."""
    try:
        with open(report_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not parse {report_path}: {e}", file=sys.stderr)
        return {}


def explain_error(error_message: str) -> str:
    """Turn a raw exception message into an explanation a reader can act on."""
    exc_type = error_message.split(":", 1)[0]
    for needle, explanation in ERROR_EXPLANATIONS:
        if needle in exc_type:
            return f"{explanation} (`{error_message.splitlines()[0].strip()}`)"
    return f"`{error_message.splitlines()[0].strip()}`"


def find_test_frame(longrepr: str) -> Optional[str]:
    """Find the first traceback frame that is in our code, not a library."""
    for line in longrepr.split("\n"):
        match = re.match(r"^(\S+\.py):(\d+):", line)
        if match and "site-packages" not in match.group(1):
            return f"{match.group(1)}:{match.group(2)}"
    return None


def clean_traceback(longrepr: str, max_lines: int = 10) -> str:
    """Keep only frames from our code and the final error lines; drop library noise."""
    kept: List[str] = []
    in_library_frame = False
    for line in longrepr.split("\n"):
        if re.match(r"^\[gw\d+\]", line):
            continue
        frame_header = re.match(r"^(\S+):\d+: in ", line)
        if frame_header:
            in_library_frame = "site-packages" in frame_header.group(1)
            if in_library_frame:
                continue
        elif in_library_frame and (line.startswith("    ") or not line.strip()):
            continue
        else:
            in_library_frame = False
        kept.append(line)
    result = "\n".join(kept).strip()
    lines = result.split("\n")
    if len(lines) > max_lines:
        result = "\n".join(lines[:max_lines]) + "\n... (truncated)"
    return result


def _as_str(value: Any) -> str:
    """pytest-json-report usually gives longrepr as a string, but not for every failure shape."""
    return value if isinstance(value, str) else ""


def extract_failures(report: Dict[str, Any], report_name: str) -> List[Dict[str, str]]:
    """Extract failure details from a parsed report."""
    failures = []

    if not report or "tests" not in report:
        return failures

    for test in report["tests"]:
        outcome = test.get("outcome")
        if outcome not in ("failed", "error"):
            continue

        nodeid = test.get("nodeid", "unknown")

        # Determine where the failure occurred (call, setup, or teardown)
        failure_stage = None
        failure_info = None

        for stage in ["call", "setup", "teardown"]:
            if stage in test and test[stage].get("outcome") in ("failed", "error"):
                failure_stage = stage
                failure_info = test[stage]
                break

        if not failure_info:
            continue

        crash = failure_info.get("crash") or {}
        if not isinstance(crash, dict):
            crash = {}
        longrepr = _as_str(failure_info.get("longrepr"))

        error_message = crash.get("message") or ""
        if not error_message:
            # No structured crash info — fall back to the final "E ..." line of the traceback.
            error_lines = [ln[1:].strip() for ln in longrepr.split("\n") if ln.startswith("E ")]
            error_message = error_lines[-1] if error_lines else "Unknown error"

        # The crash location often points into httpx/botocore internals; the frame
        # in our own test file is what a reader actually needs.
        location = find_test_frame(longrepr)
        if not location:
            crash_path = str(crash.get("path", ""))
            if crash_path and "site-packages" not in crash_path:
                location = f"{crash_path}:{crash.get('lineno', '?')}"

        failures.append(
            {
                "test": nodeid,
                "stage": failure_stage or "unknown",
                "explanation": explain_error(error_message),
                "location": location or "",
                "traceback": clean_traceback(longrepr) if longrepr else "",
                "report": report_name,
            }
        )

    return failures


def extract_suite_problems(report: Dict[str, Any], report_name: str, test_failures: int) -> List[str]:
    """Report suite-level breakage that never shows up as an individual test failure."""
    label = SUITE_LABELS.get(report_name, report_name)
    problems = []

    for collector in report.get("collectors", []):
        if collector.get("outcome") != "error":
            continue
        nodeid = collector.get("nodeid", "unknown")
        snippet = clean_traceback(_as_str(collector.get("longrepr")), max_lines=5)
        problem = f"**{label}**: could not even collect `{nodeid}` — an import or fixture error, not a service issue."
        if snippet:
            problem += f"\n```\n{snippet}\n```"
        problems.append(problem)

    exitcode = report.get("exitcode")
    # A non-zero exit with no per-test failures and no collection errors means the
    # run broke without producing per-test results (interrupted, internal error, ...).
    if test_failures == 0 and not problems and isinstance(exitcode, int) and exitcode != 0:
        meaning = PYTEST_EXIT_MEANINGS.get(exitcode, "unknown exit code")
        problems.append(
            f"**{label}**: pytest exited with code {exitcode} ({meaning}) "
            f"but no individual test failure was recorded — check the run logs."
        )

    return problems


def format_failures_for_alert(failures: List[Dict[str, str]], max_failures: int = 5) -> str:
    """Format failure details into a readable alert message."""
    if not failures:
        return ""

    total_failures = len(failures)
    display_failures = failures[:max_failures]

    lines = [f"**{total_failures} test(s) failed**"]

    for i, failure in enumerate(display_failures, 1):
        suite = SUITE_LABELS.get(failure["report"], failure["report"])
        stage = STAGE_LABELS.get(failure["stage"], failure["stage"])
        lines.append(f"\n**{i}. `{failure['test']}`** — {suite}")
        lines.append(f"   What happened: {failure['explanation']}")
        lines.append(f"   Failed {stage}.")

        if failure["location"]:
            lines.append(f"   Test code: `{failure['location']}`")

        if failure["traceback"]:
            lines.append("```")
            lines.append(failure["traceback"])
            lines.append("```")

    if total_failures > max_failures:
        lines.append(f"\n... and {total_failures - max_failures} more failure(s) — see the run for the full list.")

    return "\n".join(lines)


def build_alert_message(report_paths: List[Path]) -> str:
    """Build the full alert body from all report files, covering suite-level breakage."""
    all_failures: List[Dict[str, str]] = []
    suite_problems: List[str] = []

    for report_path in report_paths:
        report_name = report_path.stem
        label = SUITE_LABELS.get(report_name, report_name)

        if not report_path.exists():
            suite_problems.append(
                f"**{label}**: no report file was written — the pytest step likely crashed "
                f"before any test ran (dependency install failure, startup error, or the job "
                f"was cut short). Check the run logs."
            )
            continue

        report = parse_report(report_path)
        if not report:
            suite_problems.append(
                f"**{label}**: the report file exists but could not be parsed — the pytest "
                f"step was probably killed mid-run. Check the run logs."
            )
            continue

        failures = extract_failures(report, report_name)
        all_failures.extend(failures)
        suite_problems.extend(extract_suite_problems(report, report_name, len(failures)))

    # Sort by report name then test name for consistency
    all_failures.sort(key=lambda f: (f["report"], f["test"]))

    sections = []
    failures_text = format_failures_for_alert(all_failures)
    if failures_text:
        sections.append(failures_text)
    if suite_problems:
        sections.append("**Suite-level problems:**\n" + "\n".join(f"- {p}" for p in suite_problems))
    if not sections:
        return (
            "The workflow failed but no test failures were captured in the reports. "
            "The failure is likely in a non-test step (dependency install, artifact upload) "
            "or the job was cancelled — check the run logs."
        )

    return "\n\n".join(sections)


def build_payload(message: str, run_url: str) -> Dict[str, str]:
    """Build the Mattermost webhook payload."""
    return {"text": (f":rotating_light: **s3.hippius.com smoke tests failed**\n[View run]({run_url})\n\n{message}")}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract failure details from pytest JSON reports")
    parser.add_argument("reports", nargs="+", help="JSON report files to parse")
    parser.add_argument("--webhook", help="Mattermost webhook URL to send alert to")
    parser.add_argument("--run-url", help="GitHub Actions run URL to include in alert")

    args = parser.parse_args()

    # Enrichment must never silence the alert: if parsing an unexpected report
    # shape raises, fall back to a bare message so the failure still reaches the team.
    try:
        alert_message = build_alert_message([Path(p) for p in args.reports])
    except Exception as e:  # noqa: BLE001
        print(f"Warning: failed to build enriched alert: {e}", file=sys.stderr)
        alert_message = "Could not extract failure details — see the CI run for logs."

    if args.webhook and args.run_url:
        send_webhook(alert_message, args.webhook, args.run_url)
    else:
        print(alert_message)


def send_webhook(message: str, webhook_url: str, run_url: str):
    """Send alert message to Mattermost webhook."""
    import subprocess

    payload = build_payload(message, run_url)

    result = subprocess.run(
        [
            "curl",
            "-fsS",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            json.dumps(payload),
            webhook_url,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Webhook failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    print("Webhook sent successfully")


if __name__ == "__main__":
    main()
