#!/usr/bin/env python3
"""
Extract detailed failure information from pytest JSON reports.

This script parses pytest-json-report output files and extracts actionable
failure context for CI alerts, including test names, error messages, and traceback snippets.
"""

import json
import sys
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List


def parse_report(report_path: Path) -> Dict[str, Any]:
    """Parse a single pytest JSON report file."""
    try:
        with open(report_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not parse {report_path}: {e}", file=sys.stderr)
        return {}


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

        # Extract error message and traceback
        crash = failure_info.get("crash", {})
        longrepr = failure_info.get("longrepr", "")

        error_message = crash.get("message", "Unknown error")
        error_file = crash.get("path", "unknown")
        error_line = crash.get("lineno", "?")

        # Create a compact traceback snippet (first few lines)
        traceback_lines = longrepr.split("\n")[:5] if longrepr else []
        traceback_snippet = "\n".join(traceback_lines).strip()

        failures.append(
            {
                "test": nodeid,
                "stage": failure_stage,
                "error": error_message,
                "file": error_file,
                "line": error_line,
                "traceback": traceback_snippet,
                "report": report_name,
            }
        )

    return failures


def format_failures_for_alert(failures: List[Dict[str, str]], max_failures: int = 5) -> str:
    """Format failure details into a compact alert message."""
    if not failures:
        return "No failures found in reports."

    total_failures = len(failures)
    display_failures = failures[:max_failures]

    lines = [f"**Failures: {total_failures}**"]

    for i, failure in enumerate(display_failures, 1):
        lines.append(f"\n{i}. **{failure['test']}** ({failure['report']})")
        lines.append(f"   Stage: {failure['stage']}")
        lines.append(f"   Error: {failure['error']}")
        lines.append(f"   Location: {failure['file']}:{failure['line']}")

        if failure["traceback"]:
            # Add traceback as a code block for readability
            lines.append("```")
            lines.append(failure["traceback"])
            lines.append("```")

    if total_failures > max_failures:
        lines.append(f"\n... and {total_failures - max_failures} more failure(s)")

    return "\n".join(lines)


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
        all_failures = []

        for report_path_str in args.reports:
            report_path = Path(report_path_str)
            report_name = report_path.stem

            report = parse_report(report_path)
            failures = extract_failures(report, report_name)
            all_failures.extend(failures)

        # Sort by report name then test name for consistency
        all_failures.sort(key=lambda f: (f["report"], f["test"]))

        alert_message = format_failures_for_alert(all_failures)
    except Exception as e:  # noqa: BLE001
        print(f"Warning: failed to build enriched alert: {e}", file=sys.stderr)
        alert_message = "Could not extract failure details — see the CI run for logs."

    if args.webhook and args.run_url:
        send_webhook(alert_message, args.webhook, args.run_url)
    else:
        print(alert_message)


def send_webhook(message: str, webhook_url: str, run_url: str):
    """Send alert message to Mattermost webhook."""
    import json as json_module

    payload = {
        "text": f":rotating_light: **s3.hippius.com smoke tests failed**\n[View run]({run_url})\n\n**Failure Details:**\n{message}\n\n![smoke tests failed](https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExcmg4NDIyMnNtMHk0cHpvc2JpcHEwczY4cXR3eDkwdDE4Y3M3bGQ5MiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/HUkOv6BNWc1HO/giphy.gif)"
    }

    import subprocess

    result = subprocess.run(
        [
            "curl",
            "-fsS",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            json_module.dumps(payload),
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
