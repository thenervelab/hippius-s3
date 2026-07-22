#!/usr/bin/env python3
"""inv-det — deterministic pre-deploy invariant proof (WI-19 §4.1, GO/NO-GO item #2).

The pre-flight gate. Runs the checks that must be green BEFORE any dynamic staging run, with no
live S3/cluster:

  1. Rust drain-core suite INCLUDING the #[ignore]d coordinator epoch/lease tests. Those 8 tests
     are the split-brain core and have **0 executed coverage under a bare `cargo test`** — they
     need `--include-ignored` AND a real Redis (`CEPHOR_TEST_REDIS_URL`). Without both, the fence
     that defends G1 is unproven.
  2. The Python unit suite (`tests/unit`), and optionally `tests/integration` (`--integration`,
     needs a live DB/Redis via docker compose).
  3. A static sole-producer audit (G4): the dead PUT-path producer `enqueue_upload`
     (hippius_s3/writer/queue.py) must stay callerless — the drain is the only enqueuer. A new call
     re-introduces a second producer, which the runtime `(chunk_id, backend)` uniqueness invariant
     would only catch after the fact.

Exit code is 0 (all gates green) / 1 (any hard-gate failure or a required check that could not run).
See stress-test/plan.md §4.1 and s3-2.1-todo.md → PRODUCTION-READINESS GATE (WI-19).

Usage:
    export CEPHOR_TEST_REDIS_URL=redis://localhost:6379   # required for the split-brain tests
    python stress-test/inv/inv_det.py
    python stress-test/inv/inv_det.py --integration        # also run tests/integration
    python stress-test/inv/inv_det.py --no-cargo           # python-only (skip the Rust suite)
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import pathlib
import re
import subprocess


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
# `\benqueue_upload(` matches a call to the dead producer but NOT enqueue_upload_to_backends( /
# enqueue_upload_request( (those have `_` after the name, not `(`).
_PRODUCER_CALL = re.compile(r"\benqueue_upload\(")


@dataclasses.dataclass
class Check:
    name: str
    gate: str
    passed: bool
    detail: str
    skipped: bool = False


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)  # noqa: S603


def _cargo_result_lines(out: str) -> str:
    """The libtest `test result: ...` summary line(s) — the signal amid the noise."""
    hits = [ln.strip() for ln in out.splitlines() if ln.strip().startswith("test result:")]
    return " | ".join(hits) if hits else out.strip().splitlines()[-1][:200] if out.strip() else ""


def check_cargo_coordinator(results: list[Check]) -> None:
    redis_url = os.environ.get("CEPHOR_TEST_REDIS_URL")
    if not redis_url:
        # Skipped, but this is NOT a pass: without the real-Redis fence tests, G1 is unproven.
        results.append(Check(
            "cargo drain-core (--include-ignored)", "inv-det #2 / G1 split-brain",
            passed=False, skipped=True,
            detail="CEPHOR_TEST_REDIS_URL unset — the 8 #[ignore]d coordinator epoch/lease tests "
                   "cannot run; export it (e.g. redis://localhost:6379) to close the coverage gap"))
        return
    proc = _run(["cargo", "test", "-p", "hippius-drain-core", "--", "--include-ignored"])
    ok = proc.returncode == 0
    results.append(Check(
        "cargo drain-core (--include-ignored)", "inv-det #2 / G1 split-brain",
        passed=ok,
        detail=_cargo_result_lines(proc.stdout) if ok else _cargo_result_lines(proc.stdout + proc.stderr)))


def check_pytest(results: list[Check], rel_path: str, required: bool) -> None:
    pytest_bin = REPO_ROOT / ".venv" / "bin" / "pytest"
    cmd = [str(pytest_bin) if pytest_bin.exists() else "pytest", rel_path, "-q"]
    proc = _run(cmd)
    ok = proc.returncode == 0
    tail = (proc.stdout or proc.stderr).strip().splitlines()[-1:] or [""]
    results.append(Check(
        f"pytest {rel_path}", "inv-det #2",
        passed=ok if required else True,
        skipped=(not required and not ok),
        detail=tail[0][:200]))


def check_sole_producer(results: list[Check]) -> None:
    """G4 static proof: the drain is the sole upload producer — `enqueue_upload` stays callerless."""
    offenders: list[str] = []
    for base in ("hippius_s3", "gateway"):
        root = REPO_ROOT / base
        if not root.exists():
            continue
        for py in root.rglob("*.py"):
            for i, line in enumerate(py.read_text().splitlines(), 1):
                if _PRODUCER_CALL.search(line) and "def enqueue_upload(" not in line:
                    offenders.append(f"{py.relative_to(REPO_ROOT)}:{i}")
    results.append(Check(
        "sole-producer static audit", "G4",
        passed=not offenders,
        detail="enqueue_upload has no callers (drain is the sole producer)" if not offenders
               else f"UNEXPECTED producer call site(s): {offenders[:5]}"))


def main() -> int:
    ap = argparse.ArgumentParser(description="inv-det — deterministic pre-deploy invariant proof")
    ap.add_argument("--no-cargo", action="store_true", help="skip the Rust drain-core suite")
    ap.add_argument("--integration", action="store_true", help="also run tests/integration (needs DB/Redis)")
    args = ap.parse_args()

    print("== inv-det: deterministic pre-deploy invariant proof ==\n")
    results: list[Check] = []
    check_sole_producer(results)
    check_pytest(results, "tests/unit", required=True)
    if args.integration:
        check_pytest(results, "tests/integration", required=True)
    if not args.no_cargo:
        check_cargo_coordinator(results)

    print(f"\n{'gate':<40} {'status':<8} detail")
    print("-" * 100)
    for r in results:
        status = "SKIP" if r.skipped else ("PASS" if r.passed else "FAIL")
        print(f"{r.name:<40} {status:<8} {r.detail}")

    # A skipped required check (e.g. the fence tests with no Redis) is NOT a pass — it means the gate
    # was not actually proven, so the run is NO-GO until it can run.
    go = all(r.passed and not r.skipped for r in results)
    print(f"\n{'🟢 inv-det GREEN' if go else '🔴 inv-det NOT PROVEN'}")
    return 0 if go else 1


if __name__ == "__main__":
    raise SystemExit(main())
