#!/usr/bin/env python3
"""inv-guard — the continuous background invariant asserter (WI-19 §4.2, the backbone).

Every dynamic run (load, soak, chaos) runs under this: it polls the drain-internal invariants
(single-leader+epoch, replication-gate coverage, stalled-drain, sole-producer, terminal
monotonicity — see `guards.py`) against staging Postgres + Prometheus on an interval, streams one
JSONL event per guard-check, and — with `--abort` — stops the moment any invariant BREACHES so the
run fails fast instead of soaking on a broken cluster. The JSONL stream is the shared contract the
chaos `gate.py` and the durability ledger consume.

JSONL event shape (one per guard per poll):
    {"run_id": str, "seq": int, "ts": float, "guard": "G1".."G9", "status": "ok"|"breach"|"skip",
     "detail": str, "value": float|null}

Usage (from the repo root):
    python stress-test/inv/inv_guard.py --guards G1,G2,G3,G4,G6 --run-id run-XYZ --out run-XYZ.jsonl
    python stress-test/inv/inv_guard.py --once            # one poll, print the guard table, exit
    python stress-test/inv/inv_guard.py --abort           # exit non-zero on the first breach
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import pathlib
import sys
import time

# allow `python stress-test/inv/inv_guard.py` from the repo root (mirrors run.py)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from harness import config  # noqa: E402
from harness.probes import ClusterProbe  # noqa: E402
from inv.guards import GuardResult, make_guard, run_once  # noqa: E402

DEFAULT_GUARDS = "G1,G2,G3,G4,G6,G9"


def _event(run_id: str, seq: int, result: GuardResult) -> dict:
    return {
        "run_id": run_id,
        "seq": seq,
        "ts": time.time(),
        "guard": result.guard,
        "status": result.status,
        "detail": result.detail,
        "value": result.value,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="inv-guard — continuous invariant asserter")
    ap.add_argument("--guards", default=DEFAULT_GUARDS, help="comma list of G1..G9 (default: G1,G2,G3,G4,G6,G9)")
    ap.add_argument("--run-id", default=f"invguard-{int(time.time())}")
    ap.add_argument("--out", type=pathlib.Path, help="append JSONL events here (default: stdout only)")
    ap.add_argument("--interval", type=float, default=10.0, help="seconds between polls")
    ap.add_argument("--once", action="store_true", help="one poll then exit")
    ap.add_argument("--abort", action="store_true", help="exit non-zero on the first breach")
    ap.add_argument("--stall-secs", type=int, default=120, help="G3 stalled-part threshold")
    ap.add_argument("--orphan-bound", type=int, default=100, help="G9 aged-pending-orphan backlog ceiling")
    ap.add_argument("--orphan-rise-window", type=int, default=5, help="G9 poll window for the rising-trend check")
    ap.add_argument("--orphan-rise-delta", type=float, default=5.0, help="G9 net backlog rise over the window that breaches")
    ap.add_argument("--env-file", type=pathlib.Path, default=None)
    args = ap.parse_args()

    names = [g.strip() for g in args.guards.split(",") if g.strip()]
    try:
        guards = [make_guard(name) for name in names]
    except KeyError as exc:
        print(f"unknown guard {exc}; valid: G1..G9", file=sys.stderr)
        return 2
    for guard in guards:  # thread per-guard thresholds through without a global
        if getattr(guard, "name", None) == "G3":
            guard._stall_secs = args.stall_secs  # noqa: SLF001
        if getattr(guard, "name", None) == "G9":
            guard._bound = args.orphan_bound  # noqa: SLF001
            guard._rise_window = args.orphan_rise_window  # noqa: SLF001
            guard._rise_delta = args.orphan_rise_delta  # noqa: SLF001

    cfg = config.load(args.env_file)
    probe = ClusterProbe(cfg)
    if not probe.available():
        print("cluster (staging Postgres via kubectl) unreachable — inv-guard needs it", file=sys.stderr)
        return 2

    out = args.out.open("a") if args.out else None
    seq = 0
    total_breaches = 0

    def emit(result: GuardResult) -> None:
        nonlocal seq
        line = json.dumps(_event(args.run_id, seq, result))
        seq += 1
        if out:
            out.write(line + "\n")
            out.flush()
        marker = {"ok": "  ", "skip": "· ", "breach": "!!"}.get(result.status, "  ")
        print(f"{marker} {result.guard:<3} {result.status:<6} {result.detail}")

    try:
        # Prometheus needs a port-forward; the pg probes go straight through kubectl exec, so a
        # failed forward only degrades the prom-based guards (G1/G2) to `skip`, never the pg ones.
        with probe.prometheus() as prom_ok:
            if not prom_ok:
                print("(prometheus port-forward failed — G1/G2 will skip)", file=sys.stderr)
            while True:
                print(f"\n== inv-guard poll (run_id={args.run_id}) ==")
                breaches = run_once(guards, probe, emit)
                total_breaches += breaches
                if breaches and args.abort:
                    print(f"\n🔴 inv-guard ABORT: {breaches} invariant breach(es)", file=sys.stderr)
                    return 1
                if args.once:
                    break
                time.sleep(args.interval)
    finally:
        if out:
            out.close()

    print(f"\n{'🔴' if total_breaches else '🟢'} inv-guard: {total_breaches} breach(es) over {seq} check(s)")
    return 1 if total_breaches else 0


if __name__ == "__main__":
    raise SystemExit(main())
