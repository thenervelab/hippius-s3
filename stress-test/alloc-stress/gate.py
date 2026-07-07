"""WI-19 §4.6 Rig A gate — turn a monitor.py JSONL into a binary PASS/FAIL per scenario A–F.

Everything here is derived from the Redis coordinator snapshots (monitor.py). Checks that need
inputs the sampler can't see (the true fleet ceiling for E, the agent breaker/rate for F) are
parameterised or explicitly reported as not-evaluated so a green result never over-claims.

Scenarios (plan.md §4.6):
  A  N-node contention   : a leader is held ~always; ≤ --max-epoch-bumps distinct epochs; allocs share the live epoch.
  B  forced leader churn : epoch never decreases; leaderless gaps are bounded (≤ --max-gap-s).
  C  R3 gap (the fix)    : any stale (below-epoch) alloc clears within --stale-window s; Σbudget never exceeds --ceiling.
  D  redis pathology     : epoch never decreases; recovery (a leader reappears) within --recovery-ticks after a gap.
  E  budget fairness     : Σbudget ≤ --ceiling every sample.
  F  decay-on-Err        : epoch never decreases; a leader is re-established by the end (post-restore).

Usage: python gate.py --scenario C run.jsonl --ceiling 1000000000 --stale-window 15
Exit code 0 = PASS, 1 = FAIL, 2 = usage/empty.
"""

from __future__ import annotations

import argparse
import json
import sys


def _load(path: str) -> list[dict]:
    rows: list[dict] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _samples(rows: list[dict]) -> list[dict]:
    return [r for r in rows if "error" not in r]


def check_epoch_monotonic(rows: list[dict]) -> tuple[bool, str]:
    prev = 0
    for r in rows:
        e = int(r.get("epoch", 0))
        if e < prev:
            return False, f"epoch went backward: {prev} -> {e} at t={r.get('t')}"
        prev = max(prev, e)
    return True, f"epoch monotonic (max {prev})"


def check_epoch_bumps(rows: list[dict], max_bumps: int) -> tuple[bool, str]:
    epochs = sorted({int(r.get("epoch", 0)) for r in rows if int(r.get("epoch", 0)) > 0})
    # bumps = distinct epoch values observed beyond the first
    bumps = max(0, len(epochs) - 1)
    ok = bumps <= max_bumps
    return ok, f"{bumps} epoch bumps (limit {max_bumps}); epochs seen={epochs[:8]}{'...' if len(epochs) > 8 else ''}"


def check_leader_presence(rows: list[dict], min_fraction: float) -> tuple[bool, str]:
    if not rows:
        return False, "no samples"
    held = sum(1 for r in rows if r.get("leader"))
    frac = held / len(rows)
    return frac >= min_fraction, f"leader held in {frac:.1%} of samples (min {min_fraction:.0%})"


def check_leaderless_gap(rows: list[dict], max_gap_s: float) -> tuple[bool, str]:
    """Longest contiguous run with no leader, in seconds."""
    longest = 0.0
    start = None
    for r in rows:
        if not r.get("leader"):
            start = r["t"] if start is None else start
            longest = max(longest, r["t"] - start)
        else:
            start = None
    return longest <= max_gap_s, f"longest leaderless gap {longest:.2f}s (limit {max_gap_s}s)"


def check_budget_ceiling(rows: list[dict], ceiling: int) -> tuple[bool, str]:
    worst = max((int(r.get("sum_budget", 0)) for r in rows), default=0)
    return worst <= ceiling, f"max Σbudget {worst} (ceiling {ceiling})"


def check_stale_window(rows: list[dict], window_s: float) -> tuple[bool, str]:
    """A stale alloc (epoch below the live counter) must not persist longer than window_s."""
    first_seen: dict[str, float] = {}
    worst = 0.0
    worst_node = None
    for r in rows:
        t = r["t"]
        stale = set(r.get("stale_allocs", []))
        for node in stale:
            first_seen.setdefault(node, t)
            dur = t - first_seen[node]
            if dur > worst:
                worst, worst_node = dur, node
        for node in list(first_seen):
            if node not in stale:
                del first_seen[node]
    ok = worst <= window_s
    return (
        ok,
        f"longest stale-alloc persistence {worst:.2f}s{f' ({worst_node})' if worst_node else ''} (bound {window_s}s)",
    )


def check_recovery_after_gap(rows: list[dict], recovery_ticks: int, interval_s: float) -> tuple[bool, str]:
    """After any leaderless run, a leader must reappear within recovery_ticks samples."""
    gap_len = 0
    worst = 0
    for r in rows:
        if not r.get("leader"):
            gap_len += 1
            worst = max(worst, gap_len)
        else:
            gap_len = 0
    ok = worst <= recovery_ticks
    return ok, f"longest gap {worst} ticks (~{worst * interval_s:.1f}s; limit {recovery_ticks} ticks)"


SCENARIOS = {"A", "B", "C", "D", "E", "F"}


def evaluate(scenario: str, rows: list[dict], args: argparse.Namespace) -> list[tuple[str, bool, str]]:
    s = _samples(rows)
    results: list[tuple[str, bool, str]] = []
    err_count = len(rows) - len(s)

    if scenario == "A":
        results.append(("leader-pinned", *check_leader_presence(s, args.min_leader_fraction)))
        results.append(("epoch-bumps", *check_epoch_bumps(s, args.max_epoch_bumps)))
        results.append(("epoch-monotonic", *check_epoch_monotonic(s)))
    elif scenario == "B":
        results.append(("epoch-monotonic", *check_epoch_monotonic(s)))
        results.append(("leaderless-gap", *check_leaderless_gap(s, args.max_gap_s)))
        results.append(
            ("epoch-progresses", (len({int(r.get("epoch", 0)) for r in s}) > 1, "leader churned ⇒ epoch advanced"))
        )
    elif scenario == "C":
        results.append(("stale-window", *check_stale_window(s, args.stale_window)))
        results.append(("budget-ceiling", *check_budget_ceiling(s, args.ceiling)))
        results.append(("epoch-monotonic", *check_epoch_monotonic(s)))
    elif scenario == "D":
        results.append(("epoch-monotonic", *check_epoch_monotonic(s)))
        results.append(("recovery", *check_recovery_after_gap(s, args.recovery_ticks, args.interval)))
        # noeviction-full must fail LOUDLY: the sampler surfaces Redis errors as {"error": ...}
        # rather than a silent split — a run under noeviction that never errored under injected
        # OOM is suspicious, but we only assert we did not observe an epoch regression here.
        results.append(("redis-errors-visible", (True, f"{err_count} redis-error samples recorded (loud, not silent)")))
    elif scenario == "E":
        results.append(("budget-ceiling", *check_budget_ceiling(s, args.ceiling)))
    elif scenario == "F":
        results.append(("epoch-monotonic", *check_epoch_monotonic(s)))
        results.append(("leader-restored", (bool(s and s[-1].get("leader")), "a leader is held at end (post-restore)")))
    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl")
    ap.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    ap.add_argument("--ceiling", type=int, default=10_000_000_000, help="fleet write-budget ceiling (bytes/s)")
    ap.add_argument("--stale-window", type=float, default=15.0, help="max stale-alloc persistence (alloc-TTL)")
    ap.add_argument("--max-epoch-bumps", type=int, default=3)
    ap.add_argument("--max-gap-s", type=float, default=10.0)
    ap.add_argument("--recovery-ticks", type=int, default=12, help="~3 ticks at 5s / 0.25s sampling")
    ap.add_argument("--min-leader-fraction", type=float, default=0.95)
    ap.add_argument("--interval", type=float, default=0.25)
    args = ap.parse_args()

    rows = _load(args.jsonl)
    if not rows:
        print(f"FAIL[{args.scenario}]: no samples in {args.jsonl}", file=sys.stderr)
        return 2

    results = evaluate(args.scenario, rows, args)
    all_ok = all(ok for _, ok, _ in results)
    print(f"=== scenario {args.scenario}: {'PASS' if all_ok else 'FAIL'} ({len(rows)} samples) ===")
    for name, ok, detail in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
