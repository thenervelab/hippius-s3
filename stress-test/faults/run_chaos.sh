#!/usr/bin/env bash
# WI-19 §4.5 chaos driver — run F1..F8 cells on staging under inv-guard + the durability ledger.
#
#   HIPPIUS_DRAIN_LIVE=1 bash stress-test/faults/run_chaos.sh F1 F2 F3 F4 F5 F6 F7 F8
#
# For each cell: start inv-guard in the background (aborts the run on ANY invariant break), inject
# the fault, hold for the cell's recovery window, then clean up. A cell PASSES iff inv-guard stayed
# green AND the fault's recovery bound held. inv-guard + the ledger are the real oracles; this
# script is the sequencer. Each cell writes run-<cell>-<ts>.jsonl (inv-guard events).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
RESULTS="${RESULTS:-$HERE/../results}"
mkdir -p "$RESULTS"

PY="${PYTHON:-python3}"
TOXI="${TOXI:-http://localhost:8474}"
INV_GUARD="$REPO_ROOT/stress-test/inv/inv_guard.py"
# Per-cell hold seconds (recovery bound + margin). Keep in step with matrix.yaml.
declare -A HOLD=([F1]=420 [F2]=90 [F3]=180 [F4]=660 [F5]=300 [F6]=660 [F7]=180 [F8]=120)

ts() { date +%Y%m%d-%H%M%S; }

if [ "${HIPPIUS_DRAIN_LIVE:-0}" != "1" ]; then
  echo "refusing to run: set HIPPIUS_DRAIN_LIVE=1 to confirm you are pointed at the intended (staging) cluster" >&2
  exit 2
fi

rc=0
for cell in "$@"; do
  echo "### cell $cell"
  run_id="$cell-$(ts)"
  out="$RESULTS/run-$run_id.jsonl"

  # inv-guard is the automated circuit breaker: it aborts (exits non-zero) the instant an invariant
  # breaks. Runs for the whole cell window in the background.
  guard_pid=""
  if [ -f "$INV_GUARD" ]; then
    # --abort makes the guard exit non-zero on the first breach, which is what the kill -0 poll
    # below (and the post-hold wait) key off to fail the cell. Without it the guard loops forever
    # and every cell reports PASS regardless of invariant state.
    "$PY" "$INV_GUARD" --run-id "$run_id" --out "$out" --abort &
    guard_pid=$!
  else
    echo "  WARN: $INV_GUARD not found — running WITHOUT the continuous invariant guard" >&2
  fi

  "$PY" "$HERE/inject.py" apply "$cell" --repo-root "$REPO_ROOT" --toxi "$TOXI" || { rc=1; echo "  inject failed"; }

  hold="${HOLD[$cell]:-180}"
  echo "  holding ${hold}s for recovery window"
  # Poll the guard: if it dies (violation) mid-hold, fail the cell immediately.
  for _ in $(seq 1 "$hold"); do
    if [ -n "$guard_pid" ] && ! kill -0 "$guard_pid" 2>/dev/null; then
      echo "  inv-guard EXITED during $cell — invariant violation, cell FAIL" >&2
      rc=1
      break
    fi
    sleep 1
  done

  "$PY" "$HERE/inject.py" cleanup "$cell" --repo-root "$REPO_ROOT" --toxi "$TOXI" || true

  if [ -n "$guard_pid" ]; then
    if kill -0 "$guard_pid" 2>/dev/null; then
      kill "$guard_pid" 2>/dev/null || true
      wait "$guard_pid" 2>/dev/null || true
      echo "  [$cell] inv-guard stayed green"
    else
      wait "$guard_pid" 2>/dev/null; gexit=$?
      echo "  [$cell] inv-guard exit=$gexit (non-zero ⇒ violation)"
      [ "$gexit" != "0" ] && rc=1
    fi
  fi
  # Belt-and-suspenders: even if the guard's exit status is lost (killed, races), a breach is
  # durably recorded in the JSONL. Grep it so a recorded breach fails the cell independently.
  if [ -f "$out" ] && grep -q '"status": *"breach"' "$out"; then
    echo "  inv-guard BREACH recorded in $cell — cell FAIL" >&2
    rc=1
  fi

  echo "  [$cell] events: $out"
done

echo "=== chaos run ${rc:+done} (rc=$rc; 0=all cells clean) ==="
exit "$rc"
