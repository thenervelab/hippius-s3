#!/usr/bin/env bash
# WI-19 §4.6 Rig A driver — run allocator-under-pressure scenarios A–F against the
# docker-compose.alloc-stress.yml fleet, each under monitor.py, gated by gate.py.
#
#   docker compose -f stress-test/compose/docker-compose.alloc-stress.yml up -d --build
#   bash stress-test/alloc-stress/run_scenario.sh A B C D E F
#
# Fault injectors use the toxiproxy control API (:8475), redis-cli (:6390), and `docker kill`
# on the alloc-N containers. Each scenario writes results/alloc-<S>-<ts>.jsonl and prints PASS/FAIL.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="${RESULTS:-$HERE/../results}"
mkdir -p "$RESULTS"

TOXI="${TOXI:-http://localhost:8475}"
REDIS_CLI=(redis-cli -u "${REDIS_URL:-redis://localhost:6390}")
PY="${PYTHON:-python3}"
INTERVAL="${INTERVAL:-0.25}"
CEILING="${CEILING:-10000000000}"

ts() { date +%Y%m%d-%H%M%S; }

# --- toxiproxy helpers (redis_queues proxy fronting the coordinator Redis) ---
toxic_add() { # name type json-attrs
  curl -fsS -X POST "$TOXI/proxies/redis_queues/toxics" \
    -d "{\"name\":\"$1\",\"type\":\"$2\",\"attributes\":$3}" >/dev/null
}
toxic_clear() {
  curl -fsS "$TOXI/proxies/redis_queues/toxics" | "$PY" -c \
    'import sys,json,urllib.request;[urllib.request.urlopen(urllib.request.Request("'"$TOXI"'/proxies/redis_queues/toxics/"+t["name"],method="DELETE")) for t in json.load(sys.stdin)]' 2>/dev/null || true
}
proxy_enabled() { # true|false
  curl -fsS -X POST "$TOXI/proxies/redis_queues" -d "{\"enabled\":$1}" >/dev/null
}

leader_container() {
  # cephor:leader -> {"instance":"alloc-N",...}; map instance id to its compose container.
  local inst
  inst="$("${REDIS_CLI[@]}" get cephor:leader 2>/dev/null | "$PY" -c 'import sys,json;
raw=sys.stdin.read().strip()
print(json.loads(raw).get("instance","") if raw else "")' 2>/dev/null || true)"
  [ -n "$inst" ] && docker ps --filter "label=com.docker.compose.service=$inst" -q | head -1
}

run_monitor() { # scenario duration -> echoes jsonl path
  local s="$1" dur="$2" out="$RESULTS/alloc-$s-$(ts).jsonl"
  "$PY" "$HERE/monitor.py" --redis "${REDIS_URL:-redis://localhost:6390}" \
    --out "$out" --interval "$INTERVAL" --duration "$dur" &
  MON_PID=$!
  echo "$out"
}

scenario_A() { # N-node contention, no fault
  local out; out="$(run_monitor A 60)"; sleep 62; wait "$MON_PID" || true
  "$PY" "$HERE/gate.py" --scenario A "$out" --interval "$INTERVAL"
}

scenario_B() { # forced leader churn: kill the leader every ~30s for ~5min
  local out; out="$(run_monitor B 300)"
  for _ in $(seq 1 9); do
    sleep 30
    local c; c="$(leader_container || true)"
    [ -n "$c" ] && { echo "  killing leader container $c"; docker kill "$c" >/dev/null 2>&1 || true; docker start "$c" >/dev/null 2>&1 || true; }
  done
  wait "$MON_PID" || true
  "$PY" "$HERE/gate.py" --scenario B "$out" --interval "$INTERVAL" --max-gap-s 10
}

scenario_C() { # R3 gap approximation: latency during handover (deterministic version = Rig B)
  local out; out="$(run_monitor C 120)"
  sleep 20; toxic_add slow_lat latency '{"latency":1200,"jitter":300}'
  sleep 20; local c; c="$(leader_container || true)"; [ -n "$c" ] && { docker kill "$c" >/dev/null 2>&1 || true; docker start "$c" >/dev/null 2>&1 || true; }
  sleep 20; toxic_clear
  wait "$MON_PID" || true
  echo "  NOTE: deterministic R3 Tier-2 lives in Rig B (cargo test alloc_stress); this is the compose approximation."
  "$PY" "$HERE/gate.py" --scenario C "$out" --interval "$INTERVAL" --stale-window 15 --ceiling "$CEILING"
}

scenario_D() { # redis pathology: latency -> timeout(hang) -> reset_peer, then clear
  local out; out="$(run_monitor D 150)"
  sleep 15; toxic_add lat latency '{"latency":600,"jitter":200}'
  sleep 25; toxic_clear; toxic_add hang timeout '{"timeout":0}'
  sleep 25; toxic_clear; toxic_add rst reset_peer '{"timeout":0}'
  sleep 25; toxic_clear
  wait "$MON_PID" || true
  "$PY" "$HERE/gate.py" --scenario D "$out" --interval "$INTERVAL" --recovery-ticks 60
}

scenario_E() { # budget fairness/ceiling under steady contention
  local out; out="$(run_monitor E 60)"; sleep 62; wait "$MON_PID" || true
  "$PY" "$HERE/gate.py" --scenario E "$out" --ceiling "$CEILING"
}

scenario_F() { # sustained Redis outage: disable proxy, then restore
  local out; out="$(run_monitor F 150)"
  sleep 20; echo "  disabling redis_queues proxy (sustained outage)"; proxy_enabled false
  sleep 60; echo "  restoring redis_queues proxy"; proxy_enabled true
  sleep 60
  wait "$MON_PID" || true
  "$PY" "$HERE/gate.py" --scenario F "$out" --interval "$INTERVAL"
}

rc=0
for s in "$@"; do
  echo "### scenario $s"
  toxic_clear; proxy_enabled true
  case "$s" in
    A) scenario_A || rc=1 ;;
    B) scenario_B || rc=1 ;;
    C) scenario_C || rc=1 ;;
    D) scenario_D || rc=1 ;;
    E) scenario_E || rc=1 ;;
    F) scenario_F || rc=1 ;;
    *) echo "unknown scenario: $s (want A-F)"; rc=2 ;;
  esac
  toxic_clear; proxy_enabled true
done
exit "$rc"
