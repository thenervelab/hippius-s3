#!/usr/bin/env bash
# System health check for hippius-s3 Kubernetes clusters
# Usage: ./scripts/system-check.sh [--namespace hippius-s3-prod] [--since 1h]

set -euo pipefail

# Defaults
NAMESPACE="hippius-s3-prod"
SINCE="1h"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace|-n) NAMESPACE="$2"; shift 2 ;;
        --since|-s) SINCE="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--namespace hippius-s3-prod|hippius-s3-staging] [--since 1h]"
            echo "  --namespace, -n   Kubernetes namespace (default: hippius-s3-prod)"
            echo "  --since, -s       Time window for log errors, e.g. 1h, 30m, 2h (default: 1h)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
header() { echo -e "\n${BOLD}${CYAN}── $1 ──${NC}"; }

ISSUES=0

# ── Pod Overview ──
header "Pod Status ($NAMESPACE)"
NOT_RUNNING=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$3 != "Running" && $3 != "Completed" {print $1, $2, $3}')
TOTAL=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')
RUNNING=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$3 == "Running"' | wc -l | tr -d ' ')
echo -e "  Pods: ${BOLD}${RUNNING}/${TOTAL} running${NC}"

if [[ -n "$NOT_RUNNING" ]]; then
    warn "Non-running pods:"
    echo "$NOT_RUNNING" | while read -r line; do echo "      $line"; done
else
    pass "All pods running"
fi

# ── Pod Restarts ──
header "Pod Restarts (> 0)"
RESTARTS=$(kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns='NAME:.metadata.name,RESTARTS:.status.containerStatuses[*].restartCount' 2>/dev/null | awk '$2 != "0" && $2 != "<none>" {print $1, $2}')
if [[ -n "$RESTARTS" ]]; then
    warn "Pods with restarts:"
    echo "$RESTARTS" | while read -r name count; do echo "      $name  restarts=$count"; done
else
    pass "No pod restarts"
fi

# ── CrashLoopBackOff / Error Detection ──
header "CrashLoop / Error Pods"
CRASH_PODS=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | awk '$3 == "CrashLoopBackOff" || $3 == "Error" || $3 == "ImagePullBackOff" {print $1, $3}')
if [[ -n "$CRASH_PODS" ]]; then
    fail "Crashing pods detected:"
    echo "$CRASH_PODS" | while read -r line; do echo "      $line"; done
    ISSUES=$((ISSUES + 1))
else
    pass "No crashing pods"
fi

# ── Redis Cluster Health ──
header "Redis Cluster"
CLUSTER_STATE=$(kubectl exec -n "$NAMESPACE" redis-cluster-0 -- redis-cli cluster info 2>/dev/null | grep cluster_state | tr -d '\r')
SLOTS_OK=$(kubectl exec -n "$NAMESPACE" redis-cluster-0 -- redis-cli cluster info 2>/dev/null | grep cluster_slots_ok | tr -d '\r')
SLOTS_FAIL=$(kubectl exec -n "$NAMESPACE" redis-cluster-0 -- redis-cli cluster info 2>/dev/null | grep cluster_slots_fail | tr -d '\r')
KNOWN_NODES=$(kubectl exec -n "$NAMESPACE" redis-cluster-0 -- redis-cli cluster info 2>/dev/null | grep cluster_known_nodes | tr -d '\r')

if echo "$CLUSTER_STATE" | grep -q "ok"; then
    pass "Cluster state: ok"
else
    fail "Cluster state: $(echo "$CLUSTER_STATE" | cut -d: -f2)"
    ISSUES=$((ISSUES + 1))
fi
echo "  $SLOTS_OK"
echo "  $SLOTS_FAIL"
echo "  $KNOWN_NODES"

# Node distribution
echo -e "  ${BOLD}Node spread:${NC}"
kubectl get pods -n "$NAMESPACE" -l app=redis-cluster -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName' --no-headers 2>/dev/null | while read -r pod node; do
    echo "      $pod → $node"
done

# ── Standalone Redis Instances ──
header "Redis Standalone Instances"
for redis in redis-accounts redis-chain redis-queues redis-rate-limiting redis-acl; do
    PONG=$(kubectl exec -n "$NAMESPACE" "${redis}-0" -- redis-cli ping 2>/dev/null || echo "FAIL")
    if [[ "$PONG" == *"PONG"* ]]; then
        pass "$redis: PONG"
    else
        fail "$redis: unreachable"
        ISSUES=$((ISSUES + 1))
    fi
done

# ── PostgreSQL (CNPG) ──
header "PostgreSQL Cluster"
PG_PODS=$(kubectl get pods -n "$NAMESPACE" -l cnpg.io/cluster=postgres --no-headers -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[*].ready,ROLE:.metadata.labels.cnpg\.io/instanceRole,NODE:.spec.nodeName' 2>/dev/null)
if [[ -n "$PG_PODS" ]]; then
    PRIMARY_COUNT=$(echo "$PG_PODS" | grep -c "primary" || true)
    REPLICA_COUNT=$(echo "$PG_PODS" | grep -c "replica" || true)
    if [[ "$PRIMARY_COUNT" -eq 1 ]]; then
        pass "Primary: 1"
    else
        fail "Primary count: $PRIMARY_COUNT (expected 1)"
        ISSUES=$((ISSUES + 1))
    fi
    pass "Replicas: $REPLICA_COUNT"
    echo "$PG_PODS" | while read -r line; do echo "      $line"; done

    # Quick connectivity check via primary
    PRIMARY_POD=$(echo "$PG_PODS" | awk '/primary/{print $1}')
    if [[ -n "$PRIMARY_POD" ]]; then
        PG_UP=$(kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -c postgres -- pg_isready -q 2>/dev/null && echo "yes" || echo "no")
        if [[ "$PG_UP" == "yes" ]]; then
            pass "Primary accepting connections"
        else
            fail "Primary not accepting connections"
            ISSUES=$((ISSUES + 1))
        fi

        # Replication lag
        LAG=$(kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -c postgres -- psql -U postgres -d hippius -t -A -c \
            "SELECT client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn, pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes FROM pg_stat_replication;" 2>/dev/null || echo "")
        if [[ -n "$LAG" ]]; then
            echo -e "  ${BOLD}Replication status:${NC}"
            echo "$LAG" | while IFS='|' read -r addr state sent write flush replay lag; do
                lag_mb=$(echo "scale=2; ${lag:-0} / 1048576" | bc 2>/dev/null || echo "?")
                if (( $(echo "${lag:-0} > 104857600" | bc 2>/dev/null || echo 0) )); then
                    warn "  $addr  state=$state  lag=${lag_mb}MB"
                else
                    pass "  $addr  state=$state  lag=${lag_mb}MB"
                fi
            done
        fi
    fi
else
    warn "No CNPG pods found"
fi

# ── OTel Collector ──
header "OpenTelemetry Collector"
OTEL_POD=$(kubectl get pods -n "$NAMESPACE" -l app=otel-collector --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
if [[ -z "$OTEL_POD" ]]; then
    OTEL_POD=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep otel-collector | awk '{print $1}' | head -1)
fi

if [[ -n "$OTEL_POD" ]]; then
    OTEL_STATUS=$(kubectl get pod -n "$NAMESPACE" "$OTEL_POD" --no-headers -o custom-columns='STATUS:.status.phase' 2>/dev/null)
    if [[ "$OTEL_STATUS" == "Running" ]]; then
        pass "OTel collector running: $OTEL_POD"
    else
        fail "OTel collector status: $OTEL_STATUS"
        ISSUES=$((ISSUES + 1))
    fi

    # Check for Tempo export errors in recent logs
    TEMPO_ERRORS=$(kubectl logs -n "$NAMESPACE" "$OTEL_POD" --since="$SINCE" 2>/dev/null | grep -c "Exporting failed" || true)
    if [[ "$TEMPO_ERRORS" -gt 0 ]]; then
        warn "Tempo export failures in last $SINCE: $TEMPO_ERRORS"
    else
        pass "No Tempo export failures in last $SINCE"
    fi
else
    warn "OTel collector pod not found"
fi

# ── Tempo ──
header "Tempo"
TEMPO_POD=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=tempo --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
if [[ -z "$TEMPO_POD" ]]; then
    TEMPO_POD=$(kubectl get pods -n monitoring --no-headers 2>/dev/null | grep tempo | awk '{print $1}' | head -1)
fi

if [[ -n "$TEMPO_POD" ]]; then
    TEMPO_STATUS=$(kubectl get pod -n monitoring "$TEMPO_POD" --no-headers -o custom-columns='STATUS:.status.phase,READY:.status.containerStatuses[0].ready' 2>/dev/null)
    pass "Tempo: $TEMPO_POD ($TEMPO_STATUS)"
else
    warn "Tempo pod not found in monitoring namespace"
fi

# ── Recent Warning Events ──
header "Warning Events (last $SINCE)"
WARNINGS=$(kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' --field-selector type=Warning 2>/dev/null | tail -10)
if [[ -n "$WARNINGS" && "$WARNINGS" != *"No resources found"* ]]; then
    warn "Recent warnings:"
    echo "$WARNINGS" | head -1
    echo "$WARNINGS" | tail -n +2 | while read -r line; do echo "      $line"; done
else
    pass "No warning events"
fi

# ── Pod Error Logs ──
header "Error Logs (last $SINCE)"
CORE_PODS="api gateway"
for APP in $CORE_PODS; do
    PODS=$(kubectl get pods -n "$NAMESPACE" -l app="$APP" --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -2)
    for POD in $PODS; do
        ERROR_COUNT=$(kubectl logs -n "$NAMESPACE" "$POD" --since="$SINCE" 2>/dev/null | grep -ciE '"level":\s*"(error|critical)"' || true)
        if [[ "$ERROR_COUNT" -gt 0 ]]; then
            warn "$POD: $ERROR_COUNT error(s)"
        else
            pass "$POD: no errors"
        fi
    done
done

# Worker error check
WORKER_PODS=$(kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | grep -E '(uploader|downloader|unpinner)')
for POD in $WORKER_PODS; do
    ERROR_COUNT=$(kubectl logs -n "$NAMESPACE" "$POD" --since="$SINCE" 2>/dev/null | grep -ciE '"level":\s*"(error|critical)"' || true)
    if [[ "$ERROR_COUNT" -gt 0 ]]; then
        warn "$POD: $ERROR_COUNT error(s)"
    fi
done

# ── Summary ──
header "Summary"
if [[ "$ISSUES" -eq 0 ]]; then
    echo -e "  ${GREEN}${BOLD}All checks passed.${NC} Namespace: $NAMESPACE, Window: $SINCE"
else
    echo -e "  ${RED}${BOLD}$ISSUES issue(s) detected.${NC} Namespace: $NAMESPACE, Window: $SINCE"
fi
echo ""
