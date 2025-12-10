#!/bin/bash

# Monitor migration progress
# Usage: ./monitor-migration.sh [state-file-prefix]

STATE_PREFIX="${1:-migration-objects-all.txt}"

echo "========================================="
echo "Migration Progress Monitor"
echo "========================================="
echo ""

# Check if state files exist
CHECKPOINT_FILE="migration_state/${STATE_PREFIX}.checkpoint"
FAILED_FILE="migration_state/${STATE_PREFIX}.failed"
SUCCESS_FILE="migration_state/${STATE_PREFIX}.success"

if [ ! -f "$CHECKPOINT_FILE" ]; then
    echo "No migration in progress for: $STATE_PREFIX"
    echo ""
    echo "Available migrations:"
    ls -1 migration_state/*.checkpoint 2>/dev/null | sed 's|migration_state/||' | sed 's|.checkpoint||' || echo "  None"
    exit 0
fi

# Read state
PROCESSED=$(cat "$CHECKPOINT_FILE" 2>/dev/null || echo "0")
SUCCEEDED=$(wc -l < "$SUCCESS_FILE" 2>/dev/null | xargs)
FAILED=$(wc -l < "$FAILED_FILE" 2>/dev/null | xargs)
TOTAL=$(wc -l < "${STATE_PREFIX}" 2>/dev/null | xargs)

# Calculate percentages
if [ $TOTAL -gt 0 ]; then
    PERCENT_DONE=$((PROCESSED * 100 / TOTAL))
    PERCENT_SUCCESS=$((SUCCEEDED * 100 / TOTAL))
    PERCENT_FAILED=$((FAILED * 100 / TOTAL))
else
    PERCENT_DONE=0
    PERCENT_SUCCESS=0
    PERCENT_FAILED=0
fi

echo "Progress: $PROCESSED / $TOTAL ($PERCENT_DONE%)"
echo "Succeeded: $SUCCEEDED ($PERCENT_SUCCESS%)"
echo "Failed: $FAILED ($PERCENT_FAILED%)"
echo ""

# Show rate
if [ -f "$CHECKPOINT_FILE" ]; then
    # Linux uses stat -c, macOS uses stat -f
    if stat -c %Y "$CHECKPOINT_FILE" >/dev/null 2>&1; then
        CHECKPOINT_MTIME=$(stat -c %Y "$CHECKPOINT_FILE")
    else
        CHECKPOINT_MTIME=$(stat -f %m "$CHECKPOINT_FILE")
    fi
    CHECKPOINT_AGE=$(( $(date +%s) - CHECKPOINT_MTIME ))

    if [ $CHECKPOINT_AGE -lt 300 ]; then
        echo "Status: Running (last update ${CHECKPOINT_AGE}s ago)"
    else
        echo "Status: Stalled (last update ${CHECKPOINT_AGE}s ago)"
    fi
else
    echo "Status: Unknown"
fi
echo ""

# Recent logs
echo "Recent activity (last 5 objects):"
ls -t migration_logs/migration_*.log 2>/dev/null | head -5 | while read log; do
    STATUS="UNKNOWN"
    if grep -q "DONE migrate" "$log" 2>/dev/null; then
        STATUS="✓ SUCCESS"
    elif grep -q "FAILED migrate" "$log" 2>/dev/null; then
        STATUS="✗ FAILED"
    fi
    TIMESTAMP=$(basename "$log" | sed 's/migration_//' | sed 's/_/ /' | sed 's/\.log//')
    echo "  $TIMESTAMP - $STATUS"
done
echo ""

# Database stats
echo "Database storage version distribution:"
docker compose exec -T db psql -U postgres -d hippius -c "
SELECT storage_version, COUNT(*) as count
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
  AND o.current_object_version = ov.object_version
GROUP BY storage_version
ORDER BY storage_version;
" 2>/dev/null | grep -v "^$"
echo ""

# Estimate time remaining
if [ $PROCESSED -gt 0 ] && [ -f "$SUCCESS_FILE" ]; then
    # Calculate rate from successful migrations only
    OLDEST_SUCCESS=$(head -1 "$SUCCESS_FILE")
    NEWEST_SUCCESS=$(tail -1 "$SUCCESS_FILE")

    if [ -n "$OLDEST_SUCCESS" ] && [ $SUCCEEDED -gt 10 ]; then
        # Simple estimate: assume linear rate
        REMAINING=$((TOTAL - PROCESSED))
        AVG_RATE=$(echo "scale=2; $SUCCEEDED / $PROCESSED" | bc -l 2>/dev/null || echo "1")

        if [ -f "migration.out" ]; then
            # Linux uses stat -c, macOS uses stat -f
            if stat -c %Y "migration.out" >/dev/null 2>&1; then
                START_TIME=$(stat -c %Y "migration.out")
            else
                START_TIME=$(stat -f %m "migration.out")
            fi
            ELAPSED=$(($(date +%s) - START_TIME))
            if [ $ELAPSED -gt 60 ] && [ $PROCESSED -gt 0 ]; then
                RATE=$(echo "scale=2; $PROCESSED / $ELAPSED" | bc -l 2>/dev/null || echo "1")
                ETA_SECONDS=$(echo "scale=0; $REMAINING / $RATE" | bc -l 2>/dev/null || echo "0")
                ETA_HOURS=$((ETA_SECONDS / 3600))
                ETA_MINS=$(((ETA_SECONDS % 3600) / 60))

                echo "Estimated time remaining: ${ETA_HOURS}h ${ETA_MINS}m"
                echo "Current rate: $(echo "scale=1; $RATE * 60" | bc -l) objects/min"
            fi
        fi
    fi
fi

echo ""
echo "To tail logs: tail -f migration.out"
echo "To retry failed: ./migrate-wrapper.sh $FAILED_FILE"
