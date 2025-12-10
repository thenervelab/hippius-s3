#!/bin/bash

# Wrapper script to run migration from inside Docker container
# Usage: ./migrate-wrapper.sh <objects-file> [--dry-run] [--continue] [--concurrency N]

# Handle Ctrl+C gracefully
trap 'echo ""; echo "Killing current migration..."; kill $MIGRATION_PID 2>/dev/null; wait $MIGRATION_PID 2>/dev/null; echo "Migration interrupted! Progress saved in checkpoint."; echo "Resume with: ./migrate-wrapper.sh migration-remaining.txt --continue"; exit 130' INT TERM

if [ $# -lt 1 ]; then
    echo "Usage: $0 <objects-file> [--dry-run] [--continue] [--concurrency N]"
    echo ""
    echo "File format (one per line):"
    echo "  bucket_name|object_key"
    echo ""
    echo "Options:"
    echo "  --dry-run         Show what would be migrated without actual migration"
    echo "  --continue        Resume from last checkpoint (skips already migrated)"
    echo "  --concurrency N   Number of parts to process in parallel per object (default: 5)"
    echo ""
    echo "Example:"
    echo "  ./migrate-wrapper.sh migration-objects-all.txt"
    echo "  ./migrate-wrapper.sh migration-objects-all.txt --dry-run"
    echo "  ./migrate-wrapper.sh migration-objects-all.txt --continue"
    echo "  ./migrate-wrapper.sh migration-objects-all.txt --concurrency 10"
    exit 1
fi

OBJECTS_FILE="$1"
DRY_RUN_FLAG=""
CONTINUE_MODE=false
CONCURRENCY=5

# Parse flags
shift
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)
            DRY_RUN_FLAG="--dry-run"
            shift
            ;;
        --continue)
            CONTINUE_MODE=true
            shift
            ;;
        --concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

if [ ! -f "$OBJECTS_FILE" ]; then
    echo "Error: File '$OBJECTS_FILE' not found"
    exit 1
fi

if [ -n "$DRY_RUN_FLAG" ] && [ "$DRY_RUN_FLAG" = "--dry-run" ]; then
    echo "DRY RUN MODE - No actual migration will occur"
    echo ""
fi

# Count objects
TOTAL=$(grep -v '^#' "$OBJECTS_FILE" | grep -v '^[[:space:]]*$' | wc -l | xargs)

echo "========================================="
echo "Migration Plan (Running Inside Docker)"
echo "========================================="
echo "Objects file: $OBJECTS_FILE"
echo "Total objects: $TOTAL"
echo "Concurrency: $CONCURRENCY parts/object"
echo "Dry run: ${DRY_RUN_FLAG:-no}"
echo "========================================="
echo ""

# Create directories
mkdir -p migration_logs
mkdir -p migration_state

# State files
CHECKPOINT_FILE="migration_state/${OBJECTS_FILE##*/}.checkpoint"
FAILED_FILE="migration_state/${OBJECTS_FILE##*/}.failed"
SUCCESS_FILE="migration_state/${OBJECTS_FILE##*/}.success"

# Initialize or load state
if [ "$CONTINUE_MODE" = true ] && [ -f "$CHECKPOINT_FILE" ]; then
    COUNTER=$(cat "$CHECKPOINT_FILE")
    echo "Resuming from object $COUNTER"
    echo ""
else
    COUNTER=0
    # Clear old state files
    > "$FAILED_FILE"
    > "$SUCCESS_FILE"
fi

# Process each line
SUCCEEDED=0
FAILED=0

while IFS='|' read -r bucket key; do
    # Skip comments and empty lines
    [[ "$bucket" =~ ^#.*$ ]] && continue
    [[ -z "$bucket" ]] && continue

    # Trim whitespace
    bucket=$(echo "$bucket" | xargs)
    key=$(echo "$key" | xargs)

    COUNTER=$((COUNTER + 1))

    # Skip if in continue mode and already processed
    if [ "$CONTINUE_MODE" = true ] && [ -f "$CHECKPOINT_FILE" ]; then
        LAST_PROCESSED=$(cat "$CHECKPOINT_FILE")
        if [ $COUNTER -le $LAST_PROCESSED ]; then
            continue
        fi
    fi

    echo "[$COUNTER/$TOTAL] Migrating: $bucket/$key"

    LOG_FILE="migration_logs/migration_${COUNTER}_$(date +%Y%m%d_%H%M%S).log"

    # Run migration inside Docker API container with timeout
    # Redirect stdin to /dev/null to prevent consuming the while loop's input
    # Timeout after 5 seconds to prevent stuck migrations
    # Run in background to allow signal handling
    timeout 5 docker compose exec -T api python hippius_s3/scripts/migrate_objects.py \
        --bucket "$bucket" \
        --key "$key" \
        --concurrency "$CONCURRENCY" \
        $DRY_RUN_FLAG \
        < /dev/null > "$LOG_FILE" 2>&1 &

    MIGRATION_PID=$!
    wait $MIGRATION_PID
    EXIT_CODE=$?

    # Show the log output
    cat "$LOG_FILE"

    if [ -z "$DRY_RUN_FLAG" ]; then
        # Check for timeout (exit code 124)
        if [ $EXIT_CODE -eq 124 ]; then
            echo "  ⏱ TIMEOUT (exceeded 5 seconds)"
            FAILED=$((FAILED + 1))
            echo "$bucket|$key" >> "$FAILED_FILE"
        # Check log for actual success/failure
        elif grep -q "FAILED migrate" "$LOG_FILE"; then
            echo "  ✗ FAILED"
            FAILED=$((FAILED + 1))
            echo "$bucket|$key" >> "$FAILED_FILE"
        elif grep -q "DONE migrate" "$LOG_FILE"; then
            echo "  ✓ SUCCESS"
            SUCCEEDED=$((SUCCEEDED + 1))
            echo "$bucket|$key" >> "$SUCCESS_FILE"
        else
            echo "  ? UNKNOWN (check $LOG_FILE)"
            FAILED=$((FAILED + 1))
            echo "$bucket|$key" >> "$FAILED_FILE"
        fi
    else
        echo "  ✓ DRY RUN"
    fi

    # Update checkpoint
    echo "$COUNTER" > "$CHECKPOINT_FILE"

    echo ""

done < "$OBJECTS_FILE"

echo "========================================="
echo "Migration Complete"
echo "========================================="
echo "Total: $TOTAL"
echo "Succeeded: $SUCCEEDED"
echo "Failed: $FAILED"
echo "========================================="

if [ -z "$DRY_RUN_FLAG" ]; then
    echo ""
    echo "State files:"
    echo "  Checkpoint: $CHECKPOINT_FILE"
    echo "  Succeeded: $SUCCESS_FILE ($(wc -l < "$SUCCESS_FILE" | xargs) objects)"
    echo "  Failed: $FAILED_FILE ($(wc -l < "$FAILED_FILE" | xargs) objects)"
    echo ""

    if [ $FAILED -gt 0 ]; then
        echo "To retry failed objects:"
        echo "  ./migrate-wrapper.sh $FAILED_FILE"
        echo ""
        echo "Failed objects can also be found in migration_logs/"
        exit 1
    else
        echo "All objects migrated successfully!"
    fi
fi
