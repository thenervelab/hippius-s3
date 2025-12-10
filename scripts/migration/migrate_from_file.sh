#!/bin/bash
set -e

# Script to migrate objects listed in a file to storage version 4
# Usage: ./migrate_from_file.sh migration-objects.txt [--dry-run]

if [ $# -lt 1 ]; then
    echo "Usage: $0 <objects-file> [--dry-run]"
    echo ""
    echo "File format (one per line):"
    echo "  bucket_name|object_key"
    echo ""
    echo "Example:"
    echo "  lol|lol.txt"
    echo "  test-bucket|test-file.bin"
    exit 1
fi

OBJECTS_FILE="$1"
DRY_RUN_FLAG=""

if [ $# -eq 2 ] && [ "$2" = "--dry-run" ]; then
    DRY_RUN_FLAG="--dry-run"
    echo "DRY RUN MODE - No actual migration will occur"
    echo ""
fi

if [ ! -f "$OBJECTS_FILE" ]; then
    echo "Error: File '$OBJECTS_FILE' not found"
    exit 1
fi

# Check if we should use virtual environment or system python
if [ -d ".venv" ]; then
    echo "Using virtual environment..."
    source .venv/bin/activate

    # Check if required packages are installed
    if ! python -c "import opentelemetry" 2>/dev/null; then
        echo ""
        echo "WARNING: Virtual environment is missing dependencies!"
        echo "Please run: /usr/local/opt/uv/bin/uv pip install -e ."
        echo ""
        echo "Attempting to use system Python instead..."
        deactivate
        PYTHON_CMD="python3"
    else
        PYTHON_CMD="python"
    fi
else
    echo "No virtual environment found, using system Python..."
    PYTHON_CMD="python3"
fi

# Count objects
TOTAL=$(grep -v '^#' "$OBJECTS_FILE" | grep -v '^[[:space:]]*$' | wc -l)
echo "========================================="
echo "Migration Plan"
echo "========================================="
echo "Objects file: $OBJECTS_FILE"
echo "Total objects: $TOTAL"
echo "Dry run: ${DRY_RUN_FLAG:-no}"
echo "========================================="
echo ""

# Create logs directory
mkdir -p migration_logs

# Process each line
COUNTER=0
SUCCEEDED=0
FAILED=0

while IFS='|' read -r bucket key || [ -n "$bucket" ]; do
    # Skip comments and empty lines
    if [[ "$bucket" =~ ^#.*$ ]] || [[ -z "$bucket" ]]; then
        continue
    fi

    # Trim whitespace
    bucket=$(echo "$bucket" | xargs)
    key=$(echo "$key" | xargs)

    COUNTER=$((COUNTER + 1))

    echo "[$COUNTER/$TOTAL] Migrating: $bucket/$key"

    LOG_FILE="migration_logs/${bucket//\//_}_${key//\//_}_$(date +%Y%m%d_%H%M%S).log"

    if $PYTHON_CMD hippius_s3/scripts/migrate_objects.py \
        --bucket "$bucket" \
        --key "$key" \
        --concurrency 1 \
        $DRY_RUN_FLAG \
        2>&1 | tee "$LOG_FILE"; then

        if [ -z "$DRY_RUN_FLAG" ]; then
            echo "  ✓ SUCCESS"
            SUCCEEDED=$((SUCCEEDED + 1))
        else
            echo "  ✓ WOULD MIGRATE"
        fi
    else
        echo "  ✗ FAILED (see $LOG_FILE)"
        FAILED=$((FAILED + 1))
    fi

    echo ""

done < "$OBJECTS_FILE"

echo "========================================="
echo "Migration Complete"
echo "========================================="
echo "Total: $TOTAL"
echo "Succeeded: $SUCCEEDED"
echo "Failed: $FAILED"
echo "========================================="

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "Failed objects can be found in migration_logs/"
    exit 1
fi
