#!/bin/bash
set -e

# Generate list of all objects to migrate from s3-staging database
# Usage: ./generate-migration-list.sh [output-file] [--storage-version N] [--bucket bucket-name]

OUTPUT_FILE="${1:-migration-objects-all.txt}"
STORAGE_VERSION=""
BUCKET_NAME=""

shift
while [[ $# -gt 0 ]]; do
    case $1 in
        --storage-version)
            STORAGE_VERSION="$2"
            shift 2
            ;;
        --bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

echo "========================================="
echo "Generating Migration List"
echo "========================================="
echo "Output file: $OUTPUT_FILE"
echo "Storage version filter: ${STORAGE_VERSION:-all (v1, v2, v3)}"
echo "Bucket filter: ${BUCKET_NAME:-all buckets}"
echo "========================================="
echo ""

# Build WHERE clause for storage version filter
if [ -n "$STORAGE_VERSION" ]; then
    VERSION_FILTER="AND ov.storage_version = $STORAGE_VERSION"
else
    VERSION_FILTER="AND ov.storage_version < 4"
fi

# Build WHERE clause for bucket filter
if [ -n "$BUCKET_NAME" ]; then
    BUCKET_FILTER="AND b.bucket_name = '$BUCKET_NAME'"
else
    BUCKET_FILTER=""
fi

# Generate migration list
docker compose exec -T db psql -U postgres -d hippius -t -A -F'|' -c "
SELECT
    b.bucket_name || '|' || o.object_key as migration_line
FROM object_versions ov
JOIN objects o ON o.object_id = ov.object_id
    AND o.current_object_version = ov.object_version
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE ov.status = 'uploaded'
    $VERSION_FILTER
    $BUCKET_FILTER
    AND (SELECT COUNT(*) FROM parts p
         WHERE p.object_id = o.object_id
         AND p.object_version = ov.object_version) > 0
ORDER BY b.bucket_name, o.object_key;
" | grep -v '^$' > "$OUTPUT_FILE"

# Count total objects
TOTAL=$(wc -l < "$OUTPUT_FILE" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

echo "Generated migration list: $OUTPUT_FILE"
echo "Total objects to migrate: $TOTAL"
echo ""
echo "Next steps:"
echo "1. Review the list: head -20 $OUTPUT_FILE"
echo "2. Run migration: ./migrate-wrapper.sh $OUTPUT_FILE"
echo ""
