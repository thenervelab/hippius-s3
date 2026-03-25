#!/bin/bash
set -euo pipefail

# Source AWS CLI environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../.aws.cli.env" ]; then
    source "$SCRIPT_DIR/../.aws.cli.env"
elif [ -f ".aws.cli.env" ]; then
    source ".aws.cli.env"
else
    echo "ERROR: .aws.cli.env not found"
    exit 1
fi

ENDPOINT=""
BUCKET="test-benchmark"
FILE_SIZE_MB="200"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -e|--endpoint) ENDPOINT="$2"; shift 2 ;;
        -b|--bucket) BUCKET="$2"; shift 2 ;;
        -s|--size) FILE_SIZE_MB="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [-e endpoint] [-b bucket] [-s size_mb]"
            echo "  -e, --endpoint  S3 endpoint URL (default: https://s3.hippius.com)"
            echo "  -b, --bucket    Bucket name (default: test-benchmark)"
            echo "  -s, --size      File size in MB (default: 200)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

ENDPOINT="${ENDPOINT:-https://s3.hippius.com}"
TEST_FILE="/tmp/s3-benchmark-${FILE_SIZE_MB}mb.bin"
TEST_KEY="benchmark/test-${FILE_SIZE_MB}mb-$(date +%s).bin"
DOWNLOAD_FILE="/tmp/s3-benchmark-download.bin"

cleanup() { rm -f "$TEST_FILE" "$DOWNLOAD_FILE"; }
trap cleanup EXIT

echo "=== S3 Benchmark ==="
echo "Endpoint: $ENDPOINT"
echo "Bucket:   $BUCKET"
echo "Size:     ${FILE_SIZE_MB} MB"
echo ""

# Create bucket if it doesn't exist
if ! aws s3api head-bucket --bucket "$BUCKET" --endpoint-url "$ENDPOINT" 2>/dev/null; then
    echo "Creating bucket: $BUCKET"
    aws s3 mb "s3://${BUCKET}" --endpoint-url "$ENDPOINT"
fi

# Generate test file
echo "--- Generating ${FILE_SIZE_MB} MB test file ---"
dd if=/dev/urandom of="$TEST_FILE" bs=1048576 count="$FILE_SIZE_MB" 2>&1
echo ""

# Upload (single-part PUT)
echo "--- Upload (single-part PUT) ---"
UPLOAD_START=$(python3 -c "import time; print(time.time())")
aws s3api put-object \
    --bucket "$BUCKET" \
    --key "$TEST_KEY" \
    --body "$TEST_FILE" \
    --endpoint-url "$ENDPOINT" \
    --cli-read-timeout 300 \
    --cli-connect-timeout 30 > /dev/null
UPLOAD_END=$(python3 -c "import time; print(time.time())")
UPLOAD_SEC=$(python3 -c "print(f'{$UPLOAD_END - $UPLOAD_START:.2f}')")
UPLOAD_SPEED=$(python3 -c "print(f'{$FILE_SIZE_MB / ($UPLOAD_END - $UPLOAD_START):.2f}')")
echo "Upload:   ${UPLOAD_SEC}s (${UPLOAD_SPEED} MB/s)"
echo ""

# Download
echo "--- Download ---"
DOWNLOAD_START=$(python3 -c "import time; print(time.time())")
aws s3 cp "s3://${BUCKET}/${TEST_KEY}" "$DOWNLOAD_FILE" \
    --endpoint-url "$ENDPOINT" \
    --no-progress \
    --cli-read-timeout 300 \
    --cli-connect-timeout 30
DOWNLOAD_END=$(python3 -c "import time; print(time.time())")
DOWNLOAD_SEC=$(python3 -c "print(f'{$DOWNLOAD_END - $DOWNLOAD_START:.2f}')")
DOWNLOAD_SPEED=$(python3 -c "print(f'{$FILE_SIZE_MB / ($DOWNLOAD_END - $DOWNLOAD_START):.2f}')")
echo "Download: ${DOWNLOAD_SEC}s (${DOWNLOAD_SPEED} MB/s)"
echo ""

# Verify
echo "--- Verify ---"
if command -v md5sum &>/dev/null; then
    UPLOAD_MD5=$(md5sum "$TEST_FILE" | awk '{print $1}')
    DOWNLOAD_MD5=$(md5sum "$DOWNLOAD_FILE" | awk '{print $1}')
else
    UPLOAD_MD5=$(md5 -q "$TEST_FILE")
    DOWNLOAD_MD5=$(md5 -q "$DOWNLOAD_FILE")
fi
if [ "$UPLOAD_MD5" = "$DOWNLOAD_MD5" ]; then
    echo "Checksum: MATCH ($UPLOAD_MD5)"
else
    echo "Checksum: MISMATCH! upload=$UPLOAD_MD5 download=$DOWNLOAD_MD5"
fi
echo ""

# Cleanup
echo "--- Cleanup ---"
aws s3 rm "s3://${BUCKET}/${TEST_KEY}" --endpoint-url "$ENDPOINT"
echo "Done."

echo ""
echo "=== Results ==="
echo "Upload:   ${UPLOAD_SEC}s (${UPLOAD_SPEED} MB/s)"
echo "Download: ${DOWNLOAD_SEC}s (${DOWNLOAD_SPEED} MB/s)"
