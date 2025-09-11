#!/bin/bash

# Encryption Demo Script
# Tests public vs private bucket encryption by uploading a file to both,
# downloading via S3 with different subaccounts, and comparing MD5 hashes

set -e  # Exit on any error

# Check if file path argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <file_path>"
    echo "Example: $0 ~/Downloads/shawarma.jpeg"
    exit 1
fi

FILE_PATH="$1"
if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File $FILE_PATH not found"
    exit 1
fi

# Check for required environment variables
if [ -z "$HIPPIUS_SEED_PHRASE" ]; then
    echo "❌ Error: HIPPIUS_SEED_PHRASE environment variable not set"
    echo "Please set the primary seed phrase"
    exit 1
fi

if [ -z "$HIPPIUS_SEED_PHRASE_2" ]; then
    echo "❌ Error: HIPPIUS_SEED_PHRASE_2 environment variable not set"
    echo "Please set the second subaccount seed phrase for testing subaccount isolation"
    exit 1
fi

# Set up AWS credentials for first subaccount
export AWS_ACCESS_KEY_ID=$(echo -n "$HIPPIUS_SEED_PHRASE" | base64)
export AWS_SECRET_ACCESS_KEY="$HIPPIUS_SEED_PHRASE"

# Extract filename without path
FILENAME=$(basename "$FILE_PATH")
BASENAME="${FILENAME%.*}"
EXTENSION="${FILENAME##*.}"

# Generate unique bucket names with timestamp
TIMESTAMP=$(date +%s)
PUBLIC_BUCKET="public-test-${TIMESTAMP}"
PRIVATE_BUCKET="private-test-${TIMESTAMP}"

# S3 endpoint configuration
ENDPOINT_URL="https://s3.hippius.com"
GATEWAY_URL="https://get.hippius.network"

echo "🚀 Starting encryption demo with file: $FILENAME"
echo "📦 Public bucket: $PUBLIC_BUCKET"
echo "🔒 Private bucket: $PRIVATE_BUCKET"

# Function to wait for CID availability
wait_for_cid() {
    local bucket="$1"
    local filename="$2"
    local max_attempts=60
    local poll_interval=5

    echo "⏳ Waiting for CID to be available for $filename in bucket $bucket..."

    for attempt in $(seq 1 $max_attempts); do
        echo "  Attempt $attempt/$max_attempts..."

        # Get object listing
        result=$(aws s3api list-objects-v2 \
            --bucket "$bucket" \
            --prefix "$filename" \
            --max-keys 1 \
            --endpoint-url "$ENDPOINT_URL" \
            --output json 2>/dev/null || echo '{}')

        # Check if Contents exists and has our file
        owner_id=$(echo "$result" | jq -r '.Contents[]? | select(.Key == "'"$filename"'") | .Owner.ID // "pending"')

        if [ "$owner_id" != "pending" ] && [ "$owner_id" != "null" ] && [ -n "$owner_id" ]; then
            echo "✅ CID available for $filename: $owner_id"
            echo "$owner_id"
            return 0
        fi

        if [ $attempt -lt $max_attempts ]; then
            sleep $poll_interval
        fi
    done

    echo "❌ Timeout waiting for CID for $filename in bucket $bucket"
    return 1
}

# Create buckets
echo "🏗️  Creating buckets..."
aws s3 mb "s3://$PUBLIC_BUCKET" --endpoint-url "$ENDPOINT_URL"
aws s3 mb "s3://$PRIVATE_BUCKET" --endpoint-url "$ENDPOINT_URL"

# Set public bucket policy
echo "🌐 Setting public bucket policy..."
aws s3api put-bucket-policy \
    --bucket "$PUBLIC_BUCKET" \
    --policy "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [
            {
                \"Effect\": \"Allow\",
                \"Principal\": \"*\",
                \"Action\": \"s3:GetObject\",
                \"Resource\": \"arn:aws:s3:::$PUBLIC_BUCKET/*\"
            }
        ]
    }" \
    --endpoint-url "$ENDPOINT_URL"

# Upload file to both buckets
echo "⬆️  Uploading $FILENAME to public bucket..."
aws s3 cp "$FILE_PATH" "s3://$PUBLIC_BUCKET/" --endpoint-url "$ENDPOINT_URL"

echo "⬆️  Uploading $FILENAME to private bucket..."
aws s3 cp "$FILE_PATH" "s3://$PRIVATE_BUCKET/" --endpoint-url "$ENDPOINT_URL"

# Wait for CIDs to become available
echo "⏳ Waiting for CID processing to complete..."
echo "📋 Getting public CID..."
PUBLIC_CID=$(wait_for_cid "$PUBLIC_BUCKET" "$FILENAME")
echo "📋 Getting private CID..."
PRIVATE_CID=$(wait_for_cid "$PRIVATE_BUCKET" "$FILENAME")

if [ -z "$PUBLIC_CID" ] || [ -z "$PRIVATE_CID" ]; then
    echo "❌ Failed to get CIDs for one or both files"
    exit 1
fi

echo ""
echo "📋 CIDs obtained:"
echo "  Public:  $PUBLIC_CID"
echo "  Private: $PRIVATE_CID"
echo ""

# Create downloads directory
DOWNLOAD_DIR="$(dirname "$FILE_PATH")/encryption_demo_downloads"
mkdir -p "$DOWNLOAD_DIR"

# Download files via S3
echo "⬇️  Downloading via S3..."
aws s3 cp "s3://$PUBLIC_BUCKET/$FILENAME" "$DOWNLOAD_DIR/s3-public-$BASENAME.$EXTENSION" --endpoint-url "$ENDPOINT_URL"
aws s3 cp "s3://$PRIVATE_BUCKET/$FILENAME" "$DOWNLOAD_DIR/s3-private-$BASENAME.$EXTENSION" --endpoint-url "$ENDPOINT_URL"

# Test subaccount isolation with different subaccount
echo ""
echo "🔐 Testing subaccount isolation..."
echo "===================================="
echo "ℹ️  Testing if different subaccounts of the same user can access the same account's private data"

# Save original credentials
ORIGINAL_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
ORIGINAL_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"

# Switch to second subaccount credentials
export AWS_ACCESS_KEY_ID=$(echo -n "$HIPPIUS_SEED_PHRASE_2" | base64)
export AWS_SECRET_ACCESS_KEY="$HIPPIUS_SEED_PHRASE_2"

echo "🔑 Switched to second subaccount (same user, different subaccount)"
echo "⬇️  Attempting downloads with different subaccount..."

# Try to download from public bucket (should work - public bucket)
echo "  Testing public bucket access with different subaccount..."
aws s3 cp "s3://$PUBLIC_BUCKET/$FILENAME" "$DOWNLOAD_DIR/s3-public-subaccount2-$BASENAME.$EXTENSION" --endpoint-url "$ENDPOINT_URL" 2>/dev/null && {
    echo "✅ Public bucket accessible from different subaccount (expected)"
} || {
    echo "❌ Public bucket NOT accessible from different subaccount (unexpected)"
    touch "$DOWNLOAD_DIR/s3-public-subaccount2-$BASENAME.$EXTENSION.failed"
}

# Try to download from private bucket (should work - same account)
echo "  Testing private bucket access with different subaccount..."
aws s3 cp "s3://$PRIVATE_BUCKET/$FILENAME" "$DOWNLOAD_DIR/s3-private-subaccount2-$BASENAME.$EXTENSION" --endpoint-url "$ENDPOINT_URL" 2>/dev/null && {
    echo "✅ Private bucket accessible from different subaccount (same account - expected)"
} || {
    echo "❌ ACCOUNT ACCESS ISSUE: Private bucket NOT accessible from same account's subaccount"
    touch "$DOWNLOAD_DIR/s3-private-subaccount2-$BASENAME.$EXTENSION.failed"
}

# Restore original credentials
export AWS_ACCESS_KEY_ID="$ORIGINAL_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$ORIGINAL_SECRET_KEY"
echo "🔄 Restored original subaccount credentials"

# Calculate and display MD5 hashes
echo ""
echo "🔍 MD5 Hash Comparison:"
echo "===================="

ORIGINAL_MD5=$(md5 "$FILE_PATH" | awk '{print $4}')
S3_PUBLIC_MD5=$(md5 "$DOWNLOAD_DIR/s3-public-$BASENAME.$EXTENSION" | awk '{print $4}')
S3_PRIVATE_MD5=$(md5 "$DOWNLOAD_DIR/s3-private-$BASENAME.$EXTENSION" | awk '{print $4}')

# Handle subaccount 2 downloads
if [ -f "$DOWNLOAD_DIR/s3-public-subaccount2-$BASENAME.$EXTENSION.failed" ]; then
    S3_PUBLIC_SUBACCOUNT2_MD5="FAILED"
else
    S3_PUBLIC_SUBACCOUNT2_MD5=$(md5 "$DOWNLOAD_DIR/s3-public-subaccount2-$BASENAME.$EXTENSION" | awk '{print $4}')
fi

if [ -f "$DOWNLOAD_DIR/s3-private-subaccount2-$BASENAME.$EXTENSION.failed" ]; then
    S3_PRIVATE_SUBACCOUNT2_MD5="FAILED"
else
    S3_PRIVATE_SUBACCOUNT2_MD5=$(md5 "$DOWNLOAD_DIR/s3-private-subaccount2-$BASENAME.$EXTENSION" | awk '{print $4}')
fi

echo "Original file:              $ORIGINAL_MD5"
echo "S3 Public (Subaccount 1):   $S3_PUBLIC_MD5"
echo "S3 Private (Subaccount 1):  $S3_PRIVATE_MD5"
echo "S3 Public (Subaccount 2):   $S3_PUBLIC_SUBACCOUNT2_MD5"
echo "S3 Private (Subaccount 2):  $S3_PRIVATE_SUBACCOUNT2_MD5"
echo ""

# Analyze results
echo "🔎 Analysis:"
echo "============"

# Subaccount 1 (original) analysis
if [ "$ORIGINAL_MD5" = "$S3_PUBLIC_MD5" ]; then
    echo "✅ S3 Public download (Subaccount 1) matches original"
else
    echo "❌ S3 Public download (Subaccount 1) differs from original"
fi

if [ "$ORIGINAL_MD5" = "$S3_PRIVATE_MD5" ]; then
    echo "✅ S3 Private download (Subaccount 1) matches original"
else
    echo "❌ S3 Private download (Subaccount 1) differs from original"
fi

# Subaccount 2 (different subaccount, same account) analysis
if [ "$S3_PUBLIC_SUBACCOUNT2_MD5" = "FAILED" ]; then
    echo "❌ S3 Public download (Subaccount 2) failed"
elif [ "$ORIGINAL_MD5" = "$S3_PUBLIC_SUBACCOUNT2_MD5" ]; then
    echo "✅ S3 Public download (Subaccount 2) matches original (public access working)"
else
    echo "⚠️  S3 Public download (Subaccount 2) differs from original (unexpected)"
fi

if [ "$S3_PRIVATE_SUBACCOUNT2_MD5" = "FAILED" ]; then
    echo "❌ S3 Private download (Subaccount 2) failed (same account access not working)"
elif [ "$ORIGINAL_MD5" = "$S3_PRIVATE_SUBACCOUNT2_MD5" ]; then
    echo "✅ S3 Private download (Subaccount 2) matches original (same account access working)"
else
    echo "⚠️  S3 Private download (Subaccount 2) accessible but corrupted (partial decryption?)"
fi


echo ""
echo "🛡️  Security Summary:"
echo "===================="
if [ "$S3_PRIVATE_SUBACCOUNT2_MD5" = "FAILED" ]; then
    echo "❌ Same account access issue: Private buckets blocked for same account's subaccounts"
else
    echo "✅ Same account access working: Private buckets accessible to same account's subaccounts"
fi

echo ""
echo "📁 Downloaded files are in: $DOWNLOAD_DIR"
echo "🏁 Encryption demo completed!"

# Cleanup option
read -p "🗑️  Delete test buckets? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧹 Cleaning up buckets..."
    aws s3 rm "s3://$PUBLIC_BUCKET" --recursive --endpoint-url "$ENDPOINT_URL"
    aws s3 rb "s3://$PUBLIC_BUCKET" --endpoint-url "$ENDPOINT_URL"
    aws s3 rm "s3://$PRIVATE_BUCKET" --recursive --endpoint-url "$ENDPOINT_URL"
    aws s3 rb "s3://$PRIVATE_BUCKET" --endpoint-url "$ENDPOINT_URL"
    echo "✅ Buckets cleaned up"
fi
