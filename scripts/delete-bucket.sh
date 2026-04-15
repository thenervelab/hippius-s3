#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Author: Hippius
#
# WARNING: This script permanently deletes data from S3 buckets.
# Deleted objects CANNOT be recovered. This action is IRREVERSIBLE.
#
# Hippius provides this script as-is, without warranty of any kind.
# You are solely responsible for any data loss resulting from using or
# modifying this script. Use at your own risk.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────
ENDPOINT_URL="${AWS_ENDPOINT_URL:-https://s3.hippius.com}"
BUCKET_FLAG=""

# ─── Usage ───────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Delete all objects from an S3 bucket.

Options:
  --bucket BUCKET   Skip interactive selection and delete this bucket directly
  --endpoint URL    S3-compatible endpoint URL (default: https://s3.hippius.com)
  -h, --help        Show this help message

Examples:
  $(basename "$0")
  $(basename "$0") --bucket my-bucket
  $(basename "$0") --endpoint https://s3.example.com --bucket my-bucket
EOF
    exit 0
}

# ─── Parse arguments ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket)
            BUCKET_FLAG="$2"
            shift 2
            ;;
        --endpoint)
            ENDPOINT_URL="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# ─── Build common aws args ──────────────────────────────────────────────────
AWS_ARGS=(--endpoint-url "$ENDPOINT_URL")

# ─── Check for AWS CLI ──────────────────────────────────────────────────────
if ! command -v aws &>/dev/null; then
    echo "Error: AWS CLI is not installed."
    echo "Install it first: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    exit 1
fi

# ─── Check for required env vars ────────────────────────────────────────────
missing=()
[[ -z "${AWS_ACCESS_KEY_ID:-}" ]] && missing+=("AWS_ACCESS_KEY_ID")
[[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]] && missing+=("AWS_SECRET_ACCESS_KEY")
[[ -z "${AWS_DEFAULT_REGION:-}" ]] && missing+=("AWS_DEFAULT_REGION")

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: The following environment variables are not set:"
    for var in "${missing[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Set them before running this script, e.g.:"
    echo "  export AWS_ACCESS_KEY_ID=your_access_key"
    echo "  export AWS_SECRET_ACCESS_KEY=your_secret_key"
    echo "  export AWS_DEFAULT_REGION=decentralized"
    exit 1
fi

# ─── Select bucket ──────────────────────────────────────────────────────────
if [[ -n "$BUCKET_FLAG" ]]; then
    BUCKET="$BUCKET_FLAG"
    if ! aws s3api head-bucket --bucket "$BUCKET" "${AWS_ARGS[@]}" 2>/dev/null; then
        echo "Error: Couldn't find bucket '${BUCKET}'"
        exit 1
    fi
else
    echo "Fetching bucket list..."
    BUCKET_LIST=$(aws s3api list-buckets "${AWS_ARGS[@]}" --query "Buckets[].Name" --output json 2>/dev/null || true)

    if [[ -z "$BUCKET_LIST" || "$BUCKET_LIST" == "null" || "$BUCKET_LIST" == "[]" ]]; then
        echo "No buckets found."
        exit 0
    fi

    BUCKETS=()
    while read -r name; do
        [[ -n "$name" ]] && BUCKETS+=("$name")
    done <<< "$(echo "$BUCKET_LIST" | tr -d '[]",' | xargs -n1)"

    if [[ ${#BUCKETS[@]} -eq 0 ]]; then
        echo "No buckets found."
        exit 0
    fi

    echo ""
    echo "Found ${#BUCKETS[@]} bucket(s):"
    echo ""
    for i in "${!BUCKETS[@]}"; do
        printf "  %3d) %s\n" $((i + 1)) "${BUCKETS[$i]}"
    done
    echo ""
    read -rp "Select a bucket (1-${#BUCKETS[@]}): " selection

    if ! [[ "$selection" =~ ^[0-9]+$ ]] || [[ $selection -lt 1 || $selection -gt ${#BUCKETS[@]} ]]; then
        echo "Invalid selection."
        exit 1
    fi

    BUCKET="${BUCKETS[$((selection - 1))]}"
fi

# ─── Show bucket stats ──────────────────────────────────────────────────────
echo ""
echo "Inspecting s3://${BUCKET} ..."
stats=$(aws s3 ls "s3://${BUCKET}/" --recursive --summarize "${AWS_ARGS[@]}" 2>/dev/null | tail -2)
total_objects=$(echo "$stats" | grep "Total Objects:" | awk '{print $3}')
total_size=$(echo "$stats" | grep "Total Size:" | awk '{print $3}')

if [[ -n "$total_objects" && "$total_objects" != "0" ]]; then
    # Human-readable size
    if [[ "$total_size" -ge 1073741824 ]]; then
        hr_size=$(awk "BEGIN {printf \"%.2f GB\", $total_size/1073741824}")
    elif [[ "$total_size" -ge 1048576 ]]; then
        hr_size=$(awk "BEGIN {printf \"%.2f MB\", $total_size/1048576}")
    elif [[ "$total_size" -ge 1024 ]]; then
        hr_size=$(awk "BEGIN {printf \"%.2f KB\", $total_size/1024}")
    else
        hr_size="${total_size} bytes"
    fi
    echo "  Objects: ${total_objects}"
    echo "  Total size: ${hr_size}"
else
    echo "  Bucket is empty."
fi

# ─── Confirm and delete ──────────────────────────────────────────────────────
echo ""
echo "This will permanently delete bucket s3://${BUCKET} and ALL objects inside it."
echo "Hint: You can run 'aws s3 ls s3://${BUCKET}/ --recursive --endpoint-url ${ENDPOINT_URL}' to list all objects first."
echo ""
read -rp "Are you sure? (y/N): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

aws s3 rb "s3://${BUCKET}" --force "${AWS_ARGS[@]}"

echo ""
echo "Done. Bucket s3://${BUCKET} has been deleted."
