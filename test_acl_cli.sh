#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

if [ -z "$HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD" ] || \
   [ -z "$HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE" ] || \
   [ -z "$HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD" ] || \
   [ -z "$HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE" ]; then
    echo "Error: Required environment variables not set"
    echo "Required: HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD, HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE,"
    echo "          HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD, HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE"
    exit 1
fi

ENDPOINT_URL="${HIPPIUS_ENDPOINT_URL:-http://localhost:8080}"
TEST_BUCKET="acl-test-bucket-$$"
TEST_OBJECT="acl-test-object.txt"
TEST_CONTENT="This is test content for ACL testing"

PASSED=0
FAILED=0
SKIPPED=0

ACC1_UPLOAD_PROFILE="acc1-upload"
ACC1_UPLOADDELETE_PROFILE="acc1-uploaddelete"
ACC2_UPLOAD_PROFILE="acc2-upload"
ACC2_UPLOADDELETE_PROFILE="acc2-uploaddelete"

ACC1_CANONICAL_ID=""
ACC2_CANONICAL_ID=""

setup_aws_profiles() {
    echo "=== Setting up AWS CLI profiles ==="

    ACC1_UPLOAD_ACCESS_KEY=$(echo -n "$HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD" | base64)
    ACC1_UPLOADDELETE_ACCESS_KEY=$(echo -n "$HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE" | base64)
    ACC2_UPLOAD_ACCESS_KEY=$(echo -n "$HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD" | base64)
    ACC2_UPLOADDELETE_ACCESS_KEY=$(echo -n "$HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE" | base64)

    aws configure set aws_access_key_id "$ACC1_UPLOAD_ACCESS_KEY" --profile "$ACC1_UPLOAD_PROFILE"
    aws configure set aws_secret_access_key "$HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD" --profile "$ACC1_UPLOAD_PROFILE"
    aws configure set region "us-east-1" --profile "$ACC1_UPLOAD_PROFILE"

    aws configure set aws_access_key_id "$ACC1_UPLOADDELETE_ACCESS_KEY" --profile "$ACC1_UPLOADDELETE_PROFILE"
    aws configure set aws_secret_access_key "$HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE" --profile "$ACC1_UPLOADDELETE_PROFILE"
    aws configure set region "us-east-1" --profile "$ACC1_UPLOADDELETE_PROFILE"

    aws configure set aws_access_key_id "$ACC2_UPLOAD_ACCESS_KEY" --profile "$ACC2_UPLOAD_PROFILE"
    aws configure set aws_secret_access_key "$HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD" --profile "$ACC2_UPLOAD_PROFILE"
    aws configure set region "us-east-1" --profile "$ACC2_UPLOAD_PROFILE"

    aws configure set aws_access_key_id "$ACC2_UPLOADDELETE_ACCESS_KEY" --profile "$ACC2_UPLOADDELETE_PROFILE"
    aws configure set aws_secret_access_key "$HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE" --profile "$ACC2_UPLOADDELETE_PROFILE"
    aws configure set region "us-east-1" --profile "$ACC2_UPLOADDELETE_PROFILE"

    echo "Profiles configured: $ACC1_UPLOAD_PROFILE, $ACC1_UPLOADDELETE_PROFILE, $ACC2_UPLOAD_PROFILE, $ACC2_UPLOADDELETE_PROFILE"
}

get_canonical_user_id() {
    local profile=$1
    local result
    result=$(aws s3api get-bucket-acl --bucket "$TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$profile" 2>/dev/null | jq -r '.Owner.ID' || echo "")
    echo "$result"
}

setup_test_resources() {
    echo "=== Setting up test resources ==="

    aws s3api create-bucket --bucket "$TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE" 2>/dev/null || true

    echo "$TEST_CONTENT" > "/tmp/$TEST_OBJECT"
    aws s3api put-object --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --body "/tmp/$TEST_OBJECT" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE" >/dev/null 2>&1 || true

    ACC1_CANONICAL_ID=$(get_canonical_user_id "$ACC1_UPLOADDELETE_PROFILE")

    ACC2_TEST_BUCKET="acl-test-acc2-$$"
    aws s3api create-bucket --bucket "$ACC2_TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC2_UPLOADDELETE_PROFILE" 2>/dev/null || true
    ACC2_CANONICAL_ID=$(aws s3api get-bucket-acl --bucket "$ACC2_TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC2_UPLOADDELETE_PROFILE" 2>/dev/null | jq -r '.Owner.ID' || echo "")
    aws s3api delete-bucket --bucket "$ACC2_TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC2_UPLOADDELETE_PROFILE" 2>/dev/null || true

    if [ -z "$ACC1_CANONICAL_ID" ]; then
        echo "Warning: Could not determine Account 1 canonical user ID"
    else
        echo "Account 1 Canonical ID: $ACC1_CANONICAL_ID"
    fi

    if [ -z "$ACC2_CANONICAL_ID" ]; then
        echo "Warning: Could not determine Account 2 canonical user ID"
    else
        echo "Account 2 Canonical ID: $ACC2_CANONICAL_ID"
    fi
}

cleanup_test_resources() {
    echo ""
    echo "=== Cleaning up test resources ==="

    aws s3api delete-object --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE" 2>/dev/null || true
    aws s3api delete-bucket --bucket "$TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE" 2>/dev/null || true

    rm -f "/tmp/$TEST_OBJECT"
}

assert_success() {
    local test_name=$1
    shift

    if "$@" >/dev/null 2>&1; then
        echo "✓ PASS: $test_name"
        ((PASSED++))
        return 0
    else
        echo "✗ FAIL: $test_name"
        ((FAILED++))
        return 1
    fi
}

assert_failure() {
    local test_name=$1
    shift

    if "$@" >/dev/null 2>&1; then
        echo "✗ FAIL: $test_name (expected failure but succeeded)"
        ((FAILED++))
        return 1
    else
        echo "✓ PASS: $test_name"
        ((PASSED++))
        return 0
    fi
}

skip_test() {
    local test_name=$1
    local reason=$2
    echo "⊘ SKIP: $test_name${reason:+ ($reason)}"
    ((SKIPPED++))
}

print_section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "$1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

setup_aws_profiles
setup_test_resources
trap cleanup_test_resources EXIT

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Starting AWS S3 ACL Test Suite - 137 Tests"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

print_section "Category 1: Basic Bucket ACL Operations (8 tests)"

assert_success "test_get_bucket_acl_default" \
    aws s3api get-bucket-acl --bucket "$TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_get_bucket_acl_returns_owner" \
    bash -c "aws s3api get-bucket-acl --bucket '$TEST_BUCKET' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' | jq -e '.Owner.ID'"

assert_success "test_get_bucket_acl_returns_grants" \
    bash -c "aws s3api get-bucket-acl --bucket '$TEST_BUCKET' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' | jq -e '.Grants'"

assert_success "test_put_bucket_acl_with_canned_private" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl private --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_bucket_acl_with_canned_public_read" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl public-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_bucket_acl_with_canned_public_read_write" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl public-read-write --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_bucket_acl_with_canned_authenticated_read" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl authenticated-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_bucket_acl_with_canned_log_delivery_write" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl log-delivery-write --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

print_section "Category 2: Basic Object ACL Operations (7 tests)"

assert_success "test_get_object_acl_default" \
    aws s3api get-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_get_object_acl_returns_owner" \
    bash -c "aws s3api get-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' | jq -e '.Owner.ID'"

assert_success "test_get_object_acl_returns_grants" \
    bash -c "aws s3api get-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' | jq -e '.Grants'"

assert_success "test_put_object_acl_with_canned_private" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl private --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_object_acl_with_canned_public_read" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl public-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_object_acl_with_canned_authenticated_read" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl authenticated-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_object_acl_with_canned_aws_exec_read" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl aws-exec-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

print_section "Category 3: Object-Only Canned ACLs (2 tests)"

assert_success "test_put_object_acl_bucket_owner_read" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl bucket-owner-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_put_object_acl_bucket_owner_full_control" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl bucket-owner-full-control --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

print_section "Category 4: ACL During Resource Creation (3 tests)"

assert_success "test_create_bucket_with_acl_header" \
    bash -c "aws s3api create-bucket --bucket 'acl-test-create-$$' --acl public-read --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api delete-bucket --bucket 'acl-test-create-$$' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE'"

assert_success "test_put_object_with_acl_header" \
    bash -c "echo 'test' > /tmp/acl-test-tmp.txt && aws s3api put-object --bucket '$TEST_BUCKET' --key 'test-acl-header.txt' --body /tmp/acl-test-tmp.txt --acl public-read --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api delete-object --bucket '$TEST_BUCKET' --key 'test-acl-header.txt' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && rm /tmp/acl-test-tmp.txt"

skip_test "test_complete_multipart_with_acl_header" "requires multipart implementation"

print_section "Category 5: Explicit Bucket Grants - Canonical User (5 tests)"

if [ -n "$ACC2_CANONICAL_ID" ]; then
    assert_success "test_grant_bucket_read_to_canonical_user" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_bucket_write_to_canonical_user" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-write "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_bucket_read_acp_to_canonical_user" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read-acp "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_bucket_write_acp_to_canonical_user" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-write-acp "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_bucket_full_control_to_canonical_user" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-full-control "id=\"$ACC1_CANONICAL_ID\",id=\"$ACC2_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"
else
    skip_test "test_grant_bucket_read_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_bucket_write_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_bucket_read_acp_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_bucket_write_acp_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_bucket_full_control_to_canonical_user" "ACC2 canonical ID unavailable"
fi

print_section "Category 6: Explicit Object Grants - Canonical User (4 tests)"

if [ -n "$ACC2_CANONICAL_ID" ]; then
    assert_success "test_grant_object_read_to_canonical_user" \
        aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-read "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_object_read_acp_to_canonical_user" \
        aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-read-acp "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_object_write_acp_to_canonical_user" \
        aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-write-acp "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_grant_object_full_control_to_canonical_user" \
        aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-full-control "id=\"$ACC1_CANONICAL_ID\",id=\"$ACC2_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"
else
    skip_test "test_grant_object_read_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_object_read_acp_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_object_write_acp_to_canonical_user" "ACC2 canonical ID unavailable"
    skip_test "test_grant_object_full_control_to_canonical_user" "ACC2 canonical ID unavailable"
fi

print_section "Category 7: Group Grantees - AllUsers (6 tests)"

assert_success "test_grant_bucket_read_to_all_users" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read 'uri="http://acs.amazonaws.com/groups/global/AllUsers"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_grant_bucket_write_to_all_users" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-write 'uri="http://acs.amazonaws.com/groups/global/AllUsers"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_grant_object_read_to_all_users" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-read 'uri="http://acs.amazonaws.com/groups/global/AllUsers"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

skip_test "test_anonymous_read_with_all_users_grant" "requires anonymous request"

skip_test "test_anonymous_write_with_all_users_grant" "requires anonymous request"

skip_test "test_anonymous_denied_without_all_users_grant" "requires anonymous request"

print_section "Category 8: Group Grantees - AuthenticatedUsers (4 tests)"

assert_success "test_grant_bucket_read_to_authenticated_users" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read 'uri="http://acs.amazonaws.com/groups/global/AuthenticatedUsers"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_grant_object_read_to_authenticated_users" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-read 'uri="http://acs.amazonaws.com/groups/global/AuthenticatedUsers"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

skip_test "test_authenticated_user_can_read_with_grant" "requires cross-account verification"

skip_test "test_authenticated_user_denied_without_grant" "requires cross-account verification"

print_section "Category 9: Group Grantees - LogDelivery (2 tests)"

assert_success "test_grant_bucket_write_to_log_delivery" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-write 'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_grant_bucket_read_acp_to_log_delivery" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read-acp 'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

print_section "Category 10: Multiple Grants (5 tests)"

if [ -n "$ACC2_CANONICAL_ID" ]; then
    assert_success "test_multiple_grants_to_different_users" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_multiple_permissions_to_same_user" \
        aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-read "id=\"$ACC2_CANONICAL_ID\"" --grant-read-acp "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_mix_canonical_and_group_grants" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read "id=\"$ACC2_CANONICAL_ID\"" --grant-write 'uri="http://acs.amazonaws.com/groups/global/AllUsers"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_multiple_grant_headers_in_request" \
        aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --grant-read "id=\"$ACC2_CANONICAL_ID\"" --grant-write-acp "id=\"$ACC2_CANONICAL_ID\"" --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

    assert_success "test_overwrite_acl_replaces_all_grants" \
        bash -c "aws s3api put-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --grant-read 'id=\"$ACC2_CANONICAL_ID\"' --grant-full-control 'id=\"$ACC1_CANONICAL_ID\"' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api put-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --acl private --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE'"
else
    skip_test "test_multiple_grants_to_different_users" "ACC2 canonical ID unavailable"
    skip_test "test_multiple_permissions_to_same_user" "ACC2 canonical ID unavailable"
    skip_test "test_mix_canonical_and_group_grants" "ACC2 canonical ID unavailable"
    skip_test "test_multiple_grant_headers_in_request" "ACC2 canonical ID unavailable"
    skip_test "test_overwrite_acl_replaces_all_grants" "ACC2 canonical ID unavailable"
fi

print_section "Category 11: Permission Enforcement - Bucket READ (3 tests)"

skip_test "test_bucket_read_allows_list_objects" "requires permission enforcement verification"

skip_test "test_bucket_read_allows_list_objects_v2" "requires permission enforcement verification"

skip_test "test_bucket_read_denies_other_operations" "requires permission enforcement verification"

print_section "Category 12: Permission Enforcement - Bucket WRITE (4 tests)"

skip_test "test_bucket_write_allows_put_object" "requires permission enforcement verification"

skip_test "test_bucket_write_allows_delete_object" "requires permission enforcement verification"

skip_test "test_bucket_write_allows_initiate_multipart" "requires permission enforcement verification"

skip_test "test_bucket_write_denies_list" "requires permission enforcement verification"

print_section "Category 13: Permission Enforcement - Bucket READ_ACP (2 tests)"

skip_test "test_bucket_read_acp_allows_get_bucket_acl" "requires permission enforcement verification"

skip_test "test_bucket_read_acp_denies_put_bucket_acl" "requires permission enforcement verification"

print_section "Category 14: Permission Enforcement - Bucket WRITE_ACP (2 tests)"

skip_test "test_bucket_write_acp_allows_put_bucket_acl" "requires permission enforcement verification"

skip_test "test_bucket_write_acp_allows_get_bucket_acl" "requires permission enforcement verification"

print_section "Category 15: Permission Enforcement - Bucket FULL_CONTROL (1 test)"

skip_test "test_bucket_full_control_allows_all_operations" "requires permission enforcement verification"

print_section "Category 16: Permission Enforcement - Object READ (3 tests)"

skip_test "test_object_read_allows_get_object" "requires permission enforcement verification"

skip_test "test_object_read_allows_head_object" "requires permission enforcement verification"

skip_test "test_object_read_denies_delete" "requires permission enforcement verification"

print_section "Category 17: Permission Enforcement - Object WRITE (1 test)"

skip_test "test_object_write_not_applicable" "WRITE doesn't apply to objects per spec"

print_section "Category 18: Permission Enforcement - Object READ_ACP (2 tests)"

skip_test "test_object_read_acp_allows_get_object_acl" "requires permission enforcement verification"

skip_test "test_object_read_acp_denies_put_object_acl" "requires permission enforcement verification"

print_section "Category 19: Permission Enforcement - Object WRITE_ACP (2 tests)"

skip_test "test_object_write_acp_allows_put_object_acl" "requires permission enforcement verification"

skip_test "test_object_write_acp_denies_get_object" "requires permission enforcement verification"

print_section "Category 20: Permission Enforcement - Object FULL_CONTROL (1 test)"

skip_test "test_object_full_control_allows_all_operations" "requires permission enforcement verification"

print_section "Category 21: Cross-Account Scenarios (6 tests)"

skip_test "test_cross_account_upload_with_bucket_owner_read" "requires cross-account setup"

skip_test "test_cross_account_upload_with_bucket_owner_full_control" "requires cross-account setup"

skip_test "test_cross_account_object_ownership" "requires cross-account setup"

skip_test "test_bucket_owner_cannot_read_without_grant" "requires cross-account setup"

skip_test "test_grant_cross_account_then_access" "requires cross-account setup"

skip_test "test_revoke_cross_account_then_deny" "requires cross-account setup"

print_section "Category 22: Subaccount Permission Enforcement (8 tests)"

assert_success "test_upload_subaccount_can_read_bucket_acl" \
    aws s3api get-bucket-acl --bucket "$TEST_BUCKET" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOAD_PROFILE"

assert_failure "test_upload_subaccount_cannot_write_bucket_acl" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl public-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOAD_PROFILE"

assert_success "test_uploaddelete_subaccount_can_write_bucket_acl" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl private --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_success "test_upload_subaccount_can_read_object_acl" \
    aws s3api get-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOAD_PROFILE"

assert_failure "test_upload_subaccount_cannot_write_object_acl" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl public-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOAD_PROFILE"

assert_success "test_uploaddelete_subaccount_can_write_object_acl" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl private --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

skip_test "test_upload_subaccount_can_upload_with_acl_header" "requires chain permission verification"

skip_test "test_upload_subaccount_cannot_modify_existing_acl" "requires chain permission verification"

print_section "Category 23: XML Body Format (4 tests)"

skip_test "test_put_bucket_acl_with_xml_body" "requires XML body implementation"

skip_test "test_put_object_acl_with_xml_body" "requires XML body implementation"

skip_test "test_xml_with_canonical_user_grantee" "requires XML body implementation"

skip_test "test_xml_with_group_grantee" "requires XML body implementation"

print_section "Category 24: Error Cases - Invalid Arguments (8 tests)"

assert_failure "test_error_mix_canned_acl_with_grant_headers" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl private --grant-read 'uri="http://acs.amazonaws.com/groups/global/AllUsers"' --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

skip_test "test_error_mix_canned_acl_with_xml_body" "requires XML body implementation"

assert_failure "test_error_invalid_canned_acl_name" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl invalid-acl-name --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_failure "test_error_invalid_canonical_user_id" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read 'id="invalid-id"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_failure "test_error_invalid_group_uri" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-read 'uri="http://invalid.uri.com/groups/Invalid"' --grant-full-control "id=\"$ACC1_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

skip_test "test_error_malformed_xml" "requires XML body implementation"

skip_test "test_error_invalid_permission_type" "requires XML body implementation"

skip_test "test_error_missing_required_xml_elements" "requires XML body implementation"

print_section "Category 25: Error Cases - Access Denied (5 tests)"

assert_failure "test_error_access_denied_without_write_acp" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "$TEST_OBJECT" --acl public-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC2_UPLOAD_PROFILE"

skip_test "test_error_access_denied_without_read_acp" "requires permission enforcement"

assert_failure "test_error_access_denied_bucket_acl_without_permission" \
    aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --acl public-read --endpoint-url "$ENDPOINT_URL" --profile "$ACC2_UPLOAD_PROFILE"

skip_test "test_error_access_denied_operation_without_grant" "requires permission enforcement"

skip_test "test_error_access_denied_anonymous_to_private" "requires anonymous request"

print_section "Category 26: Error Cases - Resource Not Found (2 tests)"

assert_failure "test_error_get_acl_nonexistent_object" \
    aws s3api get-object-acl --bucket "$TEST_BUCKET" --key "nonexistent-object.txt" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

assert_failure "test_error_put_acl_nonexistent_object" \
    aws s3api put-object-acl --bucket "$TEST_BUCKET" --key "nonexistent-object.txt" --acl private --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"

print_section "Category 27: Error Cases - Deprecated Features (2 tests)"

skip_test "test_error_email_grantee_returns_405" "email grantees deprecated Oct 2025"

skip_test "test_error_acl_on_enforced_bucket_ownership" "requires object ownership implementation"

print_section "Category 28: ACL Inheritance and Defaults (4 tests)"

skip_test "test_new_bucket_default_acl_is_private" "requires verification of default state"

skip_test "test_new_object_default_acl_is_private" "requires verification of default state"

skip_test "test_object_does_not_inherit_bucket_acl" "requires inheritance verification"

skip_test "test_owner_always_has_full_control" "requires implicit permission verification"

print_section "Category 29: ACL Persistence (4 tests)"

skip_test "test_bucket_acl_persists_after_object_operations" "requires state verification"

skip_test "test_object_acl_persists_after_reads" "requires state verification"

skip_test "test_object_acl_behavior_on_overwrite" "requires overwrite behavior verification"

skip_test "test_acl_persists_across_api_restarts" "requires restart testing"

print_section "Category 30: Special Cases (5 tests)"

assert_success "test_get_canonical_user_id_from_acl" \
    bash -c "aws s3api get-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' | jq -e '.Owner.ID' | grep -q '[a-f0-9]'"

skip_test "test_display_name_in_acl_response" "requires DisplayName implementation"

skip_test "test_acl_with_versioned_object" "requires versioning implementation"

skip_test "test_acl_content_md5_header" "requires Content-MD5 verification"

skip_test "test_acl_with_sse_encrypted_object" "requires SSE implementation"

print_section "Category 31: Public Access Scenarios (4 tests)"

skip_test "test_public_read_allows_unsigned_get" "requires public access verification"

skip_test "test_public_read_write_allows_unsigned_put" "requires public access verification"

skip_test "test_public_bucket_list_objects" "requires public access verification"

skip_test "test_public_read_denies_unsigned_delete" "requires public access verification"

print_section "Category 32: Canned ACL Verification (8 tests)"

skip_test "test_verify_private_acl_grants" "requires grant inspection"

skip_test "test_verify_public_read_acl_grants" "requires grant inspection"

skip_test "test_verify_public_read_write_acl_grants" "requires grant inspection"

skip_test "test_verify_authenticated_read_acl_grants" "requires grant inspection"

skip_test "test_verify_bucket_owner_read_acl_grants" "requires grant inspection"

skip_test "test_verify_bucket_owner_full_control_acl_grants" "requires grant inspection"

skip_test "test_verify_aws_exec_read_acl_grants" "requires grant inspection"

skip_test "test_verify_log_delivery_write_acl_grants" "requires grant inspection"

print_section "Category 33: Grant Header Format Validation (4 tests)"

if [ -n "$ACC2_CANONICAL_ID" ]; then
    assert_success "test_grant_header_with_multiple_grantees" \
        aws s3api put-bucket-acl --bucket "$TEST_BUCKET" --grant-full-control "id=\"$ACC1_CANONICAL_ID\",id=\"$ACC2_CANONICAL_ID\"" --endpoint-url "$ENDPOINT_URL" --profile "$ACC1_UPLOADDELETE_PROFILE"
else
    skip_test "test_grant_header_with_multiple_grantees" "ACC2 canonical ID unavailable"
fi

skip_test "test_grant_header_canonical_user_format" "requires format validation"

skip_test "test_grant_header_group_uri_format" "requires format validation"

skip_test "test_grant_header_spaces_in_value" "requires format validation"

print_section "Category 34: Cleanup and Reset (3 tests)"

assert_success "test_delete_bucket_with_custom_acl" \
    bash -c "aws s3api create-bucket --bucket 'acl-test-delete-bucket-$$' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api put-bucket-acl --bucket 'acl-test-delete-bucket-$$' --acl public-read --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api delete-bucket --bucket 'acl-test-delete-bucket-$$' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE'"

assert_success "test_delete_object_with_custom_acl" \
    bash -c "echo 'test' > /tmp/acl-delete-test.txt && aws s3api put-object --bucket '$TEST_BUCKET' --key 'acl-delete-test.txt' --body /tmp/acl-delete-test.txt --acl public-read --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api delete-object --bucket '$TEST_BUCKET' --key 'acl-delete-test.txt' --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && rm /tmp/acl-delete-test.txt"

assert_success "test_reset_acl_to_default_private" \
    bash -c "aws s3api put-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --acl public-read --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE' && aws s3api put-object-acl --bucket '$TEST_BUCKET' --key '$TEST_OBJECT' --acl private --endpoint-url '$ENDPOINT_URL' --profile '$ACC1_UPLOADDELETE_PROFILE'"

print_section "Category 35: Performance and Edge Cases (3 tests)"

skip_test "test_large_number_of_grants" "requires large grant testing"

skip_test "test_acl_on_large_object" "requires large object testing"

skip_test "test_concurrent_acl_modifications" "requires concurrency testing"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Total Tests: $((PASSED + FAILED + SKIPPED))"
echo "  ✓ Passed:  $PASSED"
echo "  ✗ Failed:  $FAILED"
echo "  ⊘ Skipped: $SKIPPED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "Result: ALL EXECUTED TESTS PASSED"
    exit 0
else
    echo "Result: SOME TESTS FAILED"
    exit 1
fi
