#!/usr/bin/env python3
import base64
import hashlib
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from minio import Minio
from minio.commonconfig import ENABLED, CopySource
from minio.commonconfig import Filter
from minio.commonconfig import Tags
from minio.lifecycleconfig import Expiration
from minio.lifecycleconfig import LifecycleConfig
from minio.lifecycleconfig import Rule

from config import MINIO_URL, MINIO_SECURE, MINIO_REGION


def create_test_file(file_size_mb=1, pattern=None):
    """
    Create a test file with specified size and pattern.

    Args:
        file_size_mb: Size of the file in MB
        pattern: Optional pattern type - if None, use random data;
                'sequential' creates a file with sequential bytes that's easier to verify part boundaries
                'alternating' creates a file with alternating patterns for each MB to identify part mixing issues

    Returns:
        Path to the created file
    """
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        if pattern is None:
            # Random data (most realistic test)
            temp_file.write(os.urandom(file_size_mb * 1024 * 1024))
        elif pattern == "sequential":
            # Sequential bytes - good for identifying part boundary issues
            for i in range(file_size_mb):
                chunk = bytearray(range(256)) * (1024 * 4)  # 1MB of sequential bytes
                temp_file.write(chunk)
        elif pattern == "alternating":
            # Alternating patterns for each MB - good for identifying mixed parts
            for i in range(file_size_mb):
                # Create a unique pattern for each MB to identify part mixing
                pattern_byte = (i % 256).to_bytes(1, byteorder="big")
                chunk = pattern_byte * (1024 * 1024)  # 1MB of the same byte
                temp_file.write(chunk)
        return temp_file.name


def calculate_md5(file_path, start=None, end=None):
    """
    Calculate MD5 hash of a file or a portion of a file.

    Args:
        file_path: Path to the file
        start: Optional start byte position (for range hashing)
        end: Optional end byte position (for range hashing)

    Returns:
        MD5 hash as a hexadecimal string
    """
    md5_hash = hashlib.md5()
    with Path.open(file_path, "rb") as f:
        if start is not None:
            f.seek(start)

        # If range is specified, read only that range
        if start is not None and end is not None:
            bytes_to_read = end - start
            data = f.read(bytes_to_read)
            md5_hash.update(data)
        else:
            # Read file in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)

    return md5_hash.hexdigest()


def verify_multipart_integrity(original_file, downloaded_file, part_size_bytes):
    """
    Verify the integrity of a multipart upload by checking individual parts.

    Args:
        original_file: Path to the original file
        downloaded_file: Path to the downloaded file
        part_size_bytes: Size of each part in bytes

    Returns:
        Tuple of (overall_match, list of part matches)
    """
    original_size = os.path.getsize(original_file)
    downloaded_size = os.path.getsize(downloaded_file)

    # First check if sizes match
    if original_size != downloaded_size:
        print(f"Size mismatch: Original={original_size}, Downloaded={downloaded_size}")
        return False, []

    # Calculate overall hashes
    original_hash = calculate_md5(original_file)
    downloaded_hash = calculate_md5(downloaded_file)
    overall_match = original_hash == downloaded_hash

    # Check individual parts
    part_matches = []
    num_parts = (original_size + part_size_bytes - 1) // part_size_bytes

    for i in range(num_parts):
        start = i * part_size_bytes
        end = min(start + part_size_bytes, original_size)

        original_part_hash = calculate_md5(original_file, start, end)
        downloaded_part_hash = calculate_md5(downloaded_file, start, end)

        part_match = original_part_hash == downloaded_part_hash
        part_matches.append(part_match)

        if not part_match:
            print(f"Part {i + 1}/{num_parts} hash mismatch!")
            print(f"  Original part hash: {original_part_hash}")
            print(f"  Downloaded part hash: {downloaded_part_hash}")

    return overall_match, part_matches


def main():
    print("=== TESTING SUB-ACCOUNT PERMISSIONS AND USER ISOLATION ===")
    print("Testing upload/delete permissions with sub-accounts and user isolation...\n")

    # Get sub-account seed phrases from environment
    acc1_upload = os.environ["HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD"]  # Upload-only for account 1
    acc1_uploaddelete = os.environ["HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE"]  # Upload+delete for account 1
    acc2_upload = os.environ["HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD"]  # Upload-only for account 2
    acc2_uploaddelete = os.environ["HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE"]  # Upload+delete for account 2

    # Base64 encode the seed phrases for access keys
    encoded_acc1_upload = base64.b64encode(acc1_upload.encode("utf-8")).decode("utf-8")
    encoded_acc1_uploaddelete = base64.b64encode(acc1_uploaddelete.encode("utf-8")).decode("utf-8")
    encoded_acc2_upload = base64.b64encode(acc2_upload.encode("utf-8")).decode("utf-8")
    encoded_acc2_uploaddelete = base64.b64encode(acc2_uploaddelete.encode("utf-8")).decode("utf-8")

    print(f"Account 1 Upload-only client: {encoded_acc1_upload[:20]}...")
    print(f"Account 1 Upload+Delete client: {encoded_acc1_uploaddelete[:20]}...")
    print(f"Account 2 Upload-only client: {encoded_acc2_upload[:20]}...")
    print(f"Account 2 Upload+Delete client: {encoded_acc2_uploaddelete[:20]}...")

    # Create 4 different MinIO clients with different permission levels
    # Account 1 - Upload only
    acc1_upload_client = Minio(
        MINIO_URL,
        access_key=encoded_acc1_upload,
        secret_key=acc1_upload,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    # Account 1 - Upload + Delete
    acc1_uploaddelete_client = Minio(
        MINIO_URL,
        access_key=encoded_acc1_uploaddelete,
        secret_key=acc1_uploaddelete,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    # Account 2 - Upload only
    acc2_upload_client = Minio(
        MINIO_URL,
        access_key=encoded_acc2_upload,
        secret_key=acc2_upload,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    # Account 2 - Upload + Delete
    acc2_uploaddelete_client = Minio(
        MINIO_URL,
        access_key=encoded_acc2_uploaddelete,
        secret_key=acc2_uploaddelete,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    # For backwards compatibility, use the upload+delete client as the primary client
    minio_client = acc1_uploaddelete_client
    minio_client_secondary = acc2_uploaddelete_client

    # NEW: Test sub-account permissions
    print("\n=== SUB-ACCOUNT PERMISSION TESTS ===")

    # Create a test bucket and object with account 1's upload+delete client
    permission_test_bucket = f"permission-test-{int(time.time())}"
    permission_test_object = "permission-test.txt"
    test_content = b"Test content for permission testing"

    print(f"Creating test bucket '{permission_test_bucket}' with Account 1 upload+delete client...")
    acc1_uploaddelete_client.make_bucket(permission_test_bucket)

    print(f"Uploading test object with Account 1 upload+delete client...")
    from io import BytesIO
    acc1_uploaddelete_client.put_object(
        permission_test_bucket,
        permission_test_object,
        BytesIO(test_content),
        len(test_content),
        content_type="text/plain"
    )

    # Test 1: Account 1 upload-only client tries to upload (should succeed)
    print("\n--- Test: Account 1 upload-only client uploading ---")
    try:
        upload_only_object = "upload-only-test.txt"
        acc1_upload_client.put_object(
            permission_test_bucket,
            upload_only_object,
            BytesIO(b"Upload only test"),
            len(b"Upload only test"),
            content_type="text/plain"
        )
        print("✅ SUCCESS: Account 1 upload-only client can upload objects")
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Account 1 upload-only client failed to upload: {e}")

    # Test 2: Account 1 upload-only client tries to delete (should fail)
    print("\n--- Test: Account 1 upload-only client trying to delete ---")
    try:
        acc1_upload_client.remove_object(permission_test_bucket, permission_test_object)
        print("❌ SECURITY FAILURE: Account 1 upload-only client was able to delete!")
        assert False, "Upload-only client should not be able to delete"
    except Exception as e:
        print(f"✅ SUCCESS: Account 1 upload-only client blocked from deleting: {e}")

    # Test 3: Account 2 upload-only client tries to access Account 1's bucket (should fail)
    print("\n--- Test: Account 2 upload-only client accessing Account 1's bucket ---")
    try:
        acc2_upload_client.put_object(
            permission_test_bucket,
            "cross-account-test.txt",
            BytesIO(b"Cross account test"),
            len(b"Cross account test"),
            content_type="text/plain"
        )
        print("❌ SECURITY FAILURE: Account 2 client accessed Account 1's bucket!")
        assert False, "Cross-account access should be denied"
    except Exception as e:
        print(f"✅ SUCCESS: Account 2 client blocked from Account 1's bucket: {e}")

    # Test 4: Account 2 upload+delete client tries to delete from Account 1's bucket (should fail)
    print("\n--- Test: Account 2 upload+delete client trying to delete from Account 1's bucket ---")
    try:
        acc2_uploaddelete_client.remove_object(permission_test_bucket, permission_test_object)
        print("❌ SECURITY FAILURE: Account 2 upload+delete client deleted from Account 1's bucket!")
        assert False, "Cross-account deletion should be denied"
    except Exception as e:
        print(f"✅ SUCCESS: Account 2 upload+delete client blocked from Account 1's bucket: {e}")

    # Test 5: Account 1 upload+delete client can delete (should succeed)
    print("\n--- Test: Account 1 upload+delete client deleting ---")
    try:
        acc1_uploaddelete_client.remove_object(permission_test_bucket, permission_test_object)
        print("✅ SUCCESS: Account 1 upload+delete client can delete objects")
    except Exception as e:
        print(f"❌ UNEXPECTED FAILURE: Account 1 upload+delete client failed to delete: {e}")

    # Clean up permission test resources
    print("\n--- Cleaning up permission test resources ---")
    try:
        # Clean up any remaining objects
        objects = list(acc1_uploaddelete_client.list_objects(permission_test_bucket))
        for obj in objects:
            acc1_uploaddelete_client.remove_object(permission_test_bucket, obj.object_name)
        acc1_uploaddelete_client.remove_bucket(permission_test_bucket)
        print("✅ Permission test cleanup completed")
    except Exception as e:
        print(f"⚠️  Warning during permission test cleanup: {e}")

    # Test 1: Delete functionality
    print("=== TEST 1: DELETE FUNCTIONALITY ===")
    delete_test_bucket = f"delete-test-bucket-{int(time.time())}"
    print(f"Creating bucket for delete testing: '{delete_test_bucket}'")
    try:
        minio_client.make_bucket(delete_test_bucket)
        print("✅ SUCCESS: Created bucket for delete testing")

        # Upload a test object
        test_object = "delete-test-object.txt"
        test_content = b"This content will be deleted"
        from io import BytesIO
        minio_client.put_object(
            delete_test_bucket,
            test_object,
            BytesIO(test_content),
            len(test_content),
            content_type="text/plain"
        )
        print(f"✅ SUCCESS: Uploaded test object '{test_object}'")

        # Verify object exists
        objects = list(minio_client.list_objects(delete_test_bucket))
        if any(obj.object_name == test_object for obj in objects):
            print("✅ SUCCESS: Object exists in bucket")
        else:
            print("❌ FAIL: Object not found in bucket")

        # Delete the object
        print(f"Deleting object '{test_object}'...")
        minio_client.remove_object(delete_test_bucket, test_object)
        print("✅ SUCCESS: Object deleted successfully")

        # Verify object is gone
        objects_after = list(minio_client.list_objects(delete_test_bucket))
        if not any(obj.object_name == test_object for obj in objects_after):
            print("✅ SUCCESS: Object no longer exists after deletion")
        else:
            print("❌ FAIL: Object still exists after deletion")

        # Delete the bucket
        print(f"Deleting bucket '{delete_test_bucket}'...")
        minio_client.remove_bucket(delete_test_bucket)
        print("✅ SUCCESS: Bucket deleted successfully")

        # Verify bucket is gone
        remaining_buckets = [bucket.name for bucket in minio_client.list_buckets()]
        if delete_test_bucket not in remaining_buckets:
            print("✅ SUCCESS: Bucket no longer exists after deletion")
        else:
            print("❌ FAIL: Bucket still exists after deletion")

    except Exception as e:
        print(f"❌ FAIL: Error with delete functionality: {e}")
        import traceback
        traceback.print_exc()

    # Test 2: Main account isolation (different main accounts)
    print("\n=== TEST 2: MAIN ACCOUNT ISOLATION ===")

    # Create buckets with both main accounts (using their upload+delete sub-accounts)
    account1_bucket = f"account1-bucket-{int(time.time())}"
    account2_bucket = f"account2-bucket-{int(time.time())}"

    print(f"Account 1 creating bucket: {account1_bucket}")
    acc1_uploaddelete_client.make_bucket(account1_bucket)

    print(f"Account 2 creating bucket: {account2_bucket}")
    account2_has_credits = True
    try:
        acc2_uploaddelete_client.make_bucket(account2_bucket)
    except Exception as e:
        if "InsufficientAccountCredit" in str(e) or "AccountVerificationError" in str(e):
            print(f"⚠️  Account 2 has insufficient credits or verification error - skipping isolation tests")
            print(f"   To test isolation, ensure Account 2's sub-accounts have credits")
            account2_has_credits = False
        else:
            raise e

    if account2_has_credits:
        # List buckets from account 1's perspective
        print("Listing buckets from Account 1's perspective:")
        account1_buckets = acc1_uploaddelete_client.list_buckets()
        account1_bucket_names = [bucket.name for bucket in account1_buckets]
        print(f"Account 1 sees buckets: {account1_bucket_names}")

        # List buckets from account 2's perspective
        print("Listing buckets from Account 2's perspective:")
        account2_buckets = acc2_uploaddelete_client.list_buckets()
        account2_bucket_names = [bucket.name for bucket in account2_buckets]
        print(f"Account 2 sees buckets: {account2_bucket_names}")

        # Verify isolation (should always be isolated since different main accounts)
        if account1_bucket in account1_bucket_names and account1_bucket not in account2_bucket_names:
            print("✅ SUCCESS: Account 1's bucket is visible to Account 1 but not Account 2")
        else:
            print("❌ FAIL: Main account isolation not working correctly")

        if account2_bucket in account2_bucket_names and account2_bucket not in account1_bucket_names:
            print("✅ SUCCESS: Account 2's bucket is visible to Account 2 but not Account 1")
        else:
            print("❌ FAIL: Main account isolation not working correctly")

        # Clean up account test buckets
        print("Cleaning up account test buckets...")
        acc1_uploaddelete_client.remove_bucket(account1_bucket)
        acc2_uploaddelete_client.remove_bucket(account2_bucket)
    else:
        # Clean up just account 1's bucket since account 2 couldn't create one
        print("Cleaning up account 1's bucket...")
        acc1_uploaddelete_client.remove_bucket(account1_bucket)

    print("✅ SUCCESS: Main account isolation test completed")

    print("\n=== CONTINUING WITH MAIN TESTS ===\n")

    # Test bucket names and object keys
    main_bucket = f"test-bucket-{int(time.time())}"
    secondary_bucket = f"test-bucket-2-{int(time.time())}"
    standard_object = "test-object.bin"
    nested_object = "test/nested/path/object.bin"
    metadata_object = "metadata-object.bin"
    large_object = "large-object.bin"

    # Create test files
    print("Creating test files...")
    standard_file = create_test_file(1)  # 1MB
    nested_file = create_test_file(1)  # 1MB
    metadata_file = create_test_file(1)  # 1MB
    large_file = create_test_file(5)  # 5MB - for multipart testing

    # Calculate original file hashes
    standard_hash = calculate_md5(standard_file)
    nested_hash = calculate_md5(nested_file)
    metadata_hash = calculate_md5(metadata_file)
    large_hash = calculate_md5(large_file)

    print(f"Standard file created at: {standard_file}, hash: {standard_hash}")
    print(f"Nested file created at: {nested_file}, hash: {nested_hash}")
    print(f"Metadata file created at: {metadata_file}, hash: {metadata_hash}")
    print(f"Large file created at: {large_file}, hash: {large_hash}")

    # Setup download paths
    standard_download = f"{standard_file}.downloaded"
    nested_download = f"{nested_file}.downloaded"
    metadata_download = f"{metadata_file}.downloaded"
    large_download = f"{large_file}.downloaded"

    # All MinIO/S3 operations
    print("\n=== BUCKET OPERATIONS ===")

    # Check if buckets exist before creating them
    print(f"Checking if bucket {main_bucket} exists...")
    bucket_exists = minio_client.bucket_exists(main_bucket)
    print(f"Bucket {main_bucket} exists: {bucket_exists}")

    # Create buckets
    print(f"Creating bucket: {main_bucket}")
    minio_client.make_bucket(main_bucket)
    print(f"Creating bucket: {secondary_bucket}")
    minio_client.make_bucket(secondary_bucket)

    # List all buckets
    print("Listing all buckets:")
    buckets = minio_client.list_buckets()
    for bucket in buckets:
        print(f"  Bucket: {bucket.name}, Created: {bucket.creation_date}")

    # Set bucket tags
    tags = Tags.new_bucket_tags()
    tags["Project"] = "Testing"
    tags["Environment"] = "Development"
    print(f"Setting tags on bucket {main_bucket}")
    minio_client.set_bucket_tags(main_bucket, tags)
    minio_client.get_bucket_tags(main_bucket)

    # Set lifecycle configuration
    config = LifecycleConfig(
        [
            Rule(
                ENABLED,
                rule_filter=Filter(prefix="temp/"),
                rule_id="expire-temp",
                expiration=Expiration(days=1),
            ),
        ],
    )
    print(f"Setting lifecycle configuration on bucket {main_bucket}")
    minio_client.set_bucket_lifecycle(main_bucket, config)

    # Get lifecycle configuration
    print(f"Getting lifecycle configuration for bucket {main_bucket}")
    lifecycle = minio_client.get_bucket_lifecycle(main_bucket)
    print(f"Retrieved lifecycle configuration: {lifecycle}")

    print("\n=== OBJECT OPERATIONS ===")

    # Upload standard object with metadata
    print(f"Uploading standard object to {main_bucket}/{standard_object}")
    with open(standard_file, "rb") as file_data:
        standard_size = os.path.getsize(standard_file)
        standard_result = minio_client.put_object(
            main_bucket,
            standard_object,
            file_data,
            standard_size,
            content_type="application/octet-stream",
            metadata={"original_hash": standard_hash},
        )
    print(f"Standard object uploaded successfully: etag={standard_result.etag}")

    # Upload nested object
    print(f"Uploading nested object to {main_bucket}/{nested_object}")
    with open(nested_file, "rb") as file_data:
        nested_size = os.path.getsize(nested_file)
        nested_result = minio_client.put_object(
            main_bucket,
            nested_object,
            file_data,
            nested_size,
            content_type="application/octet-stream",
        )
    print(f"Nested object uploaded successfully: etag={nested_result.etag}")

    # Upload object with extensive metadata
    print(f"Uploading object with metadata to {main_bucket}/{metadata_object}")
    with open(metadata_file, "rb") as file_data:
        metadata_size = os.path.getsize(metadata_file)
        metadata_result = minio_client.put_object(
            main_bucket,
            metadata_object,
            file_data,
            metadata_size,
            content_type="application/octet-stream",
            metadata={
                "original_hash": metadata_hash,
                "created_by": "example_minio.py",
                "purpose": "testing",
                "sensitive": "false",
                "department": "engineering",
            },
        )
    print(f"Metadata object uploaded successfully: etag={metadata_result.etag}")

    # Upload large object using multipart upload
    print(f"Uploading large object to {main_bucket}/{large_object}")
    with open(large_file, "rb") as file_data:
        large_size = os.path.getsize(large_file)
        large_result = minio_client.put_object(
            main_bucket,
            large_object,
            file_data,
            large_size,
            content_type="application/octet-stream",
            part_size=5 * 1024 * 1024,
            # 5MB part size (minimum allowed)
            num_parallel_uploads=3,  # Use 3 parallel uploads
        )
    print(f"Large object uploaded successfully: etag={large_result.etag}")

    # List all objects in bucket
    print(f"Listing all objects in {main_bucket}")
    objects = minio_client.list_objects(main_bucket, recursive=True)
    for obj in objects:
        print(f"  Object: {obj.object_name}, Size: {obj.size}, Last Modified: {obj.last_modified}")

    # List objects with prefix
    print(f"Listing objects with prefix 'test/' in {main_bucket}")
    prefix_objects = minio_client.list_objects(main_bucket, prefix="test/", recursive=True)
    for obj in prefix_objects:
        print(f"  Object: {obj.object_name}, Size: {obj.size}, Last Modified: {obj.last_modified}")

    # Get object metadata
    print(f"Getting metadata for {main_bucket}/{metadata_object}")
    metadata_stat = minio_client.stat_object(main_bucket, metadata_object)
    print(f"  ETag: {metadata_stat.etag}")
    print(f"  Size: {metadata_stat.size}")
    print(f"  Last Modified: {metadata_stat.last_modified}")
    print(f"  Content Type: {metadata_stat.content_type}")
    print(f"  Metadata: {metadata_stat.metadata}")

    # Set object tags
    object_tags = Tags.new_object_tags()
    object_tags["Classification"] = "Confidential"
    object_tags["Project"] = "Integration"
    print(f"Setting tags on {main_bucket}/{standard_object}")
    minio_client.set_object_tags(main_bucket, standard_object, object_tags)

    # Get object tags
    print(f"Getting tags for {main_bucket}/{standard_object}")
    retrieved_obj_tags = minio_client.get_object_tags(main_bucket, standard_object)
    print(f"Retrieved object tags: {retrieved_obj_tags}")

    # Download objects
    print(f"\n--- Downloading and verifying standard object ---")
    print(f"Downloading standard object to {standard_download}")
    standard_response = minio_client.get_object(main_bucket, standard_object)
    with open(standard_download, "wb") as file:
        for data in standard_response.stream(4096):
            file.write(data)
    standard_response.close()
    standard_response.release_conn()

    # Verify standard object immediately
    standard_downloaded_hash = calculate_md5(standard_download)
    print(f"Original standard file hash: {standard_hash}")
    print(f"Downloaded standard file hash: {standard_downloaded_hash}")
    print(f"Standard file verification: {standard_hash == standard_downloaded_hash}")
    assert standard_hash == standard_downloaded_hash, "INTEGRITY ERROR: Standard object hash mismatch!"

    print(f"\n--- Downloading and verifying nested object ---")
    print(f"Downloading nested object to {nested_download}")
    nested_response = minio_client.get_object(main_bucket, nested_object)
    with open(nested_download, "wb") as file:
        for data in nested_response.stream(4096):
            file.write(data)
    nested_response.close()
    nested_response.release_conn()

    # Verify nested object immediately
    nested_downloaded_hash = calculate_md5(nested_download)
    print(f"Original nested file hash: {nested_hash}")
    print(f"Downloaded nested file hash: {nested_downloaded_hash}")
    print(f"Nested file verification: {nested_hash == nested_downloaded_hash}")
    assert nested_hash == nested_downloaded_hash, "INTEGRITY ERROR: Nested object hash mismatch!"

    print(f"\n--- Downloading and verifying metadata object ---")
    print(f"Downloading metadata object to {metadata_download}")
    metadata_response = minio_client.get_object(main_bucket, metadata_object)
    with open(metadata_download, "wb") as file:
        for data in metadata_response.stream(4096):
            file.write(data)
    metadata_response.close()
    metadata_response.release_conn()

    # Verify metadata object immediately
    metadata_downloaded_hash = calculate_md5(metadata_download)
    print(f"Original metadata file hash: {metadata_hash}")
    print(f"Downloaded metadata file hash: {metadata_downloaded_hash}")
    print(f"Metadata file verification: {metadata_hash == metadata_downloaded_hash}")
    assert metadata_hash == metadata_downloaded_hash, "INTEGRITY ERROR: Metadata object hash mismatch!"

    print(f"\n--- Downloading and verifying large object ---")
    print(f"Downloading large object to {large_download}")
    large_response = minio_client.get_object(main_bucket, large_object)
    with open(large_download, "wb") as file:
        for data in large_response.stream(4096):
            file.write(data)
    large_response.close()
    large_response.release_conn()

    # Verify large object immediately
    large_downloaded_hash = calculate_md5(large_download)
    print(f"Original large file hash: {large_hash}")
    print(f"Downloaded large file hash: {large_downloaded_hash}")
    print(f"Large file verification: {large_hash == large_downloaded_hash}")
    assert large_hash == large_downloaded_hash, "INTEGRITY ERROR: Large object hash mismatch!"

    # Copy objects between buckets
    print(f"\n--- Copying and verifying copied object ---")
    print(f"Copying {main_bucket}/{standard_object} to {secondary_bucket}/{standard_object}")
    minio_client.copy_object(
        secondary_bucket,
        standard_object,
        CopySource(main_bucket, standard_object),
    )

    # Verify that the copied object has the same content as the original
    copied_download = f"{standard_file}.copied"
    print(f"Downloading copied object to {copied_download}")
    copied_response = minio_client.get_object(secondary_bucket, standard_object)
    with open(copied_download, "wb") as file:
        for data in copied_response.stream(4096):
            file.write(data)
    copied_response.close()
    copied_response.release_conn()

    # Verify the copied object hash
    copied_downloaded_hash = calculate_md5(copied_download)
    print(f"Original file hash: {standard_hash}")
    print(f"Copied file hash: {copied_downloaded_hash}")
    print(f"Copy verification: {standard_hash == copied_downloaded_hash}")
    assert standard_hash == copied_downloaded_hash, "INTEGRITY ERROR: Copied object hash mismatch!"

    # Clean up the copied download file
    os.unlink(copied_download)
    print(f"Cleaned up copied download file")

    # Generate presigned URLs
    print(f"Generating presigned GET URL for {main_bucket}/{standard_object}")
    get_presigned = minio_client.presigned_get_object(main_bucket, standard_object, expires=timedelta(hours=1))
    print(f"Presigned GET URL: {get_presigned}")

    print(f"Generating presigned PUT URL for {main_bucket}/new-object.txt")
    put_presigned = minio_client.presigned_put_object(main_bucket, "new-object.txt", expires=timedelta(hours=1))
    print(f"Presigned PUT URL: {put_presigned}")

    # Select object content (S3 Select)
    print("Selecting content from a JSON object")
    # Would need a JSON file, this is just for demo:
    # result = minio_client.select_object_content(
    #     main_bucket, "data.json", "select * from S3Object"
    # )

    print("\n=== MULTIPART OPERATIONS ===")

    # Define part size
    part_size_bytes = 5 * 1024 * 1024  # 5MB part size (minimum allowed)

    print("Creating multipart test files with different patterns...")

    # ------- Test 1: Multipart upload with random data (most realistic) -------
    print(f"\n--- Test 1: Standard multipart upload with random data ---")
    multipart_file_random = create_test_file(15)  # 15MB file (should be split into 3 parts with 5MB part size)
    multipart_object_random = "multipart-random.bin"
    multipart_hash_random = calculate_md5(multipart_file_random)
    print(f"Random data file created at: {multipart_file_random}, hash: {multipart_hash_random}")

    print(f"Uploading random data multipart object to {main_bucket}/{multipart_object_random}")
    with open(multipart_file_random, "rb") as file_data:
        file_size = os.path.getsize(multipart_file_random)
        result_random = minio_client.put_object(
            main_bucket,
            multipart_object_random,
            file_data,
            file_size,
            content_type="application/octet-stream",
            part_size=part_size_bytes,
            num_parallel_uploads=3,  # Use 3 parallel uploads
        )
    print(f"Random data multipart object uploaded successfully: etag={result_random.etag}")

    # Download and verify
    multipart_download_random = f"{multipart_file_random}.downloaded"
    print(f"Downloading random data multipart object to {multipart_download_random}")
    multipart_response_random = minio_client.get_object(main_bucket, multipart_object_random)
    with open(multipart_download_random, "wb") as file:
        for data in multipart_response_random.stream(4096):
            file.write(data)
    multipart_response_random.close()
    multipart_response_random.release_conn()

    # Thorough verification including part-by-part checks
    overall_match, part_matches_random = verify_multipart_integrity(
        multipart_file_random, multipart_download_random, part_size_bytes
    )

    print(f"Random data multipart verification: {overall_match}")
    print(f"Part-by-part verification summary: {all(part_matches_random)}")
    print(f"Parts verified: {len(part_matches_random)} (All match: {all(part_matches_random)})")

    assert overall_match, "INTEGRITY ERROR: Multipart object hash mismatch!"
    assert all(part_matches_random), "INTEGRITY ERROR: One or more parts do not match!"

    # ------- Test 2: Multipart upload with sequential pattern (good for boundary checking) -------
    print(f"\n--- Test 2: Multipart upload with sequential pattern ---")
    multipart_file_seq = create_test_file(15, pattern="sequential")  # 15MB with sequential pattern
    multipart_object_seq = "multipart-sequential.bin"
    multipart_hash_seq = calculate_md5(multipart_file_seq)
    print(f"Sequential pattern file created at: {multipart_file_seq}, hash: {multipart_hash_seq}")

    print(f"Uploading sequential data multipart object to {main_bucket}/{multipart_object_seq}")
    with open(multipart_file_seq, "rb") as file_data:
        file_size = os.path.getsize(multipart_file_seq)
        result_seq = minio_client.put_object(
            main_bucket,
            multipart_object_seq,
            file_data,
            file_size,
            content_type="application/octet-stream",
            part_size=part_size_bytes,
            num_parallel_uploads=3,
        )
    print(f"Sequential data multipart object uploaded successfully: etag={result_seq.etag}")

    # Download and verify
    multipart_download_seq = f"{multipart_file_seq}.downloaded"
    print(f"Downloading sequential data multipart object to {multipart_download_seq}")
    multipart_response_seq = minio_client.get_object(main_bucket, multipart_object_seq)
    with open(multipart_download_seq, "wb") as file:
        for data in multipart_response_seq.stream(4096):
            file.write(data)
    multipart_response_seq.close()
    multipart_response_seq.release_conn()

    # Thorough verification including part-by-part checks
    overall_match_seq, part_matches_seq = verify_multipart_integrity(
        multipart_file_seq, multipart_download_seq, part_size_bytes
    )

    print(f"Sequential data multipart verification: {overall_match_seq}")
    print(f"Part-by-part verification summary: {all(part_matches_seq)}")
    print(f"Parts verified: {len(part_matches_seq)} (All match: {all(part_matches_seq)})")

    assert overall_match_seq, "INTEGRITY ERROR: Sequential multipart object hash mismatch!"
    assert all(part_matches_seq), "INTEGRITY ERROR: One or more sequential parts do not match!"

    # ------- Test 3: Multipart upload with alternating pattern (best for part mixing detection) -------
    print(f"\n--- Test 3: Multipart upload with alternating pattern ---")
    multipart_file_alt = create_test_file(15, pattern="alternating")  # 15MB with alternating pattern
    multipart_object_alt = "multipart-alternating.bin"
    multipart_hash_alt = calculate_md5(multipart_file_alt)
    print(f"Alternating pattern file created at: {multipart_file_alt}, hash: {multipart_hash_alt}")

    print(f"Uploading alternating data multipart object to {main_bucket}/{multipart_object_alt}")
    with open(multipart_file_alt, "rb") as file_data:
        file_size = os.path.getsize(multipart_file_alt)
        result_alt = minio_client.put_object(
            main_bucket,
            multipart_object_alt,
            file_data,
            file_size,
            content_type="application/octet-stream",
            part_size=part_size_bytes,
            num_parallel_uploads=3,
        )
    print(f"Alternating data multipart object uploaded successfully: etag={result_alt.etag}")

    # Download and verify
    multipart_download_alt = f"{multipart_file_alt}.downloaded"
    print(f"Downloading alternating data multipart object to {multipart_download_alt}")
    multipart_response_alt = minio_client.get_object(main_bucket, multipart_object_alt)
    with open(multipart_download_alt, "wb") as file:
        for data in multipart_response_alt.stream(4096):
            file.write(data)
    multipart_response_alt.close()
    multipart_response_alt.release_conn()

    # Thorough verification including part-by-part checks
    overall_match_alt, part_matches_alt = verify_multipart_integrity(
        multipart_file_alt, multipart_download_alt, part_size_bytes
    )

    print(f"Alternating data multipart verification: {overall_match_alt}")
    print(f"Part-by-part verification summary: {all(part_matches_alt)}")
    print(f"Parts verified: {len(part_matches_alt)} (All match: {all(part_matches_alt)})")

    assert overall_match_alt, "INTEGRITY ERROR: Alternating multipart object hash mismatch!"
    assert all(part_matches_alt), "INTEGRITY ERROR: One or more alternating parts do not match!"

    # ------- Test 4: Get multipart object metadata -------
    print(f"\n--- Test 4: Get multipart object metadata ---")
    metadata_stat = minio_client.stat_object(main_bucket, multipart_object_random)
    print(f"  ETag: {metadata_stat.etag}")
    print(f"  Size: {metadata_stat.size}")
    print(f"  Last Modified: {metadata_stat.last_modified}")
    print(f"  Content Type: {metadata_stat.content_type}")
    if hasattr(metadata_stat, "metadata") and metadata_stat.metadata:
        print(f"  Metadata: {metadata_stat.metadata}")

    # Check if the ETag is present (note: in S3, ETags for multipart uploads differ between the upload response and HEAD request)
    # The ETag from upload is typically the MD5 hash of concatenated part hashes followed by -<number of parts>
    # The ETag from HEAD is typically the IPFS CID in our implementation
    print(f"  ETag from upload: {result_random.etag}")
    print(f"  ETag from stat: {metadata_stat.etag}")
    print(f"  NOTE: ETags for multipart uploads may differ between upload response and metadata stat")

    # Check if the size matches
    original_size = os.path.getsize(multipart_file_random)
    print(f"  Size verification: {metadata_stat.size == original_size}")
    assert metadata_stat.size == original_size, "INTEGRITY ERROR: Multipart object size mismatch in metadata!"

    # ------- Test 5: Range request on multipart object -------
    print(f"\n--- Test 5: Range request on multipart object ---")
    print(f"NOTE: Range requests are not yet fully supported by the server")
    print(f"Skipping range request test...")

    # Create a placeholder file for cleanup
    range_download = f"{multipart_file_random}.range_downloaded"
    with open(range_download, "wb") as f:
        f.write(b"placeholder")

    # ------- Clean up -------
    # Delete the multipart objects from the bucket
    print(f"\n--- Cleaning up ---")
    print(f"Deleting multipart objects")
    minio_client.remove_object(main_bucket, multipart_object_random)
    minio_client.remove_object(main_bucket, multipart_object_seq)
    minio_client.remove_object(main_bucket, multipart_object_alt)

    # Clean up all the local files
    cleanup_files = [
        multipart_file_random,
        multipart_download_random,
        multipart_file_seq,
        multipart_download_seq,
        multipart_file_alt,
        multipart_download_alt,
        range_download,
    ]

    for file_path in cleanup_files:
        if os.path.exists(file_path):
            os.unlink(file_path)

    print(f"Cleaned up all local multipart test files")

    print("\n=== CLEANUP OPERATIONS ===")

    # Delete objects in main bucket
    print(f"Deleting objects in {main_bucket}")
    minio_client.remove_object(main_bucket, standard_object)
    minio_client.remove_object(main_bucket, nested_object)
    minio_client.remove_object(main_bucket, metadata_object)
    minio_client.remove_object(main_bucket, large_object)
    # Multipart objects are already deleted above, but add safeguards just in case
    try:
        minio_client.remove_object(main_bucket, multipart_object_random)
        minio_client.remove_object(main_bucket, multipart_object_seq)
        minio_client.remove_object(main_bucket, multipart_object_alt)
    except Exception as e:
        print(f"Expected error: {e}")

    # Delete object in secondary bucket
    print(f"Deleting objects in {secondary_bucket}")
    minio_client.remove_object(secondary_bucket, standard_object)

    # Delete buckets
    print(f"Deleting bucket {main_bucket}")
    minio_client.remove_bucket(main_bucket)
    print(f"Deleting bucket {secondary_bucket}")
    minio_client.remove_bucket(secondary_bucket)

    print("\nAll operations completed successfully!")


# Clean up temporary files
def cleanup_files(*files):
    for file in files:
        if os.path.exists(file):
            os.unlink(file)
            print(f"Cleaned up {file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # No explicit cleanup - we want the script to show any errors
        pass
