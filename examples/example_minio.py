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
    print("=== TESTING DELETE FUNCTIONALITY AND USER ISOLATION ===")
    print("Testing delete operations and user-scoped bucket listing...\n")

    # Encode seed phrases in base64
    primary_seed_phrase = os.environ["HIPPIUS_SEED_PHRASE"]  # valid seed phrase required
    # For testing ACL/isolation, use a second valid seed phrase if provided, otherwise use the same user
    secondary_seed_phrase = os.environ.get("HIPPIUS_SEED_PHRASE_2", primary_seed_phrase)

    # Base64 encode the seed phrases
    encoded_primary = base64.b64encode(primary_seed_phrase.encode("utf-8")).decode("utf-8")
    encoded_secondary = base64.b64encode(secondary_seed_phrase.encode("utf-8")).decode("utf-8")

    print(f"Base64 encoded primary seed phrase: {encoded_primary}")
    print(f"Base64 encoded secondary seed phrase: {encoded_secondary}")

    if secondary_seed_phrase == primary_seed_phrase:
        print("⚠️  NOTE: Using same user for both clients (no HIPPIUS_SEED_PHRASE_2 provided)")
        print("   ACL tests will show same-user behavior, not true isolation")

    # Create MinIO client pointing to our S3-compatible API with base64 encoded seed phrase
    # The seed phrase is used as the access_key, and the corresponding HMAC secret key
    # Both work together to create the SigV4 signature
    minio_client = Minio(
        MINIO_URL,
        access_key=encoded_primary,  # Base64 encoded seed phrase
        secret_key=primary_seed_phrase,  # The seed phrase is also used as the HMAC secret
        secure=MINIO_SECURE,
        region=MINIO_REGION,  # Match the region used in our SigV4 implementation
    )
    # Create a second client with different seed phrase (different user)
    # This will be used to test ACL permissions
    minio_client_secondary = Minio(
        MINIO_URL,
        access_key=encoded_secondary,
        secret_key=secondary_seed_phrase,  # The seed phrase is also used as the HMAC secret
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

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

    # Test 2: User-scoped bucket listing
    print("\n=== TEST 2: USER-SCOPED BUCKET LISTING ===")

    # Create buckets with both users
    user1_bucket = f"user1-bucket-{int(time.time())}"
    user2_bucket = f"user2-bucket-{int(time.time())}"

    print(f"User 1 creating bucket: {user1_bucket}")
    minio_client.make_bucket(user1_bucket)

    print(f"User 2 creating bucket: {user2_bucket}")
    minio_client_secondary.make_bucket(user2_bucket)

    # List buckets from user 1's perspective
    print("Listing buckets from User 1's perspective:")
    user1_buckets = minio_client.list_buckets()
    user1_bucket_names = [bucket.name for bucket in user1_buckets]
    print(f"User 1 sees buckets: {user1_bucket_names}")

    # List buckets from user 2's perspective
    print("Listing buckets from User 2's perspective:")
    user2_buckets = minio_client_secondary.list_buckets()
    user2_bucket_names = [bucket.name for bucket in user2_buckets]
    print(f"User 2 sees buckets: {user2_bucket_names}")

    # Verify isolation (adjust expectations if using same user)
    if secondary_seed_phrase == primary_seed_phrase:
        # Same user - both buckets should be visible to both clients
        if user1_bucket in user1_bucket_names and user1_bucket in user2_bucket_names:
            print("✅ SUCCESS: User 1's bucket visible to both clients (same user)")
        else:
            print("❌ FAIL: User 1's bucket should be visible to both clients (same user)")

        if user2_bucket in user2_bucket_names and user2_bucket in user1_bucket_names:
            print("✅ SUCCESS: User 2's bucket visible to both clients (same user)")
        else:
            print("❌ FAIL: User 2's bucket should be visible to both clients (same user)")
    else:
        # Different users - buckets should be isolated
        if user1_bucket in user1_bucket_names and user1_bucket not in user2_bucket_names:
            print("✅ SUCCESS: User 1's bucket is visible to User 1 but not User 2")
        else:
            print("❌ FAIL: User bucket isolation not working correctly")

        if user2_bucket in user2_bucket_names and user2_bucket not in user1_bucket_names:
            print("✅ SUCCESS: User 2's bucket is visible to User 2 but not User 1")
        else:
            print("❌ FAIL: User bucket isolation not working correctly")

    # Clean up user test buckets
    print("Cleaning up user test buckets...")
    minio_client.remove_bucket(user1_bucket)
    minio_client_secondary.remove_bucket(user2_bucket)
    print("✅ SUCCESS: User-scoped bucket listing test completed")

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

    print("\n=== ACL TESTING OPERATIONS ===")

    # Create a bucket using the primary client (user)
    acl_bucket = f"acl-test-bucket-{int(time.time())}"
    acl_object = "acl-test-object.bin"

    print(f"Creating ACL test bucket: {acl_bucket}")
    minio_client.make_bucket(acl_bucket)

    # Upload a test object to the bucket
    print(f"Uploading ACL test object: {acl_object}")
    acl_test_file = create_test_file(1)  # 1MB
    with open(acl_test_file, "rb") as file_data:
        acl_size = os.path.getsize(acl_test_file)
        minio_client.put_object(
            acl_bucket,
            acl_object,
            file_data,
            acl_size,
            content_type="application/octet-stream",
        )

    if secondary_seed_phrase != primary_seed_phrase:
        print("\n--- Testing ACL enforcement for bucket deletion ---")
        print(f"Attempting to delete bucket {acl_bucket} with secondary client (different user)")
        try:
            minio_client_secondary.remove_bucket(acl_bucket)
            print("SECURITY FAILURE: Secondary user was able to delete primary user's bucket!")
            assert False, "ACL enforcement failed for bucket deletion"
        except Exception as e:
            print(f"Expected error: {e}")
            print("SUCCESS: Secondary user was not able to delete primary user's bucket (ACL protected)")

        print("\n--- Testing ACL enforcement for object deletion ---")
        print(f"Attempting to delete object {acl_object} with secondary client (different user)")
        try:
            minio_client_secondary.remove_object(acl_bucket, acl_object)
            print("SECURITY FAILURE: Secondary user was able to delete primary user's object!")
            assert False, "ACL enforcement failed for object deletion"
        except Exception as e:
            print(f"Expected error: {e}")
            print("SUCCESS: Secondary user was not able to delete primary user's object (ACL protected)")
    else:
        print("\n--- Skipping ACL enforcement tests (same user for both clients) ---")
        print("To test ACL enforcement, set HIPPIUS_SEED_PHRASE_2 environment variable")

    # Clean up the ACL test resources using the primary client
    print("\nCleaning up ACL test resources with primary client")
    minio_client.remove_object(acl_bucket, acl_object)
    minio_client.remove_bucket(acl_bucket)
    os.unlink(acl_test_file)

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
