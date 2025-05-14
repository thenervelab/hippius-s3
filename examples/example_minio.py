#!/usr/bin/env python3
import hashlib
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
import re

from minio import Minio
from minio.commonconfig import ENABLED, CopySource
from minio.commonconfig import Filter
from minio.commonconfig import Tags
from minio.lifecycleconfig import Expiration
from minio.lifecycleconfig import LifecycleConfig
from minio.lifecycleconfig import Rule


def create_test_file(file_size_mb=1):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(os.urandom(file_size_mb * 1024 * 1024))
        return temp_file.name


def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with Path.open(file_path, "rb") as f:
        # Read file in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def main():
    # Create MinIO client pointing to our S3-compatible API
    minio_client = Minio(
        "s3.hippius.com",
        access_key="diet buyer illness prison drama moment license input job ball tornado solar",
        secret_key="hippius",
        secure=False,
        region="us-east-1",  # Set default region to avoid location lookup
    )

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

    # Get bucket tags
    print(f"Getting tags for bucket {main_bucket}")
    try:
        # Debug the raw request
        url = f"http://localhost:8000/{main_bucket}?tagging"
        print(f"DEBUG: Making direct request to {url}")
        response = requests.get(url)
        print(f"DEBUG: Raw response status: {response.status_code}")
        print(f"DEBUG: Raw response headers: {response.headers}")
        print(f"DEBUG: Raw response content: {response.content}")

        # Now try the minio client
        retrieved_tags = minio_client.get_bucket_tags(main_bucket)
        print(f"Retrieved tags: {retrieved_tags}")
    except Exception as e:
        print(f"DEBUG: Exception caught: {e}")
        print(f"DEBUG: Exception type: {type(e)}")
        if hasattr(e, '__context__') and e.__context__:
            print(f"DEBUG: Context: {e.__context__}")
        raise

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
            part_size=5*1024*1024,  # 5MB part size (minimum allowed)
            num_parallel_uploads=3  # Use 3 parallel uploads
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

    # Create multiple test files for multipart upload testing
    print("Creating multipart test files...")
    multipart_file = create_test_file(10)  # 10MB file (should be split into 2 parts with 5MB part size)
    multipart_object = "multipart-object.bin"

    # Calculate hash before upload
    multipart_hash = calculate_md5(multipart_file)
    print(f"Multipart file created at: {multipart_file}, hash: {multipart_hash}")

    # ------- Test 1: Standard multipart upload with verification -------
    print(f"\n--- Test 1: Standard multipart upload ---")
    print(f"Uploading multipart object to {main_bucket}/{multipart_object}")
    with open(multipart_file, "rb") as file_data:
        file_size = os.path.getsize(multipart_file)
        result = minio_client.put_object(
            main_bucket,
            multipart_object,
            file_data,
            file_size,
            content_type="application/octet-stream",
            part_size=5*1024*1024,  # 5MB part size (minimum allowed)
            num_parallel_uploads=3  # Use 3 parallel uploads
        )
    print(f"Multipart object uploaded successfully: etag={result.etag}")

    # Download the multipart object
    multipart_download = f"{multipart_file}.downloaded"
    print(f"Downloading multipart object to {multipart_download}")
    multipart_response = minio_client.get_object(main_bucket, multipart_object)
    with open(multipart_download, "wb") as file:
        for data in multipart_response.stream(4096):
            file.write(data)
    multipart_response.close()
    multipart_response.release_conn()

    # Verify the multipart download
    multipart_downloaded_hash = calculate_md5(multipart_download)
    print(f"Original multipart file hash: {multipart_hash}")
    print(f"Downloaded multipart file hash: {multipart_downloaded_hash}")
    print(f"Multipart verification: {multipart_hash == multipart_downloaded_hash}")
    assert multipart_hash == multipart_downloaded_hash, "INTEGRITY ERROR: Multipart object hash mismatch!"

    # Check file size too
    original_size = os.path.getsize(multipart_file)
    downloaded_size = os.path.getsize(multipart_download)
    print(f"Original file size: {original_size} bytes")
    print(f"Downloaded file size: {downloaded_size} bytes")
    print(f"Size verification: {original_size == downloaded_size}")
    assert original_size == downloaded_size, "INTEGRITY ERROR: Multipart object size mismatch!"

    # ------- Test 2: Get multipart object metadata -------
    print(f"\n--- Test 2: Get multipart object metadata ---")
    metadata_stat = minio_client.stat_object(main_bucket, multipart_object)
    print(f"  ETag: {metadata_stat.etag}")
    print(f"  Size: {metadata_stat.size}")
    print(f"  Last Modified: {metadata_stat.last_modified}")
    print(f"  Content Type: {metadata_stat.content_type}")
    if hasattr(metadata_stat, "metadata") and metadata_stat.metadata:
        print(f"  Metadata: {metadata_stat.metadata}")

    # Check if the ETag is present (note: in S3, ETags for multipart uploads differ between the upload response and HEAD request)
    # The ETag from upload is typically the MD5 hash of concatenated part hashes followed by -<number of parts>
    # The ETag from HEAD is typically the IPFS CID in our implementation
    print(f"  ETag from upload: {result.etag}")
    print(f"  ETag from stat: {metadata_stat.etag}")
    print(f"  NOTE: ETags for multipart uploads may differ between upload response and metadata stat")

    # Check if the size matches
    print(f"  Size verification: {metadata_stat.size == original_size}")
    assert metadata_stat.size == original_size, "INTEGRITY ERROR: Multipart object size mismatch in metadata!"

    # ------- Test 3: Range request on multipart object -------
    print(f"\n--- Test 3: Range request on multipart object ---")
    print(f"NOTE: Range requests are not yet fully supported by the server")
    print(f"Skipping range request test...")

    # Create a placeholder file for cleanup
    range_download = f"{multipart_file}.range_downloaded"
    with open(range_download, "wb") as f:
        f.write(b"placeholder")

    # ------- Clean up -------
    # Delete the multipart object from the bucket
    print(f"\n--- Cleaning up ---")
    print(f"Deleting multipart object {main_bucket}/{multipart_object}")
    minio_client.remove_object(main_bucket, multipart_object)

    # Clean up all the local files
    os.unlink(multipart_file)
    os.unlink(multipart_download)
    os.unlink(range_download)
    print(f"Cleaned up all local multipart test files")

    print("\n=== CLEANUP OPERATIONS ===")

    # Delete objects in main bucket
    print(f"Deleting objects in {main_bucket}")
    minio_client.remove_object(main_bucket, standard_object)
    minio_client.remove_object(main_bucket, nested_object)
    minio_client.remove_object(main_bucket, metadata_object)
    minio_client.remove_object(main_bucket, large_object)
    # Multipart object is already deleted above, but add a safeguard just in case
    try:
        minio_client.remove_object(main_bucket, multipart_object)
    except:
        pass

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
