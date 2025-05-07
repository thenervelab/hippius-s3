#!/usr/bin/env python3
import hashlib
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from minio import Minio
from minio.commonconfig import ENABLED
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
        "localhost:8000",  # These credentials aren't actually checked by our API
        # but are required by the MinIO client
        access_key="123",
        secret_key="456",
        secure=False,
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
    retrieved_tags = minio_client.get_bucket_tags(main_bucket)
    print(f"Retrieved tags: {retrieved_tags}")

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

    # Upload large object (which might trigger multipart upload)
    print(f"Uploading large object to {main_bucket}/{large_object}")
    with open(large_file, "rb") as file_data:
        large_size = os.path.getsize(large_file)
        large_result = minio_client.put_object(
            main_bucket,
            large_object,
            file_data,
            large_size,
            content_type="application/octet-stream",
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
    print(f"Downloading standard object to {standard_download}")
    standard_response = minio_client.get_object(main_bucket, standard_object)
    with open(standard_download, "wb") as file:
        for data in standard_response.stream(4096):
            file.write(data)
    standard_response.close()
    standard_response.release_conn()

    print(f"Downloading nested object to {nested_download}")
    nested_response = minio_client.get_object(main_bucket, nested_object)
    with open(nested_download, "wb") as file:
        for data in nested_response.stream(4096):
            file.write(data)
    nested_response.close()
    nested_response.release_conn()

    print(f"Downloading metadata object to {metadata_download}")
    metadata_response = minio_client.get_object(main_bucket, metadata_object)
    with open(metadata_download, "wb") as file:
        for data in metadata_response.stream(4096):
            file.write(data)
    metadata_response.close()
    metadata_response.release_conn()

    print(f"Downloading large object to {large_download}")
    large_response = minio_client.get_object(main_bucket, large_object)
    with open(large_download, "wb") as file:
        for data in large_response.stream(4096):
            file.write(data)
    large_response.close()
    large_response.release_conn()

    # Verify downloaded files
    standard_downloaded_hash = calculate_md5(standard_download)
    nested_downloaded_hash = calculate_md5(nested_download)
    metadata_downloaded_hash = calculate_md5(metadata_download)
    large_downloaded_hash = calculate_md5(large_download)

    print(f"Standard file verification: {standard_hash == standard_downloaded_hash}")
    print(f"Nested file verification: {nested_hash == nested_downloaded_hash}")
    print(f"Metadata file verification: {metadata_hash == metadata_downloaded_hash}")
    print(f"Large file verification: {large_hash == large_downloaded_hash}")

    # Copy objects between buckets
    print(f"Copying {main_bucket}/{standard_object} to {secondary_bucket}/{standard_object}")
    minio_client.copy_object(
        secondary_bucket,
        standard_object,
        f"{main_bucket}/{standard_object}",
    )

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

    # Multipart upload
    print("\n=== MULTIPART OPERATIONS ===")

    # Initiate multipart upload
    print(f"Initiating multipart upload to {main_bucket}/multipart-object.bin")
    upload_id = minio_client.create_multipart_upload(main_bucket, "multipart-object.bin", {})
    print(f"Multipart upload initiated with ID: {upload_id}")

    # List in-progress multipart uploads
    print(f"Listing multipart uploads in {main_bucket}")
    uploads = minio_client.list_multipart_uploads(main_bucket)
    for upload in uploads:
        print(f"  Upload ID: {upload.upload_id}, Object: {upload.object_name}")

    # We could do actual part uploads here, but for brevity we'll skip to abort
    # Abort multipart upload
    print(f"Aborting multipart upload {upload_id}")
    minio_client.abort_multipart_upload(main_bucket, "multipart-object.bin", upload_id)

    print("\n=== CLEANUP OPERATIONS ===")

    # Delete objects in main bucket
    print(f"Deleting objects in {main_bucket}")
    minio_client.remove_object(main_bucket, standard_object)
    minio_client.remove_object(main_bucket, nested_object)
    minio_client.remove_object(main_bucket, metadata_object)
    minio_client.remove_object(main_bucket, large_object)

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
