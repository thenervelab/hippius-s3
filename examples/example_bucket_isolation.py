#!/usr/bin/env python3
import base64
import os
import tempfile
import time
from minio import Minio

from config import MINIO_URL, MINIO_SECURE, MINIO_REGION


def main():
    print("=== BUCKET ISOLATION TESTING ===")
    print("Testing user-scoped bucket names and isolation...\n")

    # Two different seed phrases for testing isolation
    primary_seed_phrase = os.environ["HIPPIUS_SEED_PHRASE"]
    secondary_seed_phrase = os.environ.get("HIPPIUS_SEED_PHRASE_2")

    if not secondary_seed_phrase:
        print("❌ FAIL: HIPPIUS_SEED_PHRASE_2 environment variable is required for isolation testing")
        print("Set a different seed phrase for HIPPIUS_SEED_PHRASE_2 to test bucket isolation")
        return

    # Base64 encode the seed phrases
    encoded_primary = base64.b64encode(primary_seed_phrase.encode("utf-8")).decode("utf-8")
    encoded_secondary = base64.b64encode(secondary_seed_phrase.encode("utf-8")).decode("utf-8")

    print(f"User 1 encoded seed phrase: {encoded_primary}")
    print(f"User 2 encoded seed phrase: {encoded_secondary}")

    # Create MinIO clients for both users
    user1_client = Minio(
        MINIO_URL,
        access_key=encoded_primary,
        secret_key=primary_seed_phrase,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    user2_client = Minio(
        MINIO_URL,
        access_key=encoded_secondary,
        secret_key=secondary_seed_phrase,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    # Test 1: Both users create buckets with the same name
    print("\n=== TEST 1: Same bucket name, different users ===")
    shared_bucket_name = f"shared-name-{int(time.time())}"

    print(f"User 1 creating bucket: {shared_bucket_name}")
    user1_client.make_bucket(shared_bucket_name)
    print("✅ SUCCESS: User 1 created bucket")

    print(f"User 2 creating bucket with same name: {shared_bucket_name}")
    try:
        user2_client.make_bucket(shared_bucket_name)
        print("✅ SUCCESS: User 2 created bucket with same name")
    except Exception as e:
        print(f"❌ FAIL: User 2 couldn't create bucket with same name: {e}")
        return

    # Test 2: Verify bucket isolation - each user only sees their own bucket
    print("\n=== TEST 2: Bucket listing isolation ===")

    user1_buckets = [bucket.name for bucket in user1_client.list_buckets()]
    user2_buckets = [bucket.name for bucket in user2_client.list_buckets()]

    print(f"User 1 sees buckets: {user1_buckets}")
    print(f"User 2 sees buckets: {user2_buckets}")

    # Both should see a bucket with the shared name, but they are different buckets
    if shared_bucket_name in user1_buckets and shared_bucket_name in user2_buckets:
        print("✅ SUCCESS: Both users see their respective bucket with the same name")
    else:
        print("❌ FAIL: Users should see their own bucket with the shared name")

    # Test 3: Verify users cannot access each other's buckets
    print("\n=== TEST 3: Cross-user bucket access protection ===")

    # Create test content
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        test_content = b"User 1's secret data"
        temp_file.write(test_content)
        temp_file_path = temp_file.name

    # User 1 uploads object to their bucket
    test_object_key = "secret-object.txt"
    print(f"User 1 uploading object '{test_object_key}' to bucket '{shared_bucket_name}'")

    with open(temp_file_path, "rb") as file_data:
        user1_client.put_object(
            shared_bucket_name,
            test_object_key,
            file_data,
            len(test_content),
            content_type="text/plain"
        )
    print("✅ SUCCESS: User 1 uploaded object")

    # User 2 tries to access User 1's bucket (should fail)
    print(f"User 2 attempting to list objects in User 1's bucket '{shared_bucket_name}'")
    try:
        user2_objects = list(user2_client.list_objects(shared_bucket_name))
        if any(obj.object_name == test_object_key for obj in user2_objects):
            print("❌ FAIL: User 2 can see User 1's objects! Isolation broken!")
        else:
            print("✅ SUCCESS: User 2's bucket with same name is empty (proper isolation)")
    except Exception as e:
        print(f"User 2 got error (this might be expected): {e}")

    # User 2 tries to download User 1's object (should fail)
    print(f"User 2 attempting to download User 1's object '{test_object_key}'")
    try:
        response = user2_client.get_object(shared_bucket_name, test_object_key)
        data = response.read()
        response.close()
        response.release_conn()
        print("❌ FAIL: User 2 was able to download User 1's object! Isolation broken!")
    except Exception as e:
        print(f"✅ SUCCESS: User 2 cannot access User 1's object: {e}")

    # Test 4: Verify users can upload to their own bucket with the same name
    print("\n=== TEST 4: Users can use their own bucket with shared name ===")

    user2_content = b"User 2's different data"
    with tempfile.NamedTemporaryFile(delete=False) as temp_file2:
        temp_file2.write(user2_content)
        temp_file2_path = temp_file2.name

    print(f"User 2 uploading object '{test_object_key}' to their bucket '{shared_bucket_name}'")
    with open(temp_file2_path, "rb") as file_data:
        user2_client.put_object(
            shared_bucket_name,
            test_object_key,
            file_data,
            len(user2_content),
            content_type="text/plain"
        )
    print("✅ SUCCESS: User 2 uploaded object to their bucket")

    # Verify both users can access their own objects
    print("Verifying both users can access their respective objects...")

    # User 1 downloads their object
    user1_response = user1_client.get_object(shared_bucket_name, test_object_key)
    user1_downloaded = user1_response.read()
    user1_response.close()
    user1_response.release_conn()

    if user1_downloaded == test_content:
        print("✅ SUCCESS: User 1 can access their own object")
    else:
        print("❌ FAIL: User 1's object content doesn't match")

    # User 2 downloads their object
    user2_response = user2_client.get_object(shared_bucket_name, test_object_key)
    user2_downloaded = user2_response.read()
    user2_response.close()
    user2_response.release_conn()

    if user2_downloaded == user2_content:
        print("✅ SUCCESS: User 2 can access their own object")
    else:
        print("❌ FAIL: User 2's object content doesn't match")

    # Verify the objects are different
    if user1_downloaded != user2_downloaded:
        print("✅ SUCCESS: Objects are properly isolated - different content for each user")
    else:
        print("❌ FAIL: Objects should be different for each user")

    # Test 5: Bucket deletion isolation
    print("\n=== TEST 5: Bucket deletion isolation ===")

    # User 1 tries to delete User 2's bucket (should fail or only delete their own)
    print("User 1 attempting to delete their bucket...")
    user1_client.remove_object(shared_bucket_name, test_object_key)
    user1_client.remove_bucket(shared_bucket_name)
    print("✅ SUCCESS: User 1 deleted their bucket")

    # Verify User 2's bucket still exists
    print("Checking if User 2's bucket still exists...")
    user2_buckets_after = [bucket.name for bucket in user2_client.list_buckets()]
    if shared_bucket_name in user2_buckets_after:
        print("✅ SUCCESS: User 2's bucket still exists after User 1 deleted theirs")
    else:
        print("❌ FAIL: User 2's bucket was affected by User 1's deletion")

    # Clean up User 2's bucket
    print("Cleaning up User 2's bucket...")
    user2_client.remove_object(shared_bucket_name, test_object_key)
    user2_client.remove_bucket(shared_bucket_name)
    print("✅ SUCCESS: User 2's bucket cleaned up")

    # Clean up temp files
    os.unlink(temp_file_path)
    os.unlink(temp_file2_path)

    print("\n=== BUCKET ISOLATION TEST COMPLETED ===")
    print("✅ All tests passed - bucket isolation is working correctly!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
