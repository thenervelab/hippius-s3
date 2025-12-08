import hashlib
import random
from datetime import datetime

import pytest


def test_01_cleanup_old_files(production_s3_client, session_tracker):
    from datetime import timedelta

    bucket = session_tracker.bucket
    prefix = "smoke-test/"
    retention_days = 30
    cutoff = datetime.utcnow() - timedelta(days=retention_days)

    deleted_count = 0
    paginator = production_s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) < 2:
                continue

            if parts[1] == ".index":
                continue

            try:
                session_ts = datetime.strptime(parts[1], "%Y%m%d-%H%M%S")
                if session_ts < cutoff:
                    production_s3_client.delete_object(Bucket=bucket, Key=key)
                    deleted_count += 1
            except ValueError:
                continue

    print(f"Cleanup test: deleted {deleted_count} files older than {retention_days} days")


def test_02_upload_simple_file(production_s3_client, session_tracker, file_generator):
    size = 1 * 1024 * 1024
    data, hash_md5 = file_generator(size)

    key = f"smoke-test/{session_tracker.session_id}/simple/{hash_md5}.bin"
    upload_time = datetime.utcnow().isoformat()

    response = production_s3_client.put_object(
        Bucket=session_tracker.bucket,
        Key=key,
        Body=data,
        Metadata={
            "session-id": session_tracker.session_id,
            "file-type": "simple",
            "size": str(size),
            "hash": hash_md5,
            "upload-time": upload_time,
        },
    )

    assert "ETag" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    etag = response["ETag"].strip('"')
    assert not etag.endswith("-")

    session_tracker.add_file(
        key=key, file_type="simple", size=size, hash_md5=hash_md5, upload_time=upload_time
    )

    print(f"Uploaded simple file: {key} (1 MB, hash={hash_md5[:8]}...)")


def test_03_upload_multipart_file(production_s3_client, session_tracker, file_generator):
    total_size = 24 * 1024 * 1024
    part_size = 5 * 1024 * 1024
    part_count = 5

    data, hash_md5 = file_generator(total_size)

    key = f"smoke-test/{session_tracker.session_id}/multipart/{hash_md5}.bin"
    upload_time = datetime.utcnow().isoformat()

    create_response = production_s3_client.create_multipart_upload(
        Bucket=session_tracker.bucket,
        Key=key,
        Metadata={
            "session-id": session_tracker.session_id,
            "file-type": "multipart",
            "size": str(total_size),
            "hash": hash_md5,
            "upload-time": upload_time,
        },
    )

    assert "UploadId" in create_response
    upload_id = create_response["UploadId"]

    part_etags = []
    for i in range(part_count):
        start = i * part_size
        end = start + part_size
        part_data = data[start:end]

        part_response = production_s3_client.upload_part(
            Bucket=session_tracker.bucket, Key=key, UploadId=upload_id, PartNumber=i + 1, Body=part_data
        )

        etag = part_response["ETag"]
        part_etags.append({"ETag": etag, "PartNumber": i + 1})

    assert len(part_etags) == part_count

    complete_response = production_s3_client.complete_multipart_upload(
        Bucket=session_tracker.bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": part_etags}
    )

    assert "ETag" in complete_response
    final_etag = complete_response["ETag"].strip('"')
    assert final_etag.endswith(f"-{part_count}")

    session_tracker.add_file(
        key=key, file_type="multipart", size=total_size, hash_md5=hash_md5, upload_time=upload_time
    )

    print(f"Uploaded multipart file: {key} (24 MB, {part_count} parts, hash={hash_md5[:8]}...)")


def test_04_download_current_session_files(production_s3_client, session_tracker):
    files = session_tracker.get_files()
    assert len(files) == 2

    for file_info in files:
        key = file_info["key"]
        expected_hash = file_info["hash"]
        expected_size = file_info["size"]

        response = production_s3_client.get_object(Bucket=session_tracker.bucket, Key=key)

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        downloaded_data = response["Body"].read()
        downloaded_hash = hashlib.md5(downloaded_data).hexdigest()

        assert downloaded_hash == expected_hash
        assert len(downloaded_data) == expected_size

        print(
            f"Downloaded and validated: {key} ({expected_size} bytes, hash={expected_hash[:8]}...)"
        )


def test_05_download_historical_files(production_s3_client, session_tracker):
    session_keys = session_tracker.list_historical_sessions(max_count=20)

    if not session_keys:
        pytest.skip("No historical sessions found (first run)")

    print(f"Found {len(session_keys)} historical session(s)")

    for session_key in session_keys:
        manifest = session_tracker.get_manifest(session_key.split("/")[-1].replace(".json", ""))

        if not manifest.get("files"):
            continue

        file_info = random.choice(manifest["files"])

        response = production_s3_client.get_object(Bucket=session_tracker.bucket, Key=file_info["key"])
        data = response["Body"].read()

        assert len(data) == file_info["size"]
        assert hashlib.md5(data).hexdigest() == file_info["hash"]

        print(
            f"Validated historical file: {file_info['key']} (session={manifest['session_id']}, hash={file_info['hash'][:8]}...)"
        )


def test_06_write_session_manifest(production_s3_client, session_tracker):
    session_tracker.save_manifest()

    manifest_key = f"smoke-test/.index/{session_tracker.session_id}.json"

    verify_response = production_s3_client.get_object(Bucket=session_tracker.bucket, Key=manifest_key)

    assert verify_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    import json

    retrieved_manifest = json.loads(verify_response["Body"].read())

    assert retrieved_manifest["session_id"] == session_tracker.session_id
    assert len(retrieved_manifest["files"]) == 2

    print(f"Session manifest saved: {manifest_key}")
