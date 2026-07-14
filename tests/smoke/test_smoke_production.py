import hashlib
import os
import random
import uuid
from datetime import datetime
from datetime import timezone

import httpx
import pytest


def test_01_cleanup_old_files(production_s3_client, session_tracker):
    from datetime import timedelta

    bucket = session_tracker.bucket
    prefix = "smoke-test/"
    retention_days = 30
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

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
                # Sweep stale manifests too — keeping them around after their
                # data files were deleted is what causes test_05 to download
                # NoSuchKey ghosts.
                if len(parts) < 3:
                    continue
                manifest_session_id = parts[2].removesuffix(".json")
                try:
                    manifest_ts = datetime.strptime(manifest_session_id, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if manifest_ts < cutoff:
                    production_s3_client.delete_object(Bucket=bucket, Key=key)
                    deleted_count += 1
                continue

            try:
                session_ts = datetime.strptime(parts[1], "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
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
    upload_time = datetime.now(timezone.utc).isoformat()

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

    assert "ETag" in response, "Simple 1 MB PUT did not return an ETag — the upload was not accepted/acknowledged by the gateway."
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, (
        f"Simple 1 MB PUT returned HTTP {response['ResponseMetadata']['HTTPStatusCode']} instead of 200 — object upload is failing."
    )
    etag = response["ETag"].strip('"')
    assert not etag.endswith("-"), "Simple PUT returned a multipart-style ETag (ends with '-') — a single-part upload should have a plain MD5 ETag."

    session_tracker.add_file(key=key, file_type="simple", size=size, hash_md5=hash_md5, upload_time=upload_time)

    print(f"Uploaded simple file: {key} (1 MB, hash={hash_md5[:8]}...)")


def test_03_upload_multipart_file(production_s3_client, session_tracker, file_generator):
    total_size = 24 * 1024 * 1024
    part_size = 5 * 1024 * 1024
    part_count = 5

    data, hash_md5 = file_generator(total_size)

    key = f"smoke-test/{session_tracker.session_id}/multipart/{hash_md5}.bin"
    upload_time = datetime.now(timezone.utc).isoformat()

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

    assert "UploadId" in create_response, "CreateMultipartUpload did not return an UploadId — multipart uploads cannot be started."
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

    assert len(part_etags) == part_count, (
        f"Only {len(part_etags)} of {part_count} multipart parts uploaded successfully — one or more UploadPart calls failed."
    )

    complete_response = production_s3_client.complete_multipart_upload(
        Bucket=session_tracker.bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": part_etags}
    )

    assert "ETag" in complete_response, "CompleteMultipartUpload did not return an ETag — the multipart upload failed to finalize."
    final_etag = complete_response["ETag"].strip('"')
    assert final_etag.endswith(f"-{part_count}"), (
        f"Final multipart ETag {final_etag!r} does not end with '-{part_count}' — the completed object was not assembled from all {part_count} parts."
    )

    session_tracker.add_file(
        key=key, file_type="multipart", size=total_size, hash_md5=hash_md5, upload_time=upload_time
    )

    print(f"Uploaded multipart file: {key} (24 MB, {part_count} parts, hash={hash_md5[:8]}...)")


def test_04_download_current_session_files(production_s3_client, session_tracker):
    files = session_tracker.get_files()
    assert len(files) == 2, (
        f"Expected 2 files uploaded this session (simple + multipart), tracker has {len(files)} — an earlier upload test did not record its file."
    )

    for file_info in files:
        key = file_info["key"]
        expected_hash = file_info["hash"]
        expected_size = file_info["size"]

        response = production_s3_client.get_object(Bucket=session_tracker.bucket, Key=key)

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, (
            f"GET of just-uploaded object {key} returned HTTP {response['ResponseMetadata']['HTTPStatusCode']} instead of 200 — download of a fresh object is failing."
        )

        downloaded_data = response["Body"].read()
        downloaded_hash = hashlib.md5(downloaded_data).hexdigest()

        assert downloaded_hash == expected_hash, (
            f"Downloaded content of {key} does not match what was uploaded (MD5 {downloaded_hash} != {expected_hash}) — data is being corrupted in the upload/download/decrypt path."
        )
        assert len(downloaded_data) == expected_size, (
            f"Downloaded {key} is {len(downloaded_data)} bytes but {expected_size} were uploaded — the object is being truncated or padded."
        )

        print(f"Downloaded and validated: {key} ({expected_size} bytes, hash={expected_hash[:8]}...)")


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

        print(f"Attempting to download {file_info=} from {session_tracker.bucket=}")

        response = production_s3_client.get_object(Bucket=session_tracker.bucket, Key=file_info["key"])
        data = response["Body"].read()

        assert len(data) == file_info["size"], (
            f"Historical object {file_info['key']} is {len(data)} bytes but its manifest recorded {file_info['size']} — a previously-stored object came back the wrong size (data durability/retrieval regression)."
        )
        assert hashlib.md5(data).hexdigest() == file_info["hash"], (
            f"Historical object {file_info['key']} failed MD5 check — an object uploaded in a past session no longer decrypts/reads back to its original content."
        )

        print(
            f"Validated historical file: {file_info['key']} (session={manifest['session_id']}, hash={file_info['hash'][:8]}...)"
        )


def test_06_write_session_manifest(production_s3_client, session_tracker):
    session_tracker.save_manifest()

    manifest_key = f"smoke-test/.index/{session_tracker.session_id}.json"

    verify_response = production_s3_client.get_object(Bucket=session_tracker.bucket, Key=manifest_key)

    assert verify_response["ResponseMetadata"]["HTTPStatusCode"] == 200, (
        f"Reading back the session manifest {manifest_key} returned HTTP {verify_response['ResponseMetadata']['HTTPStatusCode']} instead of 200 — the manifest we just wrote is not retrievable."
    )

    import json

    retrieved_manifest = json.loads(verify_response["Body"].read())

    assert retrieved_manifest["session_id"] == session_tracker.session_id, (
        f"Manifest read back has session_id {retrieved_manifest['session_id']!r} but we wrote {session_tracker.session_id!r} — the manifest content was not persisted correctly."
    )
    assert len(retrieved_manifest["files"]) == 2, (
        f"Manifest read back lists {len(retrieved_manifest['files'])} files, expected 2 — the manifest we persisted is missing this session's uploads."
    )

    print(f"Session manifest saved: {manifest_key}")


def test_07_presigned_url_roundtrip(production_s3_client, session_tracker, file_generator):
    size = 1 * 1024 * 1024
    data, expected_hash = file_generator(size)
    key = f"smoke-test/{session_tracker.session_id}/presigned/{uuid.uuid4()}.bin"

    put_url = production_s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": session_tracker.bucket, "Key": key},
        ExpiresIn=300,
    )
    put_resp = httpx.put(put_url, content=data, timeout=60)
    assert put_resp.status_code == 200, f"presigned PUT failed: {put_resp.status_code} {put_resp.text[:200]}"

    get_url = production_s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": session_tracker.bucket, "Key": key},
        ExpiresIn=300,
    )
    get_resp = httpx.get(get_url, timeout=60)
    assert get_resp.status_code == 200, f"presigned GET failed: {get_resp.status_code} {get_resp.text[:200]}"

    downloaded_hash = hashlib.md5(get_resp.content).hexdigest()
    assert downloaded_hash == expected_hash, f"hash mismatch: {downloaded_hash} != {expected_hash}"
    assert len(get_resp.content) == size, (
        f"Object fetched via presigned GET is {len(get_resp.content)} bytes but {size} were uploaded via presigned PUT — the presigned URL round-trip is truncating data."
    )

    print(f"Presigned roundtrip validated: {key} ({size} bytes, hash={expected_hash[:8]}...)")


def test_08_cors_preflight_on_presigned_url(production_s3_client, session_tracker):
    key = f"smoke-test/{session_tracker.session_id}/cors-probe-{uuid.uuid4()}.bin"
    put_url = production_s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": session_tracker.bucket, "Key": key},
        ExpiresIn=60,
    )

    resp = httpx.options(
        put_url,
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "content-type",
        },
        timeout=10,
    )

    assert resp.status_code in (200, 204), (
        f"preflight failed (ATS may be rejecting OPTIONS): status={resp.status_code} body={resp.text[:200]}"
    )
    assert resp.headers.get("Access-Control-Allow-Origin") == "*", (
        f"missing/unexpected Access-Control-Allow-Origin: {resp.headers.get('Access-Control-Allow-Origin')!r}"
    )
    allow_methods = resp.headers.get("Access-Control-Allow-Methods", "")
    assert "PUT" in allow_methods, f"PUT not in Access-Control-Allow-Methods: {allow_methods!r}"
    allow_headers = resp.headers.get("Access-Control-Allow-Headers", "").lower()
    assert "content-type" in allow_headers, (
        f"content-type not echoed in Access-Control-Allow-Headers: {allow_headers!r}"
    )

    print(f"CORS preflight on presigned URL OK: status={resp.status_code}, methods={allow_methods}")


def test_09_cors_preflight_on_direct_path(session_tracker):
    endpoint = os.environ.get("HIPPIUS_ENDPOINT", "https://s3.hippius.com").rstrip("/")
    direct_url = f"{endpoint}/{session_tracker.bucket}/smoke-cors-probe-{uuid.uuid4()}.bin"

    resp = httpx.options(
        direct_url,
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "content-type,authorization",
        },
        timeout=10,
    )

    assert resp.status_code in (200, 204), f"preflight failed: status={resp.status_code} body={resp.text[:200]}"
    assert resp.headers.get("Access-Control-Allow-Origin") == "*", (
        f"CORS preflight on a direct bucket path did not return Access-Control-Allow-Origin: * (got {resp.headers.get('Access-Control-Allow-Origin')!r}) — browsers will block cross-origin requests to the API."
    )
    allow_headers = resp.headers.get("Access-Control-Allow-Headers", "").lower()
    assert "content-type" in allow_headers, (
        f"CORS preflight did not allow the content-type header (got {allow_headers!r}) — browser uploads with a body will be blocked."
    )
    assert "authorization" in allow_headers, (
        f"CORS preflight did not allow the authorization header (got {allow_headers!r}) — browser requests using SigV4/bearer auth will be blocked."
    )

    print(f"CORS preflight on direct path OK: {direct_url}")
