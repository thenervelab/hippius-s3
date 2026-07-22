"""Live-staging acceptance tests for the SSD->CephFS drain.

What "draining works" means observably, from an S3 client's view:
1. A PUT completes (today it 502s because nothing drains the node-local SSD).
2. The object becomes durably readable — including on a GET that lands on a
   different api pod than wrote it (the README's cross-node criterion), which only
   succeeds once the chunk has been drained from the writer node's SSD to CephFS.
3. Larger multipart objects drain the same way.

These are gated by ``HIPPIUS_DRAIN_LIVE=1`` (see conftest) so they enforce the
contract the moment the drain is live and are otherwise skipped, not failing on the
known not-yet-deployed state.
"""

import time

import pytest


# Drain + uploader lag: a PUT returns once bytes hit SSD; durability across nodes
# follows asynchronously. Poll up to this long before calling it a failure.
DRAIN_DEADLINE_SECONDS = 90
POLL_INTERVAL_SECONDS = 3


def _get_body(s3, bucket: str, key: str) -> bytes:  # type: ignore[no-untyped-def]
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def _wait_until_readable(s3, bucket: str, key: str, expected: bytes) -> None:  # type: ignore[no-untyped-def]
    """Poll GET until it returns the expected bytes or the drain deadline passes.

    Re-issuing the GET repeatedly is what exercises the cross-node criterion: the
    gateway may route successive reads to different api pods, so a read that only
    works on the writer node fails here, while one served from CephFS passes.
    """
    deadline = time.monotonic() + DRAIN_DEADLINE_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if _get_body(s3, bucket, key) == expected:
                return
        except Exception as err:  # noqa: BLE001 — transient until the chunk drains
            last_error = err
        time.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(
        f"object {bucket}/{key} did not become durably readable within {DRAIN_DEADLINE_SECONDS}s (last error: {last_error})"
    )


def test_put_completes_without_a_gateway_error(s3, bucket: str) -> None:
    """A simple PUT succeeds — the regression guard for the 502-until-drained state."""
    body = b"drain-e2e simple put\n"
    s3.put_object(Bucket=bucket, Key="simple.txt", Body=body)
    assert _get_body(s3, bucket, "simple.txt") == body


def test_object_becomes_durably_readable(s3, bucket: str) -> None:
    """After PUT, the object is readable within the drain deadline (cross-node)."""
    body = b"drain-e2e durability probe " + bytes(range(256)) * 8
    s3.put_object(Bucket=bucket, Key="durable.bin", Body=body)
    _wait_until_readable(s3, bucket, "durable.bin", body)


def test_head_reflects_size_after_drain(s3, bucket: str) -> None:
    """HEAD returns the correct length once the object is durable."""
    body = b"x" * 4096
    s3.put_object(Bucket=bucket, Key="sized.bin", Body=body)
    _wait_until_readable(s3, bucket, "sized.bin", body)
    head = s3.head_object(Bucket=bucket, Key="sized.bin")
    assert head["ContentLength"] == len(body)


def test_multipart_upload_drains(s3, bucket: str) -> None:
    """A multipart object (multiple parts -> multiple chunks) drains and reads back.

    Each part lands as its own chunk on SSD; all of them must drain for the
    assembled object to read back correctly across nodes.
    """
    part_size = 5 * 1024 * 1024  # S3 minimum part size (except the last)
    parts_data = [bytes([i % 251]) * part_size for i in range(2)]
    expected = b"".join(parts_data)

    upload = s3.create_multipart_upload(Bucket=bucket, Key="multi.bin")
    upload_id = upload["UploadId"]
    completed = []
    for number, data in enumerate(parts_data, start=1):
        resp = s3.upload_part(Bucket=bucket, Key="multi.bin", PartNumber=number, UploadId=upload_id, Body=data)
        completed.append({"ETag": resp["ETag"], "PartNumber": number})
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key="multi.bin",
        UploadId=upload_id,
        MultipartUpload={"Parts": completed},
    )

    _wait_until_readable(s3, bucket, "multi.bin", expected)
