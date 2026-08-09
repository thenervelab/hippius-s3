"""A part re-uploaded under one upload_id must read back as exactly one attempt's bytes.

Re-uploading a part number before `CompleteMultipartUpload` is legal S3 and ordinary client
behavior (a retry, or a hedged duplicate). Every attempt at a given (object, version, part) shares
ONE directory on the ingest SSD, so before staged publishing the attempts wrote the same
`chunk_<i>.bin` names and could interleave: a reader then got some chunks from one attempt and some
from the other. Nothing detected that. The per-chunk AAD binds (bucket, object, part, chunk) and
not attempt identity, so every chunk in the mixture decrypts under a valid tag — the response was
wrong plaintext with a 200 and a correct-looking ETag.

These tests are the HTTP-level statement of the invariant: whatever the client does with part
numbers, a completed object's bytes are one attempt's, its ETag agrees with them, and no bytes of
the other attempt survive anywhere in the object. The unit tests in
tests/unit/test_upload_part_cleanup_race.py drive the individual failure paths inside the writer;
this asserts the property a client can actually observe.

Bodies are single-byte fills so a mixture is visible byte-for-byte rather than inferred, and the
two attempts differ in LENGTH as well as content: a shorter second attempt is what would leave a
stale chunk tail from the longer first one, which strands the part in the drain (it replicates only
an exact {0..num_chunks-1} set) and can surface as extra trailing bytes on a read.
"""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable


# 4 MiB is the configured chunk size, so these span 3 chunks and 2 chunks respectively: the
# republish is both different content AND fewer chunks.
MIB = 1024 * 1024
FIRST = b"A" * (9 * MIB)
SECOND = b"B" * (5 * MIB)


def _md5(body: bytes) -> str:
    return hashlib.md5(body).hexdigest()


def _etag_of(response: dict) -> str:
    """A part's ETag, unquoted. Compared as bare hex because the quoting is the wire format's
    business and every other MPU test here normalizes it the same way."""
    return str(response["ETag"]).strip('"')


def _start_mpu(client: Any, bucket: str, key: str) -> str:
    created = client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    return str(created["UploadId"])


def test_republishing_a_part_replaces_all_of_it_and_none_of_the_old_bytes_survive(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Sequential retry: upload part 1, upload it again shorter, complete, read.

    The read is the assertion that matters. A prefix overwrite would leave the first attempt's
    third chunk in place, and a part that is longer than its meta says is exactly the
    stranded-part shape the publish-time trim exists to prevent.
    """
    bucket = unique_bucket_name("mpu-republish")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "republished.bin"
    upload_id = _start_mpu(boto3_client, bucket, key)

    first = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=FIRST)
    second = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=SECOND)
    assert _etag_of(first) == _md5(FIRST)
    assert _etag_of(second) == _md5(SECOND)

    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": second["ETag"], "PartNumber": 1}]},
    )

    got = boto3_client.get_object(Bucket=bucket, Key=key)
    body = got["Body"].read()

    assert len(body) == len(SECOND), "the object must be the second attempt's LENGTH — a stale tail would be longer"
    assert body == SECOND
    assert b"A" not in body, "no byte of the first attempt may survive the republish"
    assert got["ContentLength"] == len(SECOND)


def test_two_concurrent_attempts_at_one_part_settle_on_exactly_one_of_them(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The hedged-client shape: both attempts in flight at once, different bytes and lengths.

    Which one wins is a race and deliberately not asserted — S3 is last-writer-wins for concurrent
    uploads of one part. What IS asserted is that the object is one of them WHOLE, and that the
    ETag the server reports for the part agrees with the bytes it serves. A mixture fails both: it
    matches neither candidate, and its md5 matches no reported ETag.
    """
    bucket = unique_bucket_name("mpu-hedged")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "hedged.bin"
    upload_id = _start_mpu(boto3_client, bucket, key)

    def attempt(body: bytes) -> str:
        return _etag_of(boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=body))

    with ThreadPoolExecutor(max_workers=2) as pool:
        etags = list(pool.map(attempt, [FIRST, SECOND]))

    assert set(etags) == {_md5(FIRST), _md5(SECOND)}, "each attempt hashes its own body"

    # Complete with the ETag the SERVER still holds for the part, not with a guess about who won:
    # the parts row is last-writer-wins too, and asking makes the test independent of the race.
    listed = boto3_client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
    part = next(p for p in listed["Parts"] if int(p["PartNumber"]) == 1)
    winner_etag = str(part["ETag"])
    assert _etag_of(part) in {_md5(FIRST), _md5(SECOND)}

    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": winner_etag, "PartNumber": 1}]},
    )

    body = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

    assert body in (FIRST, SECOND), "the object is a mixture of the two attempts, not either one of them"
    assert _md5(body) == _etag_of(part), "the bytes served disagree with the ETag recorded for the part"
    assert int(part["Size"]) == len(body), "the recorded part size disagrees with the bytes served"


def test_a_republished_part_survives_a_read_that_must_come_from_the_backend(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The republished bytes must be what REPLICATES, not just what the SSD cache serves.

    Every assertion above is satisfied by the ingest cache alone: the part dir holds the winning
    attempt and the reads never leave the node. But the part is also copied to the pool and uploaded
    to the backend, and those copies are taken from the part dir at whatever moment the drain and the
    uploader get to it — which is not ordered against the republish. If either captured the first
    attempt, the object reads correctly until its cache entry is evicted and then silently serves
    the OLD bytes, months later, with a matching ETag.

    So this waits for the backend registration and then clears the cache, which forces the GET down
    the pipeline path and makes the backend copy the thing under test.
    """
    from .support.cache import clear_object_cache
    from .support.cache import get_object_id_and_version
    from .support.cache import wait_for_all_backends_ready

    bucket = unique_bucket_name("mpu-republish-backend")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "republished-then-evicted.bin"
    upload_id = _start_mpu(boto3_client, bucket, key)

    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=FIRST)
    second = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=SECOND)
    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": second["ETag"], "PartNumber": 1}]},
    )

    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=60.0), (
        "the republished part never registered on the backend"
    )
    object_id, _version = get_object_id_and_version(bucket, key)
    clear_object_cache(object_id)

    got = boto3_client.get_object(Bucket=bucket, Key=key)
    body = got["Body"].read()

    assert len(body) == len(SECOND), "the backend copy is a different length than the republished part"
    assert body == SECOND, "the backend holds the FIRST attempt's bytes — a republish that never replicated"
    assert _md5(body) == _md5(SECOND)


def test_a_republished_part_reads_correctly_alongside_an_untouched_sibling(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Republishing part 1 must not disturb part 2.

    Chunk indices restart per part on disk, but the AEAD nonce and the assembled object do not —
    so a publish that wrote into the wrong part dir, or a trim that ran against the wrong part,
    shows up here as a corrupted or short tail rather than anywhere in the single-part tests.
    """
    bucket = unique_bucket_name("mpu-sibling")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "two-parts.bin"
    upload_id = _start_mpu(boto3_client, bucket, key)

    sibling = b"C" * (5 * MIB)
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=FIRST)
    part2 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=sibling)
    part1 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=SECOND)

    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"ETag": part1["ETag"], "PartNumber": 1},
                {"ETag": part2["ETag"], "PartNumber": 2},
            ]
        },
    )

    body = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

    assert body == SECOND + sibling
    assert b"A" not in body
