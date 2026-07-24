"""The read path's fail-fast is only useful if it beats the client's read timeout.

`stream_first_chunk_timeout_seconds` bounds how long a GET waits for its first chunk before
raising DownloadNotReadyError -> 503 SlowDown, which every S3 SDK retries. That is the entire
mechanism for surviving a cross-node read-after-write, where the object is legitimately not
ready for ~60s while the drain pipeline replicates it.

It was set to 90s, above boto3's 60s default read timeout, so the mechanism never fired for
anyone: the client hung up on a dead socket (not retryable) while the server was still politely
waiting, and later returned 200 to nobody — so nothing was even recorded as a failure. Observed
in prod 2026-07-23 14:50:05, a presigned GET completing with processing_time_ms=60167, 167ms
after the client gave up.

A number above the client's timeout is not a conservative choice here; it silently disables the
feature.
"""

from __future__ import annotations

from hippius_s3.config import get_config


# botocore's default read_timeout, which is what an un-configured boto3/aws-cli client uses, and
# what tests/smoke passes explicitly. Anything at or above this makes the 503 unreachable.
BOTO3_DEFAULT_READ_TIMEOUT_SECONDS = 60


def test_first_chunk_timeout_leaves_room_for_a_client_retry() -> None:
    config = get_config()
    first_chunk = config.stream_first_chunk_timeout_seconds

    assert first_chunk < BOTO3_DEFAULT_READ_TIMEOUT_SECONDS, (
        f"stream_first_chunk_timeout_seconds={first_chunk}s is at or above the client's "
        f"{BOTO3_DEFAULT_READ_TIMEOUT_SECONDS}s read timeout, so the retryable 503 can never "
        f"reach the client — it hangs up on a dead socket first, and a dead socket is not "
        f"retryable while SlowDown is."
    )
    assert first_chunk * 2 <= BOTO3_DEFAULT_READ_TIMEOUT_SECONDS, (
        f"stream_first_chunk_timeout_seconds={first_chunk}s leaves room for only one attempt "
        f"inside a {BOTO3_DEFAULT_READ_TIMEOUT_SECONDS}s client budget. A cross-node "
        f"read-after-write needs ~60s to replicate, so at least one retry must fit."
    )


def test_later_chunks_still_get_a_generous_wait() -> None:
    """Only the FIRST chunk fails fast. Once the object is proven to be arriving, a mid-stream
    wait is not a 'not ready yet' signal and must not be shortened alongside it."""
    config = get_config()

    assert config.stream_chunk_timeout_seconds > config.stream_first_chunk_timeout_seconds, (
        "the per-chunk timeout must stay well above the first-chunk timeout; shortening it "
        "would break long healthy streams that are draining normally"
    )
