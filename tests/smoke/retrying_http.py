"""A GET that retries 503 SlowDown, the way a real S3 client does.

These tests reach the gateway with raw httpx rather than boto3 — presigned URLs and anonymous
public reads are deliberately exercised as a plain HTTP client would, without SDK machinery in
the way. The cost is that they also lose the SDK's retry behaviour, and 503 SlowDown is a
NORMAL response here, not a failure:

A cross-node read-after-write is not instant. An upload lands on one node's local SSD, and a
download served by a different node has nothing to serve until the drain pipeline replicates it
— reconciler notices, drain copies to CephFS, enqueue sweep publishes, uploader uploads. Every
stage is a poll, so the round trip is ~60s. While that is in flight the read path deliberately
gives up early and returns a retryable 503 rather than holding the connection open (see
`stream_first_chunk_timeout_seconds`). boto3 and the aws-cli retry that transparently and the
user never sees it; a bare `httpx.get` does not, and reports a failure the SDK would have
absorbed.

So this helper exists to make the smoke tests behave like the clients we actually care about.
It retries ONLY 503 — any other status is returned as-is, so a genuine 500/403/404 still fails
the test immediately rather than being retried into a timeout.
"""

from __future__ import annotations

import logging
import time

import httpx


logger = logging.getLogger(__name__)

# Enough attempts to cover the ~60s replication round trip at the server's ~25s fail-fast, with
# headroom. Kept explicit rather than derived so the test does not silently follow a config change.
DEFAULT_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 5.0


def get_with_slowdown_retry(
    url: str,
    *,
    timeout: float = 60.0,
    attempts: int = DEFAULT_ATTEMPTS,
    backoff: float = DEFAULT_BACKOFF_SECONDS,
) -> httpx.Response:
    """GET `url`, retrying only on 503, and return the last response.

    Returns rather than raises on exhaustion so the caller's own assertion produces the failure
    message — a test that ends in "still 503 after N attempts" is more useful than a stack trace
    from in here.
    """
    response = httpx.get(url, timeout=timeout)
    for attempt in range(2, attempts + 1):
        if response.status_code != 503:
            return response
        logger.info("503 SlowDown (object still replicating); attempt %d/%d after %.0fs", attempt, attempts, backoff)
        time.sleep(backoff)
        response = httpx.get(url, timeout=timeout)
    return response
