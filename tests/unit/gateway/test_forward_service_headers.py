from types import SimpleNamespace
from typing import Any

from gateway.services.forward_service import _filter_hop_by_hop_raw_headers
from gateway.services.forward_service import _trusted_hippius_headers


def _request(**state: Any) -> Any:
    return SimpleNamespace(state=SimpleNamespace(**state))


def test_forwards_bucket_id_when_resolved() -> None:
    headers = _trusted_hippius_headers(_request(account_id="acct", bucket_owner_id="owner", bucket_id="buck-123"))
    assert headers["X-Hippius-Bucket-Id"] == "buck-123"
    assert headers["X-Hippius-Bucket-Owner"] == "owner"


def test_omits_bucket_id_when_absent() -> None:
    # No bucket in play (e.g. ListBuckets) — the header must not be emitted.
    headers = _trusted_hippius_headers(_request(account_id="acct"))
    assert "X-Hippius-Bucket-Id" not in headers


def test_omits_bucket_id_when_empty() -> None:
    headers = _trusted_hippius_headers(_request(account_id="acct", bucket_id=""))
    assert "X-Hippius-Bucket-Id" not in headers


def test_filters_standard_hop_by_hop_headers() -> None:
    raw = [
        (b"content-type", b"text/plain"),
        (b"connection", b"keep-alive"),
        (b"transfer-encoding", b"chunked"),
    ]
    result = _filter_hop_by_hop_raw_headers(raw)
    names = [k.decode() for k, _ in result]
    assert "content-type" in names
    assert "connection" not in names
    assert "transfer-encoding" not in names


def test_filters_date_and_server_headers() -> None:
    raw = [
        (b"content-type", b"application/json"),
        (b"date", b"Thu, 19 Mar 2026 22:52:54 GMT"),
        (b"server", b"uvicorn"),
        (b"etag", b'"abc123"'),
    ]
    result = _filter_hop_by_hop_raw_headers(raw)
    names = [k.decode() for k, _ in result]
    assert "content-type" in names
    assert "etag" in names
    assert "date" not in names
    assert "server" not in names


def test_filters_date_and_server_case_insensitive() -> None:
    raw = [
        (b"Date", b"Thu, 19 Mar 2026 22:52:54 GMT"),
        (b"Server", b"uvicorn"),
        (b"X-Custom", b"value"),
    ]
    result = _filter_hop_by_hop_raw_headers(raw)
    names = [k.decode().lower() for k, _ in result]
    assert "x-custom" in names
    assert "date" not in names
    assert "server" not in names


def test_preserves_other_headers() -> None:
    raw = [
        (b"content-type", b"text/html"),
        (b"content-length", b"42"),
        (b"accept-ranges", b"bytes"),
        (b"etag", b'"xyz"'),
        (b"x-amz-request-id", b"abc"),
    ]
    result = _filter_hop_by_hop_raw_headers(raw)
    assert len(result) == 5


def test_filters_connection_listed_headers() -> None:
    raw = [
        (b"connection", b"x-custom-hop"),
        (b"x-custom-hop", b"should-be-removed"),
        (b"content-type", b"text/plain"),
    ]
    result = _filter_hop_by_hop_raw_headers(raw)
    names = [k.decode() for k, _ in result]
    assert "content-type" in names
    assert "connection" not in names
    assert "x-custom-hop" not in names
