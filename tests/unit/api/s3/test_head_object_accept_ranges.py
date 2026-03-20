from datetime import datetime
from datetime import timezone

from hippius_s3.api.s3.common.headers import build_headers


def test_build_headers_includes_accept_ranges() -> None:
    info = {
        "content_type": "video/mp4",
        "md5_hash": "abc123",
        "created_at": datetime(2026, 3, 19, tzinfo=timezone.utc),
        "size_bytes": 1024,
    }
    headers = build_headers(info, source="cache")
    assert headers["Accept-Ranges"] == "bytes"


def test_build_headers_includes_accept_ranges_with_range() -> None:
    info = {
        "content_type": "video/mp4",
        "md5_hash": "abc123",
        "created_at": datetime(2026, 3, 19, tzinfo=timezone.utc),
        "size_bytes": 1024,
    }
    headers = build_headers(info, source="pipeline", rng=(0, 511))
    assert headers["Accept-Ranges"] == "bytes"
    assert headers["Content-Range"] == "bytes 0-511/1024"
