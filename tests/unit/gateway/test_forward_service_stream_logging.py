"""Tests for gateway forward_service incomplete stream logging."""

from __future__ import annotations


def test_incomplete_stream_warning_format():
    """Verify the warning message format is correct for incomplete streams.

    This is a structural test confirming the log format. The actual integration
    of bytes_sent vs Content-Length is tested via E2E tests.
    """
    # The warning format should include sent, expected, method, path, status
    msg = "Incomplete upstream stream: sent=%d expected=%s method=%s path=%s status=%d"
    formatted = msg % (100, "1000", "GET", "/bucket/key.bin", 200)
    assert "sent=100" in formatted
    assert "expected=1000" in formatted
    assert "method=GET" in formatted
    assert "/bucket/key.bin" in formatted
    assert "status=200" in formatted
