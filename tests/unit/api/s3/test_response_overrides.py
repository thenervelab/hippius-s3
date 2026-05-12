import pytest
from starlette.datastructures import QueryParams

from hippius_s3.api.s3.common.headers import MAX_OVERRIDE_VALUE_LEN
from hippius_s3.api.s3.common.headers import RESPONSE_OVERRIDE_PARAMS
from hippius_s3.api.s3.common.headers import apply_response_overrides
from hippius_s3.api.s3.common.headers import parse_response_overrides


def _qp(**kwargs: str) -> QueryParams:
    return QueryParams(list(kwargs.items()))


def test_empty_query_params_returns_empty() -> None:
    assert parse_response_overrides(QueryParams(), "user-123") == {}


def test_each_param_maps_to_correct_header() -> None:
    cases = {
        "response-content-type": ("Content-Type", "application/pdf"),
        "response-content-disposition": ("Content-Disposition", 'attachment; filename="x.pdf"'),
        "response-content-encoding": ("Content-Encoding", "gzip"),
        "response-content-language": ("Content-Language", "en-US"),
        "response-cache-control": ("Cache-Control", "no-cache, max-age=0"),
        "response-expires": ("Expires", "Thu, 01 Dec 2026 16:00:00 GMT"),
    }
    for qparam, (header_name, value) in cases.items():
        result = parse_response_overrides(QueryParams([(qparam, value)]), "user-123")
        assert result == {header_name: value}, f"failed for {qparam}"


def test_all_six_params_together() -> None:
    params = QueryParams(
        [
            ("response-content-type", "image/png"),
            ("response-content-disposition", "inline"),
            ("response-content-encoding", "identity"),
            ("response-content-language", "fr"),
            ("response-cache-control", "private"),
            ("response-expires", "0"),
        ]
    )
    result = parse_response_overrides(params, "user-123")
    assert set(result.keys()) == set(RESPONSE_OVERRIDE_PARAMS.values())


def test_unknown_response_params_ignored() -> None:
    params = QueryParams(
        [
            ("response-content-type", "text/plain"),
            ("response-foo", "bar"),
            ("response-content-length", "999"),
        ]
    )
    result = parse_response_overrides(params, "user-123")
    assert result == {"Content-Type": "text/plain"}


def test_unrelated_query_params_ignored() -> None:
    params = QueryParams(
        [
            ("versionId", "7"),
            ("response-content-type", "application/json"),
            ("X-Amz-Signature", "abc"),
        ]
    )
    result = parse_response_overrides(params, "user-123")
    assert result == {"Content-Type": "application/json"}


def test_anonymous_returns_empty() -> None:
    params = QueryParams(
        [
            ("response-content-disposition", 'attachment; filename="x.pdf"'),
            ("response-content-type", "application/pdf"),
        ]
    )
    assert parse_response_overrides(params, "anonymous") == {}


def test_none_auth_does_not_short_circuit() -> None:
    params = QueryParams([("response-content-type", "image/png")])
    assert parse_response_overrides(params, None) == {"Content-Type": "image/png"}


@pytest.mark.parametrize(
    "bad_value",
    [
        "foo\r\nX-Injected: yes",
        "foo\nX-Injected: yes",
        "foo\rX-Injected: yes",
        "leading\rcarriage",
        "trailing\n",
    ],
)
def test_crlf_in_value_raises(bad_value: str) -> None:
    params = QueryParams([("response-content-disposition", bad_value)])
    with pytest.raises(ValueError, match="CRLF"):
        parse_response_overrides(params, "user-123")


def test_oversize_value_raises() -> None:
    big = "x" * (MAX_OVERRIDE_VALUE_LEN + 1)
    params = QueryParams([("response-content-disposition", big)])
    with pytest.raises(ValueError, match="exceeds"):
        parse_response_overrides(params, "user-123")


def test_value_at_max_length_accepted() -> None:
    at_max = "x" * MAX_OVERRIDE_VALUE_LEN
    params = QueryParams([("response-content-disposition", at_max)])
    result = parse_response_overrides(params, "user-123")
    assert result["Content-Disposition"] == at_max


def test_empty_string_value_preserved() -> None:
    params = QueryParams([("response-cache-control", "")])
    result = parse_response_overrides(params, "user-123")
    assert result == {"Cache-Control": ""}


def test_rfc5987_utf8_filename_preserved() -> None:
    value = "attachment; filename=\"fallback.txt\"; filename*=UTF-8''%E5%90%8D%E5%89%8D.txt"
    params = QueryParams([("response-content-disposition", value)])
    result = parse_response_overrides(params, "user-123")
    assert result["Content-Disposition"] == value


def test_apply_overwrites_existing_headers() -> None:
    headers: dict[str, str] = {
        "Content-Type": "application/octet-stream",
        "ETag": '"abc"',
    }
    apply_response_overrides(
        headers,
        {"Content-Type": "application/pdf", "Content-Disposition": 'attachment; filename="x.pdf"'},
    )
    assert headers["Content-Type"] == "application/pdf"
    assert headers["Content-Disposition"] == 'attachment; filename="x.pdf"'
    assert headers["ETag"] == '"abc"'


def test_apply_with_empty_overrides_is_noop() -> None:
    headers = {"Content-Type": "text/plain"}
    apply_response_overrides(headers, {})
    assert headers == {"Content-Type": "text/plain"}
