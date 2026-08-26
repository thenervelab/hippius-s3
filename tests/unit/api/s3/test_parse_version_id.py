"""`parse_version_id` is the only thing standing between a client-supplied ``versionId`` string
and a bigint bound straight into a version-resolution query.

It used to be a bare ``int()``, which accepts far more than "a decimal number". Every shape below
either resolved to a DIFFERENT version than the caller named, or blew up past the handler's
validation as a 500 where AWS returns 400. S3 treats VersionId as an opaque string, so the only
safe reading is: bare ASCII digits, in range, or reject.
"""

from __future__ import annotations

import pytest

from hippius_s3.api.s3.common.req import parse_version_id


_MAX = 2**63 - 1


@pytest.mark.parametrize("raw", [None, "", "null"])
def test_absent_or_null_means_current(raw: str | None) -> None:
    assert parse_version_id(raw) is None


@pytest.mark.parametrize("raw,expected", [("1", 1), ("42", 42), (str(_MAX), _MAX)])
def test_plain_positive_integers_parse(raw: str, expected: int) -> None:
    assert parse_version_id(raw) == expected


@pytest.mark.parametrize(
    "raw,note",
    [
        ("1_0", "int() reads underscores as digit grouping -> version 10, not 1_0"),
        ("+3", "leading sign accepted by int()"),
        ("-0", "signed zero"),
        (" 2", "int() strips surrounding whitespace"),
        ("2 ", "trailing whitespace"),
        ("\t3", "tab"),
        ("٤", "Arabic-Indic four: str.isdigit() is True, int() returns 4"),
        ("１", "fullwidth one -> 1"),
        ("１２", "fullwidth twelve -> 12"),
        ("１_2", "mixed fullwidth and underscore"),
    ],
)
def test_shapes_that_silently_resolved_to_the_wrong_version_are_rejected(raw: str, note: str) -> None:
    with pytest.raises(ValueError):
        parse_version_id(raw)


@pytest.mark.parametrize("raw", ["abc", "1.5", "1e3", "0x10", "", " ", "1,000", "v1", "null "])
def test_non_numeric_is_rejected(raw: str) -> None:
    if raw == "":
        pytest.skip("empty string means 'current', covered above")
    with pytest.raises(ValueError):
        parse_version_id(raw)


@pytest.mark.parametrize("raw", ["0", "-1", "-99"])
def test_non_positive_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_version_id(raw)


@pytest.mark.parametrize("raw", [str(_MAX + 1), "9" * 25, "1" + "0" * 30])
def test_above_bigint_is_rejected_here_not_at_the_driver(raw: str) -> None:
    """These parsed fine and then raised inside asyncpg's int8 encoder at bind time.

    That happens after the handler's validation, so it surfaced as a 500 with a non-S3 body —
    botocore reports an XML parse failure rather than a ClientError. AWS returns 400.
    """
    with pytest.raises(ValueError):
        parse_version_id(raw)


def test_boundary_is_inclusive() -> None:
    assert parse_version_id(str(_MAX)) == _MAX
    with pytest.raises(ValueError):
        parse_version_id(str(_MAX + 1))
