"""Edge cases for the CompleteMultipartUpload body parser.

The handler tests cover the interop failures end to end; these pin the parser's contract
directly, one behaviour per test, so a future refactor cannot quietly change how a client
encoding is normalised.
"""

from __future__ import annotations

import pytest

from hippius_s3.api.s3.multipart import parse_complete_multipart_upload


def _parse(body: str) -> list[tuple[int, str]]:
    return parse_complete_multipart_upload(body.encode("utf-8"))


def _one_part(etag: str) -> str:
    return (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    )


def test_parses_boto3_quoted_etags() -> None:
    assert _parse(_one_part('"abc"')) == [(1, "abc")]


def test_parses_rust_predefined_entity_etags() -> None:
    assert _parse(_one_part("&quot;abc&quot;")) == [(1, "abc")]


def test_parses_go_numeric_charref_etags() -> None:
    assert _parse(_one_part("&#34;abc&#34;")) == [(1, "abc")]


def test_parses_bare_etag_workaround_clients() -> None:
    """Clients that worked around the old parser by sending unquoted hex keep working."""
    assert _parse(_one_part("abc")) == [(1, "abc")]


def test_parses_default_namespace() -> None:
    body = (
        '<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<Part><PartNumber>2</PartNumber><ETag>xyz</ETag></Part>"
        "</CompleteMultipartUpload>"
    )
    assert _parse(body) == [(2, "xyz")]


def test_parses_prefixed_namespace() -> None:
    body = (
        '<m:CompleteMultipartUpload xmlns:m="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<m:Part><m:PartNumber>2</m:PartNumber><m:ETag>xyz</m:ETag></m:Part>"
        "</m:CompleteMultipartUpload>"
    )
    assert _parse(body) == [(2, "xyz")]


def test_pairs_within_each_part_regardless_of_child_order() -> None:
    """ETag before PartNumber inside a Part still pairs correctly."""
    body = (
        "<CompleteMultipartUpload>"
        "<Part><ETag>first</ETag><PartNumber>1</PartNumber></Part>"
        "<Part><PartNumber>2</PartNumber><ETag>second</ETag></Part>"
        "</CompleteMultipartUpload>"
    )
    assert _parse(body) == [(1, "first"), (2, "second")]


def test_ignores_etag_outside_any_part() -> None:
    """Two independent findall lists zipped together mispaired on a stray element."""
    body = (
        "<CompleteMultipartUpload><ETag>stray</ETag>"
        "<Part><PartNumber>1</PartNumber><ETag>real</ETag></Part>"
        "</CompleteMultipartUpload>"
    )
    assert _parse(body) == [(1, "real")]


def test_preserves_document_order_for_the_ascending_check() -> None:
    """The handler rejects non-ascending parts, so the parser must not sort."""
    body = (
        "<CompleteMultipartUpload>"
        "<Part><PartNumber>2</PartNumber><ETag>b</ETag></Part>"
        "<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part>"
        "</CompleteMultipartUpload>"
    )
    assert _parse(body) == [(2, "b"), (1, "a")]


def test_strips_whitespace_around_values() -> None:
    body = (
        "<CompleteMultipartUpload><Part>"
        '<PartNumber> 1 </PartNumber><ETag>  "abc"  </ETag>'
        "</Part></CompleteMultipartUpload>"
    )
    assert _parse(body) == [(1, "abc")]


def test_returns_empty_for_a_body_with_no_parts() -> None:
    assert _parse("<CompleteMultipartUpload></CompleteMultipartUpload>") == []


def test_raises_on_missing_partnumber() -> None:
    body = "<CompleteMultipartUpload><Part><ETag>abc</ETag></Part></CompleteMultipartUpload>"
    with pytest.raises(ValueError):
        _parse(body)


def test_raises_on_missing_etag() -> None:
    body = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
    with pytest.raises(ValueError):
        _parse(body)


def test_raises_on_non_integer_partnumber() -> None:
    body = (
        "<CompleteMultipartUpload><Part><PartNumber>one</PartNumber><ETag>abc</ETag></Part></CompleteMultipartUpload>"
    )
    with pytest.raises(ValueError):
        _parse(body)


def test_raises_on_body_that_is_not_well_formed() -> None:
    with pytest.raises(ValueError):
        _parse("<CompleteMultipartUpload><Part>")


def test_raises_on_entity_reference_in_etag() -> None:
    """An unresolved entity reads as an empty ETag, so the body must be refused."""
    body = (
        "<?xml version='1.0'?><!DOCTYPE d [<!ENTITY e 'xxxxxxxxxx'>]>"
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        "<ETag>&e;</ETag></Part></CompleteMultipartUpload>"
    )
    with pytest.raises(ValueError):
        _parse(body)


@pytest.mark.parametrize(
    ("label", "etag"),
    [
        ("empty element", ""),
        ("whitespace only", "   "),
        ("quotes with nothing between them", '""'),
        ("quotes around whitespace", '"  "'),
    ],
)
def test_raises_on_an_empty_etag(label: str, etag: str) -> None:
    """Callers treat the ETag as present, so an empty one must never get through.

    The handler compares each client-asserted ETag against the stored part and rejects a
    mismatch with InvalidPart. An empty string makes that comparison vacuous, which is the
    integrity check quietly opting itself out. Quote-stripping is what makes this more than
    a theoretical case: a literal '""' is a non-empty body that normalises to nothing.
    """
    with pytest.raises(ValueError, match="empty ETag"):
        _parse(_one_part(etag))


def test_raises_on_a_self_closing_etag() -> None:
    """Same hole as an empty element, spelled the way a serialiser would emit it."""
    body = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag/></Part></CompleteMultipartUpload>"
    with pytest.raises(ValueError, match="empty ETag"):
        _parse(body)


def test_ignores_a_part_nested_below_the_root() -> None:
    """S3 specifies Part as a direct child of CompleteMultipartUpload.

    Accepting one at any depth means a body whose real part list is empty can still smuggle
    parts in through an unrelated wrapper — leniency that buys nothing, since no client sends
    this shape.
    """
    body = (
        "<CompleteMultipartUpload><Wrapper>"
        "<Part><PartNumber>1</PartNumber><ETag>abc</ETag></Part>"
        "</Wrapper></CompleteMultipartUpload>"
    )
    assert _parse(body) == []
