from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3.buckets.delete_objects_endpoint import parse_delete_request


S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _parse(body: str) -> tuple[bool, list[tuple[str, str]]]:
    return parse_delete_request(ET.fromstring(body.encode()))


def test_parses_namespaced_body() -> None:
    """botocore / aws-cli send the body with the S3 xmlns."""
    quiet, objects = _parse(f'<Delete xmlns="{S3_NS}"><Object><Key>a.txt</Key></Object></Delete>')

    assert quiet is False
    assert objects == [("a.txt", "")]


def test_parses_bare_body() -> None:
    """minio-go (mc) sends bare elements with no xmlns.

    Regression: a namespace-qualified XPath matched zero <Object> nodes here, so
    DeleteObjects returned 200 OK with an empty <DeleteResult/> and deleted nothing.
    """
    quiet, objects = _parse("<Delete><Object><Key>a.txt</Key></Object></Delete>")

    assert quiet is False
    assert objects == [("a.txt", "")]


def test_parses_bare_body_with_quiet_and_multiple_keys() -> None:
    """The exact shape minio-go emits for `mc rm`."""
    quiet, objects = _parse(
        "<Delete><Quiet>true</Quiet>"
        "<Object><Key>a.txt</Key></Object>"
        "<Object><Key>nested/deep/b.txt</Key></Object>"
        "</Delete>"
    )

    assert quiet is True
    assert objects == [("a.txt", ""), ("nested/deep/b.txt", "")]


def test_quiet_is_detected_in_both_forms() -> None:
    assert _parse(f'<Delete xmlns="{S3_NS}"><Quiet>true</Quiet></Delete>')[0] is True
    assert _parse("<Delete><Quiet>true</Quiet></Delete>")[0] is True
    assert _parse("<Delete><Quiet>false</Quiet></Delete>")[0] is False
    assert _parse("<Delete></Delete>")[0] is False


def test_version_id_is_extracted_in_both_forms() -> None:
    """VersionId drives the per-key NotImplemented error, so it must survive parsing."""
    namespaced = f'<Delete xmlns="{S3_NS}"><Object><Key>a.txt</Key><VersionId>v2</VersionId></Object></Delete>'
    bare = "<Delete><Object><Key>a.txt</Key><VersionId>v2</VersionId></Object></Delete>"

    assert _parse(namespaced)[1] == [("a.txt", "v2")]
    assert _parse(bare)[1] == [("a.txt", "v2")]


def test_missing_key_yields_empty_string() -> None:
    """Empty keys are reported as per-key MalformedXML rather than dropped silently."""
    assert _parse("<Delete><Object></Object></Delete>")[1] == [("", "")]
