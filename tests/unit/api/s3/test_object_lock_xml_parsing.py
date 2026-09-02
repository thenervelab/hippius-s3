"""The bucket Object Lock PUT body is client-supplied XML, so it must use the hardened parser.

`PUT /{bucket}?object-lock` is a new, publicly reachable, authenticated write endpoint that
parses a request body. lxml's default parser expands DTD-declared entities, so parsing one with
`etree.fromstring` inflates a billion-laughs body inside the worker before any of the schema
validation runs — on a pod shared with every other request. `parse_untrusted_xml` is the repo's
single hardened entry point (resolve_entities=False, load_dtd=False, no_network=True,
huge_tree=False) and is what CompleteMultipartUpload already uses; these pin that this endpoint
keeps using it, since the two spellings are one word apart and behave identically on every
well-formed body a normal client sends.
"""

from __future__ import annotations

from hippius_s3.api.s3.buckets.bucket_object_lock_endpoint import _parse_request_xml


_BOMB = (
    b"<?xml version='1.0'?><!DOCTYPE lolz ["
    b"<!ENTITY lol 'lollollollollollollollollollol'>"
    b"<!ENTITY lol2 '&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;'>"
    b"<!ENTITY lol3 '&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;'>"
    b"]><ObjectLockConfiguration><ObjectLockEnabled>&lol3;</ObjectLockEnabled>"
    b"</ObjectLockConfiguration>"
)


# A declared entity whose expansion is exactly the value the parser is looking for. This is the
# test that actually DISCRIMINATES: with entity resolution on, the reference becomes "Enabled"
# and the config is accepted; with it off, the element is empty and the body is refused. A
# billion-laughs body alone does NOT discriminate — it is rejected either way (the expansion is
# not "Enabled"), so a test asserting only "rejected" passes against the vulnerable parser too.
_ENTITY_ENABLED = (
    b"<?xml version='1.0'?><!DOCTYPE c [<!ENTITY e 'Enabled'>]>"
    b"<ObjectLockConfiguration><ObjectLockEnabled>&e;</ObjectLockEnabled></ObjectLockConfiguration>"
)


def test_dtd_entities_are_not_resolved() -> None:
    """Proves entity resolution is off, by using an entity that would otherwise be accepted."""
    config, error = _parse_request_xml(_ENTITY_ENABLED)

    assert config is None, "a DTD-declared entity was resolved — the parser is not hardened"
    assert error is not None
    assert error.status_code == 400


def test_entity_bomb_is_rejected() -> None:
    """The billion-laughs shape, kept for documentation.

    Note this does not by itself prove hardening (see the comment above `_ENTITY_ENABLED`): the
    expansion is not "Enabled", so it is refused with or without entity resolution. What the
    hardened parser adds is that the expansion never happens in memory at all.
    """
    config, error = _parse_request_xml(_BOMB)

    assert config is None, "an entity-bomb body must never produce a config to persist"
    assert error is not None
    assert error.status_code == 400
    assert b"lollol" not in bytes(error.body), "the entity was expanded into the response"


def test_valid_configuration_still_parses() -> None:
    """The hardening must not cost a legitimate client its request."""
    body = (
        b'<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<ObjectLockEnabled>Enabled</ObjectLockEnabled>"
        b"<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule>"
        b"</ObjectLockConfiguration>"
    )
    config, error = _parse_request_xml(body)

    assert error is None, f"a valid body was rejected: {error and bytes(error.body)!r}"
    assert config == {"enabled": True, "mode": "GOVERNANCE", "days": 30}


def test_predefined_entities_still_decode() -> None:
    """Numeric character references must still decode — some SDK XML encoders emit them, and
    turning entity resolution off wholesale would break those clients."""
    body = b"<ObjectLockConfiguration><ObjectLockEnabled>&#69;nabled</ObjectLockEnabled></ObjectLockConfiguration>"
    config, error = _parse_request_xml(body)

    assert error is None, "a character reference in a legitimate body must still decode"
    assert config == {"enabled": True}


def test_malformed_body_is_a_clean_400() -> None:
    """The hardened parser raises ValueError where lxml raised XMLSyntaxError; the endpoint has
    to catch the new type or a malformed body becomes a 500."""
    config, error = _parse_request_xml(b"<ObjectLockConfiguration><Rule>")

    assert config is None
    assert error is not None
    assert error.status_code == 400
    assert b"MalformedXML" in bytes(error.body)
