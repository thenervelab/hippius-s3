"""Contract of the hardened parser every request body goes through."""

from __future__ import annotations

import time

import pytest

from hippius_s3.xml_helpers import parse_untrusted_xml


def test_parses_a_namespaced_body() -> None:
    root = parse_untrusted_xml(b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>true</Quiet></Delete>')
    assert root.xpath("./*[local-name()='Quiet']")[0].text == "true"


def test_decodes_predefined_entities_and_character_references() -> None:
    """This is what makes Go and Rust clients work: their encoders escape quotes, and the
    values must arrive decoded even though DTD entity resolution is off."""
    root = parse_untrusted_xml(b"<D><K>&quot;a&#34;b&amp;c</K></D>")
    assert root.xpath("./*[local-name()='K']")[0].text == '"a"b&c'


def test_raises_valueerror_on_a_malformed_body() -> None:
    with pytest.raises(ValueError):
        parse_untrusted_xml(b"<D><K>")


def test_does_not_expand_internal_entities_while_parsing() -> None:
    """Billion laughs: a default parser inflates this during the parse itself.

    With resolution off the reference is kept as an unexpanded child node, so the parse stays
    cheap and ``.text`` reads as None rather than as megabytes of payload. That None is the
    property callers depend on — reading text is how every endpoint pulls values out, and an
    absent value is refused. Note lxml WILL still expand on demand if asked (``string(.)``),
    so callers must not reach for that on untrusted input.
    """
    bomb = (
        b"<?xml version='1.0'?><!DOCTYPE lolz ["
        b"<!ENTITY lol 'lollollollollollollollollollol'>"
        b"<!ENTITY lol2 '&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;'>"
        b"<!ENTITY lol3 '&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;'>"
        b"<!ENTITY lol4 '&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;'>"
        b"<!ENTITY lol5 '&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;'>"
        b"<!ENTITY lol6 '&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;'>"
        b"<!ENTITY lol7 '&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;'>"
        b"]><D><K>&lol7;</K></D>"
    )
    started = time.monotonic()
    # libxml2's own amplification guard refuses a payload this size outright, which surfaces
    # through the ValueError contract as a malformed body.
    with pytest.raises(ValueError):
        parse_untrusted_xml(bomb)
    elapsed = time.monotonic() - started

    # 30 GB if expanded; rejection must be immediate, not after materialising it.
    assert elapsed < 1.0, f"parsing took {elapsed:.2f}s, entities were expanded"


def test_leaves_a_modest_entity_unresolved_in_element_text() -> None:
    """Below the amplification guard the body parses, and the reference is kept as an
    unexpanded child node — so ``.text`` reads None rather than the payload. That None is the
    property callers depend on: reading text is how endpoints pull values out, and an absent
    value is refused. lxml will still expand on demand (``string(.)``), so callers must not
    reach for that on untrusted input.
    """
    body = b"<?xml version='1.0'?><!DOCTYPE d [<!ENTITY e 'payload'>]><D><K>&e;</K></D>"

    root = parse_untrusted_xml(body)

    key = root.xpath("./*[local-name()='K']")[0]
    assert key.text is None, "entity resolved into element text"
