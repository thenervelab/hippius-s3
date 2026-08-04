"""Contract of the hardened parser every request body goes through."""

from __future__ import annotations

import time

import pytest
from lxml import etree as _ET  # ty: ignore[unresolved-import]

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


def test_rejects_an_entity_amplification_payload() -> None:
    """Billion laughs is refused, but not by anything in our code — say so.

    libxml2 (2.11+) applies its own amplification guard and rejects this payload with the
    default parser too, so an assertion that ``parse_untrusted_xml`` raises here would pass
    just as well with the hardening removed and prove nothing about it. Keep the case as a
    regression test against a libxml2 downgrade or a build without the guard, and let
    test_leaves_a_modest_entity_unresolved_in_element_text carry the flags.
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
    # The guard surfaces through the ValueError contract as a malformed body.
    with pytest.raises(ValueError):
        parse_untrusted_xml(bomb)
    elapsed = time.monotonic() - started

    # 30 GB if expanded; rejection must be immediate, not after materialising it.
    assert elapsed < 1.0, f"parsing took {elapsed:.2f}s, entities were expanded"


def test_leaves_a_modest_entity_unresolved_in_element_text() -> None:
    """This is the case that actually pins ``resolve_entities=False``.

    A single small entity is below libxml2's amplification guard, so it is the parser flags
    and nothing else deciding the outcome — the assertion against the default parser is what
    makes that visible, and it is why this test fails if the hardening is ever dropped.

    Kept unresolved, the reference stays an unexpanded child node and ``.text`` reads None.
    That None is the property callers depend on: reading text is how endpoints pull values
    out, and an absent value is refused. lxml will still expand on demand (``string(.)``), so
    callers must not reach for that on untrusted input.
    """
    body = b"<?xml version='1.0'?><!DOCTYPE d [<!ENTITY e 'payload'>]><D><K>&e;</K></D>"

    baseline = _ET.fromstring(body).xpath("./*[local-name()='K']")[0]
    assert baseline.text == "payload", "default parser no longer resolves — this test lost its teeth"

    key = parse_untrusted_xml(body).xpath("./*[local-name()='K']")[0]
    assert key.text is None, "entity resolved into element text"
