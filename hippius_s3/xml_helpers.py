"""Typed wrappers for lxml operations to provide type safety."""

from typing import Any

import lxml.etree as _ET  # ty: ignore[unresolved-import]


# Using Any for Element types since lxml doesn't have proper type stubs
Element = Any


def create_element(tag: str, **attribs: str) -> Element:
    """Create an XML element with optional attributes."""
    return _ET.Element(tag, **attribs)


def add_subelement(parent: Element, tag: str, text: str | None = None) -> Element:
    """Add a child element to a parent element with optional text content."""
    elem = _ET.SubElement(parent, tag)
    if text is not None:
        elem.text = text
    return elem


def to_xml_bytes(
    root: Element, encoding: str = "UTF-8", xml_declaration: bool = True, pretty_print: bool = True
) -> bytes:
    """Convert an element tree to bytes with XML declaration."""
    result: bytes = _ET.tostring(root, encoding=encoding, xml_declaration=xml_declaration, pretty_print=pretty_print)
    return result


def parse_untrusted_xml(data: bytes) -> Element:
    """Parse client-supplied XML with entity expansion, DTD loading and network access off.

    The single entry point for request bodies, so no endpoint has to remember the hardening
    flags — a default parser would expand DTD-declared entities and inflate a billion-laughs
    body in memory. Predefined entities and numeric character references (``&quot;``,
    ``&#34;``) are still decoded, which is what lets clients whose XML encoders escape quotes
    round-trip correctly.

    Args:
        data: Raw request body.

    Returns:
        The parsed root element.

    Raises:
        ValueError: The body is not well-formed XML.
    """
    # A parser instance carries mutable state, so it is built per call rather than shared.
    parser = _ET.XMLParser(resolve_entities=False, load_dtd=False, no_network=True, huge_tree=False)
    try:
        return _ET.fromstring(data, parser)
    except _ET.XMLSyntaxError as exc:
        raise ValueError(str(exc)) from exc
