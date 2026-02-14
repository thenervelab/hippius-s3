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
