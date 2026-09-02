"""Per-object Object Lock: `?retention` and `?legal-hold` (Tier 2).

These are the operations that let a lock actually be SET. Everything else in Tier 2 — the unpin
gate, the hard-delete gate, the delete-path 403 — is enforcement of what these write.

Mutation rules, straight from the AWS guide, because they are the whole security model:

- **COMPLIANCE is immutable.** Its mode cannot be changed and its period cannot be shortened, by
  anyone, including the account root. Only extension is allowed.
- **GOVERNANCE can be weakened**, but only by a caller holding both the bypass permission and the
  explicit header. Extending never needs a bypass.
- **A legal hold** is freely settable and removable by anyone who may write the lock. It is not a
  retention mode and the governance bypass does not apply to it.
"""

from __future__ import annotations

import logging
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Final

from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.object_lock_enforcement import COMPLIANCE
from hippius_s3.api.s3.object_lock_enforcement import GOVERNANCE
from hippius_s3.api.s3.object_lock_enforcement import may_bypass_governance
from hippius_s3.config import get_config
from hippius_s3.utils import get_query
from hippius_s3.xml_helpers import add_subelement
from hippius_s3.xml_helpers import create_element
from hippius_s3.xml_helpers import parse_untrusted_xml
from hippius_s3.xml_helpers import to_xml_bytes


logger = logging.getLogger(__name__)

_S3_NS: Final[str] = "http://s3.amazonaws.com/doc/2006-03-01/"
_VALID_MODES: Final[frozenset[str]] = frozenset({GOVERNANCE, COMPLIANCE})


def _local(node: Any) -> str:
    """Tag name without its namespace, so both namespaced and bare bodies parse."""
    tag = str(node.tag)
    return tag.rsplit("}", 1)[-1]


def _find(root: Any, name: str) -> Any:
    for child in root:
        if _local(child) == name:
            return child
    return None


def _malformed(message: str = "The XML you provided was not well-formed or did not validate.") -> Response:
    return errors.s3_error_response("MalformedXML", message, status_code=400)


def parse_retention_body(body: bytes) -> tuple[dict[str, Any] | None, Response | None]:
    """Parse `<Retention><Mode/><RetainUntilDate/></Retention>`.

    An EMPTY body is not the same as an absent one: AWS uses an empty `Retention` element to clear
    a retention, which only GOVERNANCE-with-bypass may do, so it must reach the caller as a
    parsed intent rather than a parse error.
    """
    if not body:
        return None, _malformed("Request body is required.")
    try:
        root = parse_untrusted_xml(body)
    except ValueError:
        return None, _malformed()
    if _local(root) != "Retention":
        return None, _malformed("Expected a Retention element.")

    mode_node = _find(root, "Mode")
    date_node = _find(root, "RetainUntilDate")
    mode = (mode_node.text or "").strip() if mode_node is not None else None
    raw_date = (date_node.text or "").strip() if date_node is not None else None

    if (mode is None) != (raw_date is None):
        return None, _malformed("Mode and RetainUntilDate must be supplied together.")
    if mode is None:
        return {"mode": None, "retain_until": None}, None  # explicit clear
    if mode not in _VALID_MODES:
        return None, _malformed(f"Mode must be one of {sorted(_VALID_MODES)}.")

    try:
        # AWS emits RFC3339/ISO-8601, commonly with a trailing Z that fromisoformat rejects on
        # older Pythons; normalise it rather than depending on the interpreter version.
        parsed = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
    except ValueError:
        return None, _malformed("RetainUntilDate must be an ISO-8601 timestamp.")
    if parsed.tzinfo is None:
        # A naive timestamp compared against an aware `now()` raises at enforcement time, which
        # would turn a malformed request into a 500 on the DELETE path much later. Assume UTC.
        parsed = parsed.replace(tzinfo=timezone.utc)

    # Cap the horizon. A COMPLIANCE lock cannot be shortened by anyone, so an unbounded
    # RetainUntilDate lets one request create storage this platform can never reclaim and must
    # keep paying for — the team decision on COMPLIANCE mode records this cap as the only bound
    # that exists, since there is no bucket-policy condition key here to express it.
    max_days = get_config().object_lock_max_retention_days
    if parsed > datetime.now(timezone.utc) + timedelta(days=max_days):
        return None, errors.s3_error_response(
            "InvalidArgument",
            f"RetainUntilDate is further than {max_days} days out, the maximum retention this service accepts.",
            status_code=400,
        )
    return {"mode": mode, "retain_until": parsed}, None


def parse_legal_hold_body(body: bytes) -> tuple[bool | None, Response | None]:
    """Parse `<LegalHold><Status>ON|OFF</Status></LegalHold>`."""
    if not body:
        return None, _malformed("Request body is required.")
    try:
        root = parse_untrusted_xml(body)
    except ValueError:
        return None, _malformed()
    if _local(root) != "LegalHold":
        return None, _malformed("Expected a LegalHold element.")
    status_node = _find(root, "Status")
    status = (status_node.text or "").strip().upper() if status_node is not None else ""
    if status not in {"ON", "OFF"}:
        return None, _malformed("Status must be ON or OFF.")
    return status == "ON", None


def retention_to_xml(mode: str, retain_until: datetime) -> bytes:
    root = create_element("Retention", xmlns=_S3_NS)
    add_subelement(root, "Mode", mode)
    # AWS renders millisecond precision with a Z suffix.
    add_subelement(root, "RetainUntilDate", retain_until.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
    return to_xml_bytes(root)


def legal_hold_to_xml(on: bool) -> bytes:
    root = create_element("LegalHold", xmlns=_S3_NS)
    add_subelement(root, "Status", "ON" if on else "OFF")
    return to_xml_bytes(root)


def validate_retention_transition(
    *,
    current_mode: str | None,
    current_until: datetime | None,
    new_mode: str | None,
    new_until: datetime | None,
    is_bucket_owner: bool,
    headers: Any,
    now: datetime | None = None,
) -> Response | None:
    """Whether this retention change is permitted. Returns an error Response, or None to allow.

    Only reached once the caller is already authorised to write the lock; this is the WORM rule
    layer on top of that.
    """
    now = now or datetime.now(timezone.utc)
    active = current_until is not None and current_until > now

    if not active:
        return None  # no live retention: any new one may be set

    if current_mode == COMPLIANCE:
        # Immutable while live: no mode change, no shortening, no clearing. Extension only.
        if new_until is None or new_mode != COMPLIANCE:
            return errors.s3_error_response(
                "AccessDenied",
                "A COMPLIANCE-mode retention cannot be removed or changed before it expires.",
                status_code=403,
            )
        if new_until < current_until:
            return errors.s3_error_response(
                "AccessDenied",
                "A COMPLIANCE-mode retention period can be extended but never shortened.",
                status_code=403,
            )
        return None

    if current_mode == GOVERNANCE:
        weakening = new_until is None or new_until < current_until or new_mode != GOVERNANCE
        if weakening and not may_bypass_governance(is_bucket_owner=is_bucket_owner, headers=headers):
            return errors.s3_error_response(
                "AccessDenied",
                "Shortening or removing a GOVERNANCE-mode retention requires the bucket owner to "
                "send x-amz-bypass-governance-retention: true.",
                status_code=403,
            )
        return None

    # A live retention whose mode we do not recognise: refuse rather than let it be overwritten.
    return errors.s3_error_response(
        "AccessDenied",
        "This object version carries an unrecognised retention mode and cannot be modified.",
        status_code=403,
    )


async def load_version_lock(db: Any, *, object_id: str, object_version: int) -> Any:
    return await db.fetchrow(get_query("get_object_version_lock"), object_id, object_version)


async def store_version_lock(
    db: Any,
    *,
    object_id: str,
    object_version: int,
    mode: str | None,
    retain_until: datetime | None,
    legal_hold: bool | None,
) -> None:
    """Persist lock state. `legal_hold=None` leaves the hold untouched, which is what lets the
    retention and legal-hold endpoints write independently without clobbering each other."""
    await db.execute(
        get_query("set_object_version_lock"),
        object_id,
        object_version,
        mode,
        retain_until,
        legal_hold,
    )
