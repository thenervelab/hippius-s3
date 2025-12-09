from enum import Enum


class VersionType(str, Enum):
    """Object version type for tracking version origin."""

    USER = "user"
    MIGRATION = "migration"


class ObjectStatus(str, Enum):
    """Object version status tracking upload/pin lifecycle."""

    PUBLISHING = "publishing"
    PINNING = "pinning"
    UPLOADED = "uploaded"
    FAILED = "failed"
