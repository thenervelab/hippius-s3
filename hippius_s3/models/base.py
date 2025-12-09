from datetime import datetime
from datetime import timezone
from typing import Optional

from sqlmodel import Field


class TimestampMixin:
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        nullable=True,
    )


class SoftDeleteMixin:
    deleted_at: Optional[datetime] = Field(
        default=None,
        nullable=True,
    )

    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    def soft_delete(self) -> None:
        self.deleted_at = datetime.now(timezone.utc)

    def restore(self) -> None:
        self.deleted_at = None
