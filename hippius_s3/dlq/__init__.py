"""
DLQ (Dead Letter Queue) package for Hippius S3.

Provides reliable failure handling by persisting chunk data to disk
when uploads fail, allowing for safe recovery and reprocessing.
"""

from .logic import DLQLogic
from .storage import DLQStorage


__all__ = ["DLQLogic", "DLQStorage"]
