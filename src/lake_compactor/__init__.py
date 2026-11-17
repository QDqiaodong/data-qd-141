"""Small-file compaction and snapshot-safe reclamation for offline data lakes.

Public API:
    Lake                  -- the lake home (catalog + immutable JSONL files)
    LakeError             -- base class of all refusals
    NotInitializedError, AlreadyInitializedError,
    StaleSnapshotError, SnapshotNotFoundError, SnapshotExpiredError,
    CannotExpireHeadError, PinTokenError, SelectionError, CorruptionError
"""

from .errors import (
    AlreadyInitializedError,
    CannotExpireHeadError,
    CorruptionError,
    LakeError,
    NotInitializedError,
    PinTokenError,
    SelectionError,
    SnapshotExpiredError,
    SnapshotNotFoundError,
    StaleSnapshotError,
)
from .lake import Lake

__all__ = [
    "Lake",
    "LakeError",
    "NotInitializedError",
    "AlreadyInitializedError",
    "StaleSnapshotError",
    "SnapshotNotFoundError",
    "SnapshotExpiredError",
    "CannotExpireHeadError",
    "PinTokenError",
    "SelectionError",
    "CorruptionError",
]
__version__ = "1.0.0"
