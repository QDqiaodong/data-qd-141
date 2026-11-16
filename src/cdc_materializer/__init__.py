"""Local SQLite CDC history materializer.

Public API:
    Materializer          -- ingest engine + bi-temporal interval query
    BatchConflictError    -- raised when a known batch_id carries new bytes
    REJECTION_REASONS     -- stable rejection reason code catalogue
"""

from .store import Materializer, BatchConflictError, REJECTION_REASONS

__all__ = ["Materializer", "BatchConflictError", "REJECTION_REASONS"]
__version__ = "1.0.0"
