"""Stable error types for the lake compactor.

Every refusal raises one of these (or a plain ``ValueError`` for malformed
input documents); the CLI maps them all to exit code 2.  Error *messages* for
corruption are stable: the same damaged file always produces the same text,
so operators can alert on them.
"""


class LakeError(Exception):
    """Base class for all lake compactor refusals."""


class NotInitializedError(LakeError):
    """The home directory does not contain an initialized lake."""


class AlreadyInitializedError(LakeError):
    """init was called on a home that already holds a lake catalog."""


class StaleSnapshotError(LakeError):
    """expected_snapshot does not match the committed head snapshot."""


class SnapshotNotFoundError(LakeError):
    """The named snapshot_id never existed."""


class SnapshotExpiredError(LakeError):
    """The snapshot exists but was expired (or is already expired)."""


class CannotExpireHeadError(LakeError):
    """expire was requested for the current head snapshot."""


class PinTokenError(LakeError):
    """The pin token is unknown (never issued or already unpinned)."""


class SelectionError(LakeError):
    """A compaction file selection is invalid."""


class CorruptionError(LakeError):
    """A tool-owned file is missing or fails its integrity check.

    The message is stable for a given file and cause so a damaged lake
    reports the same error every time instead of silently dropping rows.
    """

    def __init__(self, file_id: str, relpath: str, reason: str):
        self.file_id = file_id
        self.relpath = relpath
        self.reason = reason
        super().__init__(
            f"corrupt lake file {file_id} ({relpath}): {reason}"
        )
