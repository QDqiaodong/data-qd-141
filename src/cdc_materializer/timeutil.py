"""Strict business-valid-time parsing.

Accepted inputs:
  * calendar date:        "2026-03-01"
  * offset date-time:     "2026-03-01T12:00:00+08:00" (fractional seconds ok)

Naive date-time strings are rejected so that a valid time always has an
explicit offset.  Everything is normalized to a fixed-width UTC string so
that valid-time comparisons are plain lexicographic text comparisons inside
SQLite and stored values are byte-stable across replays.
"""

from __future__ import annotations

import re
from datetime import datetime, date, timezone

# Loose pre-filter; datetime.fromisoformat is the authority afterwards.
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?"
    r"(Z|[+-]\d{2}:\d{2})$"
)


class TimeParseError(ValueError):
    """Raised when a business valid time cannot be resolved."""


def parse_valid_time(value) -> str:
    """Return a normalized ``YYYY-MM-DDTHH:MM:SS.ffffff+00:00`` string."""
    if not isinstance(value, str) or not value:
        raise TimeParseError("valid time must be a non-empty string")
    text = value.strip()
    try:
        if _DATE_RE.match(text):
            parsed = date.fromisoformat(text)
            instant = datetime(
                parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc
            )
        elif _DT_RE.match(text):
            normalized = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:  # defensive: regex already requires an offset
                raise TimeParseError(f"naive date-time is not allowed: {value!r}")
            instant = parsed.astimezone(timezone.utc)
        else:
            raise TimeParseError(f"unparseable valid time: {value!r}")
    except TimeParseError:
        raise
    except ValueError as exc:  # fromisoformat failures (bad calendar values etc.)
        raise TimeParseError(f"unparseable valid time: {value!r} ({exc})") from None
    return instant.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
