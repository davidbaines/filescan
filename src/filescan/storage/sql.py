"""SQL helpers shared by repositories and analysis modules."""

from __future__ import annotations

from pathlib import Path

# Windows attributes of cloud placeholders (iCloud, OneDrive Files On-Demand).
# Reading such a file downloads it, so they are never hashed.
FILE_ATTRIBUTE_OFFLINE = 0x1000
FILE_ATTRIBUTE_RECALL_ON_OPEN = 0x40000
FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x400000
CLOUD_PLACEHOLDER_MASK = FILE_ATTRIBUTE_OFFLINE | FILE_ATTRIBUTE_RECALL_ON_OPEN | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS

# Escape character for LIKE patterns. Windows forbids "|" in file names, so it
# never needs escaping in practice, but it is escaped anyway for safety.
LIKE_ESCAPE = "|"


def escape_like(value: str) -> str:
    """Escape LIKE wildcards so `value` matches literally."""
    return (
        value.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
        .replace("%", LIKE_ESCAPE + "%")
        .replace("_", LIKE_ESCAPE + "_")
    )


def path_prefix(path: Path | str) -> tuple[str, str]:
    """Return (exact, pattern) for matching a folder and everything below it.

    Use as ``WHERE (path = ? OR path LIKE ? ESCAPE '|')``. A drive root such as
    ``F:\\`` loses its trailing backslash before the wildcard is added, so the
    pattern is ``F:\\%`` rather than ``F:\\\\%``.
    """
    exact = str(path)
    return exact, escape_like(exact.rstrip("\\")) + "\\%"
