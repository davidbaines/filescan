from __future__ import annotations

from pathlib import Path, PureWindowsPath


def normalize_path(value: str | Path) -> Path:
    raw = PureWindowsPath(str(value))
    normalized = Path(raw)
    if raw.drive.startswith("\\\\"):
        # resolve() on a UNC path is a network round trip, and can hang on an
        # unreachable server; UNC roots are configured in final form anyway.
        return normalized
    try:
        return normalized.resolve(strict=False)
    except OSError:
        return normalized
