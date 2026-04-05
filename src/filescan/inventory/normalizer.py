from __future__ import annotations

from pathlib import Path, PureWindowsPath


def normalize_path(value: str | Path) -> Path:
    raw = PureWindowsPath(str(value))
    normalized = Path(raw)
    try:
        return normalized.resolve(strict=False)
    except OSError:
        return normalized
