from __future__ import annotations

from pathlib import Path

from filescan.dedupe.hashing import full_hash


def files_match(source: Path, destination: Path) -> bool:
    try:
        if not destination.exists():
            return False
        if source.stat().st_size != destination.stat().st_size:
            return False
        return full_hash(source) == full_hash(destination)
    except OSError:
        return False
