from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import xxhash


def full_hash(path: Path, progress: Callable[[int], None] | None = None) -> str:
    """xxh128 of the whole file, read in 1 MB chunks; progress, if given, is called with each chunk's length."""
    digest = xxhash.xxh128()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
            if progress is not None:
                progress(len(chunk))
    return digest.hexdigest()


def same_as_scanned(path: Path, size: int, mtime: float) -> bool:
    stat = path.stat()
    return stat.st_size == size and stat.st_mtime == mtime


def hash_if_unchanged(hasher, record) -> str | None:
    """The file's hash, or None if its size or date differs from the scan, before or after reading.

    A hash of changed content would be stored against the old size and date, so
    such a file is left for the next scan.
    """
    if not same_as_scanned(record.path, record.size, record.mtime):
        return None
    digest = hasher(record.path)
    return digest if same_as_scanned(record.path, record.size, record.mtime) else None
