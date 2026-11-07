from __future__ import annotations

from pathlib import Path

import xxhash

CHUNK_BYTES = 4096


def quick_hash(path: Path) -> str:
    size = path.stat().st_size
    with path.open("rb") as handle:
        head = handle.read(CHUNK_BYTES)
        if size > CHUNK_BYTES:
            handle.seek(max(0, size - CHUNK_BYTES))
            tail = handle.read(CHUNK_BYTES)
        else:
            tail = b""
    return xxhash.xxh128(head + tail + str(size).encode("utf-8")).hexdigest()


def full_hash(path: Path) -> str:
    digest = xxhash.xxh128()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
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
