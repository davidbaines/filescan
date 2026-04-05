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
