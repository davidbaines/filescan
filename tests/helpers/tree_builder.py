from __future__ import annotations

from pathlib import Path


def write_file(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def build_tree(root: Path, files: dict[str, bytes]) -> list[Path]:
    created: list[Path] = []
    for relative_path, content in files.items():
        created.append(write_file(root / relative_path, content))
    return created
