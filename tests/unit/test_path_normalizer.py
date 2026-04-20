from __future__ import annotations

from pathlib import Path

from filescan.inventory.normalizer import normalize_path


def test_normalizer_treats_windows_separator_variants_consistently() -> None:
    first = normalize_path("C:/Temp/example")
    second = normalize_path(r"C:\Temp\example")

    assert first == second


def test_normalizer_preserves_drive_root() -> None:
    path = normalize_path("D:/Archive")

    assert isinstance(path, Path)
    assert path.drive == "D:"
