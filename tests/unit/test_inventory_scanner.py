from __future__ import annotations

from pathlib import Path

from filescan.config import load_config
from filescan.inventory.scanner import InventoryScanner, ScanResult
from filescan.models import FolderRecord


def test_scan_folder_logs_inaccessible_folder_once(tmp_path: Path, write_config, monkeypatch) -> None:
    root = tmp_path / "root"
    restricted = root / "restricted"
    restricted.mkdir(parents=True, exist_ok=True)
    config = load_config(write_config([root]))
    scanner = InventoryScanner(config, rescan=True)
    logged: list[tuple[str, Path, str]] = []
    original_iterdir = Path.iterdir

    def failing_iterdir(path: Path):
        if path == restricted:
            raise PermissionError("simulated access denied")
        return original_iterdir(path)

    def capture_log(*, kind: str, path: Path, exc: OSError) -> None:
        InventoryScanner._log_skip(scanner, kind=kind, path=path, exc=exc)
        logged.append((kind, path, str(exc)))

    monkeypatch.setattr(Path, "iterdir", failing_iterdir)
    monkeypatch.setattr(scanner, "_log_skip", capture_log)

    result = scanner._scan_folder(restricted, root, 1)

    assert result.folder is None
    assert result.subdirs == []
    assert result.files == []
    assert logged == [("folder", restricted, "simulated access denied")]


def test_scan_deduplicates_repeated_subdirectories_in_queue(tmp_path: Path, write_config, monkeypatch) -> None:
    root = tmp_path / "root"
    child = root / "child"
    child.mkdir(parents=True, exist_ok=True)
    config = load_config(write_config([root]))
    scanner = InventoryScanner(config, rescan=True)
    calls: list[Path] = []

    def fake_scan_folder(folder: Path, root_path: Path, depth: int, stored_ts: float | None = None) -> ScanResult:
        calls.append(folder)
        if folder == root:
            return ScanResult(
                folder=FolderRecord(
                    path=root,
                    drive=root.drive,
                    parent_path=None,
                    depth=0,
                    file_count=0,
                    total_bytes=0,
                    mtime=0.0,
                ),
                files=[],
                subdirs=[child, child],
                raw_dirs=2,
                raw_files=0,
            )
        return ScanResult(
            folder=FolderRecord(
                path=child,
                drive=root.drive,
                parent_path=root,
                depth=1,
                file_count=0,
                total_bytes=0,
                mtime=0.0,
            ),
            files=[],
            subdirs=[],
            raw_dirs=0,
            raw_files=0,
        )

    monkeypatch.setattr(scanner, "_scan_folder", fake_scan_folder)

    scanner.scan()

    assert calls.count(root) == 1
    assert calls.count(child) == 1
