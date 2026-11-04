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
    import os

    original_scandir = os.scandir

    def failing_scandir(path):
        if Path(path) == restricted:
            raise PermissionError("simulated access denied")
        return original_scandir(path)

    def capture_log(*, kind: str, path: Path, exc: OSError) -> None:
        InventoryScanner._log_skip(scanner, kind=kind, path=path, exc=exc)
        logged.append((kind, path, str(exc)))

    monkeypatch.setattr("filescan.inventory.scanner.os.scandir", failing_scandir)
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


def _make_junction(link: Path, target: Path) -> None:
    import subprocess

    subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True)


def test_scan_skips_junctions_instead_of_following_them(tmp_path: Path, write_config) -> None:
    from filescan.storage import FileRepository, SQLiteDB
    from tests.helpers.tree_builder import build_tree

    root = tmp_path / "root"
    outside = tmp_path / "outside"
    build_tree(root, {"real/a.txt": b"aaaa"})
    build_tree(outside, {"b.txt": b"bbbb"})
    _make_junction(root / "link_out", outside)
    _make_junction(root / "link_in", root / "real")
    config = load_config(write_config([root]))

    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    paths = sorted(record.path.name for record in FileRepository(db).list_active_files())
    stored = [row[0] for row in db.conn.execute("SELECT path FROM files")]
    db.close()
    assert paths == ["a.txt"]
    assert all(str(root) in path for path in stored)


def test_rescan_detects_in_place_edit_when_folder_mtime_is_unchanged(tmp_path: Path, write_config) -> None:
    import os

    from filescan.storage import FileRepository, SQLiteDB
    from tests.helpers.tree_builder import build_tree

    root = tmp_path / "root"
    build_tree(root, {"docs/report.txt": b"version one"})
    target = root / "docs" / "report.txt"
    config = load_config(write_config([root]))
    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    record = repo.list_active_files()[0]
    repo.upsert_file_hash(record.id, quick_hash="q", full_hash="f")
    db.close()

    folder_times = (target.parent.stat().st_atime, target.parent.stat().st_mtime)
    target.write_bytes(b"version two, edited in place")
    os.utime(target.parent, folder_times)

    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    record = FileRepository(db).list_active_files()[0]
    db.close()
    assert record.size == len(b"version two, edited in place")
    assert record.full_hash is None


def test_records_read_from_database_do_not_touch_filesystem(tmp_path: Path, write_config, monkeypatch) -> None:
    from filescan.storage import FileRepository, SQLiteDB
    from tests.helpers.tree_builder import build_tree

    root = tmp_path / "root"
    build_tree(root, {"a/b.txt": b"bbbb"})
    config = load_config(write_config([root]))
    InventoryScanner(config, rescan=True).scan()

    def no_resolve(self, strict=False):
        raise AssertionError(f"resolve() called for {self}")

    monkeypatch.setattr(Path, "resolve", no_resolve)
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    assert [f.path.name for f in repo.list_active_files()] == ["b.txt"]
    assert len(repo.list_active_folders()) == 2
    db.close()


def test_scan_stores_windows_file_attributes(tmp_path: Path, write_config) -> None:
    import subprocess

    from filescan.storage import FileRepository, SQLiteDB
    from tests.helpers.tree_builder import build_tree

    root = tmp_path / "root"
    build_tree(root, {"plain.txt": b"plain", "hidden.txt": b"hidden"})
    subprocess.run(["attrib", "+h", str(root / "hidden.txt")], check=True, capture_output=True)
    config = load_config(write_config([root]))

    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    attrs = {f.filename: f.attributes for f in FileRepository(db).list_active_files()}
    db.close()
    assert attrs["hidden.txt"] & 0x2  # FILE_ATTRIBUTE_HIDDEN
    assert not attrs["plain.txt"] & 0x2
