from __future__ import annotations

from pathlib import Path

from filescan.config import load_config
from filescan.inventory.scanner import InventoryScanner
from filescan.storage import FileRepository, SQLiteDB
from tests.helpers.tree_builder import build_tree


def test_scan_pipeline_records_files_and_tombstones_removed_items(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    created = build_tree(root, {"docs/keep.txt": b"keep-me", "docs/remove.txt": b"remove-me"})
    config_path = write_config([root])
    config = load_config(config_path)

    InventoryScanner(config).scan()
    created[1].unlink()
    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    active_files = repo.list_active_files()
    missing_count = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 1").fetchone()[0]

    assert sorted(file_record.filename for file_record in active_files) == ["keep.txt"]
    assert missing_count == 1
    db.close()


def test_scan_pipeline_skips_previously_scanned_root_without_rescan(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    created = build_tree(root, {"docs/keep.txt": b"keep-me", "docs/remove.txt": b"remove-me"})
    config_path = write_config([root])
    config = load_config(config_path)

    first_scan_run_id = InventoryScanner(config).scan()
    created[1].unlink()
    second_scan_run_id = InventoryScanner(config).scan()

    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    active_files = repo.list_active_files()
    missing_count = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 1").fetchone()[0]

    assert second_scan_run_id == first_scan_run_id
    assert sorted(file_record.filename for file_record in active_files) == ["keep.txt", "remove.txt"]
    assert missing_count == 0
    db.close()
