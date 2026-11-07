from __future__ import annotations

from pathlib import Path

from filescan.cli import main
from filescan.storage import SQLiteDB
from tests.helpers.tree_builder import build_tree


def test_scan_command_indexes_files_and_records_stats(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(root, {"docs/keep.txt": b"keep-me", "docs/other.txt": b"other"})
    config_path = write_config([root])

    assert main(["--config", str(config_path), "scan"]) == 0

    db = SQLiteDB(tmp_path / "filescan" / "file_index.db")
    file_count = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0]
    stats_count = db.conn.execute("SELECT COUNT(*) FROM scan_stats").fetchone()[0]

    assert file_count == 2
    assert stats_count == 1
    db.close()


def test_hash_all_command_hashes_every_file(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(root, {"a.txt": b"one", "b/c.txt": b"two and more"})
    config_path = write_config([root])
    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "hash-all"]) == 0
    db = SQLiteDB(tmp_path / "filescan" / "file_index.db")
    hashed = db.conn.execute("SELECT COUNT(*) FROM file_hashes WHERE full_hash IS NOT NULL").fetchone()[0]
    db.close()
    assert hashed == 2
