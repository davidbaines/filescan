from __future__ import annotations

from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.duplicates import DuplicateDetector
from filescan.inventory.scanner import InventoryScanner
from filescan.storage import SQLiteDB
from tests.helpers.tree_builder import build_tree


def test_duplicates_pipeline_finds_renamed_duplicates_and_reuses_hashes(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "a/original.bin": b"abcdefgh12345678",
            "b/renamed.bin": b"abcdefgh12345678",
            "c/other.bin": b"not-a-duplicate",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=8)
    config = load_config(config_path)

    InventoryScanner(config).scan()
    first_groups = DuplicateDetector(config).run()
    second_groups = DuplicateDetector(config).run()

    db = SQLiteDB(config.database_path)
    hash_count = db.conn.execute("SELECT COUNT(*) FROM file_hashes").fetchone()[0]
    assert len(first_groups) == 1
    assert len(second_groups) == 1
    assert hash_count >= 2
    db.close()
