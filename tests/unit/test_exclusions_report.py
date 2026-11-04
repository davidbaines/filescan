from __future__ import annotations

from pathlib import Path

import yaml

from filescan.config import load_config
from filescan.inventory.scanner import InventoryScanner
from filescan.reporting.exclusions import excluded_subtrees, write_exclusions_report
from filescan.storage import SQLiteDB
from tests.helpers.tree_builder import build_tree


def test_excluded_subtrees_counts_each_subtree_once_and_keeps_user_folders(tmp_path: Path) -> None:
    root = tmp_path / "D"
    build_tree(
        root,
        {
            "Windows/System32/kernel.dll": b"k" * 100,
            "Windows/System32/drivers/x.sys": b"x" * 50,
            "Windows/notepad.exe": b"n" * 10,
            "GitHub/python/Windows/notes.md": b"keep me",
            "Work/Drivers/install.txt": b"keep me too",
            "Acronis Images/disk.tib": b"t" * 1000,
        },
    )
    anchor = str(root).replace("\\", "\\\\")
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(root)}],
                "filescan_folder": str(tmp_path / "fs"),
                "database_folder": str(tmp_path / "fs"),
                "database_filename": "db.sqlite",
                "report_filename": "r.xlsx",
                "exclusions": [
                    {"name": "old-os", "pattern": f"^{anchor}\\\\(Windows|Program Files)\\\\", "reason": "Old OS"},
                    {"name": "acronis", "pattern": r"\\Acronis[^\\]*\\", "reason": "Disk images"},
                ],
            }
        )
    )
    config = load_config(config_path)
    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    subtrees = excluded_subtrees(db, config)
    db.close()

    by_rule = {s.rule: s for s in subtrees}
    assert by_rule["old-os"].path == root / "Windows"
    assert by_rule["old-os"].file_count == 3
    assert by_rule["old-os"].total_bytes == 160
    assert by_rule["acronis"].total_bytes == 1000
    assert len(subtrees) == 2  # GitHub\python\Windows and Work\Drivers are not excluded


def test_write_exclusions_report_writes_csv_with_totals(tmp_path: Path) -> None:
    root = tmp_path / "E"
    build_tree(root, {"Gutenberg/book.txt": b"b" * 20, "Photos/a.jpg": b"p"})
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(root)}],
                "filescan_folder": str(tmp_path / "fs"),
                "database_folder": str(tmp_path / "fs"),
                "database_filename": "db.sqlite",
                "report_filename": "r.xlsx",
                "exclusions": [{"name": "corpora", "pattern": r"\\Gutenberg\\", "reason": "Public corpus"}],
            }
        )
    )
    config = load_config(config_path)
    InventoryScanner(config, rescan=True).scan()

    out = write_exclusions_report(config)

    text = out.read_text(encoding="utf-8")
    assert out.suffix == ".csv"
    assert "corpora" in text and "Gutenberg" in text and "20" in text
    assert "Photos" not in text
