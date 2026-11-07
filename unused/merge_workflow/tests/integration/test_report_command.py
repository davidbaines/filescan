from __future__ import annotations

from pathlib import Path

from filescan.cli import main
from tests.helpers.tree_builder import build_tree


def test_report_command_creates_xlsx_in_filescan_folder(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=1, similarity_threshold=0.2, merge_threshold=0.9)

    assert main(["--config", str(config_path)]) == 0
    assert main(["--config", str(config_path), "report"]) == 0

    assert (tmp_path / "filescan" / "filescan_report.xlsx").exists()
