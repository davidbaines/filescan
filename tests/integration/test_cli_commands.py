from __future__ import annotations

import contextlib
import io
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


def test_duplicates_command_persists_duplicate_groups(tmp_path: Path, write_config) -> None:
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

    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "duplicates"]) == 0

    db = SQLiteDB(tmp_path / "filescan" / "file_index.db")
    duplicate_group_count = db.conn.execute("SELECT COUNT(*) FROM duplicate_groups").fetchone()[0]
    member_count = db.conn.execute("SELECT COUNT(*) FROM duplicate_group_members").fetchone()[0]

    assert duplicate_group_count == 1
    assert member_count == 2
    db.close()


def test_similarity_command_persists_similarity_candidates(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project/sub/shared.txt": b"same-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
            "project_backup/sub/shared.txt": b"same-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=8, similarity_threshold=0.2, merge_threshold=0.9)

    assert main(["--config", str(config_path), "scan"]) == 0
    assert main(["--config", str(config_path), "duplicates"]) == 0
    assert main(["--config", str(config_path), "similarity"]) == 0

    db = SQLiteDB(tmp_path / "filescan" / "file_index.db")
    candidate_count = db.conn.execute("SELECT COUNT(*) FROM folder_similarity_candidates").fetchone()[0]

    assert candidate_count >= 1
    db.close()


def test_run_command_executes_pipeline_and_prints_summary(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(
        root,
        {
            "project/readme.txt": b"readme-data-1234",
            "project_backup/readme.txt": b"readme-data-1234",
        },
    )
    config_path = write_config([root], duplicate_size_threshold=1, similarity_threshold=0.2, merge_threshold=0.9)

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        assert main(["--config", str(config_path), "run"]) == 0

    output = stdout.getvalue()
    assert "Run summary:" in output
    assert "scan: completed" in output
    assert "duplicates: completed" in output
    assert "similarity: completed" in output
    assert "plan: completed" in output
    assert "report: completed" in output
    assert (tmp_path / "filescan" / "filescan_report.xlsx").exists()
