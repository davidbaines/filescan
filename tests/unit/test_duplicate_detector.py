from __future__ import annotations

from pathlib import Path

from filescan.dedupe import duplicates as duplicate_module
from filescan.dedupe.duplicates import DuplicateDetector
from filescan.dedupe.hashing import quick_hash as real_quick_hash
from filescan.models import ScanConfig
from tests.helpers.indexing import index_paths
from tests.helpers.tree_builder import build_tree


def test_duplicate_detector_groups_renamed_duplicates(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "one/original.bin": b"abcdefgh12345678",
            "two/renamed.bin": b"abcdefgh12345678",
            "three/other.bin": b"zzzzzzzz99999999",
        },
    )
    db, repo = make_repo(tmp_path / "duplicates.db")
    index_paths(repo, root, created)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "duplicates.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=8,
        )
    )

    groups = detector.run()

    assert len(groups) == 1
    assert sorted(file_record.filename for file_record in groups[0].files) == ["original.bin", "renamed.bin"]
    db.close()


def test_duplicate_detector_rejects_quick_hash_false_positive(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    shared_prefix = b"a" * 4096
    shared_suffix = b"z" * 4096
    created = build_tree(
        root,
        {
            "first/collision.bin": shared_prefix + (b"b" * 4096) + shared_suffix,
            "second/collision-copy.bin": shared_prefix + (b"c" * 4096) + shared_suffix,
        },
    )
    db, repo = make_repo(tmp_path / "collision.db")
    index_paths(repo, root, created)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "collision.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=8,
        )
    )

    groups = detector.run()

    assert groups == []
    db.close()


def test_duplicate_detector_skips_unreadable_files_and_continues(tmp_path: Path, make_repo, monkeypatch) -> None:
    root = tmp_path / "root"
    created = build_tree(
        root,
        {
            "one/original.bin": b"abcdefgh12345678",
            "two/renamed.bin": b"abcdefgh12345678",
            "three/locked.bin": b"zzzzzzzz99999999",
        },
    )
    db, repo = make_repo(tmp_path / "permission.db")
    index_paths(repo, root, created)
    detector = DuplicateDetector(
        ScanConfig(
            roots=[root],
            filescan_folder=tmp_path / "filescan",
            database_path=tmp_path / "permission.db",
            report_path=tmp_path / "report.xlsx",
            duplicate_size_threshold=8,
        )
    )

    def guarded_quick_hash(path: Path) -> str:
        if path.name == "locked.bin":
            raise PermissionError("simulated access denied")
        return real_quick_hash(path)

    monkeypatch.setattr(duplicate_module, "quick_hash", guarded_quick_hash)

    groups = detector.run()

    assert len(groups) == 1
    assert sorted(file_record.filename for file_record in groups[0].files) == ["original.bin", "renamed.bin"]
    db.close()
