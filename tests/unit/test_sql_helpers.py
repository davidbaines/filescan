from __future__ import annotations

import sqlite3
from pathlib import Path

from filescan.models import FolderRecord
from filescan.storage.sql import escape_like, path_prefix
from tests.helpers.indexing import index_paths
from tests.helpers.tree_builder import build_tree


def _matches(pattern_source: str, candidates: list[str]) -> list[str]:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (path TEXT)")
    conn.executemany("INSERT INTO t VALUES (?)", [(c,) for c in candidates])
    exact, pattern = path_prefix(pattern_source)
    rows = conn.execute("SELECT path FROM t WHERE path = ? OR path LIKE ? ESCAPE '|' ORDER BY path", (exact, pattern))
    return [r[0] for r in rows]


def test_escape_like_escapes_wildcards_and_escape_char() -> None:
    assert escape_like("a_b%c|d") == "a|_b|%c||d"


def test_path_prefix_treats_underscore_and_percent_literally() -> None:
    candidates = [
        "F:\\my_photos",
        "F:\\my_photos\\a.jpg",
        "F:\\myXphotos\\b.jpg",
        "F:\\100%\\c.txt",
        "F:\\1000\\d.txt",
    ]
    assert _matches("F:\\my_photos", candidates) == ["F:\\my_photos", "F:\\my_photos\\a.jpg"]
    assert _matches("F:\\100%", candidates) == ["F:\\100%\\c.txt"]


def test_path_prefix_drive_root_and_unc_root() -> None:
    assert path_prefix("F:\\") == ("F:\\", "F:\\%")
    assert path_prefix("\\\\192.168.1.133\\disk3") == ("\\\\192.168.1.133\\disk3", "\\\\192.168.1.133\\disk3\\%")


def test_mark_missing_under_root_ignores_sibling_matched_by_wildcard(tmp_path: Path, make_repo) -> None:
    root_a = tmp_path / "my_a"
    root_b = tmp_path / "myXa"
    created_a = build_tree(root_a, {"one.txt": b"one"})
    created_b = build_tree(root_b, {"two.txt": b"two"})
    db, repo = make_repo(tmp_path / "repo.db")
    index_paths(repo, root_b, created_b)
    index_paths(repo, root_a, created_a)

    # A later run that only re-indexes my_a must not mark myXa missing.
    next_run = repo.begin_scan_run()
    repo.upsert_folder(
        FolderRecord(
            path=root_a, drive=root_a.drive, parent_path=None, depth=0,
            file_count=0, total_bytes=0, mtime=root_a.stat().st_mtime, scan_run_id=next_run,
        )
    )
    repo.mark_missing_under_root(root_a, next_run)

    missing = {row[0] for row in db.conn.execute("SELECT path FROM files WHERE is_missing = 1")}
    assert all("myXa" not in path for path in missing)
    db.close()
