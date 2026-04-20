from __future__ import annotations

import json
from pathlib import Path

import pytest

from filescan.models import FolderRecord
from filescan.similarity.clusters import _cycle_master_in_dict
from filescan.similarity.merge_review import _detail_text, _list_label
from filescan.storage import FileRepository, SQLiteDB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(
    members: list[dict],
    *,
    cluster_id: str = "cluster-0001",
    min_score: float = 0.82,
    is_suppressed: bool = False,
    status: str = "pending",
) -> dict:
    return {
        "cluster_id": cluster_id,
        "min_score": min_score,
        "is_suppressed": is_suppressed,
        "status": status,
        "members": members,
    }


def _member(path: str, *, is_master: bool = False, file_count: int = 10,
            total_bytes: int = 1_000_000, unique_files: list[str] | None = None) -> dict:
    unique = unique_files or []
    return {
        "path": path,
        "is_master": is_master,
        "file_count": file_count,
        "total_bytes": total_bytes,
        "unique_file_count": len(unique),
        "unique_files": unique,
    }


# ---------------------------------------------------------------------------
# _list_label tests
# ---------------------------------------------------------------------------

def test_list_label_pending_shows_dot() -> None:
    c = _cluster([_member("C:/A/Reports", is_master=True), _member("D:/B/Reports")])
    label = _list_label(c)
    assert label.startswith("·")


def test_list_label_approved_shows_checkmark() -> None:
    c = _cluster([_member("C:/A/Reports", is_master=True), _member("D:/B")], status="approved")
    label = _list_label(c)
    assert label.startswith("✓")


def test_list_label_shows_score_and_master_name() -> None:
    c = _cluster([_member("C:/A/Reports", is_master=True, total_bytes=2_000_000), _member("D:/B")])
    label = _list_label(c)
    assert "0.82" in label
    assert "Reports" in label


# ---------------------------------------------------------------------------
# _detail_text tests
# ---------------------------------------------------------------------------

def test_detail_text_marks_master() -> None:
    c = _cluster([
        _member("C:/A/Reports", is_master=True),
        _member("D:/B/Reports"),
    ])
    text = _detail_text(c)
    assert "MASTER" in text
    assert "COPY" in text


def test_detail_text_lists_unique_files() -> None:
    c = _cluster([
        _member("C:/A/Master", is_master=True),
        _member("D:/B/Copy", unique_files=["D:/B/Copy/notes.txt", "D:/B/Copy/data.csv"]),
    ])
    text = _detail_text(c)
    assert "notes.txt" in text
    assert "data.csv" in text


def test_detail_text_suppressed_shows_note() -> None:
    c = _cluster([_member("C:/A", is_master=True), _member("C:/A/child")], is_suppressed=True)
    text = _detail_text(c)
    assert "suppressed" in text


# ---------------------------------------------------------------------------
# _cycle_master_in_dict tests
# ---------------------------------------------------------------------------

def _setup_db(tmp_path: Path) -> tuple[SQLiteDB, FileRepository, int]:
    db = SQLiteDB(tmp_path / "test.db")
    repo = FileRepository(db)
    run_id = repo.begin_scan_run()
    return db, repo, run_id


def _add_folder(repo: FileRepository, path: Path, run_id: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    repo.upsert_folder(FolderRecord(
        path=path, drive=path.drive, parent_path=path.parent,
        depth=1, file_count=5, total_bytes=500_000,
        mtime=path.stat().st_mtime, scan_run_id=run_id,
    ))


def test_cycle_master_rotates_to_next_member(tmp_path: Path) -> None:
    db, repo, run_id = _setup_db(tmp_path)
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    for f in (folder_a, folder_b):
        _add_folder(repo, f, run_id)

    cluster = _cluster([
        _member(str(folder_a), is_master=True),
        _member(str(folder_b), is_master=False),
    ])

    _cycle_master_in_dict(cluster, db)
    db.close()

    assert not cluster["members"][0]["is_master"]
    assert cluster["members"][1]["is_master"]


def test_cycle_master_wraps_around(tmp_path: Path) -> None:
    db, repo, run_id = _setup_db(tmp_path)
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    for f in (folder_a, folder_b):
        _add_folder(repo, f, run_id)

    cluster = _cluster([
        _member(str(folder_a), is_master=False),
        _member(str(folder_b), is_master=True),
    ])

    _cycle_master_in_dict(cluster, db)
    db.close()

    assert cluster["members"][0]["is_master"]
    assert not cluster["members"][1]["is_master"]


def test_cycle_master_recomputes_unique_files(tmp_path: Path) -> None:
    db, repo, run_id = _setup_db(tmp_path)
    folder_a = tmp_path / "A"
    folder_b = tmp_path / "B"
    for f in (folder_a, folder_b):
        _add_folder(repo, f, run_id)

    folder_a_id = repo.get_folder_id(folder_a)
    folder_b_id = repo.get_folder_id(folder_b)

    shared_hash = "aabbcc"
    unique_hash = "112233"

    # shared file in A
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (folder_a_id, "shared.txt", str(folder_a / "shared.txt"), 100, 0.0, 0.0, run_id),
    )
    fid = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.conn.execute("INSERT INTO file_hashes (file_id, full_hash) VALUES (?,?)", (fid, shared_hash))

    # shared file in B
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (folder_b_id, "shared.txt", str(folder_b / "shared.txt"), 100, 0.0, 0.0, run_id),
    )
    fid = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.conn.execute("INSERT INTO file_hashes (file_id, full_hash) VALUES (?,?)", (fid, shared_hash))

    # unique file in B only
    db.conn.execute(
        "INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id) VALUES (?,?,?,?,?,?,?)",
        (folder_b_id, "unique.txt", str(folder_b / "unique.txt"), 200, 0.0, 0.0, run_id),
    )
    fid = int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    db.conn.execute("INSERT INTO file_hashes (file_id, full_hash) VALUES (?,?)", (fid, unique_hash))
    db.conn.commit()

    # Start: A is master → B should have 1 unique file
    cluster = _cluster([
        _member(str(folder_a), is_master=True),
        _member(str(folder_b), is_master=False),
    ])
    _cycle_master_in_dict(cluster, db)

    # After cycle: B is master → A has 0 unique files (all of A's files exist in B)
    master = next(m for m in cluster["members"] if m["is_master"])
    copy = next(m for m in cluster["members"] if not m["is_master"])
    assert master["path"] == str(folder_b)
    assert copy["unique_file_count"] == 0

    db.close()
