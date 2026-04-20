from __future__ import annotations

import sqlite3
from pathlib import Path

from filescan.models import FolderRecord
from filescan.storage import FileRepository, SQLiteDB
from tests.helpers.indexing import index_paths
from tests.helpers.tree_builder import build_tree


def test_repository_round_trips_paths_and_marks_missing(tmp_path: Path, make_repo) -> None:
    root = tmp_path / "root"
    created = build_tree(root, {"folder/file.txt": b"content-1234"})
    db, repo = make_repo(tmp_path / "repository.db")
    scan_run_id = index_paths(repo, root, created)

    folders = repo.list_active_folders()
    files = repo.list_active_files()
    assert any(isinstance(folder.path, Path) for folder in folders)
    assert any(isinstance(file_record.path, Path) for file_record in files)

    created[0].unlink()
    next_run_id = repo.begin_scan_run()
    repo.upsert_folder(
        FolderRecord(
            path=root,
            drive=root.drive,
            parent_path=None,
            depth=0,
            file_count=0,
            total_bytes=0,
            mtime=root.stat().st_mtime,
            scan_run_id=next_run_id,
        )
    )
    repo.mark_missing_under_root(root, next_run_id)
    missing_count = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 1").fetchone()[0]

    assert scan_run_id < next_run_id
    assert missing_count == 1
    db.close()


def test_legacy_file_hashes_table_is_migrated_before_hash_updates(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            path TEXT NOT NULL UNIQUE,
            size INTEGER NOT NULL,
            mtime REAL NOT NULL,
            ctime REAL NOT NULL,
            scan_run_id INTEGER NOT NULL DEFAULT 0,
            is_missing INTEGER NOT NULL DEFAULT 0,
            last_scanned_at TEXT
        );

        CREATE TABLE file_hashes (
            file_id INTEGER PRIMARY KEY,
            quick_hash TEXT,
            full_hash TEXT
        );
        """
    )
    conn.execute(
        """
        INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id, is_missing, last_scanned_at)
        VALUES (1, 'file.txt', 'C:/temp/file.txt', 10, 0.0, 0.0, 1, 0, CURRENT_TIMESTAMP)
        """
    )
    conn.execute("INSERT INTO file_hashes (file_id, quick_hash, full_hash) VALUES (1, NULL, NULL)")
    conn.commit()
    conn.close()

    db = SQLiteDB(db_path)
    repo = FileRepository(db)
    repo.upsert_file_hash(1, quick_hash="quick-digest")

    columns = {row[1] for row in db.conn.execute("PRAGMA table_info(file_hashes)").fetchall()}
    row = db.conn.execute("SELECT quick_hash, updated_at FROM file_hashes WHERE file_id = 1").fetchone()

    assert "updated_at" in columns
    assert row["quick_hash"] == "quick-digest"
    assert row["updated_at"] is not None
    db.close()
