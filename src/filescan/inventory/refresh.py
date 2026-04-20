from __future__ import annotations

from pathlib import Path

from filescan.progress import track
from filescan.storage.db import SQLiteDB


def refresh_file_records(db: SQLiteDB, file_paths: list[Path]) -> int:
    """Stat each path; update size/mtime/ctime or mark missing in DB.
    One commit at end. Returns count of rows changed."""
    if not file_paths:
        return 0
    changes = 0
    for path in track(file_paths, desc="checking files", unit="file"):
        try:
            st = path.stat()
            cursor = db.conn.execute(
                """UPDATE files
                   SET size=?, mtime=?, ctime=?, is_missing=0, last_scanned_at=CURRENT_TIMESTAMP
                   WHERE path=?""",
                (st.st_size, st.st_mtime, st.st_ctime, str(path)),
            )
        except OSError:
            cursor = db.conn.execute(
                "UPDATE files SET is_missing=1 WHERE path=?", (str(path),)
            )
        changes += cursor.rowcount
    db.conn.commit()
    return changes


def refresh_folder_subtrees(db: SQLiteDB, folder_paths: list[Path]) -> int:
    """For each folder, check existence and stat all DB file records under it.
    Marks folders/files missing if gone. Returns count of rows changed."""
    if not folder_paths:
        return 0
    changes = 0
    all_file_paths: list[Path] = []

    for root in folder_paths:
        root_str = str(root)
        like_pattern = root_str.rstrip("\\") + "\\%"
        if not root.is_dir():
            c1 = db.conn.execute(
                "UPDATE folders SET is_missing=1 WHERE path=? OR path LIKE ?",
                (root_str, like_pattern),
            )
            c2 = db.conn.execute(
                """UPDATE files SET is_missing=1
                   WHERE folder_id IN (
                       SELECT id FROM folders WHERE path=? OR path LIKE ?
                   )""",
                (root_str, like_pattern),
            )
            changes += c1.rowcount + c2.rowcount
        else:
            rows = db.conn.execute(
                """SELECT fi.path FROM files fi
                   JOIN folders fo ON fo.id = fi.folder_id
                   WHERE (fo.path = ? OR fo.path LIKE ?) AND fi.is_missing = 0""",
                (root_str, like_pattern),
            ).fetchall()
            all_file_paths.extend(Path(row["path"]) for row in rows)

    db.conn.commit()
    changes += refresh_file_records(db, all_file_paths)
    return changes
