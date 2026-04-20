from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from filescan.inventory.normalizer import normalize_path
from filescan.models import DuplicateGroup, FileRecord, FolderRecord, FolderSimilarityCandidate
from filescan.storage.db import SQLiteDB


class FileRepository:
    def __init__(self, db: SQLiteDB) -> None:
        self.db = db
        self._folder_cache: dict[Path, int] = {}

    def begin_scan_run(self) -> int:
        cursor = self.db.conn.execute("INSERT INTO scan_runs DEFAULT VALUES")
        self.db.conn.commit()
        return int(cursor.lastrowid)

    def latest_scan_run_id(self) -> int:
        row = self.db.conn.execute("SELECT MAX(id) AS latest_id FROM scan_runs").fetchone()
        latest_id = 0 if row is None or row["latest_id"] is None else int(row["latest_id"])
        return latest_id

    def _path_prefix(self, root: Path) -> tuple[str, str]:
        root_str = str(root)
        # Drive roots (e.g. "F:\") already end with a backslash — strip it before
        # adding the LIKE wildcard so the pattern stays "F:\%" not "F:\\%".
        return root_str, root_str.rstrip("\\") + "\\%"

    def get_folder_id(self, folder_path: Path) -> int | None:
        normalized = normalize_path(folder_path)
        cached = self._folder_cache.get(normalized)
        if cached is not None:
            return cached
        row = self.db.conn.execute("SELECT id FROM folders WHERE path = ?", (str(normalized),)).fetchone()
        if row is None:
            return None
        folder_id = int(row["id"])
        self._folder_cache[normalized] = folder_id
        return folder_id

    def upsert_folder(self, folder: FolderRecord) -> int:
        parent_id = self.get_folder_id(folder.parent_path) if folder.parent_path else None
        self.db.conn.execute(
            """
            INSERT INTO folders (path, drive, parent_id, depth, file_count, total_bytes, mtime, scan_run_id, is_missing, last_scanned_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(path) DO UPDATE SET
                drive = excluded.drive,
                parent_id = excluded.parent_id,
                depth = excluded.depth,
                file_count = excluded.file_count,
                total_bytes = excluded.total_bytes,
                mtime = excluded.mtime,
                scan_run_id = excluded.scan_run_id,
                is_missing = 0,
                last_scanned_at = CURRENT_TIMESTAMP
            """,
            (
                str(folder.path),
                folder.drive,
                parent_id,
                folder.depth,
                folder.file_count,
                folder.total_bytes,
                folder.mtime,
                folder.scan_run_id,
            ),
        )
        self.db.conn.commit()
        folder_id = self.get_folder_id(folder.path)
        assert folder_id is not None
        return folder_id

    def upsert_files(self, folder_path: Path, files: list[FileRecord], scan_run_id: int) -> None:
        folder_id = self.get_folder_id(folder_path)
        if folder_id is None:
            raise ValueError(f"Folder must exist before files are upserted: {folder_path}")
        for file_record in files:
            self.db.conn.execute(
                """
                INSERT INTO files (folder_id, filename, path, size, mtime, ctime, scan_run_id, is_missing, last_scanned_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
                ON CONFLICT(path) DO UPDATE SET
                    folder_id = excluded.folder_id,
                    filename = excluded.filename,
                    size = excluded.size,
                    mtime = excluded.mtime,
                    ctime = excluded.ctime,
                    scan_run_id = excluded.scan_run_id,
                    is_missing = 0,
                    last_scanned_at = CURRENT_TIMESTAMP
                """,
                (
                    folder_id,
                    file_record.filename,
                    str(file_record.path),
                    file_record.size,
                    file_record.mtime,
                    file_record.ctime,
                    scan_run_id,
                ),
            )
        self.db.conn.commit()

    def mark_missing_under_root(self, root: Path, scan_run_id: int) -> None:
        exact, prefix = self._path_prefix(root)
        self.db.conn.execute(
            """
            UPDATE files
            SET is_missing = 1
            WHERE (path = ? OR path LIKE ?)
              AND scan_run_id < ?
            """,
            (exact, prefix, scan_run_id),
        )
        self.db.conn.execute(
            """
            UPDATE folders
            SET is_missing = 1
            WHERE (path = ? OR path LIKE ?)
              AND scan_run_id < ?
            """,
            (exact, prefix, scan_run_id),
        )
        self.db.conn.commit()

    def promote_folder_and_files(self, folder_path: Path, scan_run_id: int) -> None:
        """Delta scan: bring a skipped (unchanged) folder and its direct files up to scan_run_id."""
        folder_str = str(folder_path)
        self.db.conn.execute(
            "UPDATE folders SET scan_run_id = ? WHERE path = ? AND scan_run_id < ?",
            (scan_run_id, folder_str, scan_run_id),
        )
        self.db.conn.execute(
            """UPDATE files SET scan_run_id = ?
               WHERE folder_id = (SELECT id FROM folders WHERE path = ?)
                 AND scan_run_id < ?""",
            (scan_run_id, folder_str, scan_run_id),
        )
        self.db.conn.commit()

    def upsert_scan_stats(
        self,
        *,
        root: Path,
        scan_run_id: int,
        total_folders: int,
        total_files: int,
        indexed_folders: int,
        indexed_files: int,
    ) -> None:
        self.db.conn.execute(
            """
            INSERT INTO scan_stats (root_path, scan_run_id, total_folders, total_files, indexed_folders, indexed_files, last_scanned_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(root_path) DO UPDATE SET
                scan_run_id = excluded.scan_run_id,
                total_folders = excluded.total_folders,
                total_files = excluded.total_files,
                indexed_folders = excluded.indexed_folders,
                indexed_files = excluded.indexed_files,
                last_scanned_at = CURRENT_TIMESTAMP
            """,
            (str(root), scan_run_id, total_folders, total_files, indexed_folders, indexed_files),
        )
        self.db.conn.commit()

    def get_scan_stats(self, root: Path) -> dict[str, int | str] | None:
        row = self.db.conn.execute(
            """
            SELECT root_path, scan_run_id, total_folders, total_files, indexed_folders, indexed_files, last_scanned_at
            FROM scan_stats
            WHERE root_path = ?
            """,
            (str(root),),
        ).fetchone()
        return None if row is None else dict(row)

    def get_stage_scan_run_id(self, stage: str) -> int:
        row = self.db.conn.execute("SELECT scan_run_id FROM analysis_state WHERE stage = ?", (stage,)).fetchone()
        if row is None:
            return 0
        return int(row["scan_run_id"])

    def set_stage_scan_run_id(self, stage: str, scan_run_id: int) -> None:
        self.db.conn.execute(
            """
            INSERT INTO analysis_state (stage, scan_run_id, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(stage) DO UPDATE SET
                scan_run_id = excluded.scan_run_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            (stage, scan_run_id),
        )
        self.db.conn.commit()

    def list_active_folders(self) -> list[FolderRecord]:
        return list(self.iter_active_folders())

    def count_active_folders(self) -> int:
        row = self.db.conn.execute("SELECT COUNT(*) AS count FROM folders WHERE is_missing = 0").fetchone()
        return 0 if row is None else int(row["count"])

    def _folder_from_row(self, row) -> FolderRecord:
        return FolderRecord(
            id=int(row["id"]),
            path=normalize_path(row["path"]),
            drive=str(row["drive"]),
            parent_id=row["parent_id"],
            parent_path=normalize_path(row["parent_path"]) if row["parent_path"] else None,
            depth=int(row["depth"]),
            file_count=int(row["file_count"]),
            total_bytes=int(row["total_bytes"]),
            mtime=row["mtime"],
            scan_run_id=int(row["scan_run_id"]),
            is_missing=bool(row["is_missing"]),
        )

    def iter_active_folders(self):
        rows = self.db.conn.execute(
            """
            SELECT f.id, f.path, f.drive, f.parent_id, pf.path AS parent_path, f.depth,
                   f.file_count, f.total_bytes, f.mtime, f.scan_run_id, f.is_missing
            FROM folders f
            LEFT JOIN folders pf ON pf.id = f.parent_id
            WHERE f.is_missing = 0
            ORDER BY f.depth, f.path
            """
        )
        for row in rows:
            yield self._folder_from_row(row)

    def list_active_files(self, *, min_size: int = 0) -> list[FileRecord]:
        return list(self.iter_active_files(min_size=min_size))

    def count_active_files(self, *, min_size: int = 0) -> int:
        row = self.db.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM files
            JOIN folders ON folders.id = files.folder_id
            WHERE files.is_missing = 0
              AND folders.is_missing = 0
              AND files.size >= ?
            """,
            (min_size,),
        ).fetchone()
        return 0 if row is None else int(row["count"])

    def _file_from_row(self, row) -> FileRecord:
        return FileRecord(
            id=int(row["id"]),
            folder_id=int(row["folder_id"]),
            path=normalize_path(row["path"]),
            folder_path=normalize_path(row["folder_path"]),
            filename=str(row["filename"]),
            size=int(row["size"]),
            mtime=float(row["mtime"]),
            ctime=float(row["ctime"]),
            scan_run_id=int(row["scan_run_id"]),
            is_missing=bool(row["is_missing"]),
            quick_hash=row["quick_hash"],
            full_hash=row["full_hash"],
        )

    def iter_active_files(self, *, min_size: int = 0):
        rows = self.db.conn.execute(
            """
            SELECT files.id, files.folder_id, folders.path AS folder_path, files.filename, files.path, files.size,
                   files.mtime, files.ctime, files.scan_run_id, files.is_missing,
                   file_hashes.quick_hash, file_hashes.full_hash
            FROM files
            JOIN folders ON folders.id = files.folder_id
            LEFT JOIN file_hashes ON file_hashes.file_id = files.id
            WHERE files.is_missing = 0
              AND folders.is_missing = 0
              AND files.size >= ?
            ORDER BY files.path
            """,
            (min_size,),
        )
        for row in rows:
            yield self._file_from_row(row)

    def upsert_file_hash(self, file_id: int, *, quick_hash: str | None = None, full_hash: str | None = None) -> None:
        self.db.conn.execute(
            """
            INSERT INTO file_hashes (file_id, quick_hash, full_hash, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(file_id) DO UPDATE SET
                quick_hash = COALESCE(excluded.quick_hash, file_hashes.quick_hash),
                full_hash = COALESCE(excluded.full_hash, file_hashes.full_hash),
                updated_at = CURRENT_TIMESTAMP
            """,
            (file_id, quick_hash, full_hash),
        )
        self.db.conn.commit()

    def replace_duplicate_groups(self, groups: list[DuplicateGroup]) -> None:
        self.db.conn.execute("DELETE FROM duplicate_group_members")
        self.db.conn.execute("DELETE FROM duplicate_groups")
        for group in groups:
            cursor = self.db.conn.execute(
                """
                INSERT INTO duplicate_groups (full_hash, size_bytes, file_count, total_bytes, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (group.full_hash, group.size_bytes, group.file_count, group.total_bytes),
            )
            group_id = int(cursor.lastrowid)
            for file_record in group.files:
                if file_record.id is None:
                    raise ValueError("Duplicate groups require file ids.")
                self.db.conn.execute(
                    "INSERT INTO duplicate_group_members (group_id, file_id) VALUES (?, ?)",
                    (group_id, file_record.id),
                )
        self.db.conn.commit()

    def list_duplicate_groups(self) -> list[DuplicateGroup]:
        return list(self.iter_duplicate_groups())

    def count_duplicate_groups(self) -> int:
        row = self.db.conn.execute("SELECT COUNT(*) AS count FROM duplicate_groups").fetchone()
        return 0 if row is None else int(row["count"])

    def iter_duplicate_groups(self):
        rows = self.db.conn.execute(
            """
            SELECT dg.id AS group_id, dg.full_hash, dg.size_bytes,
                   files.id AS file_id, files.folder_id, folders.path AS folder_path, files.filename, files.path,
                   files.size, files.mtime, files.ctime, files.scan_run_id, files.is_missing,
                   file_hashes.quick_hash, file_hashes.full_hash
            FROM duplicate_groups dg
            JOIN duplicate_group_members dgm ON dgm.group_id = dg.id
            JOIN files ON files.id = dgm.file_id
            JOIN folders ON folders.id = files.folder_id
            LEFT JOIN file_hashes ON file_hashes.file_id = files.id
            WHERE files.is_missing = 0
            ORDER BY dg.id, files.path
            """
        )
        current_group_id: int | None = None
        current_full_hash = ""
        current_size_bytes = 0
        current_files: list[FileRecord] = []
        for row in rows:
            group_id = int(row["group_id"])
            if current_group_id is None:
                current_group_id = group_id
                current_full_hash = str(row["full_hash"])
                current_size_bytes = int(row["size_bytes"])
            elif group_id != current_group_id:
                yield DuplicateGroup(
                    id=current_group_id,
                    full_hash=current_full_hash,
                    size_bytes=current_size_bytes,
                    files=tuple(current_files),
                )
                current_group_id = group_id
                current_full_hash = str(row["full_hash"])
                current_size_bytes = int(row["size_bytes"])
                current_files = []
            current_files.append(
                FileRecord(
                    id=int(row["file_id"]),
                    folder_id=int(row["folder_id"]),
                    folder_path=normalize_path(row["folder_path"]),
                    path=normalize_path(row["path"]),
                    filename=str(row["filename"]),
                    size=int(row["size"]),
                    mtime=float(row["mtime"]),
                    ctime=float(row["ctime"]),
                    scan_run_id=int(row["scan_run_id"]),
                    is_missing=bool(row["is_missing"]),
                    quick_hash=row["quick_hash"],
                    full_hash=row["full_hash"],
                )
            )
        if current_group_id is not None:
            yield DuplicateGroup(
                id=current_group_id,
                full_hash=current_full_hash,
                size_bytes=current_size_bytes,
                files=tuple(current_files),
            )

    def replace_similarity_candidates(self, candidates: list[FolderSimilarityCandidate]) -> None:
        self.db.conn.execute("DELETE FROM folder_similarity_candidates")
        for candidate in candidates:
            if candidate.folder_a_id is None or candidate.folder_b_id is None:
                candidate.folder_a_id = self.get_folder_id(candidate.folder_a)
                candidate.folder_b_id = self.get_folder_id(candidate.folder_b)
            self.db.conn.execute(
                """
                INSERT INTO folder_similarity_candidates (
                    folder_a_id, folder_b_id, score, shared_duplicate_files, shared_signatures,
                    name_bonus, size_ratio, file_count_ratio, reason, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    candidate.folder_a_id,
                    candidate.folder_b_id,
                    candidate.score,
                    candidate.shared_duplicate_files,
                    candidate.shared_signatures,
                    candidate.name_bonus,
                    candidate.size_ratio,
                    candidate.file_count_ratio,
                    candidate.reason,
                ),
            )
        self.db.conn.commit()

    def list_similarity_candidates(self) -> list[FolderSimilarityCandidate]:
        rows = self.db.conn.execute(
            """
            SELECT c.id, c.folder_a_id, c.folder_b_id, c.score, c.shared_duplicate_files,
                   c.shared_signatures, c.name_bonus, c.size_ratio, c.file_count_ratio, c.reason,
                   fa.path AS folder_a_path, fb.path AS folder_b_path
            FROM folder_similarity_candidates c
            JOIN folders fa ON fa.id = c.folder_a_id
            JOIN folders fb ON fb.id = c.folder_b_id
            ORDER BY c.score DESC, folder_a_path, folder_b_path
            """
        ).fetchall()
        return [
            FolderSimilarityCandidate(
                id=int(row["id"]),
                folder_a_id=int(row["folder_a_id"]),
                folder_b_id=int(row["folder_b_id"]),
                folder_a=normalize_path(row["folder_a_path"]),
                folder_b=normalize_path(row["folder_b_path"]),
                score=float(row["score"]),
                shared_duplicate_files=int(row["shared_duplicate_files"]),
                shared_signatures=int(row["shared_signatures"]),
                name_bonus=float(row["name_bonus"]),
                size_ratio=float(row["size_ratio"]),
                file_count_ratio=float(row["file_count_ratio"]),
                reason=str(row["reason"]),
            )
            for row in rows
        ]

    def list_scan_stats(self) -> list[dict[str, int | str]]:
        rows = self.db.conn.execute(
            """
            SELECT root_path, scan_run_id, total_folders, total_files, indexed_folders, indexed_files, last_scanned_at
            FROM scan_stats
            ORDER BY root_path
            """
        ).fetchall()
        return [dict(row) for row in rows]
