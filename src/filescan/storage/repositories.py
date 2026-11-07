from __future__ import annotations

from pathlib import Path

from filescan.models import FileRecord, FolderRecord
from filescan.storage.db import SQLiteDB
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK, path_prefix


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
        return path_prefix(root)

    def get_folder_id(self, folder_path: Path) -> int | None:
        normalized = Path(folder_path)
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
            # A changed size or mtime means the stored hashes may describe old content.
            self.db.conn.execute(
                """
                DELETE FROM file_hashes
                WHERE file_id = (
                    SELECT id FROM files WHERE path = ? AND (size != ? OR mtime != ?)
                )
                """,
                (str(file_record.path), file_record.size, file_record.mtime),
            )
            self.db.conn.execute(
                """
                INSERT INTO files (
                    folder_id, filename, path, size, mtime, ctime, scan_run_id, is_missing, last_scanned_at, attributes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(path) DO UPDATE SET
                    folder_id = excluded.folder_id,
                    filename = excluded.filename,
                    size = excluded.size,
                    mtime = excluded.mtime,
                    ctime = excluded.ctime,
                    scan_run_id = excluded.scan_run_id,
                    is_missing = 0,
                    last_scanned_at = CURRENT_TIMESTAMP,
                    attributes = excluded.attributes
                """,
                (
                    folder_id,
                    file_record.filename,
                    str(file_record.path),
                    file_record.size,
                    file_record.mtime,
                    file_record.ctime,
                    scan_run_id,
                    file_record.attributes,
                ),
            )
        self.db.conn.commit()

    def mark_missing_under_root(self, root: Path, scan_run_id: int) -> None:
        exact, prefix = self._path_prefix(root)
        self.db.conn.execute(
            """
            UPDATE files
            SET is_missing = 1
            WHERE (path = ? OR path LIKE ? ESCAPE '|')
              AND scan_run_id < ?
            """,
            (exact, prefix, scan_run_id),
        )
        self.db.conn.execute(
            """
            UPDATE folders
            SET is_missing = 1
            WHERE (path = ? OR path LIKE ? ESCAPE '|')
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

    def list_active_folders(self) -> list[FolderRecord]:
        return list(self.iter_active_folders())

    def count_active_folders(self) -> int:
        row = self.db.conn.execute("SELECT COUNT(*) AS count FROM folders WHERE is_missing = 0").fetchone()
        return 0 if row is None else int(row["count"])

    def _folder_from_row(self, row) -> FolderRecord:
        return FolderRecord(
            id=int(row["id"]),
            path=Path(row["path"]),
            drive=str(row["drive"]),
            parent_id=row["parent_id"],
            parent_path=Path(row["parent_path"]) if row["parent_path"] else None,
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

    def files_without_full_hash(self, *, skip_roots: list[Path] = ()) -> list[FileRecord]:
        """Active files with no full hash, outside `skip_roots`. Cloud placeholders are left out."""
        skip = "".join(" AND NOT (files.path = ? OR files.path LIKE ? ESCAPE '|')" for _ in skip_roots)
        params = [value for root in skip_roots for value in path_prefix(root)]
        rows = self.db.conn.execute(
            f"""
            SELECT files.id, files.folder_id, folders.path AS folder_path, files.filename, files.path, files.size,
                   files.mtime, files.ctime, files.scan_run_id, files.is_missing, files.attributes,
                   file_hashes.quick_hash, file_hashes.full_hash
            FROM files
            JOIN folders ON folders.id = files.folder_id
            LEFT JOIN file_hashes ON file_hashes.file_id = files.id
            WHERE files.is_missing = 0
              AND folders.is_missing = 0
              AND (file_hashes.full_hash IS NULL OR file_hashes.full_hash = '')
              AND (files.attributes & ?) = 0
              {skip}
            """,
            (CLOUD_PLACEHOLDER_MASK, *params),
        )
        return [self._file_from_row(row) for row in rows]

    def list_active_files(self, *, min_size: int = 0) -> list[FileRecord]:
        return list(self.iter_active_files(min_size=min_size))

    def _file_from_row(self, row) -> FileRecord:
        return FileRecord(
            id=int(row["id"]),
            folder_id=int(row["folder_id"]),
            path=Path(row["path"]),
            folder_path=Path(row["folder_path"]),
            filename=str(row["filename"]),
            size=int(row["size"]),
            mtime=float(row["mtime"]),
            ctime=float(row["ctime"]),
            scan_run_id=int(row["scan_run_id"]),
            is_missing=bool(row["is_missing"]),
            quick_hash=row["quick_hash"],
            full_hash=row["full_hash"],
            attributes=int(row["attributes"]) if "attributes" in row.keys() else 0,
        )

    def iter_active_files(self, *, min_size: int = 0):
        rows = self.db.conn.execute(
            """
            SELECT files.id, files.folder_id, folders.path AS folder_path, files.filename, files.path, files.size,
                   files.mtime, files.ctime, files.scan_run_id, files.is_missing, files.attributes,
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

    def upsert_file_hash(
        self, file_id: int, *, quick_hash: str | None = None, full_hash: str | None = None, commit: bool = True
    ) -> None:
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
        if commit:
            self.db.conn.commit()
