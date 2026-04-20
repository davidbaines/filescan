from __future__ import annotations

import sqlite3
from pathlib import Path


class SQLiteDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        try:
            try:
                self.conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.OperationalError:
                self.conn.execute("PRAGMA journal_mode=DELETE")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self._create_tables()
            self._migrate_legacy_schema()
            self._create_indexes()
        except sqlite3.OperationalError as exc:
            self.conn.close()
            raise sqlite3.OperationalError(f"{exc} (database: {self.db_path})") from exc

    def _create_tables(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS scan_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                drive TEXT NOT NULL,
                parent_id INTEGER REFERENCES folders(id),
                depth INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                mtime REAL,
                scan_run_id INTEGER NOT NULL,
                is_missing INTEGER NOT NULL DEFAULT 0,
                last_scanned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_id INTEGER NOT NULL REFERENCES folders(id),
                filename TEXT NOT NULL,
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL,
                mtime REAL NOT NULL,
                ctime REAL NOT NULL,
                scan_run_id INTEGER NOT NULL,
                is_missing INTEGER NOT NULL DEFAULT 0,
                last_scanned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS file_hashes (
                file_id INTEGER PRIMARY KEY REFERENCES files(id),
                quick_hash TEXT,
                full_hash TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS duplicate_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_hash TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                file_count INTEGER NOT NULL,
                total_bytes INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(full_hash, size_bytes)
            );

            CREATE TABLE IF NOT EXISTS duplicate_group_members (
                group_id INTEGER NOT NULL REFERENCES duplicate_groups(id),
                file_id INTEGER NOT NULL UNIQUE REFERENCES files(id),
                PRIMARY KEY (group_id, file_id)
            );

            CREATE TABLE IF NOT EXISTS folder_similarity_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_a_id INTEGER NOT NULL REFERENCES folders(id),
                folder_b_id INTEGER NOT NULL REFERENCES folders(id),
                score REAL NOT NULL,
                shared_duplicate_files INTEGER NOT NULL,
                shared_signatures INTEGER NOT NULL,
                name_bonus REAL NOT NULL,
                size_ratio REAL NOT NULL,
                file_count_ratio REAL NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (folder_a_id, folder_b_id)
            );

            CREATE TABLE IF NOT EXISTS scan_stats (
                root_path TEXT PRIMARY KEY,
                scan_run_id INTEGER NOT NULL,
                total_folders INTEGER NOT NULL,
                total_files INTEGER NOT NULL,
                indexed_folders INTEGER NOT NULL,
                indexed_files INTEGER NOT NULL,
                last_scanned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS analysis_state (
                stage TEXT PRIMARY KEY,
                scan_run_id INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.conn.commit()

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_column(self, table_name: str, column_name: str, definition: str) -> None:
        if column_name in self._table_columns(table_name):
            return
        self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")

    def _migrate_scan_stats_table(self) -> None:
        columns = self._table_columns("scan_stats")
        if not columns:
            return
        if "root_path" in columns:
            self._ensure_column("scan_stats", "scan_run_id", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("scan_stats", "indexed_folders", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("scan_stats", "indexed_files", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("scan_stats", "last_scanned_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
            return

        if "folder_root" not in columns:
            return

        self.conn.execute("ALTER TABLE scan_stats RENAME TO scan_stats_legacy")
        self.conn.execute(
            """
            CREATE TABLE scan_stats (
                root_path TEXT PRIMARY KEY,
                scan_run_id INTEGER NOT NULL,
                total_folders INTEGER NOT NULL,
                total_files INTEGER NOT NULL,
                indexed_folders INTEGER NOT NULL,
                indexed_files INTEGER NOT NULL,
                last_scanned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO scan_stats (root_path, scan_run_id, total_folders, total_files, indexed_folders, indexed_files, last_scanned_at)
            SELECT
                folder_root,
                0,
                total_folders,
                total_files,
                scanned_folders,
                scanned_files,
                COALESCE(last_scanned, CURRENT_TIMESTAMP)
            FROM scan_stats_legacy
            """
        )
        self.conn.execute("DROP TABLE scan_stats_legacy")

    def _migrate_legacy_schema(self) -> None:
        self._ensure_column("folders", "parent_id", "INTEGER REFERENCES folders(id)")
        self._ensure_column("folders", "mtime", "REAL")
        self._ensure_column("folders", "scan_run_id", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("folders", "is_missing", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("folders", "last_scanned_at", "TEXT")
        folder_columns = self._table_columns("folders")
        if "last_scanned" in folder_columns and "last_scanned_at" in folder_columns:
            self.conn.execute(
                """
                UPDATE folders
                SET last_scanned_at = COALESCE(last_scanned, last_scanned_at)
                WHERE last_scanned IS NOT NULL
                """
            )
        self.conn.execute(
            """
            UPDATE folders
            SET last_scanned_at = COALESCE(last_scanned_at, CURRENT_TIMESTAMP)
            """
        )

        self._ensure_column("files", "scan_run_id", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("files", "is_missing", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("files", "last_scanned_at", "TEXT")
        self.conn.execute(
            """
            UPDATE files
            SET last_scanned_at = COALESCE(last_scanned_at, CURRENT_TIMESTAMP)
            """
        )

        self._ensure_column("file_hashes", "updated_at", "TEXT")
        self.conn.execute(
            """
            UPDATE file_hashes
            SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
            """
        )

        self._ensure_column("duplicate_groups", "created_at", "TEXT")
        self.conn.execute(
            """
            UPDATE duplicate_groups
            SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)
            """
        )

        self._ensure_column("folder_similarity_candidates", "created_at", "TEXT")
        self.conn.execute(
            """
            UPDATE folder_similarity_candidates
            SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)
            """
        )

        self._ensure_column("analysis_state", "updated_at", "TEXT")
        self.conn.execute(
            """
            UPDATE analysis_state
            SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
            """
        )

        self._migrate_scan_stats_table()
        self.conn.commit()

    def _create_indexes(self) -> None:
        self.conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_files_folder ON files(folder_id);
            CREATE INDEX IF NOT EXISTS idx_files_scan ON files(scan_run_id, is_missing);
            CREATE INDEX IF NOT EXISTS idx_folders_parent ON folders(parent_id);
            CREATE INDEX IF NOT EXISTS idx_folders_scan ON folders(scan_run_id, is_missing);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def validate_database_ready(db_path: Path) -> None:
    db = SQLiteDB(db_path)
    db.close()
