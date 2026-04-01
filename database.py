import sqlite3

from pathlib import Path



class FileDB:

    "SQLite database for file indexing and duplicate analysis"

    def __init__(self, db_path):

        self.db_path = Path(db_path)

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.db_path))

        self.conn.execute("PRAGMA journal_mode=WAL")

        self.conn.execute("PRAGMA foreign_keys=ON")

        self._create_tables()



    def _create_tables(self):

        self.conn.executescript("""

            CREATE TABLE IF NOT EXISTS folders (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                path TEXT UNIQUE NOT NULL,

                drive TEXT NOT NULL,

                file_count INTEGER DEFAULT 0,

                total_bytes INTEGER DEFAULT 0,

                depth INTEGER DEFAULT 0,

                last_scanned TIMESTAMP

            );

            CREATE TABLE IF NOT EXISTS files (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                folder_id INTEGER NOT NULL,

                filename TEXT NOT NULL,

                path TEXT UNIQUE NOT NULL,

                size INTEGER NOT NULL,

                mtime REAL NOT NULL,

                ctime REAL NOT NULL,

                FOREIGN KEY (folder_id) REFERENCES folders(id)

            );

            CREATE INDEX IF NOT EXISTS idx_files_folder ON files(folder_id);

            CREATE INDEX IF NOT EXISTS idx_files_name_size ON files(filename, size);

            CREATE INDEX IF NOT EXISTS idx_folders_drive ON folders(drive);

        """)

        self.conn.commit()



    def upsert_folder(self, path, drive, file_count, total_bytes, depth):

        "Insert or update a folder record"

        self.conn.execute(

            """INSERT INTO folders (path, drive, file_count, total_bytes, depth, last_scanned)

               VALUES (?, ?, ?, ?, ?, datetime('now'))

               ON CONFLICT(path) DO UPDATE SET

               file_count=excluded.file_count, total_bytes=excluded.total_bytes,

               depth=excluded.depth, last_scanned=excluded.last_scanned""",

            (str(path), drive, file_count, total_bytes, depth))



    def upsert_file(self, folder_id, filename, path, size, mtime, ctime):

        "Insert or update a file record"

        self.conn.execute(

            """INSERT INTO files (folder_id, filename, path, size, mtime, ctime)

               VALUES (?, ?, ?, ?, ?, ?)

               ON CONFLICT(path) DO UPDATE SET

               folder_id=excluded.folder_id, filename=excluded.filename,

               size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime""",

            (folder_id, filename, str(path), size, mtime, ctime))



    def get_folder_id(self, path):

        "Get folder id by path, or None"

        row = self.conn.execute("SELECT id FROM folders WHERE path=?", (str(path),)).fetchone()

        return row[0] if row else None



    def needs_rescan(self, path, mtime):

        "Check if file needs rescanning based on mtime"

        row = self.conn.execute("SELECT mtime FROM files WHERE path=?", (str(path),)).fetchone()

        return row is None or row[0] < mtime



    def commit(self): self.conn.commit()

    def close(self): self.conn.close()
