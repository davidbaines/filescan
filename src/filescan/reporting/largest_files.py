from __future__ import annotations

import subprocess
from pathlib import Path

from send2trash import send2trash
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Header, Static

from filescan.config import load_config
from filescan.inventory.refresh import refresh_file_records
from filescan.models import ScanConfig
from filescan.storage import SQLiteDB


def _fmt_size(b: int) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.1f} MB"
    return f"{b / 1024:.1f} KB"


def _query_large_files(db: SQLiteDB, config: ScanConfig) -> list[tuple[Path, int]]:
    results: list[tuple[Path, int]] = []
    for root in config.roots:
        threshold = config.large_file_size_for(root)
        root_str = str(root)
        like_pattern = root_str.rstrip("\\") + "\\%"
        rows = db.conn.execute(
            """
            SELECT f.path, f.size
            FROM files f
            JOIN folders fo ON fo.id = f.folder_id
            WHERE (fo.path = ? OR fo.path LIKE ?)
              AND f.size >= ?
              AND f.is_missing = 0
              AND fo.is_missing = 0
            """,
            (root_str, like_pattern, threshold),
        ).fetchall()
        for row in rows:
            try:
                results.append((Path(row["path"]), int(row["size"])))
            except (OSError, ValueError) as exc:
                print(f"Skipping file record: {exc}")
    seen: set[Path] = set()
    deduped: list[tuple[Path, int]] = []
    for path, size in sorted(results, key=lambda r: r[1], reverse=True):
        if path not in seen:
            seen.add(path)
            deduped.append((path, size))
    return deduped


def find_large_files(config: ScanConfig) -> list[tuple[Path, int]]:
    db = SQLiteDB(config.database_path)
    try:
        initial = _query_large_files(db, config)
        if initial:
            print(f"  Checking {len(initial)} file(s) against disk...")
            refresh_file_records(db, [p for p, _ in initial])
        return _query_large_files(db, config)
    finally:
        db.close()


class LargestFilesApp(App[None]):
    TITLE = "Largest Files"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("escape", "quit", "Quit"),
        Binding("enter", "open_folder", "Open Folder"),
        Binding("space", "toggle_mark", "Mark/Unmark", priority=True),
        Binding("c", "clear_marks", "Clear Marks"),
        Binding("ctrl+d", "delete_marked", "Move to Recycle Bin"),
    ]
    DEFAULT_CSS = """
    DataTable {
        height: 1fr;
    }
    #status {
        height: 1;
        background: $accent;
        color: $text;
        padding: 0 1;
    }
    """

    def __init__(self, files: list[tuple[Path, int]]) -> None:
        super().__init__()
        self._files = list(files)
        self._marked: set[int] = set()

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(id="table", cursor_type="row")
        yield Static(id="status")
        yield Footer()

    def on_mount(self) -> None:
        self._build_table()
        self._refresh_status()

    def _build_table(self) -> None:
        table = self.query_one("#table", DataTable)
        table.clear(columns=True)
        table.add_column("", key="mark", width=2)
        table.add_column("Rank", key="rank")
        table.add_column("Size", key="size")
        table.add_column("Filename", key="filename")
        table.add_column("Path", key="path")
        for i, (path, size) in enumerate(self._files):
            mark = "✓" if i in self._marked else ""
            table.add_row(mark, str(i + 1), _fmt_size(size), path.name, str(path.parent), key=str(i))

    def _refresh_status(self) -> None:
        if not self._marked:
            msg = f"{len(self._files)} files  |  Enter: open folder   Space: mark   Ctrl+D: recycle   C: clear   Q: quit"
        else:
            total = sum(self._files[i][1] for i in self._marked if i < len(self._files))
            msg = (
                f"{len(self._marked)} marked — {_fmt_size(total)}  |  "
                f"{len(self._files)} total  |  Ctrl+D: move to recycle bin   C: clear marks   Q: quit"
            )
        self.query_one("#status", Static).update(msg)

    def action_open_folder(self) -> None:
        table = self.query_one("#table", DataTable)
        row = table.cursor_row
        if 0 <= row < len(self._files):
            path, _ = self._files[row]
            try:
                subprocess.Popen(["explorer.exe", str(path.parent)])
            except OSError:
                pass

    def action_toggle_mark(self) -> None:
        table = self.query_one("#table", DataTable)
        row = table.cursor_row
        if not (0 <= row < len(self._files)):
            return
        if row in self._marked:
            self._marked.discard(row)
            table.update_cell(str(row), "mark", "")
        else:
            self._marked.add(row)
            table.update_cell(str(row), "mark", "✓")
        self._refresh_status()

    def action_clear_marks(self) -> None:
        table = self.query_one("#table", DataTable)
        for i in self._marked:
            if i < len(self._files):
                table.update_cell(str(i), "mark", "")
        self._marked.clear()
        self._refresh_status()

    def action_delete_marked(self) -> None:
        if not self._marked:
            return
        errors = 0
        deleted: set[int] = set()
        for i in sorted(self._marked):
            if i >= len(self._files):
                continue
            path, _ = self._files[i]
            try:
                send2trash(str(path))
                deleted.add(i)
            except Exception:
                errors += 1
        self._files = [f for i, f in enumerate(self._files) if i not in deleted]
        self._marked = set()
        self._build_table()
        if errors:
            self.query_one("#status", Static).update(
                f"{errors} file(s) could not be moved to recycle bin — check permissions."
            )
        else:
            self._refresh_status()


def run_largest(config_path: Path) -> None:
    config = load_config(config_path)
    print("Querying database for large files...")
    files = find_large_files(config)
    if not files:
        print(
            f"No files found above the configured threshold "
            f"({_fmt_size(config.large_file_size)})."
        )
        print("Run 'filescan scan' first if the database is empty.")
        return
    print(f"Found {len(files)} files. Opening viewer...")
    LargestFilesApp(files).run()
