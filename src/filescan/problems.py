"""Files and folders filescan could not read, kept in the database.

Permission errors on old system folders and some downloads come back on every
run, so they are stored rather than only logged, and the report lists them by
folder for the user to delete or deal with. filescan never changes
permissions to get round them.

Each step replaces only what it actually tried again, so an interrupted run
never wipes the list:

- scan: every problem under each PC root it walked. A folder already on the
  list is not tried again, and its entry is kept;
- import: every problem under each server root it listed, including names
  Windows cannot represent;
- hash: the files it tried to hash. A file already on the list is not tried
  again. A file that now has a hash, or has gone, drops off the list.

`scan --retry-unreadable` and `hash-all --retry-unreadable` try everything on
the list again, for when the user has made some of it readable.

Files that cleanup could not move into quarantine are read from each cleanup
plan's action log when the report is written.
"""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path, PureWindowsPath

from filescan.models import ScanConfig
from filescan.storage import SQLiteDB
from filescan.storage.sql import path_prefix

Entry = tuple[str, str, str]  # (kind, path, problem); kind is "file" or "folder"


def ensure_table(db: SQLiteDB) -> None:
    db.conn.execute("""CREATE TABLE IF NOT EXISTS access_problems (
        stage TEXT NOT NULL, path TEXT NOT NULL, kind TEXT NOT NULL, problem TEXT NOT NULL,
        seen_at TEXT NOT NULL, PRIMARY KEY (stage, path))""")


def _insert(db: SQLiteDB, stage: str, entries: list[Entry]) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    db.conn.executemany("INSERT OR REPLACE INTO access_problems VALUES (?, ?, ?, ?, ?)",
                        [(stage, str(path), kind, str(problem), now) for kind, path, problem in entries])


def replace_under(db: SQLiteDB, stage: str, root: Path | str, entries: list[Entry]) -> None:
    """Replace this stage's problems under root with entries (the result of walking root again)."""
    ensure_table(db)
    exact, pattern = path_prefix(root)
    db.conn.execute("DELETE FROM access_problems WHERE stage = ? AND (path = ? OR path LIKE ? ESCAPE '|')",
                    (stage, exact, pattern))
    _insert(db, stage, entries)
    db.conn.commit()


def replace_tried(db: SQLiteDB, stage: str, tried: list[str], entries: list[Entry]) -> None:
    """Replace this stage's problems for the paths it tried; drop any whose file now has a hash or has gone."""
    ensure_table(db)
    db.conn.executemany("DELETE FROM access_problems WHERE stage = ? AND path = ?", [(stage, p) for p in tried])
    _insert(db, stage, entries)
    db.conn.execute("""DELETE FROM access_problems WHERE stage = ? AND path IN (
                           SELECT f.path FROM files f LEFT JOIN file_hashes h ON h.file_id = f.id
                           WHERE f.is_missing = 1 OR (h.full_hash IS NOT NULL AND h.full_hash != ''))""", (stage,))
    db.conn.commit()


def recorded(db: SQLiteDB, stage: str, root: Path | str | None = None) -> dict[str, tuple[str, str]]:
    """path -> (kind, problem) for what this stage could not read before, under root if given."""
    ensure_table(db)
    if root is None:
        rows = db.conn.execute("SELECT path, kind, problem FROM access_problems WHERE stage = ?", (stage,))
    else:
        exact, pattern = path_prefix(root)
        rows = db.conn.execute("SELECT path, kind, problem FROM access_problems WHERE stage = ? "
                               "AND (path = ? OR path LIKE ? ESCAPE '|')", (stage, exact, pattern))
    return {r[0]: (r[1], r[2]) for r in rows}


def count(db: SQLiteDB) -> int:
    ensure_table(db)
    return int(db.conn.execute("SELECT COUNT(*) FROM access_problems").fetchone()[0])


# ---------------------------------------------------------------- the report


def _cleanup_errors(config: ScanConfig) -> list[tuple[str, str, str]]:
    """(plan, path, error) for every planned file whose last cleanup action failed and is not in quarantine."""
    out = []
    for folder in sorted((config.filescan_folder / "cleanup").glob("*")):
        log = folder / "actions.jsonl"
        if not log.exists():
            continue
        last: dict[str, dict] = {}
        quarantined: set[str] = set()
        with log.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                last[r["path"]] = r
                if r.get("op") == "quarantine" and r.get("status") == "moved":
                    quarantined.add(r["path"])
                elif r.get("op") in ("restore", "purge") and r.get("status") in ("moved", "purged"):
                    quarantined.discard(r["path"])
        for path, r in last.items():
            if r.get("status") == "error" and path not in quarantined:
                out.append((folder.name, path, str(r.get("error", ""))))
    return out


def rollup_folder(path: str, levels: int = 2) -> str:
    """The folder to report a problem under: the drive or share and `levels` folders below it."""
    p = PureWindowsPath(path)
    parts = p.parts
    keep = 1 + levels
    return str(PureWindowsPath(*parts[:keep])) if len(parts) > keep else str(p.parent if p.suffix else p)


def write_unreadable_report(config: ScanConfig) -> tuple[Path, Path]:
    """unreadable_files.csv (every item) and unreadable_by_folder.csv (totals by folder, two levels down)."""
    db = SQLiteDB(config.database_path)
    try:
        ensure_table(db)
        rows = [(r["stage"], r["kind"], r["path"], r["problem"]) for r in
                db.conn.execute("SELECT stage, kind, path, problem FROM access_problems ORDER BY path")]
    finally:
        db.close()
    rows += [(f"cleanup {plan}", "file", path, error) for plan, path, error in _cleanup_errors(config)]

    out_dir = config.filescan_folder
    files_csv = out_dir / "unreadable_files.csv"
    with files_csv.open("w", newline="", encoding="utf-8-sig", errors="replace") as handle:
        writer = csv.writer(handle)
        writer.writerow(["step", "kind", "path", "problem"])
        writer.writerows(rows)

    by_folder: dict[tuple[str, str], list] = defaultdict(lambda: [0, Counter()])
    for stage, _kind, path, problem in rows:
        entry = by_folder[(rollup_folder(path), stage)]
        entry[0] += 1
        entry[1][problem.split(":")[0][:80]] += 1
    folder_csv = out_dir / "unreadable_by_folder.csv"
    with folder_csv.open("w", newline="", encoding="utf-8-sig", errors="replace") as handle:
        writer = csv.writer(handle)
        writer.writerow(["folder", "step", "items", "most common problem"])
        for (folder, stage), (n, problems) in sorted(by_folder.items(), key=lambda kv: -kv[1][0]):
            writer.writerow([folder, stage, n, problems.most_common(1)[0][0]])
    print(f"{len(rows):,} files and folders could not be read or moved, in {len(by_folder):,} folders "
          f"(two levels below each drive or share).")
    return folder_csv, files_csv
