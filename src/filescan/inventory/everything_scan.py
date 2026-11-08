"""Update one PC root in the database from Everything's list instead of walking it.

The result matches a folder walk (`scanner.py`): the same exclusions, skip
folders, extension and size filters, the drive marker and quarantine left out,
junctions and symbolic links not followed (cloud folders, which are also
reparse points, are followed), and the same folder records. Every file's size,
dates and attributes are compared with the database, so a file edited in place
is caught, and only new, changed and gone files and folders are written.

It returns None, having written nothing, when the walk should be used instead:
es.exe is missing, fails or times out, Everything lists nothing for a root that
has files, or the list would mark more than a small share of the root's files
as gone, which suggests Everything's index is incomplete (still loading, or set
to leave some folders or hidden files out).

Differences from a walk, by necessity:

- Everything runs as a service and lists folders the user cannot open. Folders
  already on the Unreadable list are left out, as the walk leaves them out; a
  folder that becomes unreadable later is listed, and hash-all then records
  its files as unreadable.
- Everything's index can hold files and folders that have been deleted (2,339
  under D:\\Windows on 8 November), does not always update a folder's date when
  its contents change, and gives OneDrive and iCloud files attribute bits that
  the directory listing hides. So every new or changed file and folder is
  checked on disk before it is written: one that has gone is left out, and the
  size, dates and (for a cloud file) attributes come from the file system. A
  deleted file that the database still holds as present, and that Everything
  still lists unchanged, stays recorded; `--rescan` walks and corrects that.
- A name that is not valid Unicode cannot be stored in the database. Everything
  shows its bad characters as U+FFFD; such files and folders are put on the
  Unreadable list and left out. (The walk could not store them either.)
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import TYPE_CHECKING

from filescan.inventory.everything import EverythingUnavailable, Runner, export, read_export, stat_attributes
from filescan.inventory.volumes import MARKER
from filescan.models import FileRecord, FolderRecord
from filescan.problems import recorded
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB

if TYPE_CHECKING:
    from filescan.inventory.scanner import InventoryScanner

_COMMIT_SECONDS = 5
# More gone than this share of the root's files (and more than _GONE_FLOOR) means walk instead.
GONE_SHARE = 0.05
_GONE_FLOOR = 1000
_BAD_NAME = "the name is not valid Unicode, so filescan cannot store it"


@dataclass(slots=True)
class EverythingScan:
    folders: int = 0
    files: int = 0
    written_folders: int = 0
    written_files: int = 0
    gone_folders: int = 0
    gone_files: int = 0
    problems: list[tuple[str, str, str]] = field(default_factory=list)  # for the Unreadable list
    left_out: int = 0  # folders already on the Unreadable list
    stale: int = 0  # files and folders in Everything's index that are no longer on disk


def _is_link(path: str) -> bool:
    """True for a symbolic link or junction; a cloud placeholder folder is a reparse point but not a link."""
    try:
        return os.path.islink(path) or os.path.isjunction(path)
    except OSError:
        return True


def _readable(path: str) -> bool:
    try:
        with os.scandir(path) as entries:
            next(entries, None)
        return True
    except OSError:
        return False


def _not_empty(root: Path) -> bool:
    try:
        with os.scandir(root) as entries:
            return any(entry.name != MARKER for entry in entries)
    except OSError:
        return False


def _db_rows(db: SQLiteDB, table: str, columns: str, root: Path) -> dict[str, tuple]:
    """path -> the other columns, for the root's active rows. A range on path uses the index."""
    exact = str(root)
    prefix = exact.rstrip("\\") + "\\"
    upper = prefix[:-1] + chr(ord("\\") + 1)
    rows = db.conn.execute(f"SELECT path, {columns} FROM {table} WHERE is_missing = 0 "
                           "AND (path = ? OR (path >= ? AND path < ?))", (exact, prefix, upper))
    return {row[0]: tuple(row[1:]) for row in rows}


def scan_root_with_everything(scanner: InventoryScanner, db: SQLiteDB, repo: FileRepository, root: Path,
                              scan_run_id: int, runner: Runner) -> EverythingScan | None:
    """Bring root's records up to date from Everything. None (nothing written) means walk it instead."""
    config = scanner.config
    root_str = str(root)
    root_key = root_str.rstrip("\\")  # the parent of a top-level entry: "F:" for F:\, the path otherwise
    out_dir = config.filescan_folder
    out_dir.mkdir(parents=True, exist_ok=True)
    result = EverythingScan()

    # Folders already on the Unreadable list are left out, unless asked to try again and now readable.
    unreadable = {path.lower(): problem for path, (kind, problem) in recorded(db, "scan", root).items()
                  if kind == "folder" and not (scanner.retry_unreadable and _readable(path))}
    skip = {str(p).rstrip("\\").lower() for p in config.skip_paths}

    started = time.monotonic()
    try:
        folder_csv = export(root, folders=True, runner=runner, out_dir=out_dir)
        listed_folders = list(read_export(folder_csv, root))
        file_csv = export(root, folders=False, runner=runner, out_dir=out_dir)
    except EverythingUnavailable as exc:
        print(f"  Everything could not be used ({exc}); walking the folders instead.")
        return None

    db_files = _db_rows(db, "files", "size, mtime, ctime, attributes", root)
    db_folders = {(root_key if p == root_str else p): v
                  for p, v in _db_rows(db, "folders", "file_count, total_bytes, mtime", root).items()}
    active_files = len(db_files)

    # Folders, parents first, so a folder is kept only if its parent is.
    depth_of: dict[str, int] = {root_key: 0}
    mtime_of: dict[str, float] = {}
    try:
        mtime_of[root_key] = root.stat().st_mtime
    except OSError as exc:
        print(f"  {root} could not be read ({exc}); walking the folders instead.")
        file_csv.unlink(missing_ok=True)
        return None
    listed_folders.sort(key=lambda e: e.path.count("\\"))
    for entry in listed_folders:
        parent, _, name = entry.path.rpartition("\\")
        if parent not in depth_of or name in config.exclude_folders:
            continue
        lower = entry.path.lower()
        if lower in skip:
            continue
        if lower in unreadable:
            result.problems.append(("folder", entry.path, unreadable[lower]))
            result.left_out += 1
            continue
        if "\ufffd" in name:
            result.problems.append(("folder", entry.path, _BAD_NAME))
            continue
        if entry.is_reparse_point and _is_link(entry.path):
            continue
        mtime = entry.mtime
        before = db_folders.get(entry.path)
        if before is None or before[2] != mtime:
            # New, or its date differs: ask the folder. Everything's index can hold a folder that has been
            # deleted, and does not always update a folder's date when its contents change.
            try:
                mtime = os.stat(entry.path).st_mtime
            except FileNotFoundError:
                result.stale += 1
                continue
            except OSError:
                pass
        depth_of[entry.path] = depth_of[parent] + 1
        mtime_of[entry.path] = mtime

    def db_path(key: str) -> str:
        return root_str if key == root_key else key

    counts: dict[str, int] = defaultdict(int)
    sizes: dict[str, int] = defaultdict(int)
    changed: dict[str, list[FileRecord]] = defaultdict(list)
    listed_files = 0
    with progress_bar(desc=f"compare {root.name or root.drive} with Everything", unit="file") as bar:
        for entry in read_export(file_csv, root):
            listed_files += 1
            if listed_files % 10_000 == 0:
                bar.update(10_000)
            parent, _, name = entry.path.rpartition("\\")
            if parent not in depth_of:
                continue
            if "\ufffd" in name:
                result.problems.append(("file", entry.path, _BAD_NAME))
                continue
            if name == MARKER or PureWindowsPath(name).suffix.lower() in config.exclude_extensions:
                continue
            if entry.is_reparse_point and _is_link(entry.path):
                continue
            now = (entry.size, entry.mtime, entry.ctime, stat_attributes(entry.attributes))
            before = db_files.get(entry.path)
            if before != now:
                # New or changed: check it on disk. Everything's index can hold a file that has been deleted,
                # and for a OneDrive or iCloud file it gives bits (reparse point, offline) that the directory
                # listing used by the walk hides; lstat gives what the listing gives.
                try:
                    st = os.lstat(entry.path)
                except FileNotFoundError:
                    result.stale += 1
                    continue
                except OSError:
                    st = None
                if st is not None:
                    attributes = st.st_file_attributes if entry.is_reparse_point else now[3]
                    now = (st.st_size, st.st_mtime, st.st_ctime, attributes)
            if now[0] < config.min_file_size or (config.max_file_size is not None and now[0] > config.max_file_size):
                continue
            db_files.pop(entry.path, None)
            counts[parent] += 1
            sizes[parent] += now[0]
            if before != now:
                changed[parent].append(FileRecord(
                    path=Path(entry.path), folder_path=Path(db_path(parent)), filename=name, size=now[0],
                    mtime=now[1], ctime=now[2], attributes=now[3]))
        bar.update(listed_files % 10_000)

    if not listed_folders and not listed_files and _not_empty(root):
        print(f"  Everything listed nothing under {root}, which is not empty (is the drive in its index?); "
              "walking the folders instead.")
        return None
    gone_files = len(db_files)
    if gone_files > max(_GONE_FLOOR, GONE_SHARE * active_files):
        print(f"  Everything's list would mark {gone_files:,} of {active_files:,} files as gone, which suggests "
              "its index is incomplete; walking the folders instead.")
        return None

    # Write: folders parents first (a file needs its folder's row), then files, then what has gone.
    last_commit = time.monotonic()
    for key in sorted(depth_of, key=lambda k: depth_of[k]):
        before = db_folders.pop(key, None)
        now = (counts.get(key, 0), sizes.get(key, 0), mtime_of[key])
        if before != now or key in changed:
            parent = key.rpartition("\\")[0]
            repo.upsert_folder(FolderRecord(
                path=Path(db_path(key)), drive=root.drive, parent_path=None if key == root_key else Path(db_path(parent)),
                depth=depth_of[key], file_count=now[0], total_bytes=now[1], mtime=now[2], scan_run_id=scan_run_id))
            result.written_folders += 1
        if key in changed:
            repo.upsert_files(Path(db_path(key)), changed[key], scan_run_id)
            result.written_files += len(changed[key])
        if time.monotonic() - last_commit > _COMMIT_SECONDS:
            db.conn.commit()
            last_commit = time.monotonic()
    db.conn.executemany("UPDATE files SET is_missing = 1 WHERE path = ?", [(p,) for p in db_files])
    db.conn.executemany("UPDATE folders SET is_missing = 1 WHERE path = ?",
                        [(db_path(k),) for k in db_folders])
    db.conn.commit()

    result.folders, result.files = len(depth_of), sum(counts.values())
    result.gone_files, result.gone_folders = gone_files, len(db_folders)
    print(f"  Everything listed {result.files:,} files in {result.folders:,} folders in "
          f"{time.monotonic() - started:.0f}s: {result.written_files:,} files new or changed, "
          f"{result.gone_files:,} gone; {result.written_folders:,} folders new or changed, "
          f"{result.gone_folders:,} gone.")
    if result.stale:
        print(f"  {result.stale:,} files and folders in Everything's index are no longer on disk and were left out.")
    return result
