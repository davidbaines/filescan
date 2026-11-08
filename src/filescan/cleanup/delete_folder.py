"""`filescan delete-folder`: delete whole PC folders at once, and update the database.

This is for folders that are plainly not wanted, where deciding folder by
folder in a review would take far longer than it is worth. It is an explicit
command with two runs:

1. Without --yes it only reports, for each folder: what the database holds
   (files, bytes, how much has an identical copy kept elsewhere and how much
   has none), what is on disk now (files added since the last scan have never
   been compared with anything), and any Git repositories and links inside.
   The files with no known copy go to a CSV for checking.
2. With --yes it deletes each folder and everything below it, then marks the
   deleted files and folders missing in the database. Deletion is final: there
   is no quarantine, and the Recycle Bin is not used. Running it again finishes
   anything left behind.

Safety:
- Only folders below a configured PC root, never a root itself, never a
  folder that contains or sits inside a pinned folder, and only when the
  root's drive is the one configured (volume serial).
- Junctions and symbolic links are removed as links. filescan never goes
  through them, because an old Windows profile has junctions that point into
  the live profile on C:.
- A folder reached through a junction or link above it is refused.
- filescan never changes permissions. Anything it cannot delete is logged and
  appears on the workbook's Unreadable sheet for the user to deal with.

Each run with --yes writes <filescan folder>/deletions/<time>.jsonl: one
record per folder, plus one per file or folder that could not be removed.
"""

from __future__ import annotations

import csv
import json
import os
import stat
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PureWindowsPath

from filescan.cleanup.plan import PlanEntry, _git_repos, gb, mark_coverage, norm, root_configs, root_for
from filescan.inventory.volumes import root_problem
from filescan.models import ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB
from filescan.storage.sql import escape_like

LONG = "\\\\?\\"


def long_path(path: str) -> str:
    """An extended-length path, so folders deeper than 260 characters can be removed."""
    if path.startswith(LONG):
        return path
    if path.startswith("\\\\"):
        return LONG + "UNC\\" + path[2:]
    return LONG + path


def short_path(path: str) -> str:
    if path.startswith(LONG + "UNC\\"):
        return "\\\\" + path[len(LONG) + 4:]
    return path[len(LONG):] if path.startswith(LONG) else path


def normalise(folder: str) -> str:
    """Absolute, backslashes, no trailing separator. Case is kept as given."""
    return str(PureWindowsPath(os.path.abspath(folder))).rstrip("\\")


def _is_link(entry: os.DirEntry) -> bool:
    try:
        return entry.is_symlink() or entry.is_junction()
    except OSError:
        return True  # if we cannot tell, treat it as a link and never go through it


def is_link_path(path: str) -> bool:
    try:
        return os.path.islink(path) or os.path.isjunction(path)
    except OSError:
        return True


def real_location(path: str) -> str:
    """Where a path really leads once every junction and link in it is followed, as a plain path."""
    return short_path(os.path.realpath(long_path(short_path(path)))).rstrip("\\")


def leads_elsewhere(path: str) -> bool:
    """True if a junction or link anywhere in the path (not only at its end) takes it somewhere else.

    An old profile on D: has junctions such as "Application Data" that lead into
    the live profile on C:, so a path that looks like D:\\... can be on C:.
    """
    try:
        return real_location(path).lower() != short_path(path).rstrip("\\").lower()
    except OSError:
        return True  # if we cannot tell, treat it as leading elsewhere


# ---------------------------------------------------------------- walking


@dataclass
class Item:
    kind: str  # "file", "link", "dir" (after everything inside it) or "error"
    path: str  # extended-length path
    size: int = 0
    error: str = ""


def walk(top: str) -> Iterator[Item]:
    """Every file, link and folder below `top`, children before their folder. Never goes through a link.

    """
    try:
        entries = list(os.scandir(top))
    except OSError as exc:
        yield Item("error", top, error=str(exc))
        return
    for entry in entries:
        if _is_link(entry):
            yield Item("link", entry.path)
            continue
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError as exc:
            yield Item("error", entry.path, error=str(exc))
            continue
        if is_dir:
            yield from walk(entry.path)
        else:
            try:
                size = entry.stat(follow_symlinks=False).st_size
            except OSError:
                size = 0
            yield Item("file", entry.path, size)
    yield Item("dir", top)


def remove_link(path: str) -> None:
    """Remove a junction or symbolic link itself, never what it points to."""
    try:
        os.rmdir(path)  # a junction or a directory symlink
    except OSError:
        os.unlink(path)  # a file symlink


def remove_file(path: str) -> None:
    try:
        os.remove(path)
    except PermissionError:
        os.chmod(path, stat.S_IWRITE)  # clear the read-only attribute, then try again
        os.remove(path)


def remove_folder(path: str) -> None:
    """Remove an empty folder, clearing its read-only attribute if needed."""
    try:
        os.rmdir(path)
    except PermissionError:
        os.chmod(path, stat.S_IWRITE)
        os.rmdir(path)


# ---------------------------------------------------------------- checks and the report


@dataclass
class FolderReport:
    path: str
    problem: str = ""
    exists: bool = True
    db_files: int = 0
    db_bytes: int = 0
    covered_bytes: int = 0
    unique_files: int = 0
    unique_bytes: int = 0
    disk_files: int = 0
    disk_bytes: int = 0
    new_files: int = 0
    new_bytes: int = 0
    links: list[str] = field(default_factory=list)
    unreadable: int = 0
    git_repos: list[str] = field(default_factory=list)
    unique_by_child: Counter = field(default_factory=Counter)
    entries: list[PlanEntry] = field(default_factory=list)


def check_folder(path: str, config: ScanConfig) -> str:
    """Why a folder may not be deleted, or "" if it may."""
    if len(path) < 3 or path[1] != ":":
        return "only folders on a PC drive (such as D:\\...) can be deleted this way"
    rc = root_for(path, root_configs(config))
    if rc is None:
        return "it is not below a configured root (a root itself is never deleted)"
    if rc.location != "pc":
        return "it is on the server; use review and cleanup for server folders"
    problem = root_problem(rc)
    if problem:
        return problem
    low = norm(path)
    for pinned in config.pinned:
        p = norm(str(pinned))
        if low == p or low.startswith(p + "\\") or p.startswith(low + "\\"):
            return f"it overlaps the pinned folder {pinned}"
    if os.path.lexists(long_path(path)) and is_link_path(long_path(path)):
        return "it is itself a junction or link; delete the link in Explorer instead"
    if leads_elsewhere(path):
        return (f"a folder above it is a junction or link, so it is really {real_location(path)}; "
                "nothing is deleted through a link")
    return ""


def _folder_clause(path: str) -> tuple[str, tuple[str, str]]:
    return "(lower(fo.path) = lower(?) OR fo.path LIKE ? ESCAPE '|')", (path, escape_like(path) + "\\%")


def assess(db: SQLiteDB, config: ScanConfig, folders: list[str]) -> list[FolderReport]:
    reports = [FolderReport(path=normalise(f)) for f in folders]
    for r in reports:
        r.problem = check_folder(r.path, config)
    ok = [r for r in reports if not r.problem]
    repos = _git_repos(db) if ok else []
    for r in ok:
        clause, params = _folder_clause(r.path)
        rows = db.conn.execute(
            f"""SELECT f.id, f.path, f.size, f.mtime, h.full_hash FROM files f
                JOIN folders fo ON fo.id = f.folder_id LEFT JOIN file_hashes h ON h.file_id = f.id
                WHERE f.is_missing = 0 AND {clause}""", params).fetchall()
        r.entries = [PlanEntry(file_id=int(x["id"]), path=x["path"], location="pc", size=int(x["size"]),
                               mtime=float(x["mtime"]), full_hash=x["full_hash"] or "", reason="delete-folder",
                               unit=r.path) for x in rows]
        r.db_files = len(r.entries)
        r.db_bytes = sum(e.size for e in r.entries)
        low = norm(r.path)
        r.git_repos = [g for g in repos if g == low or g.startswith(low + "\\")]
    # All folders together, so two of them never count as each other's copy.
    everything = [e for r in ok for e in r.entries]
    if everything:
        print("Looking for copies kept elsewhere...")
        mark_coverage(db, config, everything)
    for r in ok:
        for e in r.entries:
            if e.status == "covered":
                r.covered_bytes += e.size
            else:
                r.unique_files += 1
                r.unique_bytes += e.size
                rest = e.path[len(r.path) + 1:]
                child = rest.split("\\", 1)[0] if "\\" in rest else "(files directly in the folder)"
                r.unique_by_child[child] += e.size
        _look_on_disk(r)
    return reports


def _look_on_disk(r: FolderReport) -> None:
    top = long_path(r.path)
    if not os.path.isdir(top):
        r.exists = False
        return
    known = {e.path.lower() for e in r.entries}
    with progress_bar(desc=f"looking in {r.path}", unit="file") as bar:
        for item in walk(top):
            if item.kind == "file":
                r.disk_files += 1
                r.disk_bytes += item.size
                if short_path(item.path).lower() not in known:
                    r.new_files += 1
                    r.new_bytes += item.size
                bar.update(1)
            elif item.kind == "link":
                r.links.append(short_path(item.path))
            elif item.kind == "error":
                r.unreadable += 1


def print_report(reports: list[FolderReport]) -> None:
    for r in reports:
        print(f"\n{r.path}")
        if r.problem:
            print(f"  Refused: {r.problem}.")
            continue
        print(f"  In the database: {r.db_files:,} files, {gb(r.db_bytes)}")
        print(f"    with an identical copy kept elsewhere: {gb(r.covered_bytes)}")
        print(f"    with no known copy: {gb(r.unique_bytes)} in {r.unique_files:,} files")
        for child, size in r.unique_by_child.most_common(5):
            print(f"      {gb(size):>10}  {child}")
        if not r.exists:
            print("  Not on disk any more; --yes only marks it missing in the database.")
            continue
        print(f"  On disk now: {r.disk_files:,} files, {gb(r.disk_bytes)}")
        if r.new_files:
            print(f"    {r.new_files:,} files ({gb(r.new_bytes)}) are not in the database, so they have never "
                  "been compared with anything")
        if r.unreadable:
            print(f"    {r.unreadable:,} folders could not be read (permissions); filescan cannot delete them")
        if r.links:
            print(f"  {len(r.links):,} junctions or links inside. Only the links are removed, never what they "
                  f"point to. First: {r.links[0]}")
        if r.git_repos:
            print(f"  {len(r.git_repos):,} Git repositories inside, such as {r.git_repos[0]}")


def write_unique_csv(reports: list[FolderReport], out: Path) -> Path | None:
    rows = [e for r in reports if not r.problem for e in r.entries if e.status != "covered"]
    if not rows:
        return None
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["size", "full_hash", "path"])
        for e in sorted(rows, key=lambda e: -e.size):
            writer.writerow([e.size, e.full_hash, e.path])
    return out


# ---------------------------------------------------------------- deleting


class _Log:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = path.open("a", encoding="utf-8")

    def write(self, record: dict) -> None:
        self.handle.write(json.dumps({**record, "at": datetime.now().isoformat(timespec="seconds")}) + "\n")
        self.handle.flush()

    def close(self) -> None:
        self.handle.close()


def delete_tree(top: str, log: _Log) -> Counter:
    """Delete everything below top, then top. Never goes through a link. Returns counts."""
    counts: Counter = Counter()
    with progress_bar(desc=f"deleting {short_path(top)}", unit="file") as bar:
        for item in walk(top):
            try:
                if item.kind == "file":
                    remove_file(item.path)
                    counts["files"] += 1
                    counts["bytes"] += item.size
                    bar.update(1)
                elif item.kind == "link":
                    remove_link(item.path)
                    counts["links"] += 1
                elif item.kind == "dir":
                    remove_folder(item.path)
                    counts["folders"] += 1
                else:
                    counts["unreadable"] += 1
                    log.write({"kind": "error", "path": short_path(item.path), "error": item.error})
            except OSError as exc:
                counts["errors" if item.kind != "dir" else "folders left"] += 1
                log.write({"kind": "error", "path": short_path(item.path), "error": str(exc)})
    return counts


def mark_deleted(db: SQLiteDB, path: str, entries: list[PlanEntry]) -> int:
    """Mark what has gone from disk as missing in the database. Returns the number of files marked."""
    clause, params = _folder_clause(path)
    if not os.path.lexists(long_path(path)):
        db.conn.execute(f"UPDATE folders SET is_missing = 1 WHERE id IN (SELECT fo.id FROM folders fo WHERE {clause})",
                        params)
        n = db.conn.execute(
            f"""UPDATE files SET is_missing = 1 WHERE is_missing = 0 AND folder_id IN
                (SELECT fo.id FROM folders fo WHERE {clause})""", params).rowcount
        db.conn.commit()
        return n
    gone = [(e.file_id,) for e in entries if not os.path.lexists(long_path(e.path))]
    db.conn.executemany("UPDATE files SET is_missing = 1 WHERE id = ?", gone)
    folders = db.conn.execute(f"SELECT fo.id, fo.path FROM folders fo WHERE {clause}", params).fetchall()
    db.conn.executemany("UPDATE folders SET is_missing = 1 WHERE id = ?",
                        [(f["id"],) for f in folders if not os.path.lexists(long_path(f["path"]))])
    db.conn.commit()
    return len(gone)


def run_delete_folder(config: ScanConfig, folders: list[str], *, yes: bool = False) -> list[FolderReport]:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = config.filescan_folder / "deletions"
    db = SQLiteDB(config.database_path)
    try:
        reports = assess(db, config, folders)
        print_report(reports)
        unique_csv = write_unique_csv(reports, out_dir / f"{stamp}-no-known-copy.csv")
        if unique_csv:
            print(f"\nFiles with no known copy are listed in {unique_csv}")
        allowed = [r for r in reports if not r.problem]
        if not yes:
            print(f"\nNothing was deleted. To delete {'this folder' if len(allowed) == 1 else 'these folders'} "
                  "for good (no quarantine, no Recycle Bin), run the same command with --yes.")
            return reports
        log = _Log(out_dir / f"{stamp}.jsonl")
        try:
            for r in allowed:
                counts: Counter = Counter()
                if r.exists:
                    counts = delete_tree(long_path(r.path), log)
                marked = mark_deleted(db, r.path, r.entries)
                gone = not os.path.lexists(long_path(r.path))
                log.write({"kind": "folder", "path": r.path, "deleted_files": counts["files"],
                           "deleted_bytes": counts["bytes"], "links_removed": counts["links"],
                           "errors": counts["errors"] + counts["unreadable"], "gone": gone,
                           "marked_missing": marked, "no_known_copy_bytes": r.unique_bytes})
                print(f"\n{r.path}: deleted {counts['files']:,} files ({gb(counts['bytes'])}) and "
                      f"{counts['links']:,} links; {marked:,} files marked missing in the database.")
                if not gone:
                    print(f"  {counts['errors'] + counts['unreadable']:,} items could not be removed; see "
                          f"{log.handle.name}. They are listed on the workbook's Unreadable sheet "
                          "after the next scan; delete them or make them readable yourself.")
        finally:
            log.close()
        return reports
    finally:
        db.close()
