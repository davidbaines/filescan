"""Import the server's file inventory from `fsagent list`, run over SSH.

The server's disks are listed locally by server/fsagent.py, which is streamed
to the server's python3 on stdin, so nothing needs installing for this step.
Each POSIX path is mapped to the UNC path the PC uses for the same file, and
stored like any scanned file. Names Windows cannot represent are reported, not
imported.
"""

from __future__ import annotations

import csv
import json
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath

from filescan.config import load_config
from filescan.models import FileRecord, FolderRecord, RootConfig, ScanConfig
from filescan.progress import SkipLog, progress_bar
from filescan.storage import FileRepository, SQLiteDB

FSAGENT = Path(__file__).resolve().parents[3] / "server" / "fsagent.py"

_WINDOWS_ILLEGAL = set('<>:"|?*\\') | {chr(c) for c in range(32)}
_WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}

Runner = Callable[[RootConfig], Iterator[str]]


def mtime_from_ns(mtime_ns: int) -> float:
    """The one conversion from fsagent's integer mtime to the stored float.

    The stale-hash check compares stored and incoming mtimes exactly, so every
    import must convert the same way.
    """
    return mtime_ns / 1_000_000_000


def _windows_safe(component: str) -> bool:
    if not component or component.endswith((".", " ")):
        return False
    if any(ch in _WINDOWS_ILLEGAL for ch in component):
        return False
    return component.split(".")[0].upper() not in _WINDOWS_RESERVED


def posix_to_unc(posix_path: str, server_root: str, unc_root: Path) -> Path | None:
    """Map a server path to its UNC path, or None if Windows cannot represent it."""
    root = server_root.rstrip("/")
    if posix_path == root:
        return unc_root
    if not posix_path.startswith(root + "/"):
        return None
    parts = posix_path[len(root) + 1:].split("/")
    try:
        "".join(parts).encode("utf-8")
    except UnicodeEncodeError:
        return None
    if not all(_windows_safe(part) for part in parts):
        return None
    return Path(PureWindowsPath(unc_root, *parts))


def ssh_runner(config: ScanConfig) -> Runner:
    def run(root_config: RootConfig) -> Iterator[str]:
        command = ["ssh", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=60", config.server_ssh, config.server_python, "-", "list",
                   root_config.server_path]
        for name in root_config.exclude_dirs:
            command += ["--exclude-dir", name]
        with FSAGENT.open("rb") as script:
            process = subprocess.Popen(command, stdin=script, stdout=subprocess.PIPE)
            assert process.stdout is not None
            for raw in process.stdout:
                yield raw.decode("utf-8")
            code = process.wait()
        if code != 0:
            # Never treat a failed listing as "everything was deleted".
            raise RuntimeError(f"fsagent list failed for {root_config.server_path} (exit {code})")

    return run


@dataclass
class ImportSummary:
    folders: int = 0
    files: int = 0
    errors: int = 0
    unrepresentable: list[str] = field(default_factory=list)


@dataclass
class _PendingFolder:
    path: Path
    mtime: float
    files: list[FileRecord] = field(default_factory=list)


class ServerImporter:
    def __init__(self, config: ScanConfig, *, runner: Runner | None = None) -> None:
        self.config = config
        self.runner = runner or ssh_runner(config)
        self.skips = SkipLog(config.filescan_folder, "import-server")

    def _flush(self, repo: FileRepository, pending: _PendingFolder, root: Path, scan_run_id: int) -> None:
        depth = len(pending.path.parts) - len(root.parts)
        repo.upsert_folder(
            FolderRecord(
                path=pending.path,
                drive=root.drive,
                parent_path=None if pending.path == root else pending.path.parent,
                depth=depth,
                file_count=len(pending.files),
                total_bytes=sum(f.size for f in pending.files),
                mtime=pending.mtime,
                scan_run_id=scan_run_id,
            )
        )
        repo.upsert_files(pending.path, pending.files, scan_run_id)

    def _import_root(self, repo: FileRepository, root_config: RootConfig, scan_run_id: int,
                     summary: ImportSummary) -> None:
        root = root_config.path
        server_root = root_config.server_path or ""
        pending: _PendingFolder | None = None
        folders = files = 0
        # Collect the whole listing before writing anything, so a listing that
        # fails part-way leaves the database untouched.
        lines = list(self.runner(root_config))
        with progress_bar(desc=f"import {root}", total=len(lines), unit="entry") as bar:
            self.skips.attach(bar)
            for line in lines:
                bar.update(1)
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                kind = record.get("type")
                if kind == "error":
                    summary.errors += 1
                    self.skips.add("server error", record.get("path"), record.get("error"))
                    continue
                unc = None if record.get("bad_name") else posix_to_unc(record["path"], server_root, root)
                if unc is None:
                    summary.unrepresentable.append(record["path"])
                    continue
                if kind == "d":
                    if pending is not None:
                        self._flush(repo, pending, root, scan_run_id)
                    pending = _PendingFolder(path=unc, mtime=mtime_from_ns(record["mtime_ns"]))
                    folders += 1
                elif kind == "f":
                    if pending is None or unc.parent != pending.path:
                        # fsagent lists a folder's files straight after the folder.
                        summary.errors += 1
                        self.skips.add("out-of-order file in listing", record["path"], "out of order")
                        continue
                    mtime = mtime_from_ns(record["mtime_ns"])
                    pending.files.append(
                        FileRecord(path=unc, folder_path=pending.path, filename=unc.name,
                                   size=int(record["size"]), mtime=mtime, ctime=mtime)
                    )
                    files += 1
        if pending is not None:
            self._flush(repo, pending, root, scan_run_id)
        if folders == 0:
            raise RuntimeError(f"Listing of {server_root} returned no folders; nothing imported.")
        repo.mark_missing_under_root(root, scan_run_id)
        repo.upsert_scan_stats(root=root, scan_run_id=scan_run_id, total_folders=folders, total_files=files,
                               indexed_folders=folders, indexed_files=files)
        summary.folders += folders
        summary.files += files

    def run(self) -> ImportSummary:
        summary = ImportSummary()
        server_roots = [rc for rc in self.config.root_configs if rc.location == "server"]
        if not server_roots:
            print("No server roots configured.")
            return summary
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        try:
            for root_config in server_roots:
                scan_run_id = repo.begin_scan_run()
                try:
                    self._import_root(repo, root_config, scan_run_id, summary)
                except (RuntimeError, ValueError) as exc:
                    print(f"Import of {root_config.path} abandoned: {exc}")
        finally:
            db.close()
        self.skips.summary()
        if summary.unrepresentable:
            out = self.config.filescan_folder / "server_unrepresentable_names.csv"
            out.parent.mkdir(parents=True, exist_ok=True)
            with out.open("w", newline="", encoding="utf-8", errors="surrogateescape") as handle:
                writer = csv.writer(handle)
                writer.writerow(["server_path"])
                writer.writerows([p] for p in summary.unrepresentable)
            print(f"{len(summary.unrepresentable):,} server names cannot be represented on Windows: {out}")
        print(f"Imported {summary.folders:,} folders and {summary.files:,} files; {summary.errors:,} errors.")
        return summary


def run_import_server(config_path: str | Path) -> ImportSummary:
    return ServerImporter(load_config(config_path)).run()
