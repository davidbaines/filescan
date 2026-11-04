from __future__ import annotations

import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from filescan.config import load_config
from filescan.inventory.normalizer import normalize_path
from filescan.models import FileRecord, FolderRecord, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB

_MIN_FREE_DB_BYTES = 500 * 1024 * 1024  # 500 MB hard floor


def _is_link(entry: os.DirEntry) -> bool:
    """True for symlinks and NTFS junctions, which the scanner never follows."""
    try:
        return entry.is_symlink() or entry.is_junction()
    except OSError:
        return True


# Windows error codes that are expected during a system-wide scan and logged silently.
_SILENT_WINERRORS: frozenset[int] = frozenset({
    2,   # ERROR_FILE_NOT_FOUND  — file disappeared between listing and stat
    3,   # ERROR_PATH_NOT_FOUND  — path vanished mid-scan
    5,   # ERROR_ACCESS_DENIED   — no permission to read folder/file
})


@dataclass(slots=True)
class ScanResult:
    folder: FolderRecord | None
    files: list[FileRecord]
    subdirs: list[Path]
    raw_dirs: int
    raw_files: int
    skipped: bool = field(default=False)


class InventoryScanner:
    def __init__(self, config: ScanConfig, *, rescan: bool = False, delta: bool = False) -> None:
        self.config = config
        self.rescan = rescan
        self.delta = delta and not rescan  # rescan overrides delta
        self._logged_skips: set[tuple[str, Path, str]] = set()
        self._skip_paths = config.skip_paths

    def _log_skip(self, *, kind: str, path: Path, exc: OSError) -> None:
        if getattr(exc, "winerror", None) in _SILENT_WINERRORS:
            return
        key = (kind, Path(path), str(exc))
        if key in self._logged_skips:
            return
        self._logged_skips.add(key)
        print(f"Skipping {kind}: {path} ({exc})")

    def _safe_stat(self, path: Path, *, kind: str) -> object | None:
        try:
            return path.stat()
        except OSError as exc:
            self._log_skip(kind=kind, path=path, exc=exc)
            return None

    def _excluded_folder(self, path: Path) -> bool:
        return path.name in self.config.exclude_folders or path in self._skip_paths

    def _keep_dir(self, entry: os.DirEntry) -> bool:
        return entry.name not in self.config.exclude_folders and Path(entry.path) not in self._skip_paths

    def _accepted_file_stat(self, entry: os.DirEntry) -> os.stat_result | None:
        """Return the entry's stat if the file passes the size and extension filters.

        On Windows the stat comes from the directory listing, so this costs no
        extra filesystem call, which matters on network shares.
        """
        if Path(entry.name).suffix.lower() in self.config.exclude_extensions:
            return None
        try:
            stat = entry.stat(follow_symlinks=False)
        except OSError as exc:
            self._log_skip(kind="file", path=Path(entry.path), exc=exc)
            return None
        size = stat.st_size
        if size < self.config.min_file_size:
            return None
        if self.config.max_file_size is not None and size > self.config.max_file_size:
            return None
        return stat

    def _list_folder(self, folder: Path) -> tuple[list[os.DirEntry], list[os.DirEntry]] | None:
        """Split a folder's entries into (dirs, files), skipping links. None if unreadable."""
        dirs: list[os.DirEntry] = []
        files: list[os.DirEntry] = []
        try:
            with os.scandir(folder) as iterator:
                for entry in iterator:
                    if _is_link(entry):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            dirs.append(entry)
                        elif entry.is_file(follow_symlinks=False):
                            files.append(entry)
                    except OSError as exc:
                        self._log_skip(kind="entry", path=Path(entry.path), exc=exc)
        except OSError as exc:
            self._log_skip(kind="folder", path=folder, exc=exc)
            return None
        return dirs, files

    def _scan_folder(self, folder: Path, root: Path, depth: int, stored_ts: float | None = None) -> ScanResult:
        if stored_ts is not None:
            try:
                if folder.stat().st_mtime <= stored_ts:
                    listing = self._list_folder(folder)
                    if listing is None:
                        return ScanResult(folder=None, files=[], subdirs=[], raw_dirs=0, raw_files=0)
                    subdirs = [Path(e.path) for e in listing[0] if self._keep_dir(e)]
                    return ScanResult(folder=None, files=[], subdirs=subdirs, raw_dirs=0, raw_files=0, skipped=True)
            except OSError:
                pass  # can't stat — fall through to full scan

        listing = self._list_folder(folder)
        if listing is None:
            return ScanResult(folder=None, files=[], subdirs=[], raw_dirs=0, raw_files=0)
        all_dirs, all_files = listing

        subdirs = [Path(entry.path) for entry in all_dirs if self._keep_dir(entry)]
        files: list[FileRecord] = []
        total_bytes = 0
        for entry in all_files:
            stat = self._accepted_file_stat(entry)
            if stat is None:
                continue
            total_bytes += stat.st_size
            files.append(
                FileRecord(
                    path=Path(entry.path),
                    folder_path=folder,
                    filename=entry.name,
                    size=stat.st_size,
                    mtime=stat.st_mtime,
                    ctime=stat.st_ctime,
                    attributes=getattr(stat, "st_file_attributes", 0),
                )
            )
        folder_stat = self._safe_stat(folder, kind="folder")
        return ScanResult(
            folder=None
            if folder_stat is None
            else FolderRecord(
                path=folder,
                drive=root.drive,
                parent_path=None if folder == root else folder.parent,
                depth=depth,
                file_count=len(files),
                total_bytes=total_bytes,
                mtime=folder_stat.st_mtime,
            ),
            files=files,
            subdirs=subdirs,
            raw_dirs=len(all_dirs),
            raw_files=len(all_files),
        )

    def _count_folders(self, root: Path) -> int | None:
        """Count folders before the scan so the progress bar has a total.

        Network roots return None: a pre-walk would double the round trips.
        """
        if str(root).startswith("\\\\"):
            return None
        count = 1  # root itself
        stack: list[Path] = [root]
        while stack:
            listing = self._list_folder_quiet(stack.pop())
            for entry in listing:
                if self._keep_dir(entry):
                    stack.append(Path(entry.path))
                    count += 1
        return count

    @staticmethod
    def _list_folder_quiet(folder: Path) -> list[os.DirEntry]:
        dirs: list[os.DirEntry] = []
        try:
            with os.scandir(folder) as iterator:
                for entry in iterator:
                    try:
                        if not _is_link(entry) and entry.is_dir(follow_symlinks=False):
                            dirs.append(entry)
                    except OSError:
                        pass
        except OSError:
            pass
        return dirs

    def _load_folder_scan_times(self, db: SQLiteDB) -> dict[Path, float]:
        """Pre-load folder paths → last_scanned_at as Unix timestamps for delta comparison."""
        result: dict[Path, float] = {}
        for row in db.conn.execute(
            "SELECT path, last_scanned_at FROM folders WHERE is_missing = 0"
        ).fetchall():
            try:
                ts = datetime.fromisoformat(row["last_scanned_at"]).replace(tzinfo=timezone.utc).timestamp()
                result[Path(row["path"])] = ts
            except (ValueError, OSError):
                pass
        return result

    def scan(self) -> int:
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        scan_run_id: int | None = None
        latest_scan_run_id = repo.latest_scan_run_id()

        folder_scan_times: dict[Path, float] = (
            self._load_folder_scan_times(db) if self.delta else {}
        )

        # Server roots are listed by the server helper, never walked over SMB.
        for root in self.config.pc_roots:
            if not root.is_dir():
                continue
            existing_stats = repo.get_scan_stats(root)
            if existing_stats is not None and not self.rescan and not self.delta:
                print(f"Skipping previously scanned root without --rescan: {root}")
                latest_scan_run_id = max(latest_scan_run_id, int(existing_stats["scan_run_id"]))
                continue
            if scan_run_id is None:
                scan_run_id = repo.begin_scan_run()
            normalized_root = normalize_path(root)
            if self.delta:
                db_count = repo.count_active_folders()
                print(f"Delta scan of {root} (up to {db_count:,} folders to check)...")
                folder_count = None
            else:
                print(f"Counting folders in {root}...", end="", flush=True)
                folder_count = self._count_folders(normalized_root)
                print(" skipped (network root)" if folder_count is None else f" {folder_count:,}")
            queue: list[tuple[Path, int]] = [(normalized_root, 0)]
            visited: set[Path] = {normalized_root}
            total_folders = 1
            total_files = 0
            indexed_folders = 0
            indexed_files = 0
            skipped_folders = 0
            with progress_bar(desc=f"scan {root.name or root.drive}", total=folder_count, unit="folder") as bar:
                with ThreadPoolExecutor(max_workers=self.config.worker_count) as pool:
                    while queue:
                        futures = {
                            pool.submit(
                                self._scan_folder, folder, root, depth,
                                folder_scan_times.get(folder) if self.delta else None,
                            ): (folder, depth)
                            for folder, depth in queue
                        }
                        queue = []
                        for future in as_completed(futures):
                            folder, depth = futures[future]
                            result = future.result()
                            if result.skipped:
                                repo.promote_folder_and_files(folder, scan_run_id)
                                skipped_folders += 1
                            elif result.folder is not None:
                                result.folder.scan_run_id = scan_run_id
                                repo.upsert_folder(result.folder)
                                repo.upsert_files(result.folder.path, result.files, scan_run_id)
                                total_files += result.raw_files
                                indexed_folders += 1
                                indexed_files += len(result.files)
                                if indexed_folders % 500 == 0:
                                    try:
                                        free = shutil.disk_usage(self.config.database_path.parent).free
                                        if free < _MIN_FREE_DB_BYTES:
                                            db.close()
                                            raise SystemExit(
                                                f"Scan aborted: only {free // (1024 * 1024)} MB remaining on "
                                                f"{self.config.database_path.parent}. "
                                                "Progress has been saved — rerun after freeing space."
                                            )
                                    except OSError:
                                        pass
                            new_subdirs: list[Path] = []
                            for subdir in result.subdirs:
                                if subdir in visited:
                                    continue
                                visited.add(subdir)
                                new_subdirs.append(subdir)
                            queue.extend((subdir, depth + 1) for subdir in new_subdirs)
                            total_folders += len(new_subdirs)
                            bar.update(1)
            if self.delta:
                print(f"  {indexed_folders} folders rescanned, {skipped_folders} unchanged")
            repo.mark_missing_under_root(root, scan_run_id)
            repo.upsert_scan_stats(
                root=root,
                scan_run_id=scan_run_id,
                total_folders=total_folders,
                total_files=total_files,
                indexed_folders=indexed_folders,
                indexed_files=indexed_files,
            )
        db.close()
        return scan_run_id if scan_run_id is not None else latest_scan_run_id


def run_scan(config_path: str | Path, *, rescan: bool = False, delta: bool = False) -> int:
    return InventoryScanner(load_config(config_path), rescan=rescan, delta=delta).scan()
