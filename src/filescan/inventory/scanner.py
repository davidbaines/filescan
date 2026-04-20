from __future__ import annotations

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


def _is_dir_safe(path: Path) -> bool:
    try:
        return path.is_dir()
    except OSError:
        return False

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

    def _log_skip(self, *, kind: str, path: Path, exc: OSError) -> None:
        if getattr(exc, "winerror", None) in _SILENT_WINERRORS:
            return
        normalized_path = normalize_path(path)
        key = (kind, normalized_path, str(exc))
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
        return path.name in self.config.exclude_folders

    def _excluded_file(self, path: Path) -> object | None:
        suffix = path.suffix.lower()
        if suffix in self.config.exclude_extensions:
            return None
        stat = self._safe_stat(path, kind="file")
        if stat is None:
            return None
        size = stat.st_size
        if size < self.config.min_file_size:
            return None
        if self.config.max_file_size is not None and size > self.config.max_file_size:
            return None
        return stat

    def _scan_folder(self, folder: Path, root: Path, depth: int, stored_ts: float | None = None) -> ScanResult:
        if stored_ts is not None:
            try:
                if folder.stat().st_mtime <= stored_ts:
                    try:
                        entries = list(folder.iterdir())
                    except OSError as exc:
                        self._log_skip(kind="folder", path=folder, exc=exc)
                        return ScanResult(folder=None, files=[], subdirs=[], raw_dirs=0, raw_files=0)
                    subdirs = [
                        normalize_path(e) for e in entries
                        if _is_dir_safe(e) and not self._excluded_folder(e)
                    ]
                    return ScanResult(folder=None, files=[], subdirs=subdirs, raw_dirs=0, raw_files=0, skipped=True)
            except OSError:
                pass  # can't stat — fall through to full scan

        try:
            entries = list(folder.iterdir())
        except OSError as exc:
            self._log_skip(kind="folder", path=folder, exc=exc)
            return ScanResult(folder=None, files=[], subdirs=[], raw_dirs=0, raw_files=0)

        all_dirs: list[Path] = []
        all_files: list[Path] = []
        for entry in entries:
            try:
                if entry.is_dir():
                    all_dirs.append(entry)
                elif entry.is_file():
                    all_files.append(entry)
            except OSError as exc:
                self._log_skip(kind="entry", path=entry, exc=exc)
        subdirs = [normalize_path(entry) for entry in all_dirs if not self._excluded_folder(entry)]
        files: list[FileRecord] = []
        total_bytes = 0
        for entry in all_files:
            stat = self._excluded_file(entry)
            if stat is None:
                continue
            total_bytes += stat.st_size
            files.append(
                FileRecord(
                    path=normalize_path(entry),
                    folder_path=normalize_path(folder),
                    filename=entry.name,
                    size=stat.st_size,
                    mtime=stat.st_mtime,
                    ctime=stat.st_ctime,
                )
            )
        folder_stat = self._safe_stat(folder, kind="folder")
        normalized_folder = normalize_path(folder)
        return ScanResult(
            folder=None
            if folder_stat is None
            else FolderRecord(
                path=normalized_folder,
                drive=root.drive,
                parent_path=None if normalized_folder == root else normalize_path(folder.parent),
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

    def _count_folders(self, root: Path) -> int:
        """Fast metadata-only walk to count folders before the scan starts."""
        count = 1  # root itself
        stack: list[Path] = [root]
        while stack:
            folder = stack.pop()
            try:
                for entry in folder.iterdir():
                    try:
                        if entry.is_dir() and entry.name not in self.config.exclude_folders:
                            stack.append(entry)
                            count += 1
                    except OSError:
                        pass
            except OSError:
                pass
        return count

    def _load_folder_scan_times(self, db: SQLiteDB) -> dict[Path, float]:
        """Pre-load folder paths → last_scanned_at as Unix timestamps for delta comparison."""
        result: dict[Path, float] = {}
        for row in db.conn.execute(
            "SELECT path, last_scanned_at FROM folders WHERE is_missing = 0"
        ).fetchall():
            try:
                ts = datetime.fromisoformat(row["last_scanned_at"]).replace(tzinfo=timezone.utc).timestamp()
                result[normalize_path(row["path"])] = ts
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

        for root in self.config.roots:
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
                print(f" {folder_count:,}")
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
