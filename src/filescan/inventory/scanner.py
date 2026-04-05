from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from filescan.config import load_config
from filescan.inventory.normalizer import normalize_path
from filescan.models import FileRecord, FolderRecord, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB


@dataclass(slots=True)
class ScanResult:
    folder: FolderRecord | None
    files: list[FileRecord]
    subdirs: list[Path]
    raw_dirs: int
    raw_files: int


class InventoryScanner:
    def __init__(self, config: ScanConfig, *, rescan: bool = False) -> None:
        self.config = config
        self.rescan = rescan

    def _log_skip(self, *, kind: str, path: Path, exc: OSError) -> None:
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

    def _scan_folder(self, folder: Path, root: Path, depth: int) -> ScanResult:
        try:
            entries = list(folder.iterdir())
        except OSError as exc:
            self._log_skip(kind="folder", path=folder, exc=exc)
            entries = []

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

    def scan(self) -> int:
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        scan_run_id: int | None = None
        latest_scan_run_id = repo.latest_scan_run_id()

        for root in self.config.roots:
            if not root.is_dir():
                continue
            existing_stats = repo.get_scan_stats(root)
            if existing_stats is not None and not self.rescan:
                print(f"Skipping previously scanned root without --rescan: {root}")
                latest_scan_run_id = max(latest_scan_run_id, int(existing_stats["scan_run_id"]))
                continue
            if scan_run_id is None:
                scan_run_id = repo.begin_scan_run()
            queue: list[tuple[Path, int]] = [(root, 0)]
            total_folders = 1
            total_files = 0
            indexed_folders = 0
            indexed_files = 0
            with progress_bar(desc=f"scan {root.name or root.drive}", unit="folder") as bar:
                with ThreadPoolExecutor(max_workers=self.config.worker_count) as pool:
                    while queue:
                        futures = {
                            pool.submit(self._scan_folder, folder, root, depth): (folder, depth)
                            for folder, depth in queue
                        }
                        queue = []
                        for future in as_completed(futures):
                            result = future.result()
                            if result.folder is None:
                                bar.update(1)
                                continue
                            result.folder.scan_run_id = scan_run_id
                            repo.upsert_folder(result.folder)
                            repo.upsert_files(result.folder.path, result.files, scan_run_id)
                            queue.extend((subdir, result.folder.depth + 1) for subdir in result.subdirs)
                            total_folders += result.raw_dirs
                            total_files += result.raw_files
                            indexed_folders += 1
                            indexed_files += len(result.files)
                            bar.update(1)
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


def run_scan(config_path: str | Path, *, rescan: bool = False) -> int:
    return InventoryScanner(load_config(config_path), rescan=rescan).scan()
