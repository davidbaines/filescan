from __future__ import annotations

from pathlib import Path

from filescan.inventory.normalizer import normalize_path
from filescan.models import FileRecord, FolderRecord
from filescan.storage import FileRepository


def index_paths(repo: FileRepository, root: Path, files: list[Path]) -> int:
    normalized_root = normalize_path(root)
    scan_run_id = repo.begin_scan_run()
    folders: dict[Path, list[Path]] = {}
    for file_path in files:
        folders.setdefault(normalize_path(file_path.parent), []).append(file_path)
    folder_paths = sorted({normalized_root, *folders.keys()}, key=lambda item: (len(item.parts), str(item)))
    for folder_path in folder_paths:
        children = folders.get(folder_path, [])
        total_bytes = sum(child.stat().st_size for child in children)
        stat = folder_path.stat()
        repo.upsert_folder(
            FolderRecord(
                path=folder_path,
                drive=normalized_root.drive,
                parent_path=None if folder_path == normalized_root else normalize_path(folder_path.parent),
                depth=len(folder_path.relative_to(normalized_root).parts) if folder_path != normalized_root else 0,
                file_count=len(children),
                total_bytes=total_bytes,
                mtime=stat.st_mtime,
                scan_run_id=scan_run_id,
            )
        )
        repo.upsert_files(
            folder_path,
            [
                FileRecord(
                    path=normalize_path(file_path),
                    folder_path=folder_path,
                    filename=file_path.name,
                    size=file_path.stat().st_size,
                    mtime=file_path.stat().st_mtime,
                    ctime=file_path.stat().st_ctime,
                )
                for file_path in children
            ],
            scan_run_id,
        )
    repo.mark_missing_under_root(normalized_root, scan_run_id)
    return scan_run_id
