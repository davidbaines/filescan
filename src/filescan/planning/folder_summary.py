from __future__ import annotations

from collections import Counter

from filescan.models import FileRecord, FolderRecord


class FolderSummaryBuilder:
    def build(self, folder: FolderRecord, files: list[FileRecord]) -> dict[str, object]:
        suffixes = Counter(file_record.path.suffix.lower() for file_record in files if file_record.folder_path == folder.path)
        return {
            "path": str(folder.path),
            "file_count": folder.file_count,
            "total_bytes": folder.total_bytes,
            "top_suffixes": [suffix for suffix, _ in suffixes.most_common(5)],
        }
