from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from filescan.config import load_config
from filescan.models import DuplicateGroup, FileRecord, FolderRecord, FolderSimilarityCandidate, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB


class FolderSimilarityAnalyser:
    def __init__(self, config: ScanConfig, *, rescan: bool = False) -> None:
        self.config = config
        self.rescan = rescan

    def _children_by_parent(self, folders: list[FolderRecord]) -> dict[int | None, list[FolderRecord]]:
        children: dict[int | None, list[FolderRecord]] = defaultdict(list)
        for folder in folders:
            children[folder.parent_id].append(folder)
        return children

    def _ancestor_map(self, folders: list[FolderRecord]) -> dict[int, set[int]]:
        folder_by_id = {folder.id: folder for folder in folders if folder.id is not None}
        ancestors: dict[int, set[int]] = {}
        with progress_bar(desc="similarity ancestors", total=len(folders), unit="folder") as bar:
            for folder in folders:
                if folder.id is None:
                    bar.update(1)
                    continue
                parent_id = folder.parent_id
                lineage: set[int] = set()
                while parent_id is not None:
                    lineage.add(parent_id)
                    parent_folder = folder_by_id.get(parent_id)
                    if parent_folder is None:
                        break
                    parent_id = parent_folder.parent_id
                ancestors[folder.id] = lineage
                bar.update(1)
        return ancestors

    def _aggregate_folder_data(
        self,
        folders: list[FolderRecord],
        files: list[FileRecord],
        duplicate_groups: list[DuplicateGroup],
    ) -> dict[int, dict[str, object]]:
        children = self._children_by_parent(folders)
        folder_count = sum(1 for folder in folders if folder.id is not None)
        files_by_folder: dict[int, list[FileRecord]] = defaultdict(list)
        with progress_bar(desc="similarity files", total=len(files), unit="file") as bar:
            for file_record in files:
                if file_record.folder_id is not None:
                    files_by_folder[file_record.folder_id].append(file_record)
                bar.update(1)
        duplicate_hashes_by_folder: dict[int, set[str]] = defaultdict(set)
        with progress_bar(desc="similarity duplicates", total=len(duplicate_groups), unit="group") as bar:
            for group in duplicate_groups:
                for file_record in group.files:
                    if file_record.folder_id is not None:
                        duplicate_hashes_by_folder[file_record.folder_id].add(group.full_hash)
                bar.update(1)

        aggregates: dict[int, dict[str, object]] = {}

        with progress_bar(desc="similarity aggregate", total=folder_count, unit="folder") as bar:
            def visit(folder: FolderRecord) -> dict[str, object]:
                if folder.id is None:
                    raise ValueError("Folder must have an id before similarity analysis.")
                direct_files = files_by_folder.get(folder.id, [])
                signatures = {(file_record.filename.lower(), file_record.size) for file_record in direct_files}
                hashes = set(duplicate_hashes_by_folder.get(folder.id, set()))
                total_bytes = sum(file_record.size for file_record in direct_files)
                total_files = len(direct_files)
                for child in children.get(folder.id, []):
                    child_data = visit(child)
                    signatures |= child_data["signatures"]
                    hashes |= child_data["duplicate_hashes"]
                    total_bytes += int(child_data["total_bytes"])
                    total_files += int(child_data["total_files"])
                data = {
                    "folder": folder,
                    "signatures": signatures,
                    "duplicate_hashes": hashes,
                    "total_bytes": total_bytes,
                    "total_files": total_files,
                }
                aggregates[folder.id] = data
                bar.update(1)
                return data

            for folder in folders:
                if folder.parent_id is None:
                    visit(folder)
        return aggregates

    def _candidate_pairs(self, aggregates: dict[int, dict[str, object]], max_common: int = 100) -> set[tuple[int, int]]:
        signature_index: dict[tuple[str, int], set[int]] = defaultdict(set)
        duplicate_index: dict[str, set[int]] = defaultdict(set)
        with progress_bar(desc="similarity index", total=len(aggregates), unit="folder") as bar:
            for folder_id, data in aggregates.items():
                for signature in data["signatures"]:
                    signature_index[signature].add(folder_id)
                for digest in data["duplicate_hashes"]:
                    duplicate_index[digest].add(folder_id)
                bar.update(1)
        pairs: set[tuple[int, int]] = set()
        total_buckets = len(signature_index) + len(duplicate_index)
        with progress_bar(desc="similarity candidates", total=total_buckets, unit="bucket") as bar:
            for index in (signature_index, duplicate_index):
                for folder_ids in index.values():
                    if len(folder_ids) >= 2 and len(folder_ids) <= max_common:
                        ids = sorted(folder_ids)
                        for left_index, left in enumerate(ids):
                            for right in ids[left_index + 1 :]:
                                pairs.add((left, right))
                    bar.update(1)
        return pairs

    def run(self) -> list[FolderSimilarityCandidate]:
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        latest_scan_run_id = repo.latest_scan_run_id()
        duplicates_scan_run_id = repo.get_stage_scan_run_id("duplicates")
        similarity_scan_run_id = repo.get_stage_scan_run_id("similarity")
        if not self.rescan and duplicates_scan_run_id >= latest_scan_run_id and similarity_scan_run_id >= latest_scan_run_id:
            print("Skipping folder similarity analysis without --rescan; existing results are current.")
            candidates = repo.list_similarity_candidates()
            db.close()
            return candidates
        total_folders = repo.count_active_folders()
        folders: list[FolderRecord] = []
        with progress_bar(desc="similarity load folders", total=total_folders, unit="folder") as bar:
            for folder in repo.iter_active_folders():
                folders.append(folder)
                bar.update(1)

        total_files = repo.count_active_files()
        files: list[FileRecord] = []
        with progress_bar(desc="similarity load files", total=total_files, unit="file") as bar:
            for file_record in repo.iter_active_files():
                files.append(file_record)
                bar.update(1)

        total_duplicate_groups = repo.count_duplicate_groups()
        duplicate_groups: list[DuplicateGroup] = []
        with progress_bar(desc="similarity load duplicates", total=total_duplicate_groups, unit="group") as bar:
            for group in repo.iter_duplicate_groups():
                duplicate_groups.append(group)
                bar.update(1)
        aggregates = self._aggregate_folder_data(folders, files, duplicate_groups)
        ancestors = self._ancestor_map(folders)
        folder_by_id = {folder.id: folder for folder in folders if folder.id is not None}
        pairs = sorted(self._candidate_pairs(aggregates))

        candidates: list[FolderSimilarityCandidate] = []
        with progress_bar(desc="similarity score", total=len(pairs), unit="pair") as bar:
            for left_id, right_id in pairs:
                if left_id in ancestors.get(right_id, set()) or right_id in ancestors.get(left_id, set()):
                    bar.update(1)
                    continue
                left = aggregates[left_id]
                right = aggregates[right_id]
                left_signatures = left["signatures"]
                right_signatures = right["signatures"]
                left_hashes = left["duplicate_hashes"]
                right_hashes = right["duplicate_hashes"]
                left_total_files = int(left["total_files"])
                right_total_files = int(right["total_files"])
                left_total_bytes = int(left["total_bytes"])
                right_total_bytes = int(right["total_bytes"])
                if not left_total_files or not right_total_files:
                    bar.update(1)
                    continue
                file_count_ratio = min(left_total_files, right_total_files) / max(left_total_files, right_total_files)
                size_ratio = min(left_total_bytes, right_total_bytes) / max(left_total_bytes, right_total_bytes) if left_total_bytes and right_total_bytes else 0.0
                if file_count_ratio < 0.2 or size_ratio < 0.2:
                    bar.update(1)
                    continue
                shared_signatures = len(left_signatures & right_signatures)
                signature_union = len(left_signatures | right_signatures)
                signature_score = shared_signatures / signature_union if signature_union else 0.0
                shared_duplicates = len(left_hashes & right_hashes)
                duplicate_union = len(left_hashes | right_hashes)
                duplicate_score = shared_duplicates / duplicate_union if duplicate_union else 0.0
                names = {folder_by_id[left_id].path.name.lower(), folder_by_id[right_id].path.name.lower()}
                name_bonus = 0.2 if any(token in name for name in names for token in self.config.backup_name_tokens) else 0.0
                score = (0.55 * duplicate_score) + (0.35 * signature_score) + (0.10 * name_bonus)
                if score >= self.config.similarity_threshold:
                    reason = (
                        f"shared_duplicate_files={shared_duplicates}, "
                        f"shared_signatures={shared_signatures}, "
                        f"name_bonus={name_bonus:.2f}"
                    )
                    candidates.append(
                        FolderSimilarityCandidate(
                            folder_a=folder_by_id[left_id].path,
                            folder_b=folder_by_id[right_id].path,
                            folder_a_id=left_id,
                            folder_b_id=right_id,
                            score=score,
                            shared_duplicate_files=shared_duplicates,
                            shared_signatures=shared_signatures,
                            name_bonus=name_bonus,
                            size_ratio=size_ratio,
                            file_count_ratio=file_count_ratio,
                            reason=reason,
                        )
                    )
                bar.update(1)

        repo.replace_similarity_candidates(candidates)
        repo.set_stage_scan_run_id("similarity", latest_scan_run_id)
        db.close()
        return candidates


def run_similarity(config_path: str | Path, *, rescan: bool = False) -> list[FolderSimilarityCandidate]:
    return FolderSimilarityAnalyser(load_config(config_path), rescan=rescan).run()
