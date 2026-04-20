from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.hashing import full_hash, quick_hash
from filescan.models import DuplicateGroup, FileRecord, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB


class DuplicateDetector:
    def __init__(self, config: ScanConfig, *, rescan: bool = False) -> None:
        self.config = config
        self.rescan = rescan

    def _log_hash_skip(self, path: Path, exc: OSError, *, full: bool) -> None:
        hash_name = "full" if full else "quick"
        print(f"Skipping {hash_name} hash for {path}: {exc}")

    def _populate_hashes(self, repo: FileRepository, files: list[FileRecord], *, full: bool, bar) -> None:
        attr = "full_hash" if full else "quick_hash"
        hasher = full_hash if full else quick_hash
        pending = [file_record for file_record in files if getattr(file_record, attr) is None]
        if not pending:
            return
        with ThreadPoolExecutor(max_workers=self.config.worker_count) as pool:
            futures = {pool.submit(hasher, file_record.path): file_record for file_record in pending}
            for future in as_completed(futures):
                file_record = futures[future]
                try:
                    digest = future.result()
                except OSError as exc:
                    self._log_hash_skip(file_record.path, exc, full=full)
                    bar.update(1)
                    continue
                setattr(file_record, attr, digest)
                if file_record.id is None:
                    raise ValueError("Hashed file must have an id.")
                if full:
                    repo.upsert_file_hash(file_record.id, full_hash=digest)
                else:
                    repo.upsert_file_hash(file_record.id, quick_hash=digest)
                bar.update(1)

    def run(self) -> list[DuplicateGroup]:
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        latest_scan_run_id = repo.latest_scan_run_id()
        if not self.rescan and repo.get_stage_scan_run_id("duplicates") >= latest_scan_run_id:
            print("Skipping duplicate analysis without --rescan; existing results are current.")
            groups = repo.list_duplicate_groups()
            db.close()
            return groups
        total_candidate_files = repo.count_active_files(min_size=self.config.duplicate_size_threshold)
        files: list[FileRecord] = []
        with progress_bar(desc="duplicate load files", total=total_candidate_files, unit="file") as bar:
            for file_record in repo.iter_active_files(min_size=self.config.duplicate_size_threshold):
                files.append(file_record)
                bar.update(1)
        by_size: dict[int, list[FileRecord]] = defaultdict(list)
        with progress_bar(desc="duplicate size groups", total=len(files), unit="file") as bar:
            for file_record in files:
                by_size[file_record.size].append(file_record)
                bar.update(1)
        size_candidates = [group for group in by_size.values() if len(group) > 1]

        quick_candidates: list[list[FileRecord]] = []
        total_quick_hashes = sum(
            1 for group in size_candidates for file_record in group if file_record.quick_hash is None
        )
        with progress_bar(desc="quick hash", total=total_quick_hashes, unit="file") as bar:
            for group in size_candidates:
                self._populate_hashes(repo, group, full=False, bar=bar)
                by_quick: dict[str, list[FileRecord]] = defaultdict(list)
                for file_record in group:
                    if file_record.quick_hash is not None:
                        by_quick[file_record.quick_hash].append(file_record)
                quick_candidates.extend(candidate for candidate in by_quick.values() if len(candidate) > 1)

        confirmed_groups: list[DuplicateGroup] = []
        total_full_hashes = sum(
            1 for group in quick_candidates for file_record in group if file_record.full_hash is None
        )
        with progress_bar(desc="full hash", total=total_full_hashes, unit="file") as bar:
            for group in quick_candidates:
                self._populate_hashes(repo, group, full=True, bar=bar)
                by_full: dict[str, list[FileRecord]] = defaultdict(list)
                for file_record in group:
                    if file_record.full_hash is not None:
                        by_full[file_record.full_hash].append(file_record)
                for digest, matches in by_full.items():
                    if len(matches) > 1:
                        confirmed_groups.append(
                            DuplicateGroup(
                                full_hash=digest,
                                size_bytes=matches[0].size,
                                files=tuple(sorted(matches, key=lambda item: str(item.path))),
                            )
                        )

        repo.replace_duplicate_groups(confirmed_groups)
        repo.set_stage_scan_run_id("duplicates", latest_scan_run_id)
        db.close()
        return confirmed_groups


def run_duplicates(config_path: str | Path, *, rescan: bool = False) -> list[DuplicateGroup]:
    return DuplicateDetector(load_config(config_path), rescan=rescan).run()
