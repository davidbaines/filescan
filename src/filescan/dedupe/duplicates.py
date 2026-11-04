from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.hashing import full_hash, quick_hash
from filescan.models import DuplicateGroup, FileRecord, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB


# Hash rows are committed in batches: a crash loses at most this many hashes or
# this many seconds of work, instead of paying for one commit per file.
_COMMIT_EVERY_ROWS = 500
_COMMIT_EVERY_SECONDS = 5.0


class DuplicateDetector:
    def __init__(self, config: ScanConfig, *, rescan: bool = False) -> None:
        self.config = config
        self.rescan = rescan
        self._uncommitted = 0
        self._last_commit = time.monotonic()

    def _maybe_commit(self, repo: FileRepository, *, force: bool = False) -> None:
        if self._uncommitted == 0:
            return
        due = self._uncommitted >= _COMMIT_EVERY_ROWS or time.monotonic() - self._last_commit >= _COMMIT_EVERY_SECONDS
        if force or due:
            repo.db.conn.commit()
            self._uncommitted = 0
            self._last_commit = time.monotonic()

    def _log_hash_skip(self, path: Path, exc: OSError, *, full: bool) -> None:
        hash_name = "full" if full else "quick"
        print(f"Skipping {hash_name} hash for {path}: {exc}")

    @staticmethod
    def _is_server(path: Path, server_roots: list[Path]) -> bool:
        text = str(path).lower()
        return any(
            text == str(r).lower().rstrip("\\") or text.startswith(str(r).lower().rstrip("\\") + "\\")
            for r in server_roots
        )

    def _populate_hashes(
        self, repo: FileRepository, files: list[FileRecord], *, full: bool, bar, pool: ThreadPoolExecutor
    ) -> None:
        attr = "full_hash" if full else "quick_hash"
        hasher = full_hash if full else quick_hash
        pending = [file_record for file_record in files if getattr(file_record, attr) is None]
        if not pending:
            return
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
                repo.upsert_file_hash(file_record.id, full_hash=digest, commit=False)
            else:
                repo.upsert_file_hash(file_record.id, quick_hash=digest, commit=False)
            self._uncommitted += 1
            bar.update(1)
        self._maybe_commit(repo)

    def run(self) -> list[DuplicateGroup]:
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        latest_scan_run_id = repo.latest_scan_run_id()
        if not self.rescan and repo.get_stage_scan_run_id("duplicates") >= latest_scan_run_id:
            print("Skipping duplicate analysis without --rescan; existing results are current.")
            groups = repo.list_duplicate_groups()
            db.close()
            return groups
        # Group by size in SQL and stream one size group at a time, so memory stays
        # flat however many files are indexed.
        # Server files are hashed on the server by fsagent, never over SMB.
        server_roots = self.config.server_roots
        size_groups = repo.duplicate_size_groups(
            min_size=self.config.duplicate_size_threshold, hashed_only_roots=server_roots
        )
        total_candidate_files = sum(count for _, count in size_groups)
        print(f"  {len(size_groups):,} shared sizes covering {total_candidate_files:,} files")

        confirmed_groups: list[DuplicateGroup] = []
        with (
            progress_bar(desc="duplicate size groups", total=total_candidate_files, unit="file") as groups_bar,
            progress_bar(desc="quick hash", unit="file") as quick_bar,
            progress_bar(desc="full hash", unit="file") as full_bar,
            ThreadPoolExecutor(max_workers=self.config.worker_count) as pool,
        ):
            for size, count in size_groups:
                group = repo.files_of_size(size, hashed_only_roots=server_roots)
                if any(self._is_server(f.path, server_roots) for f in group):
                    # Server files carry only fsagent's full hash, so compare this
                    # size group on full hashes; local files are hashed as needed.
                    candidates = [group]
                else:
                    self._populate_hashes(repo, group, full=False, bar=quick_bar, pool=pool)
                    by_quick: dict[str, list[FileRecord]] = defaultdict(list)
                    for file_record in group:
                        if file_record.quick_hash is not None:
                            by_quick[file_record.quick_hash].append(file_record)
                    candidates = list(by_quick.values())
                for candidate in candidates:
                    if len(candidate) < 2:
                        continue
                    self._populate_hashes(repo, candidate, full=True, bar=full_bar, pool=pool)
                    by_full: dict[str, list[FileRecord]] = defaultdict(list)
                    for file_record in candidate:
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
                groups_bar.update(count)

        self._maybe_commit(repo, force=True)
        repo.replace_duplicate_groups(confirmed_groups)
        repo.set_stage_scan_run_id("duplicates", latest_scan_run_id)
        db.close()
        return confirmed_groups


def run_duplicates(config_path: str | Path, *, rescan: bool = False) -> list[DuplicateGroup]:
    return DuplicateDetector(load_config(config_path), rescan=rescan).run()
