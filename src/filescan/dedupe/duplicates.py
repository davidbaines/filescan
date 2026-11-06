from __future__ import annotations

import json
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
# this many seconds of work, instead of paying for one commit per file. Rows are
# held in memory until then and written in one short transaction, so the write
# lock is never held while a large file is being read and other filescan
# commands are not locked out.
_COMMIT_EVERY_ROWS = 500
_COMMIT_EVERY_SECONDS = 5.0

SUMMARY_FILE = "duplicates.json"


def summary_path(config: ScanConfig) -> Path:
    """Where each duplicates run records what it read; `filescan status` shows it."""
    return config.filescan_folder / SUMMARY_FILE


def _same_as_scanned(path: Path, record: FileRecord) -> bool:
    stat = path.stat()
    return stat.st_size == record.size and stat.st_mtime == record.mtime


def _hash_if_unchanged(hasher, record: FileRecord) -> str | None:
    """The file's hash, or None if its size or date differs from the scan, before or after reading."""
    if not _same_as_scanned(record.path, record):
        return None
    digest = hasher(record.path)
    return digest if _same_as_scanned(record.path, record) else None


class DuplicateDetector:
    def __init__(self, config: ScanConfig, *, rescan: bool = False) -> None:
        self.config = config
        self.rescan = rescan
        self._pending: list[tuple[int, bool, str]] = []  # (file id, is full hash, digest)
        self._last_commit = time.monotonic()
        self.reads = {"quick": 0, "full": 0}
        self.unreadable = 0
        self.changed: set[int | None] = set()

    def _maybe_commit(self, repo: FileRepository, *, force: bool = False) -> None:
        if not self._pending:
            return
        due = len(self._pending) >= _COMMIT_EVERY_ROWS or time.monotonic() - self._last_commit >= _COMMIT_EVERY_SECONDS
        if force or due:
            for file_id, full, digest in self._pending:
                if full:
                    repo.upsert_file_hash(file_id, full_hash=digest, commit=False)
                else:
                    repo.upsert_file_hash(file_id, quick_hash=digest, commit=False)
            repo.db.conn.commit()
            self._pending.clear()
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
        futures = {pool.submit(_hash_if_unchanged, hasher, file_record): file_record for file_record in pending}
        for future in as_completed(futures):
            file_record = futures[future]
            try:
                digest = future.result()
            except OSError as exc:
                self._log_hash_skip(file_record.path, exc, full=full)
                self.unreadable += 1
                bar.update(1)
                continue
            if digest is None:
                # Changed since the scan: a hash of the new content would be stored
                # against the old size and date, so leave it for the next scan.
                self.changed.add(file_record.id)
                bar.update(1)
                continue
            self.reads["full" if full else "quick"] += 1
            setattr(file_record, attr, digest)
            if file_record.id is None:
                raise ValueError("Hashed file must have an id.")
            self._pending.append((file_record.id, full, digest))
            bar.update(1)
            self._maybe_commit(repo)
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
        total_candidate_files = sum(count for _, count, _ in size_groups)
        unhashed = sum(n for _, _, n in size_groups)
        print(
            f"  {len(size_groups):,} shared sizes covering {total_candidate_files:,} files: "
            f"{total_candidate_files - unhashed:,} already hashed, {unhashed:,} not hashed yet."
        )
        print(
            "  Only files with no hash are read, plus a full hash where two quick hashes match."
        )

        confirmed_groups: list[DuplicateGroup] = []
        with (
            progress_bar(desc="size groups checked", total=len(size_groups), unit="group") as groups_bar,
            progress_bar(desc="quick hash (files read)", unit="file") as quick_bar,
            progress_bar(desc="full hash (files read)", unit="file") as full_bar,
            ThreadPoolExecutor(max_workers=self.config.worker_count) as pool,
        ):
            for size, count, _ in size_groups:
                group = repo.files_of_size(size, hashed_only_roots=server_roots)
                if all(f.full_hash is not None for f in group) or any(
                    self._is_server(f.path, server_roots) for f in group
                ):
                    # Compare on full hashes when every file already has one (nothing
                    # to read), or when server files are present, since they carry
                    # only fsagent's full hash; local files are hashed as needed.
                    candidates = [group]
                else:
                    self._populate_hashes(repo, group, full=False, bar=quick_bar, pool=pool)
                    by_quick: dict[str, list[FileRecord]] = defaultdict(list)
                    for file_record in group:
                        if file_record.quick_hash is not None:
                            by_quick[file_record.quick_hash].append(file_record)
                    candidates = list(by_quick.values())
                # Collected across all quick-hash buckets: a stale stored quick hash can
                # put two copies of the same content in different buckets.
                by_full: dict[str, list[FileRecord]] = defaultdict(list)
                for candidate in candidates:
                    if len(candidate) < 2:
                        continue
                    self._populate_hashes(repo, candidate, full=True, bar=full_bar, pool=pool)
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
                groups_bar.update(1)

        self._maybe_commit(repo, force=True)
        repo.replace_duplicate_groups(confirmed_groups)
        repo.set_stage_scan_run_id("duplicates", latest_scan_run_id)
        db.close()
        self._write_summary(files=total_candidate_files, unhashed=unhashed, groups=len(confirmed_groups))
        print(
            f"  Read {self.reads['quick']:,} quick and {self.reads['full']:,} full hashes; "
            f"{self.unreadable:,} files could not be read; {len(self.changed):,} have changed since the scan "
            "and were not hashed (scan again to include them)."
        )
        return confirmed_groups

    def _write_summary(self, *, files: int, unhashed: int, groups: int) -> None:
        summary = {
            "finished": time.strftime("%Y-%m-%d %H:%M:%S"),
            "files": files,
            "unhashed_before": unhashed,
            "quick_read": self.reads["quick"],
            "full_read": self.reads["full"],
            "unreadable": self.unreadable,
            "changed_since_scan": len(self.changed),
            "duplicate_groups": groups,
        }
        try:
            path = summary_path(self.config)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(summary, indent=1), encoding="utf-8")
        except OSError as exc:
            print(f"Could not save the run summary: {exc}")


def run_duplicates(config_path: str | Path, *, rescan: bool = False) -> list[DuplicateGroup]:
    return DuplicateDetector(load_config(config_path), rescan=rescan).run()
