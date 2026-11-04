"""Task 2: which PC files have no identical copy on the server?

1. Sizes are compared first: a PC file whose size occurs nowhere on the server
   cannot have a copy there, so it is never read.
2. Files whose size occurs on both sides get full hashes: PC files are hashed
   locally, server files on the server by fsagent.
3. Every PC file without a matching server hash is written to a CSV with its
   status, together with totals per top folder.

Cloud placeholders are never read; they are reported as unchecked.
"""

from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from filescan.config import load_config
from filescan.dedupe.hashing import full_hash
from filescan.inventory.server_hash import HashRunner, ServerHasher
from filescan.models import FileRecord, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import FileRepository, SQLiteDB
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK


@dataclass
class CompareResult:
    files_csv: Path
    summary_csv: Path
    missing_files: int
    missing_bytes: int


@dataclass
class _Row:
    id: int
    path: str
    folder: str
    size: int
    attributes: int
    full_hash: str | None


def _root_of(path: str, roots: list[str]) -> str | None:
    lowered = path.lower()
    for root in roots:
        if lowered == root.lower().rstrip("\\") or lowered.startswith(root.lower().rstrip("\\") + "\\"):
            return root
    return None


def _load_rows(db: SQLiteDB, config: ScanConfig) -> tuple[list[_Row], list[_Row]]:
    pc_roots = [str(p) for p in config.pc_roots]
    server_roots = [str(p) for p in config.server_roots]
    total = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0]
    pc_rows: list[_Row] = []
    server_rows: list[_Row] = []
    query = """
        SELECT f.id, f.path, fo.path AS folder, f.size, f.attributes, h.full_hash
        FROM files f
        JOIN folders fo ON fo.id = f.folder_id
        LEFT JOIN file_hashes h ON h.file_id = f.id
        WHERE f.is_missing = 0 AND fo.is_missing = 0
    """
    with progress_bar(desc="compare load files", total=total, unit="file") as bar:
        for r in db.conn.execute(query):
            bar.update(1)
            row = _Row(int(r["id"]), r["path"], r["folder"], int(r["size"]), int(r["attributes"]), r["full_hash"])
            if _root_of(row.path, server_roots):
                server_rows.append(row)
            elif _root_of(row.path, pc_roots):
                pc_rows.append(row)
    return pc_rows, server_rows


def _hash_pc_files(db: SQLiteDB, rows: list[_Row], workers: int) -> None:
    repo = FileRepository(db)
    with progress_bar(desc="compare hash pc", total=len(rows), unit="file") as bar, \
            ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(full_hash, Path(row.path)): row for row in rows}
        done = 0
        for future in as_completed(futures):
            row = futures[future]
            bar.update(1)
            try:
                row.full_hash = future.result()
            except OSError as exc:
                print(f"Skipping hash for {row.path}: {exc}")
                continue
            repo.upsert_file_hash(row.id, full_hash=row.full_hash, commit=False)
            done += 1
            if done % 500 == 0:
                db.conn.commit()
    db.conn.commit()


def _top_folder(path: str, pc_roots: list[str]) -> str:
    root = _root_of(path, pc_roots) or ""
    rest = path[len(root.rstrip("\\")) + 1:]
    first = rest.split("\\", 1)[0] if "\\" in rest else ""
    return root.rstrip("\\") + ("\\" + first if first else "")


def run_compare_for_config(config: ScanConfig, *, server_runner: HashRunner | None = None) -> CompareResult:
    db = SQLiteDB(config.database_path)
    try:
        pc_rows, server_rows = _load_rows(db, config)
        server_sizes = {r.size for r in server_rows if not r.attributes & CLOUD_PLACEHOLDER_MASK}
        pc_sizes = {r.size for r in pc_rows if not r.attributes & CLOUD_PLACEHOLDER_MASK}
        shared = server_sizes & pc_sizes
        pc_todo = [r for r in pc_rows if r.size in shared and r.full_hash is None
                   and not r.attributes & CLOUD_PLACEHOLDER_MASK]
        server_todo = [r for r in server_rows if r.size in shared and r.full_hash is None]
        print(f"{len(shared):,} sizes occur on both sides: {len(pc_todo):,} PC and "
              f"{len(server_todo):,} server files need hashing")
        if pc_todo:
            _hash_pc_files(db, pc_todo, config.worker_count)
    finally:
        db.close()

    if server_todo:
        records = [FileRecord(path=Path(r.path), folder_path=Path(r.folder), filename=Path(r.path).name,
                              size=r.size, mtime=0.0, ctime=0.0, id=r.id) for r in server_todo]
        ServerHasher(config, runner=server_runner).hash_files(records)

    db = SQLiteDB(config.database_path)
    try:
        pc_rows, server_rows = _load_rows(db, config)
    finally:
        db.close()
    server_hashes = {r.full_hash for r in server_rows if r.full_hash}

    pc_roots = [str(p) for p in config.pc_roots]
    rule_cache: dict[str, str] = {}

    def excluded_by(folder: str) -> str:
        if folder not in rule_cache:
            rule = next((r for r in config.exclusions if r.matches(Path(folder))), None)
            rule_cache[folder] = rule.name if rule else ""
        return rule_cache[folder]

    out_dir = config.filescan_folder
    out_dir.mkdir(parents=True, exist_ok=True)
    files_csv = out_dir / "compare_missing_files.csv"
    summary_csv = out_dir / "compare_summary.csv"
    totals: dict[str, list[int]] = {}
    missing_files = missing_bytes = 0
    with files_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["status", "excluded_by", "size", "path"])
        for row in pc_rows:
            top = _top_folder(row.path, pc_roots)
            entry = totals.setdefault(top, [0, 0, 0, 0, 0])
            entry[0] += 1
            entry[1] += row.size
            if row.attributes & CLOUD_PLACEHOLDER_MASK:
                status = "cloud-placeholder"
            elif row.size not in server_sizes:
                status = "no-size-match"
            elif row.full_hash is None:
                status = "not-hashed"
            elif row.full_hash in server_hashes:
                continue
            else:
                status = "content-differs"
            rule = excluded_by(row.folder)
            writer.writerow([status, rule, row.size, row.path])
            missing_files += 1
            missing_bytes += row.size
            entry[2] += 1
            entry[3] += row.size
            if rule:
                entry[4] += row.size

    with summary_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["top_folder", "files", "bytes", "missing_files", "missing_bytes",
                         "missing_bytes_excluded", "missing_GB_not_excluded"])
        for top, (files, size, mfiles, mbytes, excl) in sorted(totals.items(), key=lambda kv: -kv[1][3]):
            writer.writerow([top, files, size, mfiles, mbytes, excl, round((mbytes - excl) / 1e9, 3)])

    print(f"PC files with no identical copy on the server: {missing_files:,} ({missing_bytes / 1e9:,.1f} GB)")
    print(f"  {files_csv}\n  {summary_csv}")
    return CompareResult(files_csv, summary_csv, missing_files, missing_bytes)


def run_compare(config_path: str | Path) -> CompareResult:
    return run_compare_for_config(load_config(config_path))
