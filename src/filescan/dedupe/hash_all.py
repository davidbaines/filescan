"""Give every file a full hash: `filescan hash-all`.

Every later step works from content hashes: duplicates, similar folder trees,
the build plan, verify-coverage and cleanup. So every active file gets a full
hash, whatever its size, and the database then holds the whole picture.

- PC files are hashed here, with two readers per drive, so a hard disk is not
  made to seek between many files at once. A hash is stored only if the file's
  size and date still match the scan.
- Server files are hashed on the server by `fsagent hash`.
- Cloud placeholders are never read (reading one downloads it), and the master
  root (disk2) is left alone.

It is restartable: hashes are committed every few hundred files, and a second
run hashes only what is still missing, so it also suits a top-up after a scan.
A file whose size or date changes loses its hash at the next scan. Files that
cannot be read go to the log in the logs folder under the filescan folder and are tried
again on the next run.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PureWindowsPath

from filescan.cleanup.plan import root_configs, root_for
from filescan.config import load_config
from filescan.dedupe.duplicates import _hash_if_unchanged
from filescan.dedupe.hashing import full_hash
from filescan.inventory.server_hash import HashRunner, ServerHasher
from filescan.models import FileRecord, ScanConfig
from filescan.progress import SkipLog, progress_bar
from filescan.storage import FileRepository, SQLiteDB
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK

READERS_PER_DRIVE = 2
COMMIT_EVERY_ROWS = 500
COMMIT_EVERY_SECONDS = 5.0


def files_to_hash(db: SQLiteDB, config: ScanConfig) -> list[FileRecord]:
    """Active files with no full hash, outside the master root, that are not cloud placeholders."""
    masters = [str(r.path).rstrip("\\").lower() for r in root_configs(config) if r.role == "master"]
    query = f"""
        SELECT f.id, f.path, fo.path AS folder, f.filename, f.size, f.mtime, f.ctime
        FROM files f
        JOIN folders fo ON fo.id = f.folder_id
        LEFT JOIN file_hashes h ON h.file_id = f.id
        WHERE f.is_missing = 0 AND fo.is_missing = 0 AND (h.full_hash IS NULL OR h.full_hash = '')
          AND (f.attributes & {CLOUD_PLACEHOLDER_MASK}) = 0
    """
    out = []
    for r in db.conn.execute(query):
        low = r["path"].lower()
        if any(low.startswith(m + "\\") for m in masters):
            continue
        out.append(FileRecord(path=Path(r["path"]), folder_path=Path(r["folder"]), filename=r["filename"],
                              size=int(r["size"]), mtime=float(r["mtime"]), ctime=float(r["ctime"]),
                              id=int(r["id"])))
    return out


def _drive(record: FileRecord) -> str:
    return PureWindowsPath(str(record.path)).drive.upper()


def hash_pc_files(db: SQLiteDB, config: ScanConfig, records: list[FileRecord]) -> dict[str, int]:
    """Hash PC files, a few at a time per drive. Returns counts: hashed, unreadable, changed."""
    repo = FileRepository(db)
    skips = SkipLog(config.filescan_folder, "hash-all")
    counts = {"hashed": 0, "unreadable": 0, "changed": 0}
    by_drive: dict[str, list[FileRecord]] = {}
    for record in records:
        by_drive.setdefault(_drive(record), []).append(record)
    pools = {drive: ThreadPoolExecutor(max_workers=READERS_PER_DRIVE) for drive in by_drive}
    pending = 0
    last_commit = time.monotonic()
    try:
        with progress_bar(desc="PC files hashed", total=sum(r.size for r in records), unit="B",
                          unit_scale=True) as bar:
            skips.attach(bar)
            futures = {pools[drive].submit(_hash_if_unchanged, full_hash, record): record
                       for drive, recs in by_drive.items() for record in recs}
            for future in as_completed(futures):
                record = futures[future]
                bar.update(record.size)
                try:
                    digest = future.result()
                except OSError as exc:
                    skips.add("full hash", record.path, exc)
                    counts["unreadable"] += 1
                    continue
                if digest is None:
                    skips.add("changed since the scan", record.path, "changed since the scan")
                    counts["changed"] += 1
                    continue
                repo.upsert_file_hash(int(record.id), full_hash=digest, commit=False)
                counts["hashed"] += 1
                pending += 1
                if pending >= COMMIT_EVERY_ROWS or time.monotonic() - last_commit >= COMMIT_EVERY_SECONDS:
                    db.conn.commit()
                    pending, last_commit = 0, time.monotonic()
            db.conn.commit()
    finally:
        for pool in pools.values():
            pool.shutdown(cancel_futures=True)
        db.conn.commit()
    skips.summary()
    return counts


def run_hash_all_for_config(config: ScanConfig, *, server_runner: HashRunner | None = None) -> dict:
    db = SQLiteDB(config.database_path)
    try:
        records = files_to_hash(db, config)
        roots = root_configs(config)
        pc, server = [], []
        for record in records:
            root = root_for(str(record.path), roots)
            (server if root is not None and root.location == "server" else pc).append(record)
        gb = lambda rs: sum(r.size for r in rs) / 1e9  # noqa: E731
        print(f"{len(pc):,} PC files ({gb(pc):,.1f} GB) and {len(server):,} server files "
              f"({gb(server):,.1f} GB) have no full hash.")
        result = hash_pc_files(db, config, pc) if pc else {}
    finally:
        db.close()
    if server:
        summary = ServerHasher(config, runner=server_runner).hash_files(server)
        result["server_hashed"] = summary.hashed
        result["server_stale"] = summary.stale
    if pc:
        print(f"PC: {result['hashed']:,} hashed, {result['unreadable']:,} could not be read, "
              f"{result['changed']:,} changed since the scan (scan again to include them).")
    return result


def run_hash_all(config_path) -> dict:
    return run_hash_all_for_config(load_config(config_path))
