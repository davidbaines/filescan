"""Give every file a full hash: `filescan hash-all`.

Every later step works from content hashes: duplicates, similar folder trees,
the build plan, verify-coverage and cleanup. So every active file gets a full
hash, whatever its size, and the database then holds the whole picture.

- PC files are hashed here, two at a time per drive, so a hard disk is not
  made to seek between many files at once. Only a few files per drive are
  queued at any moment, so memory stays flat however many files there are. A
  hash is stored only if the file's size and date still match the scan.
- Server files are hashed on the server by `fsagent hash`, never over SMB.
- Cloud placeholders are never read (reading one downloads it), the master
  root (disk2) is left alone, and a file under no configured root is skipped.

It is restartable: hashes are committed every few hundred files, and a second
run hashes only what is still missing, so it also suits a top-up after a scan.
A file whose size or date changes loses its hash at the next scan. Files that
cannot be read are written to the log in the filescan folder's logs folder and
are tried again on the next run.
"""

from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import PureWindowsPath

from filescan.cleanup.plan import root_configs, root_for
from filescan.config import load_config
from filescan.dedupe.hashing import full_hash, hash_if_unchanged
from filescan.inventory.server_hash import HashRunner, ServerHasher, unc_to_posix
from filescan.models import FileRecord, ScanConfig
from filescan.problems import replace_tried
from filescan.progress import SkipLog, progress_bar
from filescan.storage import FileRepository, SQLiteDB

READERS_PER_DRIVE = 2
QUEUED_PER_DRIVE = READERS_PER_DRIVE * 4
COMMIT_EVERY_ROWS = 500
COMMIT_EVERY_SECONDS = 5.0


def _drive(record: FileRecord) -> str:
    return PureWindowsPath(str(record.path)).drive.upper()


def hash_pc_files(db: SQLiteDB, config: ScanConfig, records: list[FileRecord]) -> dict[str, int]:
    """Hash PC files, a few at a time per drive. Returns counts: hashed, unreadable, changed."""
    repo = FileRepository(db)
    skips = SkipLog(config.filescan_folder, "hash-all", keep=True)
    counts = {"hashed": 0, "unreadable": 0, "changed": 0}
    queues: dict[str, list[FileRecord]] = {}
    for record in records:
        queues.setdefault(_drive(record), []).append(record)
    for queue in queues.values():
        queue.reverse()  # pop() from the end takes them in their original order
    pools = {drive: ThreadPoolExecutor(max_workers=READERS_PER_DRIVE) for drive in queues}
    in_flight: dict[Future, tuple[str, FileRecord]] = {}
    queued = {drive: 0 for drive in queues}
    pending, last_commit = 0, time.monotonic()

    def top_up() -> None:
        for drive, queue in queues.items():
            while queue and queued[drive] < QUEUED_PER_DRIVE:
                record = queue.pop()
                in_flight[pools[drive].submit(hash_if_unchanged, full_hash, record)] = (drive, record)
                queued[drive] += 1

    try:
        with progress_bar(desc="PC files hashed", total=sum(r.size for r in records), unit="B",
                          unit_scale=True) as bar:
            skips.attach(bar)
            top_up()
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    drive, record = in_flight.pop(future)
                    queued[drive] -= 1
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
                top_up()
    finally:
        for pool in pools.values():
            pool.shutdown(cancel_futures=True)
        db.conn.commit()
    skips.summary()
    failed = [("file", path, problem) for what, path, problem in skips.entries or [] if what == "full hash"]
    replace_tried(db, "hash", [str(r.path) for r in records], failed)
    return counts


def run_hash_all_for_config(config: ScanConfig, *, server_runner: HashRunner | None = None) -> dict:
    roots = root_configs(config)
    result = {"hashed": 0, "unreadable": 0, "changed": 0, "outside_roots": 0,
              "server_hashed": 0, "server_stale": 0, "server_errors": 0}
    db = SQLiteDB(config.database_path)
    try:
        masters = [r.path for r in roots if r.role == "master"]
        records = FileRepository(db).files_without_full_hash(skip_roots=masters)
        pc, server = [], []
        for record in records:
            root = root_for(str(record.path), roots)
            if root is None:
                result["outside_roots"] += 1  # e.g. a root since removed from config.yml
            elif root.location == "server":
                if unc_to_posix(record.path, root) is None:
                    result["server_errors"] += 1
                else:
                    server.append(record)
            else:
                pc.append(record)
        gb = lambda rs: sum(r.size for r in rs) / 1e9  # noqa: E731
        print(f"{len(pc):,} PC files ({gb(pc):,.1f} GB) and {len(server):,} server files "
              f"({gb(server):,.1f} GB) have no full hash.")
        if result["outside_roots"]:
            print(f"{result['outside_roots']:,} files are under no configured root and are left alone.")
        if pc:
            result.update(hash_pc_files(db, config, pc))
    finally:
        db.close()
    if server:
        summary = ServerHasher(config, runner=server_runner).hash_files(server)
        result["server_hashed"] = summary.hashed
        result["server_stale"] = summary.stale
        result["server_errors"] += summary.errors
        db = SQLiteDB(config.database_path)
        try:
            replace_tried(db, "hash", [str(r.path) for r in server], [("file", p, e) for p, e in summary.failed])
        finally:
            db.close()
    print(f"PC: {result['hashed']:,} hashed, {result['unreadable']:,} could not be read, "
          f"{result['changed']:,} changed since the scan (scan again to include them).")
    print(f"Server: {result['server_hashed']:,} hashed, {result['server_stale']:,} changed since the import, "
          f"{result['server_errors']:,} errors.")
    print("Run `filescan build-plan` again so the plan uses the new hashes.")
    return result


def run_hash_all(config_path) -> dict:
    return run_hash_all_for_config(load_config(config_path))
