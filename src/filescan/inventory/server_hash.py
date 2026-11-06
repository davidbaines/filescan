"""Hash server files on the server with `fsagent hash`, and import the results.

The list of paths is uploaded to the server's work folder, fsagent hashes the
files at local disk speed (resuming any earlier run), and the results come
back over SSH. A hash is stored only if the file's size and mtime still match
the inventory; anything else is counted as stale and needs a fresh import.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from filescan.inventory.server_import import FSAGENT, mtime_from_ns, posix_to_unc
from filescan.models import FileRecord, RootConfig, ScanConfig
from filescan.storage import FileRepository, SQLiteDB

WORK_DIR = "fsagent-work"  # relative to the SSH user's home folder

HashRunner = Callable[[RootConfig, list[str]], Iterable[dict]]


def unc_to_posix(unc: Path, root: RootConfig) -> str | None:
    """Map a UNC path under a server root to the server's POSIX path."""
    root_str = str(root.path).rstrip("\\")
    path_str = str(unc).rstrip("\\")
    if path_str.lower() == root_str.lower():
        return root.server_path
    if not path_str.lower().startswith(root_str.lower() + "\\"):
        return None
    return f"{root.server_path}/" + path_str[len(root_str) + 1:].replace("\\", "/")


def ssh_hash_runner(config: ScanConfig) -> HashRunner:
    def run(root_config: RootConfig, posix_paths: list[str]) -> Iterable[dict]:
        job = "hash-" + (root_config.path.name or str(root_config.path).strip("\\").replace("\\", "_"))
        job_in, job_out = f"{WORK_DIR}/{job}.in.jsonl", f"{WORK_DIR}/{job}.out.jsonl"
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=60", config.server_ssh]
        payload = "".join(json.dumps(p) + "\n" for p in posix_paths).encode("utf-8", "surrogateescape")
        subprocess.run(ssh + [f"mkdir -p {WORK_DIR} && cat > {job_in}"], input=payload, check=True)
        with FSAGENT.open("rb") as script:
            # fsagent prints progress on stderr, which passes straight through.
            subprocess.run(ssh + [config.server_python, "-", "hash", "--input", job_in, "--output", job_out],
                           stdin=script, check=True)
        out = subprocess.run(ssh + [f"cat {job_out}"], capture_output=True, check=True).stdout
        wanted = set(posix_paths)
        for line in out.decode("utf-8", "surrogateescape").splitlines():
            line = line.strip()
            if not line.startswith("{") or not line.endswith("}"):
                continue
            record = json.loads(line)
            if record.get("path") in wanted:
                yield record

    return run


@dataclass
class HashSummary:
    hashed: int = 0
    stale: int = 0
    errors: int = 0


class ServerHasher:
    def __init__(self, config: ScanConfig, *, runner: HashRunner | None = None) -> None:
        self.config = config
        self.runner = runner or ssh_hash_runner(config)

    def hash_files(self, files: list[FileRecord]) -> HashSummary:
        summary = HashSummary()
        by_root: dict[RootConfig, list[str]] = {}
        server_roots = [rc for rc in self.config.root_configs if rc.location == "server"]
        for record in files:
            for root in server_roots:
                posix = unc_to_posix(record.path, root)
                if posix is not None:
                    by_root.setdefault(root, []).append(posix)
                    break
        if not by_root:
            return summary
        db = SQLiteDB(self.config.database_path)
        repo = FileRepository(db)
        try:
            for root, posix_paths in by_root.items():
                print(f"Hashing {len(posix_paths):,} files on the server under {root.server_path}...")
                for result in self.runner(root, posix_paths):
                    if "xxh128" not in result:
                        summary.errors += 1
                        continue
                    unc = posix_to_unc(result["path"], root.server_path or "", root.path)
                    row = None if unc is None else db.conn.execute(
                        "SELECT id, size, mtime FROM files WHERE path = ? AND is_missing = 0", (str(unc),)
                    ).fetchone()
                    if (
                        row is None
                        or int(row["size"]) != int(result["size"])
                        or float(row["mtime"]) != mtime_from_ns(int(result["mtime_ns"]))
                    ):
                        summary.stale += 1
                        continue
                    repo.upsert_file_hash(int(row["id"]), full_hash=result["xxh128"], commit=False)
                    summary.hashed += 1
                    if summary.hashed % 500 == 0:
                        db.conn.commit()
                db.conn.commit()
            if summary.hashed:
                # New server hashes can make more PC files comparable, so `status`
                # compares this time with the last duplicates run.
                repo.set_stage_scan_run_id("server-hashes", repo.latest_scan_run_id())
        finally:
            db.close()
        print(f"Server hashes stored: {summary.hashed:,}; stale: {summary.stale:,}; errors: {summary.errors:,}")
        return summary
