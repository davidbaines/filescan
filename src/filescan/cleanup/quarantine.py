"""Carry out a cleanup plan: quarantine, restore, and finally purge.

Quarantine is the default and is reversible. Each planned file is renamed into
<root>\\_filescan_quarantine\\<plan id>\\<path below the root>, on the same disk,
so the move is instant and needs no space. A rename never copies: a file that
would have to cross disks is reported and left where it is. Before each move
the file's size and mtime are compared with the plan; a file that has changed
since the scan is left alone. Folders left empty are removed.

Files on the PC are moved by filescan; files on the server by `fsagent move`
over SSH, because the server's shares are read-only.

Every result is appended to actions.jsonl in the plan folder, so a run can stop
and resume, and so `cleanup-restore` knows what to put back. A quarantined
file is marked missing in the database at once, so it no longer counts as a
copy of anything, and the scanner never indexes the quarantine folder.

`cleanup-purge` deletes quarantined files for good, only once they have sat in
quarantine for a number of days (14 by default), and only with --yes. A
"covered" file is purged only if its copy is still there with the same size;
otherwise it stays in quarantine and is reported.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from pathlib import Path

from filescan.cleanup.plan import PlanEntry, gb, read_plan, root_configs, root_for
from filescan.config import load_config
from filescan.inventory.server_hash import WORK_DIR, unc_to_posix
from filescan.inventory.server_import import FSAGENT
from filescan.models import QUARANTINE_DIR, RootConfig, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB

_spec = importlib.util.spec_from_file_location("fsagent", FSAGENT)
fsagent = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fsagent)  # the same move and remove code runs on both machines

# (root, op, manifest entries) -> result records, each carrying the entry's key
ServerRunner = Callable[[RootConfig, str, list[dict]], Iterable[dict]]

_COMMIT_EVERY = 500


def quarantine_root(root: RootConfig) -> str:
    return str(root.path).rstrip("\\") + "\\" + QUARANTINE_DIR


def quarantine_path(path: str, root: RootConfig, plan_id: str) -> str:
    r = str(root.path).rstrip("\\")
    return quarantine_root(root) + "\\" + plan_id + "\\" + path[len(r) + 1:]


def ssh_runner(config: ScanConfig, plan_id: str) -> ServerRunner:
    def run(root: RootConfig, op: str, entries: list[dict]) -> Iterable[dict]:
        name = root.path.name or str(root.path).strip("\\").replace("\\", "_")
        # A new job per run: whether a file still needs moving is decided from
        # the disk (move_one treats "gone from src, present at dst" as moved),
        # never from an earlier run's log.
        job = f"{WORK_DIR}/cleanup-{plan_id}-{op}-{name}-{datetime.now():%Y%m%d-%H%M%S}"
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=60", config.server_ssh]
        payload = "".join(json.dumps(e) + "\n" for e in entries).encode("utf-8", "surrogateescape")
        subprocess.run(ssh + [f"mkdir -p {WORK_DIR} && cat > {job}.manifest.jsonl"], input=payload, check=True)
        with FSAGENT.open("rb") as script:
            # fsagent prints progress on stderr; a non-zero exit only means some files failed.
            subprocess.run(ssh + [config.server_python, "-", op, "--manifest", f"{job}.manifest.jsonl",
                                  "--log", f"{job}.log.jsonl"], stdin=script, check=False)
        out = subprocess.run(ssh + [f"cat {job}.log.jsonl"], capture_output=True, check=True).stdout
        for line in out.decode("utf-8", "surrogateescape").splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                yield json.loads(line)

    return run


# ---------------------------------------------------------------- log


def _read_log(folder: Path) -> list[dict]:
    path = folder / "actions.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # a line cut short by a crash
    return records


def last_states(folder: Path) -> dict[str, dict]:
    """The latest logged record for each original path."""
    return {r["path"]: r for r in _read_log(folder)}


class _Log:
    def __init__(self, folder: Path) -> None:
        self.handle = (folder / "actions.jsonl").open("a", encoding="utf-8")

    def write(self, record: dict) -> None:
        self.handle.write(json.dumps({**record, "at": datetime.now().isoformat(timespec="seconds")}) + "\n")
        self.handle.flush()

    def close(self) -> None:
        self.handle.close()


class _Missing:
    """Batch updates of files.is_missing, committed every few hundred rows."""

    def __init__(self, db: SQLiteDB) -> None:
        self.db = db
        self.pending: list[tuple[int, int]] = []

    def set(self, file_id: int, missing: bool) -> None:
        self.pending.append((1 if missing else 0, file_id))
        if len(self.pending) >= _COMMIT_EVERY:
            self.flush()

    def flush(self) -> None:
        if self.pending:
            self.db.conn.executemany("UPDATE files SET is_missing = ? WHERE id = ?", self.pending)
            self.db.conn.commit()
            self.pending.clear()


# ---------------------------------------------------------------- the three operations


def reason_matches(entry_reason: str, wanted: str | None) -> bool:
    """`wanted` is a full reason such as review:server-trash, or a prefix ending in ':' such as rule:."""
    if not wanted:
        return True
    return entry_reason == wanted or (wanted.endswith(":") and entry_reason.startswith(wanted))


def _jobs(entries: list[PlanEntry], config: ScanConfig, only: str | None,
          reason: str | None = None) -> list[tuple[PlanEntry, RootConfig]]:
    roots = root_configs(config)
    jobs = []
    for e in entries:
        if (only and e.location != only) or not reason_matches(e.reason, reason):
            continue
        root = root_for(e.path, roots)
        if root is not None:
            jobs.append((e, root))
    return jobs


def _execute(config: ScanConfig, folder: Path, jobs: list[tuple[PlanEntry, RootConfig, dict]], op: str,
             runner: ServerRunner | None, on_result: Callable[[PlanEntry, dict], None]) -> Counter:
    """Run move or remove for each (entry, root, action) job, locally or on the server."""
    counts: Counter = Counter()
    log = _Log(folder)
    try:
        local = [j for j in jobs if j[1].location != "server"]
        with progress_bar(desc=f"{op} on the PC", total=len(local), unit="file") as bar:
            for entry, _root, action in local:
                if op == "move":
                    result = fsagent.move_one(action["src"], action["dst"], entry.size, entry.mtime, action["stop"])
                else:
                    result = fsagent.remove_one(action["path"], entry.size, action["stop"])
                record = {"path": entry.path, "op": action["op"], **result}
                log.write(record)
                on_result(entry, record)
                counts[result["status"]] += 1
                bar.update(1)
        by_root: dict[RootConfig, list[tuple[PlanEntry, dict]]] = {}
        for entry, root, action in jobs:
            if root.location == "server":
                by_root.setdefault(root, []).append((entry, action))
        for root, items in by_root.items():
            if runner is None:
                runner = ssh_runner(config, folder.name)
            key = "src" if op == "move" else "path"
            manifest, back = [], {}
            for entry, action in items:
                posix = {k: unc_to_posix(Path(action[k]), root) for k in ("src", "dst", "path", "stop") if k in action}
                if any(v is None for v in posix.values()):
                    continue
                manifest.append({**posix, "size": entry.size, "mtime": entry.mtime})
                back[posix[key]] = (entry, action)
            print(f"{op}: {len(manifest):,} files on the server under {root.server_path}...")
            latest: dict[str, dict] = {}
            for result in runner(root, op, manifest):
                if result.get(key) in back:
                    latest[result[key]] = result
            for posix_key, result in latest.items():
                entry, action = back[posix_key]
                record = {"path": entry.path, "op": action["op"],
                          **{k: v for k, v in result.items() if k not in ("src", "dst", "path")}}
                log.write(record)
                on_result(entry, record)
                counts[result["status"]] += 1
    finally:
        log.close()
    return counts


def quarantine(config: ScanConfig, plan_path: Path, *, only: str | None = None, dry_run: bool = False,
               runner: ServerRunner | None = None, reason: str | None = None) -> Counter:
    folder, entries = read_plan(plan_path)
    states = last_states(folder)
    jobs = []
    for entry, root in _jobs(entries, config, only, reason):
        if states.get(entry.path, {}).get("status") == "moved" and states[entry.path].get("op") == "quarantine":
            continue  # already in quarantine
        jobs.append((entry, root, {"op": "quarantine", "src": entry.path,
                                   "dst": quarantine_path(entry.path, root, folder.name),
                                   "stop": str(root.path)}))
    if dry_run:
        size = sum(e.size for e, _, _ in jobs)
        print(f"Dry run: {len(jobs):,} files ({gb(size)}) would be moved to quarantine. Nothing was moved.")
        return Counter(would_move=len(jobs))
    db = SQLiteDB(config.database_path)
    missing = _Missing(db)
    try:
        def on_result(entry: PlanEntry, record: dict) -> None:
            if record["status"] == "moved":
                missing.set(entry.file_id, True)
        counts = _execute(config, folder, jobs, "move", runner, on_result)
        missing.flush()
    finally:
        db.close()
    return counts


def restore(config: ScanConfig, plan_path: Path, *, only: str | None = None,
            runner: ServerRunner | None = None, reason: str | None = None) -> Counter:
    folder, entries = read_plan(plan_path)
    states = last_states(folder)
    jobs = []
    for entry, root in _jobs(entries, config, only, reason):
        state = states.get(entry.path, {})
        if state.get("status") == "moved" and state.get("op") == "quarantine":
            jobs.append((entry, root, {"op": "restore", "src": quarantine_path(entry.path, root, folder.name),
                                       "dst": entry.path, "stop": quarantine_root(root)}))
    db = SQLiteDB(config.database_path)
    missing = _Missing(db)
    try:
        def on_result(entry: PlanEntry, record: dict) -> None:
            if record["status"] == "moved":
                missing.set(entry.file_id, False)
        counts = _execute(config, folder, jobs, "move", runner, on_result)
        missing.flush()
    finally:
        db.close()
    return counts


def _copy_still_there(entry: PlanEntry) -> bool:
    try:
        return Path(entry.copy_path).stat().st_size == entry.size
    except OSError:
        return False


def purge(config: ScanConfig, plan_path: Path, *, older_than_days: float = 14, yes: bool = False,
          only: str | None = None, runner: ServerRunner | None = None,
          now: datetime | None = None, reason: str | None = None) -> Counter:
    folder, entries = read_plan(plan_path)
    states = last_states(folder)
    cutoff = (now or datetime.now()) - timedelta(days=older_than_days)
    jobs, held = [], Counter()
    for entry, root in _jobs(entries, config, only, reason):
        state = states.get(entry.path, {})
        if not (state.get("status") == "moved" and state.get("op") == "quarantine"):
            continue
        if datetime.fromisoformat(state["at"]) > cutoff:
            held["in quarantine for less than the waiting period"] += 1
            continue
        if entry.status == "covered" and not _copy_still_there(entry):
            held["its copy is no longer there"] += 1
            continue
        jobs.append((entry, root, {"op": "purge", "path": quarantine_path(entry.path, root, folder.name),
                                   "stop": quarantine_root(root)}))
    size = sum(e.size for e, _, _ in jobs)
    for reason, n in held.items():
        print(f"Kept in quarantine ({reason}): {n:,} files")
    if not yes:
        print(f"{len(jobs):,} quarantined files ({gb(size)}) can be deleted for good. "
              "Nothing was deleted; add --yes to delete them.")
        return Counter(would_purge=len(jobs), **held)
    counts = _execute(config, folder, jobs, "remove", runner, lambda e, r: None)
    counts.update(held)
    return counts


# ---------------------------------------------------------------- CLI entry points


def _print_counts(title: str, counts: Counter) -> None:
    print(title)
    for status, n in counts.most_common():
        print(f"  {status:12} {n:>10,}")


def run_cleanup(config_path, plan: str, *, only: str | None = None, dry_run: bool = False,
                reason: str | None = None) -> None:
    config = load_config(config_path)
    counts = quarantine(config, Path(plan), only=only, dry_run=dry_run, reason=reason)
    if not dry_run:
        _print_counts("Quarantine results (moved = in quarantine; changed, missing and error = left alone):", counts)
        print(f"To put files back: filescan cleanup-restore --plan \"{plan}\"")


def run_cleanup_restore(config_path, plan: str, *, only: str | None = None, reason: str | None = None) -> None:
    config = load_config(config_path)
    _print_counts("Restore results (moved = back in place):", restore(config, Path(plan), only=only, reason=reason))
    print("If the drive was rescanned while the files were in quarantine, rescan it again so they reappear.")


def run_cleanup_purge(config_path, plan: str, *, older_than_days: float, yes: bool, only: str | None,
                      reason: str | None = None) -> None:
    config = load_config(config_path)
    counts = purge(config, Path(plan), older_than_days=older_than_days, yes=yes, only=only, reason=reason)
    if yes:
        _print_counts("Purge results (purged = deleted for good):", counts)
