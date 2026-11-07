"""`filescan status`: where the work stands and what to run next.

It only reads: the database, the review CSVs, the cleanup plan folders, the
folder-deletion logs and layout.yml. It never runs a stage itself.

Reading a large actions.jsonl takes a while, so each plan's summary is cached
in <plan folder>/status.json and reused until actions.jsonl changes.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from filescan.cleanup.plan import gb
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK
from filescan.inventory.volumes import root_problem
from filescan.models import RootConfig, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB

PURGE_WAIT_DAYS = 14

# Final state of a file in a cleanup plan, from the last record logged for its path.
IN_QUARANTINE, RESTORED, PURGED, ERROR, LEFT_ALONE = range(5)
_STATE_NAMES = {IN_QUARANTINE: "in quarantine", RESTORED: "restored", PURGED: "purged",
                ERROR: "error", LEFT_ALONE: "left alone"}


def _state_of(record: dict) -> int:
    op, status = record.get("op"), record.get("status")
    if status == "error":
        return ERROR
    if op == "quarantine" and status == "moved":
        return IN_QUARANTINE
    if op == "restore" and status == "moved":
        return RESTORED
    if op == "purge" and status == "purged":
        return PURGED
    return LEFT_ALONE  # changed, missing or conflict: the file was not touched


def summarise_actions(actions: Path) -> dict:
    """Count each path's final state in a plan's actions.jsonl; cached in status.json beside it."""
    cache = actions.parent / "status.json"
    st = actions.stat()
    key = [st.st_size, st.st_mtime_ns]
    try:
        cached = json.loads(cache.read_text(encoding="utf-8"))
        if cached.get("key") == key:
            return cached
    except (OSError, ValueError):
        pass
    last: dict[int, int] = {}  # hash of the path -> final state; keeps memory small for millions of paths
    first_q = last_q = ""
    with actions.open("rb") as handle, progress_bar(desc=f"reading {actions.parent.name}", total=st.st_size,
                                                    unit="B") as bar:
        bar.unit_scale = True
        for line in handle:
            bar.update(len(line))
            try:
                record = json.loads(line)
            except ValueError:
                continue  # a line cut short by a crash
            state = _state_of(record)
            last[hash(record.get("path", ""))] = state
            if state == IN_QUARANTINE:
                at = record.get("at", "")
                first_q = min(first_q, at) if first_q else at
                last_q = max(last_q, at)
    counts = Counter(last.values())
    summary = {"key": key, "counts": {_STATE_NAMES[s]: n for s, n in counts.items()},
               "first_quarantined": first_q, "last_quarantined": last_q}
    try:
        cache.write_text(json.dumps(summary), encoding="utf-8")
    except OSError:
        pass
    return summary


# ---------------------------------------------------------------- gathering


@dataclass
class Status:
    todo: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)

    def section(self, title: str) -> None:
        self.lines += ["", title]

    def add(self, text: str) -> None:
        self.lines.append("  " + text)


def _when(text: str | None) -> str:
    return text[:16].replace("T", " ") if text else "never"


def _roots(status: Status, db: SQLiteDB, config: ScanConfig) -> None:
    status.section("Inventory")
    roots: list[RootConfig] = list(config.root_configs) or [RootConfig(path=r) for r in config.roots]
    stats = {row["root_path"].lower(): row for row in db.conn.execute("SELECT * FROM scan_stats")}
    cutoff = datetime.now() - config.scan_max_age
    width = max((len(str(r.path)) for r in roots), default=10)
    for rc in roots:
        row = stats.get(str(rc.path).lower())
        if rc.location == "pc":
            problem = root_problem(rc)
            drive = "drive OK" if problem is None else problem
        else:
            drive = "server"
        files = f"{row['indexed_files']:>10,} files" if row else " " * 16
        label = f" ({rc.name})" if rc.name else ""
        status.add(f"{str(rc.path):<{width}}  scanned {_when(row['last_scanned_at'] if row else None)}  {files}  "
                   f"{drive}{label}")
        if row is None:
            status.todo.append(f"{'scan' if rc.location == 'pc' else 'import-server'}: {rc.path} has never "
                               "been scanned.")
        elif datetime.fromisoformat(row["last_scanned_at"]) < cutoff and rc.role != "master":
            status.todo.append(f"{'scan' if rc.location == 'pc' else 'import-server'}: {rc.path} was last "
                               f"scanned more than {config.scan_max_age.days} days ago.")
    _hashes(status, db)


def _hashes(status: Status, db: SQLiteDB) -> None:
    """How many files still lack a full hash. Unreadable files stay in this count until they can be read."""
    row = db.conn.execute(
        f"""SELECT COUNT(*) AS n, COALESCE(SUM(f.size), 0) AS bytes
            FROM files f JOIN folders fo ON fo.id = f.folder_id
            LEFT JOIN file_hashes h ON h.file_id = f.id
            WHERE f.is_missing = 0 AND fo.is_missing = 0 AND (h.full_hash IS NULL OR h.full_hash = '')
              AND (f.attributes & {CLOUD_PLACEHOLDER_MASK}) = 0""").fetchone()
    if row["n"]:
        status.add(f"hashes: {row['n']:,} files ({row['bytes'] / 1e9:,.1f} GB) have no full hash")
        status.todo.append("hash-all: give the remaining files a full hash (it reads only what is missing; files "
                           "it cannot read are listed in its log).")
    else:
        status.add("hashes: every file has a full hash")


def _reviews(status: Status, config: ScanConfig) -> None:
    status.section("Reviews (from review_<scope>.csv, written when you close a review)")
    for rule in config.exclusions:
        if rule.disposition != "delete-candidate":
            continue
        path = config.filescan_folder / f"review_{rule.name}.csv"
        if not path.exists():
            status.add(f"{rule.name:22} not started")
            status.todo.append(f"review {rule.name}: not started.")
            continue
        decisions: Counter = Counter()
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                decisions[row["decision"] or "undecided"] += 1
        total = sum(decisions.values())
        when = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        status.add(f"{rule.name:22} {total:>6,} folders: {decisions['trash']:,} trash, {decisions['keep']:,} keep, "
                   f"{decisions['undecided']:,} undecided  (saved {when})")
        if decisions["undecided"]:
            n = decisions["undecided"]
            status.todo.append(f"review {rule.name}: {n:,} folder{'' if n == 1 else 's'} undecided.")


def _plans(status: Status, config: ScanConfig, now: datetime) -> None:
    status.section("Cleanup plans")
    folders = sorted(p for p in (config.filescan_folder / "cleanup").glob("*") if (p / "files.csv").exists())
    if not folders:
        status.add("none yet")
        return
    for folder in folders:
        try:
            meta = json.loads((folder / "plan.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            meta = {}
        status.add(f"{folder.name}: {meta.get('files', 0):,} files, {gb(meta.get('bytes', 0))} planned")
        actions = folder / "actions.jsonl"
        if not actions.exists():
            status.add("    nothing moved yet")
            status.todo.append(f"cleanup --plan \"{folder}\" --dry-run, then without --dry-run: the plan has not "
                               "been run.")
            continue
        summary = summarise_actions(actions)
        counts = summary["counts"]
        status.add("    " + ", ".join(f"{name} {counts.get(name, 0):,}" for name in _STATE_NAMES.values()))
        if counts.get("in quarantine") and summary["first_quarantined"]:
            first = datetime.fromisoformat(summary["first_quarantined"]) + timedelta(days=PURGE_WAIT_DAYS)
            last = datetime.fromisoformat(summary["last_quarantined"]) + timedelta(days=PURGE_WAIT_DAYS)
            status.add(f"    can be purged from {first:%Y-%m-%d} (all of it from {last:%Y-%m-%d})")
            if now >= first:
                status.todo.append(f"cleanup-purge --plan \"{folder}\": quarantined files have waited "
                                   f"{PURGE_WAIT_DAYS} days. Run it without --yes first to see the totals.")
        if counts.get("error"):
            status.todo.append(f"cleanup --plan \"{folder}\": {counts['error']:,} files could not be moved. Fix "
                               "their permissions, then run it again; it skips what is already done.")


def _deletions(status: Status, config: ScanConfig) -> None:
    logs = sorted((config.filescan_folder / "deletions").glob("*.jsonl"))
    if not logs:
        return
    folders = files = size = 0
    for log in logs:
        for line in log.read_text(encoding="utf-8").splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("kind") == "folder":
                folders += 1
                files += record.get("deleted_files", 0)
                size += record.get("deleted_bytes", 0)
    status.section("Folders deleted with delete-folder")
    status.add(f"{folders:,} folders, {files:,} files, {gb(size)} (logs in {config.filescan_folder / 'deletions'})")


def _layout(status: Status, config: ScanConfig) -> None:
    status.section("Organised layout (Phases 5 to 7)")
    layout = config.filescan_folder / "layout.yml"
    if not layout.exists():
        status.add(f"{layout} does not exist yet")
        status.todo.append("layout.yml: write the map from source folders to categories.")
        return
    # Count entries only: a line that is wholly a comment may mention the marker to explain it.
    open_items = sum(1 for line in layout.read_text(encoding="utf-8").splitlines()
                     if "# ?" in line and not line.lstrip().startswith("#"))
    status.add(f"{layout}: {open_items} entries marked '# ?' still need a decision")
    if open_items:
        status.todo.append(f"layout.yml: settle the {open_items} entries marked '# ?'.")
    builds = sorted((config.filescan_folder / "build").glob("*/plan.json"))
    if not builds:
        status.add("build-plan: no plan yet")
        status.todo.append("build-plan: decide where each kept file goes (copies nothing).")
    else:
        meta = json.loads(builds[-1].read_text(encoding="utf-8"))
        status.add(f"build-plan {meta['plan_id']}: {meta['items']:,} files, {meta['bytes'] / 1e9:,.1f} GB; "
                   f"Unsorted {meta['unsorted_bytes'] / 1e9:,.1f} GB; {meta['unhashed']:,} kept files without a hash")
        if layout.stat().st_mtime > builds[-1].stat().st_mtime:
            status.todo.append("build-plan: layout.yml has changed since the last plan; run it again.")
        status.todo.append(f"Review the build plan in {builds[-1].parent} (summary.csv, unsorted.csv, renamed.csv).")
    status.add("copy-master, verify-coverage and pc-plan are not built yet")


def gather(config: ScanConfig, *, config_path: Path | None = None, config_source: str = "",
           now: datetime | None = None) -> Status:
    now = now or datetime.now()
    status = Status()
    if config_path is not None:
        status.lines.append(f"Config: {config_path}" + (f"  (from {config_source})" if config_source else ""))
    status.lines.append(f"Database: {config.database_path}")
    db = SQLiteDB(config.database_path)
    try:
        _roots(status, db, config)
    finally:
        db.close()
    _reviews(status, config)
    _plans(status, config, now)
    _deletions(status, config)
    _layout(status, config)
    return status


def run_status(config: ScanConfig, *, config_path: Path | None = None, config_source: str = "") -> Status:
    status = gather(config, config_path=config_path, config_source=config_source)
    print("\n".join(status.lines))
    print("\nTo do, in order:")
    for i, item in enumerate(status.todo, 1):
        print(f"  {i}. {item}")
    if not status.todo:
        print("  Nothing outstanding.")
    return status
