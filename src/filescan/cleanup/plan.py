"""Build a cleanup plan: every file to quarantine, for the user to review.

A plan lists files, not folders, as they were at the last scan, so a file added
to a trashed folder later is never touched. It draws on two sources:

- review decisions (`filescan review <scope>`): files under a folder marked
  trash. A "tree" unit covers everything below its path; a "files" unit (the
  files directly in a folder) covers only those files. The most specific
  decision wins, so anything under a folder marked keep stays out, and
  undecided folders stay out;
- unwanted-data rules with disposition delete-candidate (`unwanted_matches`).
  Rules marked review are never included.

Some files are never planned, whatever the decision: files under a pinned
folder, files in a keep-in-place exclusion, and cloud placeholders. Files that
come from a rule (not from a review) are also left alone inside Git
repositories, since build output there belongs to the repository.

Each file is marked "covered" when an identical copy (same full hash) exists
somewhere being kept: outside the plan, outside every delete-candidate
exclusion and not matched by an unwanted rule. Otherwise it is "unique". Two
planned copies of a file never cover each other.

The plan is written to <filescan folder>/cleanup/<plan id>/: files.csv (one row
per file, read by `filescan cleanup`), summary.csv (totals by reason and
folder, for review) and plan.json.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path, PureWindowsPath

from filescan.config import load_config
from filescan.exclusion_index import classify_folders, is_doomed
from filescan.models import RootConfig, ScanConfig
from filescan.progress import progress_bar, track
from filescan.review.model import FILES, TRASH, TREE
from filescan.storage import SQLiteDB
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK

# Folders that belong to the live volume, never planned whatever a review decided.
_VOLUME_FOLDERS = {"system volume information", "$recycle.bin"}

PLAN_FIELDS = ["location", "status", "reason", "unit", "size", "mtime", "full_hash", "copy_path", "path", "file_id"]


@dataclass
class PlanEntry:
    file_id: int
    path: str
    location: str  # "pc" or "server"
    size: int
    mtime: float
    full_hash: str
    reason: str  # "review:<scope>" or "rule:<name>"
    unit: str  # the reviewed folder, or the top folder for a rule
    status: str = "unique"  # or "covered"
    copy_path: str = ""


@dataclass
class Plan:
    plan_id: str
    entries: list[PlanEntry] = field(default_factory=list)
    held: Counter = field(default_factory=Counter)  # reason left out -> files


# ---------------------------------------------------------------- roots


def root_configs(config: ScanConfig) -> list[RootConfig]:
    return list(config.root_configs) or [RootConfig(path=root) for root in config.roots]


def root_for(path: str, roots: list[RootConfig]) -> RootConfig | None:
    """The most specific configured root containing path."""
    low = path.lower()
    best = None
    for rc in roots:
        r = norm(str(rc.path))
        if low.startswith(r + "\\") and (best is None or len(r) > len(norm(str(best.path)))):
            best = rc
    return best


def _under(path_low: str, prefixes: list[str]) -> bool:
    return any(path_low == p or path_low.startswith(p + "\\") for p in prefixes)


# ---------------------------------------------------------------- decisions


def norm(path: str) -> str:
    """Lower case without a trailing separator, so D:\\ becomes d: and paths compare by prefix."""
    return path.rstrip("\\").lower()


def deciding_key(folder: str, decisions: dict[tuple[str, str], object]) -> tuple[str, str] | None:
    """The key of the decision that applies to files directly in `folder`: the most specific wins.

    `decisions` is keyed by (norm(path), kind). A "files" decision applies only
    to the folder itself; a "tree" decision applies to it and everything below.
    """
    current = norm(folder)
    for key in ((current, FILES), (current, TREE)):
        if key in decisions:
            return key
    while "\\" in current:
        current = current.rpartition("\\")[0]
        if (current, TREE) in decisions:
            return (current, TREE)
    return None


def effective_decision(folder: str, decisions: dict[tuple[str, str], str]) -> str | None:
    key = deciding_key(folder, decisions)
    return None if key is None else decisions[key]


_FILE_COLUMNS = """f.id, f.path, f.size, f.mtime, f.attributes, f.folder_id, fo.path AS folder,
                   COALESCE(h.full_hash, '') AS full_hash"""
_FILE_JOINS = """FROM files f JOIN folders fo ON fo.id = f.folder_id
                 LEFT JOIN file_hashes h ON h.file_id = f.id"""


def _review_rows(db: SQLiteDB):
    """Yield (row, scope, decided path) for every file a trash decision covers.

    One pass over the folders works out which decision applies to each, then
    one query fetches the files of every trashed folder. (A path query per
    decision cannot use an index and would scan every file each time.)
    """
    rows = db.conn.execute("SELECT scope, path, kind, decision FROM review_decisions").fetchall()
    decisions = {(norm(r["path"]), r["kind"]): r for r in rows}
    if not any(r["decision"] == TRASH for r in rows):
        return
    trashed: dict[int, tuple[str, str]] = {}
    folders = db.conn.execute("SELECT id, path FROM folders WHERE is_missing = 0")
    for folder in track(folders, desc="folders checked against review decisions", unit="folder"):
        key = deciding_key(folder["path"], decisions)
        if key is not None and decisions[key]["decision"] == TRASH:
            trashed[int(folder["id"])] = (decisions[key]["scope"], decisions[key]["path"])
    db.conn.execute("CREATE TEMP TABLE IF NOT EXISTS plan_folders (id INTEGER PRIMARY KEY)")
    db.conn.execute("DELETE FROM plan_folders")
    db.conn.executemany("INSERT INTO plan_folders VALUES (?)", ((i,) for i in trashed))
    for row in db.conn.execute(
            f"""SELECT {_FILE_COLUMNS} FROM plan_folders t JOIN files f ON f.folder_id = t.id
                JOIN folders fo ON fo.id = f.folder_id LEFT JOIN file_hashes h ON h.file_id = f.id
                WHERE f.is_missing = 0""").fetchall():
        scope, decided = trashed[int(row["folder_id"])]
        yield row, scope, decided
    db.conn.execute("DELETE FROM plan_folders")


def _rule_rows(db: SQLiteDB):
    yield from db.conn.execute(
        f"""SELECT {_FILE_COLUMNS}, u.rule {_FILE_JOINS} JOIN unwanted_matches u ON u.file_id = f.id
            WHERE u.disposition = 'delete-candidate' AND f.is_missing = 0 AND fo.is_missing = 0"""
    )


def _git_repos(db: SQLiteDB) -> list[str]:
    rows = db.conn.execute(r"SELECT path FROM folders WHERE is_missing = 0 AND path LIKE '%\.git'")
    return sorted({norm(r["path"])[:-len("\\.git")] for r in rows if norm(r["path"]).endswith("\\.git")})


def _top_folder(folder: str, root: RootConfig | None) -> str:
    if root is None:
        return folder
    r = str(root.path).rstrip("\\")
    rest = folder[len(r) + 1:]
    first = rest.split("\\", 1)[0]
    return r + ("\\" + first if first else "")


# ---------------------------------------------------------------- coverage


def mark_coverage(db: SQLiteDB, config: ScanConfig, entries: list[PlanEntry],
                  folder_rules: dict | None = None) -> None:
    """Mark entries "covered" when a copy survives outside the plan and every doomed area."""
    hashes = {e.full_hash for e in entries if e.full_hash}
    if not hashes:
        return
    if folder_rules is None:
        folder_rules = classify_folders(db, config)
    conn = db.conn
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS plan_hashes (h TEXT PRIMARY KEY)")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS plan_ids (id INTEGER PRIMARY KEY)")
    conn.execute("DELETE FROM plan_hashes")
    conn.execute("DELETE FROM plan_ids")
    conn.executemany("INSERT OR IGNORE INTO plan_hashes VALUES (?)", ((h,) for h in hashes))
    conn.executemany("INSERT OR IGNORE INTO plan_ids VALUES (?)", ((e.file_id,) for e in entries))
    copies: dict[str, str] = {}
    for row in conn.execute(
        """SELECT h.full_hash, f.path, f.folder_id, f.attributes
            FROM plan_hashes t
            JOIN file_hashes h ON h.full_hash = t.h
            JOIN files f ON f.id = h.file_id
            JOIN folders fo ON fo.id = f.folder_id
            LEFT JOIN unwanted_matches u ON u.file_id = f.id
            LEFT JOIN plan_ids p ON p.id = f.id
            WHERE f.is_missing = 0 AND fo.is_missing = 0 AND u.file_id IS NULL AND p.id IS NULL"""
    ):
        if row["full_hash"] in copies or is_doomed(folder_rules.get(int(row["folder_id"]))):
            continue
        if int(row["attributes"]) & CLOUD_PLACEHOLDER_MASK:
            continue
        copies[row["full_hash"]] = row["path"]
    conn.execute("DELETE FROM plan_hashes")
    conn.execute("DELETE FROM plan_ids")
    for e in entries:
        copy = copies.get(e.full_hash) if e.full_hash else None
        e.status, e.copy_path = ("covered", copy) if copy else ("unique", "")


# ---------------------------------------------------------------- building


def build_plan(db: SQLiteDB, config: ScanConfig, *, only: str | None = None,
               include_rules: bool = True, plan_id: str | None = None) -> Plan:
    plan = Plan(plan_id or datetime.now().strftime("%Y%m%d-%H%M%S"))
    roots = root_configs(config)
    pinned = [norm(str(p)) for p in config.pinned]
    repos = _git_repos(db)
    print("Classifying folders against the exclusion rules...")
    folder_rules = classify_folders(db, config)
    seen: set[int] = set()
    keep_rules = [r for r in config.exclusions if r.disposition == "keep-in-place"]
    kept_folder: dict[int, bool] = {}

    def in_keep_area(folder_id: int, folder: str) -> bool:
        # Any keep-in-place rule protects a folder, even if a delete-candidate
        # rule comes first in the config (old-os-in-backups inside evelyn).
        if folder_id not in kept_folder:
            kept_folder[folder_id] = any(r.matches(PureWindowsPath(folder)) for r in keep_rules)
        return kept_folder[folder_id]

    def consider(row, reason: str, unit: str | None, *, from_rule: bool) -> None:
        file_id = int(row["id"])
        if file_id in seen:
            return
        seen.add(file_id)
        path = row["path"]
        low = norm(path)
        if in_keep_area(int(row["folder_id"]), row["folder"]):
            plan.held["keep-in-place exclusion"] += 1
            return
        if _under(low, pinned):
            plan.held["pinned folder"] += 1
            return
        if _VOLUME_FOLDERS & set(low.split("\\")):
            plan.held["Windows volume folder (System Volume Information, $RECYCLE.BIN)"] += 1
            return
        if int(row["attributes"]) & CLOUD_PLACEHOLDER_MASK:
            plan.held["cloud placeholder"] += 1
            return
        if from_rule and _under(low, repos):
            plan.held["inside a Git repository"] += 1
            return
        root = root_for(path, roots)
        if root is None:
            plan.held["outside every configured root"] += 1
            return
        if from_rule and root.location == "pc" and path[len(str(root.path).rstrip("\\")) + 1:].startswith("."):
            # A dot folder at the top of a PC root, such as C:\\Users\\David\\.claude, belongs to a live program.
            plan.held["live program folder (dot folder in the profile)"] += 1
            return
        if only and root.location != only:
            return
        plan.entries.append(PlanEntry(
            file_id=file_id, path=path, location=root.location, size=int(row["size"]),
            mtime=float(row["mtime"]), full_hash=row["full_hash"], reason=reason,
            unit=unit or _top_folder(row["folder"], root),
        ))

    with progress_bar(desc="files from review decisions", unit="file") as bar:
        for row, scope, decided in _review_rows(db):
            consider(row, f"review:{scope}", decided, from_rule=False)
            bar.update(1)
    if include_rules:
        with progress_bar(desc="files from delete-candidate rules", unit="file") as bar:
            for row in _rule_rows(db):
                consider(row, f"rule:{row['rule']}", None, from_rule=True)
                bar.update(1)
    print("Looking for copies kept elsewhere...")
    mark_coverage(db, config, plan.entries, folder_rules)
    return plan


# ---------------------------------------------------------------- writing


def write_plan(plan: Plan, out_root: Path) -> Path:
    out = out_root / plan.plan_id
    out.mkdir(parents=True, exist_ok=False)
    with (out / "files.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_FIELDS)
        writer.writeheader()
        for e in plan.entries:
            writer.writerow({k: v for k, v in asdict(e).items()})
    groups: dict[tuple[str, str, str], list[int]] = {}
    for e in plan.entries:
        g = groups.setdefault((e.reason, e.location, e.unit), [0, 0, 0])
        g[0] += 1
        g[1] += e.size
        g[2] += e.size if e.status == "unique" else 0
    with (out / "summary.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["reason", "location", "folder", "files", "bytes", "unique_bytes", "covered_bytes"])
        for (reason, location, unit), (n, b, u) in sorted(groups.items(), key=lambda kv: -kv[1][1]):
            writer.writerow([reason, location, unit, n, b, u, b - u])
    meta = {"plan_id": plan.plan_id, "created": datetime.now().isoformat(timespec="seconds"),
            "files": len(plan.entries), "bytes": sum(e.size for e in plan.entries),
            "held": dict(plan.held)}
    (out / "plan.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return out


def read_plan(plan_path: Path) -> tuple[Path, list[PlanEntry]]:
    """Read a plan from its folder or its files.csv. Returns (plan folder, entries)."""
    plan_path = Path(plan_path)
    folder = plan_path if plan_path.is_dir() else plan_path.parent
    with (folder / "files.csv").open(newline="", encoding="utf-8-sig") as handle:
        entries = [PlanEntry(file_id=int(r["file_id"]), path=r["path"], location=r["location"],
                             size=int(r["size"]), mtime=float(r["mtime"]), full_hash=r["full_hash"],
                             reason=r["reason"], unit=r["unit"], status=r["status"], copy_path=r["copy_path"])
                   for r in csv.DictReader(handle)]
    return folder, entries


def gb(n: int) -> str:
    return f"{n / 1e9:,.1f} GB"


def run_cleanup_plan(config_path, *, only: str | None = None, include_rules: bool = True) -> Path:
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    try:
        plan = build_plan(db, config, only=only, include_rules=include_rules)
    finally:
        db.close()
    out = write_plan(plan, config.filescan_folder / "cleanup")
    by: dict[tuple[str, str], list[int]] = {}
    for e in plan.entries:
        t = by.setdefault((e.reason, e.location), [0, 0, 0])
        t[0] += 1
        t[1] += e.size
        t[2] += e.size if e.status == "unique" else 0
    print(f"\n{'reason':40} {'where':6} {'files':>10} {'size':>12} {'unique':>12}")
    for (reason, location), (n, b, u) in sorted(by.items(), key=lambda kv: -kv[1][1]):
        print(f"{reason:40} {location:6} {n:>10,} {gb(b):>12} {gb(u):>12}")
    total = sum(e.size for e in plan.entries)
    print(f"{'total':47} {len(plan.entries):>10,} {gb(total):>12}")
    for reason, n in plan.held.items():
        print(f"Left out ({reason}): {n:,} files")
    print(f"\nPlan written to {out}\n  summary.csv: totals by folder, for review\n  files.csv: every file\n"
          f"Nothing has been moved. To quarantine these files: filescan cleanup --plan \"{out}\"")
    return out
