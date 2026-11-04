"""Phase 4 checkpoint: how big is the organised set, and where does it fit?

Counts distinct content, so each set of identical files is counted once:
files with a full hash by that hash, files with only a quick hash by size and
quick hash, and anything else as its own content. Unhashed files mostly have a
size no other file shares, so they really are distinct.

The organised set is everything outside the exclusions, plus content kept in
a review (`filescan review`). It is measured three ways: as it stands, without
the unwanted-data matches marked delete-candidate, and without every
unwanted-data match. Run `filescan unwanted-report` first so its matches are
current. Nothing on disk changes.
"""

from __future__ import annotations

import csv
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from filescan.config import load_config
from filescan.exclusion_index import classify_folders
from filescan.models import ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK

PC, SERVER = 1, 2
NORMAL, REVIEW, DELETE = 0, 1, 2
_LEVEL = {"review": REVIEW, "delete-candidate": DELETE}


@dataclass
class Measure:
    distinct: dict[int, list[int]] = field(default_factory=dict)  # level cut-off -> [pc-only, server-only, both]
    from_reviews: int = 0
    placeholders: int = 0
    excluded_files: int = 0


def _kept_review_paths(db: SQLiteDB) -> list[tuple[str, str]]:
    rows = db.conn.execute("SELECT path, kind FROM review_decisions WHERE decision = 'keep'").fetchall()
    return [(r["path"].lower(), r["kind"]) for r in rows]


def _in_kept_review(folder: str, kept: list[tuple[str, str]]) -> bool:
    low = folder.lower()
    for path, kind in kept:
        if low == path or (kind == "tree" and low.startswith(path + "\\")):
            return True
    return False


def measure(db: SQLiteDB, config: ScanConfig) -> Measure:
    excluded = classify_folders(db, config)
    kept_reviews = _kept_review_paths(db)
    review_folders: dict[int, bool] = {}
    content: dict[object, list[int]] = {}  # key -> [size, location mask, best level]
    result = Measure()
    from_reviews: dict[object, int] = {}
    total = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0]
    query = """
        SELECT f.id, f.folder_id, fo.path AS folder, f.path, f.size, f.attributes,
               h.full_hash, h.quick_hash, u.disposition
        FROM files f
        JOIN folders fo ON fo.id = f.folder_id
        LEFT JOIN file_hashes h ON h.file_id = f.id
        LEFT JOIN unwanted_matches u ON u.file_id = f.id
        WHERE f.is_missing = 0 AND fo.is_missing = 0
    """
    with progress_bar(desc="measure", total=total, unit="file") as bar:
        for r in db.conn.execute(query):
            bar.update(1)
            folder_id = int(r["folder_id"])
            reviewed_keep = False
            if folder_id in excluded:
                if kept_reviews:
                    if folder_id not in review_folders:
                        review_folders[folder_id] = _in_kept_review(r["folder"], kept_reviews)
                    reviewed_keep = review_folders[folder_id]
                if not reviewed_keep:
                    result.excluded_files += 1
                    continue
            if int(r["attributes"]) & CLOUD_PLACEHOLDER_MASK:
                result.placeholders += 1
                continue
            size = int(r["size"])
            if r["full_hash"]:
                key: object = r["full_hash"]
            elif r["quick_hash"]:
                key = ("q", size, r["quick_hash"])
            else:
                key = int(r["id"])
            location = SERVER if str(r["path"]).startswith("\\\\") else PC
            level = _LEVEL.get(r["disposition"], NORMAL)
            entry = content.get(key)
            if entry is None:
                content[key] = [size, location, level]
            else:
                entry[1] |= location
                entry[2] = min(entry[2], level)
            if reviewed_keep:
                from_reviews[key] = size
    for cut in (DELETE, REVIEW, NORMAL):
        split = [0, 0, 0]
        for size, location, level in content.values():
            if level <= cut:
                split[{PC: 0, SERVER: 1, PC | SERVER: 2}[location]] += size
        result.distinct[cut] = split
    result.from_reviews = sum(from_reviews.values())
    return result


def _pc_drives(config: ScanConfig) -> list[tuple[str, int, int]]:
    drives = []
    for root in config.pc_roots:
        drive = Path(root).drive + "\\"
        if drive not in [d[0] for d in drives]:
            try:
                usage = shutil.disk_usage(drive)
                drives.append((drive, usage.total, usage.free))
            except OSError:
                pass
    return drives


def _server_disks(config: ScanConfig) -> list[tuple[str, int, int]]:
    disks = []
    if not config.server_ssh:
        return disks
    for root in config.root_configs:
        if root.location != "server" or not root.server_path:
            continue
        try:
            out = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", config.server_ssh,
                 f"df -B1 --output=size,avail '{root.server_path}' | tail -1"],
                capture_output=True, text=True, timeout=30, check=True,
            ).stdout.split()
            disks.append((f"{root.path.name or root.path} ({root.role})", int(out[0]), int(out[1])))
        except (OSError, subprocess.SubprocessError, ValueError, IndexError):
            disks.append((f"{root.path} (size unavailable)", 0, 0))
    return disks


def run_measure_for_config(config: ScanConfig) -> Path:
    db = SQLiteDB(config.database_path)
    try:
        has_matches = db.conn.execute("SELECT COUNT(*) FROM unwanted_matches").fetchone()[0] > 0
        result = measure(db, config)
    finally:
        db.close()
    labels = {DELETE: "organised set, as it stands",
              REVIEW: "without unwanted delete-candidates",
              NORMAL: "without any unwanted-data match"}
    out = config.filescan_folder / "measure.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    print(f"\n{'distinct content':<40} {'total':>9} {'PC only':>9} {'server only':>12} {'both':>9}")
    for cut in (DELETE, REVIEW, NORMAL):
        pc_only, server_only, both = result.distinct[cut]
        total = pc_only + server_only + both
        rows.append([labels[cut], total, pc_only, server_only, both])
        print(f"{labels[cut]:<40} {total / 1e9:>7.0f}GB {pc_only / 1e9:>7.0f}GB {server_only / 1e9:>10.0f}GB "
              f"{both / 1e9:>7.0f}GB")
    if not has_matches:
        print("  (no unwanted-data matches stored: run `filescan unwanted-report` first)")
    print(f"Included from reviews marked keep: {result.from_reviews / 1e9:,.1f} GB. "
          f"Cloud placeholders not counted: {result.placeholders:,} files.")
    print(f"\n{'disk':<40} {'size':>9} {'free':>9}")
    disks = [("PC " + d, t, f) for d, t, f in _pc_drives(config)] + \
            [("server " + d, t, f) for d, t, f in _server_disks(config)]
    for name, size, free in disks:
        print(f"{name:<40} {size / 1e9:>7.0f}GB {free / 1e9:>7.0f}GB")
    with out.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["measure", "bytes", "pc_only_bytes", "server_only_bytes", "both_bytes"])
        writer.writerows(rows)
        writer.writerow(["included from reviews marked keep", result.from_reviews, "", "", ""])
        for name, size, free in disks:
            writer.writerow([f"disk {name} size / free", size, free, "", ""])
    print(f"  {out}")
    return out


def run_measure(config_path: str | Path) -> Path:
    return run_measure_for_config(load_config(config_path))
