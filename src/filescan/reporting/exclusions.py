"""Report the data that the configured exclusion rules leave out of the organised set.

Exclusions are applied at query time, so this report always reflects the rules
as they stand in config.yml. Review it before the rules are relied on, and again
before any disk holding excluded data is wiped.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from filescan.config import load_config
from filescan.models import ExclusionRule, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB


@dataclass(slots=True)
class ExcludedSubtree:
    rule: str
    reason: str
    path: Path
    disposition: str = "delete-candidate"
    file_count: int = 0
    total_bytes: int = 0


def _first_match(rules: tuple[ExclusionRule, ...], folder: Path) -> ExclusionRule | None:
    for rule in rules:
        if rule.matches(folder):
            return rule
    return None


def excluded_subtrees(db: SQLiteDB, config: ScanConfig) -> list[ExcludedSubtree]:
    """Return the top-most folder of each excluded subtree with its recursive totals.

    Folder rows store only their direct files, so totals are summed over every
    folder inside the subtree. A folder below an already-excluded folder is
    counted under that ancestor, never twice.
    """
    if not config.exclusions:
        return []
    total = db.conn.execute("SELECT COUNT(*) FROM folders WHERE is_missing = 0").fetchone()[0]
    rows = db.conn.execute("SELECT path, file_count, total_bytes FROM folders WHERE is_missing = 0")
    subtrees: dict[Path, ExcludedSubtree] = {}
    # Folders arrive in arbitrary order, so first find every matching folder,
    # then attribute each one to its top-most matching ancestor.
    matched: list[tuple[Path, ExclusionRule, int, int]] = []
    with progress_bar(desc="exclusion rules", total=total, unit="folder") as bar:
        for row in rows:
            folder = Path(row["path"])
            rule = _first_match(config.exclusions, folder)
            if rule is not None:
                matched.append((folder, rule, int(row["file_count"]), int(row["total_bytes"])))
            bar.update(1)
    matched_paths = {folder for folder, _, _, _ in matched}
    for folder, rule, file_count, total_bytes in matched:
        top = folder
        for ancestor in folder.parents:
            if ancestor in matched_paths:
                top = ancestor
        if top == folder:
            subtrees.setdefault(
                folder,
                ExcludedSubtree(rule=rule.name, reason=rule.reason, path=folder, disposition=rule.disposition),
            )
    for folder, _rule, file_count, total_bytes in matched:
        top = folder
        for ancestor in folder.parents:
            if ancestor in subtrees:
                top = ancestor
        subtree = subtrees[top]
        subtree.file_count += file_count
        subtree.total_bytes += total_bytes
    return sorted(subtrees.values(), key=lambda s: (-s.total_bytes, str(s.path)))


def write_exclusions_report(config: ScanConfig) -> Path:
    db = SQLiteDB(config.database_path)
    try:
        subtrees = excluded_subtrees(db, config)
    finally:
        db.close()
    out = config.filescan_folder / "exclusions_report.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    totals: dict[str, list[int]] = {}
    for s in subtrees:
        entry = totals.setdefault(s.rule, [0, 0, 0])
        entry[0] += 1
        entry[1] += s.file_count
        entry[2] += s.total_bytes
    with out.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rule", "disposition", "reason", "path", "files", "bytes", "GB"])
        for s in subtrees:
            writer.writerow(
                [s.rule, s.disposition, s.reason, str(s.path), s.file_count, s.total_bytes,
                 round(s.total_bytes / 1e9, 3)]
            )
    print(f"{'rule':<20} {'subtrees':>9} {'files':>10} {'GB':>9}")
    for rule, (count, files, size) in sorted(totals.items(), key=lambda kv: -kv[1][2]):
        print(f"{rule:<20} {count:>9,} {files:>10,} {size / 1e9:>9.1f}")
    return out


def run_exclusions_report(config_path: str | Path) -> Path:
    return write_exclusions_report(load_config(config_path))
