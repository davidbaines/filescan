"""Classify every folder in the database by the first exclusion rule it matches.

Several commands need to know, for millions of files, whether a file lies in
an excluded area. Rules match folder paths, so each folder is classified once
and files are looked up by folder id.
"""

from __future__ import annotations

from pathlib import Path

from filescan.models import ExclusionRule, ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB


def first_rule(rules: tuple[ExclusionRule, ...], folder: str) -> ExclusionRule | None:
    path = Path(folder)
    for rule in rules:
        if rule.matches(path):
            return rule
    return None


def classify_folders(db: SQLiteDB, config: ScanConfig) -> dict[int, ExclusionRule]:
    """Return {folder id: first matching exclusion rule} for every active excluded folder."""
    if not config.exclusions:
        return {}
    total = db.conn.execute("SELECT COUNT(*) FROM folders WHERE is_missing = 0").fetchone()[0]
    result: dict[int, ExclusionRule] = {}
    with progress_bar(desc="classify folders", total=total, unit="folder") as bar:
        for row in db.conn.execute("SELECT id, path FROM folders WHERE is_missing = 0"):
            bar.update(1)
            rule = first_rule(config.exclusions, row["path"])
            if rule is not None:
                result[int(row["id"])] = rule
    return result


def is_doomed(rule: ExclusionRule | None) -> bool:
    """True if files under this rule are candidates for deletion, so a copy there does not count as kept."""
    return rule is not None and rule.disposition == "delete-candidate"
