"""A review session: one scope's units, their suggestions and the stored decisions.

Every decision is written to the `review_decisions` table as it is made, so a
session can stop at any point and resume where it left off.
"""

from __future__ import annotations

import csv
from pathlib import Path, PureWindowsPath

from filescan.cleanup.unwanted import UnwantedRule, load_unwanted_rules
from filescan.exclusion_index import classify_folders
from filescan.models import ScanConfig
from filescan.review.model import (
    FILES,
    KEEP,
    TRASH,
    TREE,
    Suggestion,
    Unit,
    ancestors,
    build_units,
    distinct_unique_bytes,
    kept_hashes,
    load_files,
    mark_unique,
    recompute_unique,
    scope_roots,
)
from filescan.storage import SQLiteDB

DECISIONS = (KEEP, TRASH)


class ReviewSession:
    def __init__(self, db: SQLiteDB, config: ScanConfig, scope: str, *, split_gb: float = 5.0,
                 unwanted_rules: list[UnwantedRule] | None = None) -> None:
        self.db = db
        self.config = config
        self.scope = scope
        self.split_bytes = int(split_gb * 1e9)
        self.unwanted_rules = unwanted_rules if unwanted_rules is not None else load_unwanted_rules(config)
        self.roots: list[str] = []
        self.files_by_root: dict = {}
        self.units: list[Unit] = []
        self.user_splits: set[str] = set()

    # ------------------------------------------------------------ loading

    def load(self) -> None:
        print(f"Finding the folders in scope {self.scope!r}...")
        folder_rules = classify_folders(self.db, self.config)
        self.roots = scope_roots(self.db, self.config, self.scope, folder_rules)
        self.files_by_root = load_files(self.db, self.roots)
        all_files = [f for files in self.files_by_root.values() for f in files]
        print(f"{len(self.roots):,} folders, {len(all_files):,} files. Looking for copies in kept locations...")
        hashes = {f.full_hash for f in all_files if f.full_hash}
        kept = kept_hashes(self.db, self.config, hashes, folder_rules)
        mark_unique(all_files, kept)
        self.rebuild()

    def _suggest(self, path: str, kind: str) -> Suggestion | None:
        folder = PureWindowsPath(path)
        for rule in self.config.exclusions:
            if rule.name != self.scope and rule.disposition == "delete-candidate" and rule.matches(folder):
                return Suggestion(TRASH, f"{rule.name}: {rule.reason}")
        for rule in self.unwanted_rules:
            if rule.match == "folder" and rule.disposition == "delete-candidate" and rule.matches_folder(path):
                return Suggestion(TRASH, f"{rule.name}: {rule.reason}")
        return None

    def rebuild(self) -> None:
        stored = self.stored_decisions()
        force_split = set(self.user_splits)
        no_split: set[str] = set()
        for (path, kind) in stored:
            if kind == FILES:
                force_split.add(path)
            else:
                no_split.add(path)
            for root in self.roots:
                if path.lower() == root.lower() or path.lower().startswith(root.lower() + "\\"):
                    force_split.update(ancestors(path, root))
        units: list[Unit] = []
        for root in self.roots:
            units.extend(build_units(root, self.files_by_root[root], split_bytes=self.split_bytes,
                                     force_split=force_split, no_split=no_split, suggest=self._suggest))
        for unit in units:
            unit.decision = stored.get((unit.path.lower(), unit.kind))
            recompute_unique([unit])
            if unit.suggestion is None and unit.unique_bytes == 0:
                unit.suggestion = Suggestion(TRASH, "every file has a copy in a kept location")
        recompute_unique(units)
        units.sort(key=lambda u: (-u.unique_bytes, u.path.lower()))
        self.units = units

    # ------------------------------------------------------------ decisions

    def stored_decisions(self) -> dict[tuple[str, str], str]:
        rows = self.db.conn.execute(
            "SELECT path, kind, decision FROM review_decisions WHERE scope = ?", (self.scope,)
        )
        return {(r["path"].lower(), r["kind"]): r["decision"] for r in rows}

    def decide(self, unit: Unit, decision: str) -> None:
        if decision not in DECISIONS:
            raise ValueError(decision)
        self.db.conn.execute(
            """
            INSERT INTO review_decisions (scope, path, kind, decision, suggestion, unique_bytes, total_bytes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (scope, path, kind) DO UPDATE SET
                decision = excluded.decision, suggestion = excluded.suggestion,
                unique_bytes = excluded.unique_bytes, total_bytes = excluded.total_bytes,
                decided_at = CURRENT_TIMESTAMP
            """,
            (self.scope, unit.path, unit.kind, decision,
             unit.suggestion.decision if unit.suggestion else None, unit.unique_bytes, unit.total_bytes),
        )
        self.db.conn.commit()
        unit.decision = decision
        recompute_unique(self.units)

    def clear(self, unit: Unit) -> None:
        self.db.conn.execute(
            "DELETE FROM review_decisions WHERE scope = ? AND path = ? AND kind = ?",
            (self.scope, unit.path, unit.kind),
        )
        self.db.conn.commit()
        unit.decision = None
        recompute_unique(self.units)

    def split(self, unit: Unit) -> list[Unit]:
        """Replace a tree unit by its children. Returns the new units, or [] if it cannot be split."""
        if unit.kind != TREE or not unit.has_children or unit.decision is not None:
            return []
        self.user_splits.add(unit.path)
        before = {u.key for u in self.units}
        self.rebuild()
        return [u for u in self.units if u.key not in before]

    # ------------------------------------------------------------ summaries

    def totals(self) -> dict[str, int]:
        return {
            "units": len(self.units),
            "decided": sum(1 for u in self.units if u.decision),
            "unique": distinct_unique_bytes(self.units),
            "keep": distinct_unique_bytes(self.units, KEEP),
            "trash": distinct_unique_bytes(self.units, TRASH),
            "undecided": distinct_unique_bytes(self.units, None),
        }

    def export_csv(self, out_dir: Path | None = None) -> Path:
        out_dir = out_dir or self.config.filescan_folder
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"review_{self.scope}.csv"
        tmp = out.with_suffix(".csv.tmp")
        with tmp.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["decision", "suggestion", "reason", "unique_bytes", "total_bytes", "files", "kind", "path"])
            for u in self.units:
                writer.writerow([u.decision or "", u.suggestion.decision if u.suggestion else "",
                                 u.suggestion.reason if u.suggestion else "", u.unique_bytes, u.total_bytes,
                                 len(u.files), u.kind, u.path])
        tmp.replace(out)
        return out
