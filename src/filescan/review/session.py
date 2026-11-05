"""A review session: one scope's units, their suggestions and the stored decisions.

Every decision is written to the `review_decisions` table as it is made, so a
session can stop at any point and resume where it left off.

Another filescan command may hold the database's write lock for minutes (for
example `duplicates` writing its groups). A decision is therefore first
appended to a journal file, `review_<scope>.journal.jsonl` beside the reports,
and then written to the database with a short wait. If the database is busy the
decision stays in the journal and is written later: on the next decision, every
few seconds while the screen is open, on quit, or when the session next loads.
The journal is emptied once its decisions are in the database.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
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

# How long a decision waits for another command's write lock before it is left
# in the journal, so the screen never freezes.
_SHORT_WAIT_MS = 500


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
        self.journal_path = config.filescan_folder / f"review_{scope}.journal.jsonl"
        self.pending: list[dict] = self._read_journal()

    # ------------------------------------------------------------ loading

    def load(self) -> None:
        print(f"Finding the folders in scope {self.scope!r}...")
        folder_rules = classify_folders(self.db, self.config)
        self.roots = scope_roots(self.db, self.config, self.scope, folder_rules)
        self.save()  # decisions left in the journal last time
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
        stored = {(r["path"].lower(), r["kind"]): r["decision"] for r in rows}
        for entry in self.pending:  # decisions not yet in the database, oldest first
            key = (entry["path"].lower(), entry["kind"])
            if entry["decision"] is None:
                stored.pop(key, None)
            else:
                stored[key] = entry["decision"]
        return stored

    def decide(self, unit: Unit, decision: str, *, save: bool = True) -> None:
        """Record a decision. With save=False it is only journalled; call save() afterwards."""
        if decision not in DECISIONS:
            raise ValueError(decision)
        self._record(unit, decision, save=save)
        unit.decision = decision
        recompute_unique(self.units)

    def clear(self, unit: Unit) -> None:
        self._record(unit, None, save=True)
        unit.decision = None
        recompute_unique(self.units)

    # ------------------------------------------------------------ journal

    def _read_journal(self) -> list[dict]:
        try:
            lines = self.journal_path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return []
        entries = []
        for line in lines:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # a line cut short by a crash; every earlier line is intact
        return entries

    def _record(self, unit: Unit, decision: str | None, *, save: bool) -> None:
        entry = {"path": unit.path, "kind": unit.kind, "decision": decision,
                 "suggestion": unit.suggestion.decision if unit.suggestion else None,
                 "unique_bytes": unit.unique_bytes, "total_bytes": unit.total_bytes}
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.journal_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self.pending.append(entry)
        if save:
            self.save()

    def save(self, *, wait_ms: int = _SHORT_WAIT_MS) -> bool:
        """Write journalled decisions to the database. False if it is still locked."""
        if not self.pending:
            return True
        conn = self.db.conn
        conn.execute(f"PRAGMA busy_timeout = {int(wait_ms)}")
        try:
            for e in self.pending:
                if e["decision"] is None:
                    conn.execute("DELETE FROM review_decisions WHERE scope = ? AND path = ? AND kind = ?",
                                 (self.scope, e["path"], e["kind"]))
                else:
                    conn.execute(
                        """
                        INSERT INTO review_decisions
                            (scope, path, kind, decision, suggestion, unique_bytes, total_bytes)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (scope, path, kind) DO UPDATE SET
                            decision = excluded.decision, suggestion = excluded.suggestion,
                            unique_bytes = excluded.unique_bytes, total_bytes = excluded.total_bytes,
                            decided_at = CURRENT_TIMESTAMP
                        """,
                        (self.scope, e["path"], e["kind"], e["decision"], e["suggestion"],
                         e["unique_bytes"], e["total_bytes"]),
                    )
            conn.commit()
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if "locked" not in str(exc) and "busy" not in str(exc):
                raise
            return False
        finally:
            conn.execute("PRAGMA busy_timeout = 60000")
        self.pending.clear()
        self.journal_path.unlink(missing_ok=True)
        return True

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
