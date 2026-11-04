"""Find unwanted data with the rules in unwanted_rules.yml, and report it by folder.

Areas already covered by an exclusion in config.yml are skipped, because the
exclusions report accounts for them. Each matched file is counted under one
rule, the first that matches. Folder-rule matches are rolled up to the matched
folder; file-rule matches are rolled up to the top folder under their root,
which keeps the report to a readable length. Every matched file is also
listed in a second CSV.

A version rule recognises copies that carry a version stamp in their name,
such as Windows File History's "report (2017_02_23 14_58_33 UTC).docx". It
matches every version except the newest of each file in the same folder.

The report changes nothing on disk. Its matches are also stored in the
unwanted_matches table (replaced on every run) for `filescan measure`.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from filescan.config import load_config
from filescan.exclusion_index import classify_folders
from filescan.models import ScanConfig
from filescan.progress import progress_bar
from filescan.storage import SQLiteDB

DEFAULT_RULES = Path(__file__).with_name("unwanted_rules.yml")
_DISPOSITIONS = ("delete-candidate", "review")


@dataclass(frozen=True)
class UnwantedRule:
    name: str
    match: str
    pattern: re.Pattern[str]
    category: str
    disposition: str
    reason: str
    within: re.Pattern[str] | None = None

    def folder_match_end(self, folder: str) -> int | None:
        """For a folder rule: the length of the matched folder prefix, or None."""
        m = self.pattern.search(folder.rstrip("\\") + "\\")
        return None if m is None else m.end() - 1

    def matches_folder(self, folder: str) -> bool:
        return self.match == "folder" and self.folder_match_end(folder) is not None

    def version_stamp(self, filename: str) -> tuple[str, str] | None:
        """For a version rule: (name without the stamp, stamp), or None."""
        if self.match != "version":
            return None
        m = self.pattern.search(filename)
        if m is None:
            return None
        return (filename[:m.start()] + filename[m.end():]).lower(), m.group(1)

    def matches_file(self, folder: str, filename: str) -> bool:
        if self.match != "file":
            return False
        if self.within is not None and not self.within.search(folder.rstrip("\\") + "\\"):
            return False
        return self.pattern.search(filename) is not None


def parse_rules(data: dict) -> list[UnwantedRule]:
    rules = []
    for item in (data or {}).get("rules") or []:
        name = item.get("name")
        match = item.get("match")
        pattern = item.get("pattern")
        disposition = item.get("disposition", "review")
        if not name or match not in ("file", "folder", "version") or not pattern:
            raise ValueError(f"Unwanted rule needs name, match (file, folder or version) and pattern: {item!r}")
        if disposition not in _DISPOSITIONS:
            raise ValueError(f"Unwanted rule {name!r}: disposition must be one of {_DISPOSITIONS}")
        if match == "folder" and not str(pattern).endswith("\\\\"):
            raise ValueError(f"Folder rule {name!r}: the pattern must end with \\\\ so it matches whole folders")
        if match == "version" and re.compile(str(pattern)).groups != 1:
            raise ValueError(f"Version rule {name!r}: the pattern needs exactly one group, the sortable stamp")
        within = item.get("within")
        rules.append(UnwantedRule(
            name=str(name), match=match, pattern=re.compile(str(pattern), re.IGNORECASE),
            category=str(item.get("category", name)), disposition=disposition,
            reason=str(item.get("reason", "")),
            within=re.compile(str(within), re.IGNORECASE) if within else None,
        ))
    return rules


def load_unwanted_rules(config: ScanConfig | None = None, path: Path | None = None) -> list[UnwantedRule]:
    path = path or getattr(config, "unwanted_rules_path", None) or DEFAULT_RULES
    with Path(path).open(encoding="utf-8") as handle:
        return parse_rules(yaml.safe_load(handle))


def _newest_versions(db: SQLiteDB, version_rules: list[UnwantedRule]) -> dict[tuple[str, int, str], str]:
    """{(rule, folder id, name without stamp): newest stamp} for every versioned file."""
    newest: dict[tuple[str, int, str], str] = {}
    if not version_rules:
        return newest
    for folder_id, filename in db.conn.execute("SELECT folder_id, filename FROM files WHERE is_missing = 0"):
        for rule in version_rules:
            found = rule.version_stamp(filename)
            if found is not None:
                key = (rule.name, int(folder_id), found[0])
                if found[1] > newest.get(key, ""):
                    newest[key] = found[1]
                break
    return newest


def _older_version_rule(version_rules: list[UnwantedRule], newest: dict[tuple[str, int, str], str],
                        folder_id: int, filename: str) -> UnwantedRule | None:
    for rule in version_rules:
        found = rule.version_stamp(filename)
        if found is not None:
            return rule if found[1] < newest[(rule.name, folder_id, found[0])] else None
    return None


def _top_folder(folder: str, roots: list[str]) -> str:
    low = folder.lower()
    for root in roots:
        r = root.rstrip("\\")
        if low == r.lower() or low.startswith(r.lower() + "\\"):
            rest = folder[len(r) + 1:]
            first = rest.split("\\", 1)[0]
            return r + ("\\" + first if first else "")
    return folder


@dataclass
class _Row:
    rule: UnwantedRule
    folder: str
    files: int = 0
    bytes: int = 0
    content: dict[str, int] = field(default_factory=dict)


@dataclass
class UnwantedResult:
    report_csv: Path
    files_csv: Path
    by_rule: dict[str, tuple[int, int, int]]  # name -> files, bytes, distinct bytes


def run_unwanted_report_for_config(config: ScanConfig, rules: list[UnwantedRule] | None = None) -> UnwantedResult:
    rules = rules if rules is not None else load_unwanted_rules(config)
    folder_rules = [r for r in rules if r.match == "folder"]
    file_rules = [r for r in rules if r.match == "file"]
    version_rules = [r for r in rules if r.match == "version"]
    roots = sorted((str(p) for p in config.roots), key=len, reverse=True)
    db = SQLiteDB(config.database_path)
    out_dir = config.filescan_folder
    out_dir.mkdir(parents=True, exist_ok=True)
    report_csv = out_dir / "unwanted_report.csv"
    files_csv = out_dir / "unwanted_files.csv"
    rows: dict[tuple[str, str], _Row] = {}
    rule_content: dict[str, dict[str, int]] = {r.name: {} for r in rules}
    rule_counts: dict[str, list[int]] = {r.name: [0, 0] for r in rules}
    matches: list[tuple[int, str, str]] = []
    try:
        excluded = classify_folders(db, config)
        newest = _newest_versions(db, version_rules)
        folder_cache: dict[int, tuple[UnwantedRule, str] | None] = {}
        total = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0]
        query = """
            SELECT f.id, f.folder_id, fo.path AS folder, f.filename, f.path, f.size, h.full_hash
            FROM files f
            JOIN folders fo ON fo.id = f.folder_id
            LEFT JOIN file_hashes h ON h.file_id = f.id
            WHERE f.is_missing = 0 AND fo.is_missing = 0
        """
        with files_csv.open("w", newline="", encoding="utf-8-sig") as fh, \
                progress_bar(desc="unwanted rules", total=total, unit="file") as bar:
            writer = csv.writer(fh)
            writer.writerow(["rule", "category", "disposition", "size", "path"])
            for r in db.conn.execute(query):
                bar.update(1)
                folder_id = int(r["folder_id"])
                if folder_id in excluded:
                    continue
                folder = r["folder"]
                if folder_id not in folder_cache:
                    hit = None
                    for rule in folder_rules:
                        end = rule.folder_match_end(folder)
                        if end is not None:
                            hit = (rule, (folder.rstrip("\\") + "\\")[:end])
                            break
                    folder_cache[folder_id] = hit
                hit = folder_cache[folder_id]
                if hit is None:
                    rule = _older_version_rule(version_rules, newest, folder_id, r["filename"])
                    if rule is None:
                        rule = next((fr for fr in file_rules if fr.matches_file(folder, r["filename"])), None)
                    if rule is None:
                        continue
                    hit = (rule, _top_folder(folder, roots))
                rule, rollup = hit
                size = int(r["size"])
                key = r["full_hash"] or "path:" + r["path"]
                row = rows.setdefault((rule.name, rollup.lower()), _Row(rule, rollup))
                row.files += 1
                row.bytes += size
                row.content[key] = size
                rule_content[rule.name][key] = size
                rule_counts[rule.name][0] += 1
                rule_counts[rule.name][1] += size
                writer.writerow([rule.name, rule.category, rule.disposition, size, r["path"]])
                matches.append((int(r["id"]), rule.name, rule.disposition))
        # One short transaction at the end, so other commands are not locked out meanwhile.
        db.conn.execute("DELETE FROM unwanted_matches")
        db.conn.executemany("INSERT INTO unwanted_matches (file_id, rule, disposition) VALUES (?, ?, ?)", matches)
        db.conn.commit()
    finally:
        db.close()

    with report_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rule", "category", "disposition", "reason", "folder", "files", "bytes", "distinct_bytes", "GB"])
        for row in sorted(rows.values(), key=lambda x: -x.bytes):
            writer.writerow([row.rule.name, row.rule.category, row.rule.disposition, row.rule.reason, row.folder,
                             row.files, row.bytes, sum(row.content.values()), round(row.bytes / 1e9, 3)])

    by_rule = {name: (c[0], c[1], sum(rule_content[name].values())) for name, c in rule_counts.items()}
    print(f"{'rule':<20} {'disposition':<17} {'files':>10} {'GB':>9} {'distinct GB':>12}")
    for rule in sorted(rules, key=lambda x: -by_rule[x.name][1]):
        files, size, distinct = by_rule[rule.name]
        print(f"{rule.name:<20} {rule.disposition:<17} {files:>10,} {size / 1e9:>9.1f} {distinct / 1e9:>12.1f}")
    print(f"  {report_csv}\n  {files_csv}")
    return UnwantedResult(report_csv, files_csv, by_rule)


def run_unwanted_report(config_path: str | Path) -> UnwantedResult:
    return run_unwanted_report_for_config(load_config(config_path))
