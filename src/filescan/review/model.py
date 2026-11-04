"""Review a deletion-candidate area folder by folder, deciding keep or trash.

A scope is one delete-candidate exclusion rule, such as the server's Trash.
Its files are grouped into review units: whole folder trees, split into their
children while a tree holds more unique data than one decision should cover.

A file is *unique* when no copy of its content survives in a kept location.
A copy counts only if it lies outside every delete-candidate exclusion, so a
copy in another area that may itself be deleted does not make a file safe.
Files without a full hash count as unique, which errs on the side of keeping.
Choosing to keep a unit makes its content kept, so identical files in other
units of the same scope stop counting as unique.

Deciding records a decision only. Nothing here deletes or moves a file.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import PureWindowsPath

from filescan.exclusion_index import classify_folders, first_rule, is_doomed
from filescan.models import ExclusionRule, ScanConfig
from filescan.storage import SQLiteDB
from filescan.storage.sql import path_prefix

SEP = "\\"
TREE, FILES = "tree", "files"
KEEP, TRASH = "keep", "trash"
MAX_SPLIT_CHILDREN = 300


@dataclass(slots=True)
class ReviewFile:
    path: str
    folder: str
    size: int
    full_hash: str | None
    unique: bool = True

    @property
    def content_key(self) -> str:
        # Unhashed files cannot be matched, so each one is its own content.
        return self.full_hash or "path:" + self.path


@dataclass
class Suggestion:
    decision: str
    reason: str


@dataclass
class Unit:
    path: str
    kind: str  # TREE: the folder and everything below; FILES: only the files directly in the folder
    root: str
    files: list[ReviewFile]
    has_children: bool = False
    suggestion: Suggestion | None = None
    decision: str | None = None
    unique_bytes: int = 0
    _unique_content: dict[str, int] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str]:
        return self.path, self.kind

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)

    @property
    def label_path(self) -> str:
        return short_path(self.path) + (f"{SEP}(files here)" if self.kind == FILES else "")

    def unique_files(self) -> list[ReviewFile]:
        return [f for f in self.files if f.unique]

    def largest_unique(self, n: int = 10) -> list[ReviewFile]:
        return sorted(self.unique_files(), key=lambda f: -f.size)[:n]

    def top_extensions(self, n: int = 6) -> list[tuple[str, int]]:
        counts: Counter[str] = Counter()
        for f in self.unique_files():
            counts[PureWindowsPath(f.path).suffix.lower() or "(none)"] += f.size
        return counts.most_common(n)


def short_path(path: str, width: int = 50) -> str:
    """The drive (C:) or server disk (disk3) and as much of the end of the path as fits.

    The detail pane always shows the full path.
    """
    if path.startswith(SEP * 2):
        parts = path[2:].split(SEP)
        head, rest = parts[1] if len(parts) > 1 else parts[0], parts[2:]
    else:
        parts = path.split(SEP)
        head, rest = parts[0], parts[1:]
    tail: list[str] = []
    for part in reversed(rest):
        if tail and len(SEP.join([part, *tail])) > width:
            break
        tail.insert(0, part)
    elided = len(tail) < len(rest)
    return head + SEP + ("\u2026" + SEP if elided else "") + SEP.join(tail)


# ---------------------------------------------------------------- loading


def scope_rule(config: ScanConfig, scope: str) -> ExclusionRule:
    for rule in config.exclusions:
        if rule.name == scope:
            if rule.disposition != "delete-candidate":
                raise ValueError(f"Exclusion {scope!r} is keep-in-place; only delete-candidate rules can be reviewed.")
            return rule
    names = [r.name for r in config.exclusions if r.disposition == "delete-candidate"]
    raise ValueError(f"No exclusion named {scope!r}. Reviewable scopes: {', '.join(names)}")


def scope_roots(db: SQLiteDB, config: ScanConfig, scope: str,
                folder_rules: dict[int, ExclusionRule] | None = None) -> list[str]:
    """The top-most folders the scope's rule matches, where no earlier rule claims them."""
    rule = scope_rule(config, scope)
    matched: set[str] = set()
    for row in db.conn.execute("SELECT id, path FROM folders WHERE is_missing = 0"):
        folder = row["path"]
        found = folder_rules.get(int(row["id"])) if folder_rules is not None else first_rule(config.exclusions, folder)
        if found is rule:
            matched.add(folder)
    roots = []
    for folder in sorted(matched, key=len):
        if not any(folder.lower().startswith(r.lower() + SEP) for r in roots):
            roots.append(folder)
    return sorted(roots)


def load_files(db: SQLiteDB, roots: Iterable[str]) -> dict[str, list[ReviewFile]]:
    """Every active file under each root, keyed by root."""
    result: dict[str, list[ReviewFile]] = {}
    for root in roots:
        exact, like = path_prefix(PureWindowsPath(root))
        rows = db.conn.execute(
            """
            SELECT f.path, fo.path AS folder, f.size, h.full_hash
            FROM folders fo
            JOIN files f ON f.folder_id = fo.id
            LEFT JOIN file_hashes h ON h.file_id = f.id
            WHERE (fo.path = ? OR fo.path LIKE ? ESCAPE '|') AND fo.is_missing = 0 AND f.is_missing = 0
            """,
            (exact, like),
        )
        result[root] = [ReviewFile(r["path"], r["folder"], int(r["size"]), r["full_hash"]) for r in rows]
    return result


def kept_hashes(db: SQLiteDB, config: ScanConfig, hashes: set[str],
                folder_rules: dict[int, ExclusionRule] | None = None) -> set[str]:
    """The subset of `hashes` with at least one copy outside every delete-candidate exclusion."""
    if not hashes:
        return set()
    if folder_rules is None:
        folder_rules = classify_folders(db, config)
    db.conn.execute("CREATE TEMP TABLE IF NOT EXISTS review_hashes (h TEXT PRIMARY KEY)")
    db.conn.execute("DELETE FROM review_hashes")
    db.conn.executemany("INSERT OR IGNORE INTO review_hashes VALUES (?)", ((h,) for h in hashes))
    kept: set[str] = set()
    rows = db.conn.execute(
        """
        SELECT h.full_hash, f.folder_id
        FROM review_hashes t
        JOIN file_hashes h ON h.full_hash = t.h
        JOIN files f ON f.id = h.file_id
        JOIN folders fo ON fo.id = f.folder_id
        WHERE f.is_missing = 0 AND fo.is_missing = 0
        """
    )
    for full_hash, folder_id in rows:
        if full_hash not in kept and not is_doomed(folder_rules.get(int(folder_id))):
            kept.add(full_hash)
    db.conn.execute("DELETE FROM review_hashes")
    return kept


def mark_unique(files: Iterable[ReviewFile], kept: set[str]) -> None:
    for f in files:
        f.unique = not (f.full_hash and f.full_hash in kept)


# ---------------------------------------------------------------- units


@dataclass
class _Node:
    path: str
    files: list[ReviewFile] = field(default_factory=list)
    children: dict[str, _Node] = field(default_factory=dict)
    rec_unique: int = 0

    def all_files(self) -> list[ReviewFile]:
        out: list[ReviewFile] = []
        stack = [self]
        while stack:
            node = stack.pop()
            out.extend(node.files)
            stack.extend(node.children.values())
        return out


def _parent(path: str) -> str:
    return path.rsplit(SEP, 1)[0] if SEP in path.rstrip(SEP) else path


def _build_tree(root: str, files: list[ReviewFile]) -> _Node:
    nodes: dict[str, _Node] = {root.lower(): _Node(root)}

    def ensure(folder: str) -> _Node:
        chain = []
        while folder.lower() not in nodes:
            chain.append(folder)
            parent = _parent(folder)
            if parent == folder:
                raise ValueError(f"{folder} is not under {root}")
            folder = parent
        node = nodes[folder.lower()]
        for path in reversed(chain):
            child = _Node(path)
            node.children[path.lower()] = child
            nodes[path.lower()] = child
            node = child
        return node

    for f in files:
        ensure(f.folder).files.append(f)
    for node in sorted(nodes.values(), key=lambda n: -n.path.count(SEP)):
        node.rec_unique += sum(f.size for f in node.files if f.unique)
        if node.path.lower() != root.lower():
            nodes[_parent(node.path).lower()].rec_unique += node.rec_unique
    return nodes[root.lower()]


Suggester = Callable[[str, str], Suggestion | None]


def build_units(
    root: str,
    files: list[ReviewFile],
    *,
    split_bytes: int,
    force_split: set[str] = frozenset(),
    no_split: set[str] = frozenset(),
    suggest: Suggester | None = None,
) -> list[Unit]:
    """Turn one scope root into non-overlapping review units.

    A tree is split into its children (plus a unit for the files directly in
    it) while it holds more than `split_bytes` of unique data, unless a rule
    already suggests a decision for the whole tree, it has too many children
    to review one by one, or it carries a decision itself. Paths in
    `force_split` are always split; paths are compared case-insensitively.
    """
    force = {p.lower() for p in force_split}
    keep_whole = {p.lower() for p in no_split}
    units: list[Unit] = []

    def suggestion_for(path: str, kind: str) -> Suggestion | None:
        return suggest(path, kind) if suggest else None

    def emit(node: _Node) -> None:
        low = node.path.lower()
        suggestion = suggestion_for(node.path, TREE)
        wants_split = low in force or (
            node.rec_unique > split_bytes
            and suggestion is None
            and len(node.children) <= MAX_SPLIT_CHILDREN
        )
        if node.children and wants_split and low not in keep_whole:
            for child in sorted(node.children.values(), key=lambda n: n.path.lower()):
                emit(child)
            if node.files:
                units.append(Unit(node.path, FILES, root, list(node.files), False, suggestion_for(node.path, FILES)))
        else:
            units.append(Unit(node.path, TREE, root, node.all_files(), bool(node.children), suggestion))

    emit(_build_tree(root, files))
    return units


def recompute_unique(units: list[Unit]) -> None:
    """Unique bytes per unit, counting each content once per unit.

    Content in a unit marked keep is kept, so it no longer counts as unique
    in any other unit.
    """
    kept_by_decision: set[str] = set()
    for unit in units:
        if unit.decision == KEEP:
            kept_by_decision.update(f.content_key for f in unit.files if f.unique)
    for unit in units:
        content: dict[str, int] = {}
        for f in unit.files:
            if f.unique:
                content[f.content_key] = f.size
        unit._unique_content = content
        unit.unique_bytes = sum(
            size for key, size in content.items() if unit.decision == KEEP or key not in kept_by_decision
        )


def distinct_unique_bytes(units: Iterable[Unit], decision: str | None = "any") -> int:
    """Unique bytes across units, each content counted once. Pass a decision to filter units."""
    content: dict[str, int] = {}
    for unit in units:
        if decision != "any" and unit.decision != decision:
            continue
        for f in unit.files:
            if f.unique:
                content[f.content_key] = f.size
    return sum(content.values())


def ancestors(path: str, root: str) -> list[str]:
    out = []
    while path.lower() != root.lower():
        parent = _parent(path)
        if parent == path:
            break
        out.append(parent)
        path = parent
    return out
