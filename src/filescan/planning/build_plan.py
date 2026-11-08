"""Phase 5: decide where every kept file goes in the organised layout.

`filescan build-plan` reads layout.yml and the database and writes a plan. It
copies nothing and writes nothing outside its plan folder and the database.

Which files are kept: every active file under a source root, except files in
an exclusion (unless a review kept their folder), files matched by a
delete-candidate unwanted-data rule, files under a pinned or stay-in-place
folder, cloud placeholders and Windows volume folders.

One copy of each content: files with the same size and full hash are one
content. The kept copy takes the destination of the layout entry that comes
first in layout.yml; within one entry, the shortest destination wins. Every
other location is listed in dropped.csv. Files without a full hash are each
treated as distinct content and listed in unhashed.csv: they are kept, but
verify-coverage needs a hash for each before anything is deleted.

Git repositories, and each subfolder of a keep_whole folder in layout.yml, are
kept whole and stay out of the deduplication in both directions: their files
are never dropped, and no other file is dropped because they hold the same
content. Two such units that land on the same destination are kept once if
their files are identical; otherwise the later one is renamed. Unwanted-data
matches are kept inside Git repositories (build output belongs to the
repository) but left out of keep_whole folders like anywhere else.

Name clashes (different content wanting the same destination, compared without
regard to case, as Windows does) are resolved in layout order: the first keeps
the name and later ones become ``name_from_<label>.ext``, where the label comes
from the source folder (``E-Work``, ``F-Data-D-Work``).

Each kept content has one source to copy from: a server copy if there is one,
so the copy stays on the server, otherwise the PC file. The manifest carries
the kept location's mtime, which every copier must apply.

Output, in <filescan folder>/build/<plan id>/: manifest.csv, dropped.csv,
renamed.csv, unhashed.csv, unsorted.csv, problems.csv, summary.csv,
server_copy.jsonl (fsagent copy), pc_copy.csv and plan.json. The manifest is
also stored in the database table build_manifest under the plan id.
"""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PureWindowsPath

from filescan.cleanup.plan import _VOLUME_FOLDERS, norm, root_configs, root_for
from filescan.config import load_config
from filescan.exclusion_index import classify_folders
from filescan.inventory.server_hash import unc_to_posix
from filescan.models import RootConfig, ScanConfig
from filescan.planning.layout import Layout, LayoutEntry, destination_dir, label, load_layout, origin
from filescan.progress import progress_bar
from filescan.reporting.measure import _in_kept_review, _kept_review_paths
from filescan.storage import SQLiteDB
from filescan.storage.sql import CLOUD_PLACEHOLDER_MASK

MAX_NAME_BYTES = 255  # ext4 on the server
PC_PATH_WARN = 240  # Windows paths near MAX_PATH (260) trouble older programs
UNMAPPED_ORDER = 1_000_000  # unmapped files lose to every layout entry


@dataclass(slots=True)
class Candidate:
    file_id: int
    path: str
    size: int
    mtime: float
    full_hash: str
    location: str  # pc or server
    order: int  # layout entry order, UNMAPPED_ORDER when unmapped
    dest: str  # wanted destination, relative, "/" separators
    label: str  # for renaming on a clash
    repo: str = ""  # outermost Git repository or keep_whole unit containing the file (lower case), or ""


@dataclass(slots=True)
class Item:
    """One kept content (or one repository file) and where it goes."""
    dest: str
    winner: Candidate
    source: Candidate  # where to copy from
    wanted: str = ""  # the destination before a rename, if renamed


@dataclass
class BuildPlan:
    plan_id: str
    items: list[Item] = field(default_factory=list)
    dropped: list[tuple[Candidate, str, str]] = field(default_factory=list)  # (file, reason, kept as)
    renamed: list[tuple[str, str, str]] = field(default_factory=list)  # (source path, wanted, given)
    problems: list[tuple[str, str, str]] = field(default_factory=list)  # (kind, dest, detail)
    left_out: Counter = field(default_factory=Counter)  # reason -> files
    left_out_bytes: Counter = field(default_factory=Counter)
    unused_entries: list[LayoutEntry] = field(default_factory=list)
    candidates: int = 0


# ---------------------------------------------------------------- choosing the kept files


def _git_repos(db: SQLiteDB) -> list[str]:
    """Outermost repository folders (lower case): a nested repository belongs to its parent.

    A folder counts as a repository only if it holds .git\\HEAD, or a .git file (a submodule or
    worktree link). A stray .git folder with nothing in it (E:\\GitHub has one) does not count.
    """
    rows = db.conn.execute(
        r"""SELECT path FROM files WHERE is_missing = 0
            AND (path LIKE '%\.git\HEAD' OR (filename = '.git' AND path LIKE '%\.git'))""")
    repos = set()
    for r in rows:
        low = norm(r["path"])
        if low.endswith("\\.git\\head"):
            repos.add(low[:-len("\\.git\\head")])
        elif low.endswith("\\.git"):
            repos.add(low[:-len("\\.git")])
    repos = sorted(repos)
    outer: list[str] = []
    for repo in repos:  # sorted, so a parent comes before its children
        if not any(repo.startswith(o + "\\") for o in outer):
            outer.append(repo)
    return outer


def _repo_of(folder_low: str, repos: set[str]) -> str:
    current, found = folder_low, ""
    while True:
        if current in repos:
            found = current
        parent = current.rpartition("\\")[0]
        if not parent or parent == current or len(parent) < 2:
            return found
        current = parent


@dataclass(slots=True)
class _FolderInfo:
    skip: str  # reason to leave the folder's files out, or ""
    order: int = 0
    dest_dir: str = ""
    label: str = ""
    repo: str = ""  # the unit kept whole: a Git repository or a keep_whole subfolder
    location: str = "pc"
    git: bool = False  # the unit is a Git repository


def _classify_folder(folder: str, folder_id: int, *, layout: Layout, roots: list[RootConfig],
                     excluded: dict, kept_reviews: list, pinned: list[str], repos: set[str],
                     keep_rules: list) -> _FolderInfo:
    low = norm(folder)
    root = root_for(folder + "\\x", roots)  # root_for wants a path below the root
    if root is None:
        return _FolderInfo("outside every configured root")
    if root.role != "source":
        return _FolderInfo(f"{root.role} root")
    # A keep-in-place exclusion (Evelyn's data, Backblaze-hosted data) always applies, even where another
    # rule matched first and a review kept the folder.
    keep_rule = next((r for r in keep_rules if r.matches(PureWindowsPath(folder))), None)
    if keep_rule is not None:
        return _FolderInfo(f"exclusion: {keep_rule.name}")
    if folder_id in excluded and not (kept_reviews and _in_kept_review(folder, kept_reviews)):
        return _FolderInfo(f"exclusion: {excluded[folder_id].name}")
    if any(low == p or low.startswith(p + "\\") for p in pinned):
        return _FolderInfo("pinned folder (Phase 7 mirror)")
    if layout.is_stay_in_place(folder):
        return _FolderInfo("stay_in_place (Phase 7 mirror)")
    if _VOLUME_FOLDERS & set(low.split("\\")):
        return _FolderInfo("Windows volume folder")
    entry, dest_dir = destination_dir(folder, layout)
    if entry is None:
        order, lab = UNMAPPED_ORDER, origin(folder)
    else:
        order, lab = entry.order, label(entry.source)
    git_repo, whole = _repo_of(low, repos), layout.whole_unit(folder)
    # The outer of the two is the unit, so a repository inside a keep_whole subfolder stays part of it.
    unit = min((u for u in (git_repo, whole) if u), key=len, default="")
    return _FolderInfo("", order, dest_dir, lab, unit, root.location, git=bool(unit) and unit == git_repo)


def collect_candidates(db: SQLiteDB, config: ScanConfig, layout: Layout, plan: BuildPlan) -> list[Candidate]:
    roots = root_configs(config)
    print("Classifying folders against the exclusion rules...")
    excluded = classify_folders(db, config)
    kept_reviews = _kept_review_paths(db)
    pinned = [norm(str(p)) for p in config.pinned]
    repos = set(_git_repos(db))
    keep_rules = [r for r in config.exclusions if r.disposition == "keep-in-place"]
    folders: dict[int, _FolderInfo] = {}
    used_entries: set[int] = set()
    out: list[Candidate] = []
    total = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0]
    query = """
        SELECT f.id, f.folder_id, fo.path AS folder, f.path, f.filename, f.size, f.mtime, f.attributes,
               COALESCE(h.full_hash, '') AS full_hash, u.disposition
        FROM files f
        JOIN folders fo ON fo.id = f.folder_id
        LEFT JOIN file_hashes h ON h.file_id = f.id
        LEFT JOIN unwanted_matches u ON u.file_id = f.id
        WHERE f.is_missing = 0 AND fo.is_missing = 0
    """
    with progress_bar(desc="files checked", total=total, unit="file") as bar:
        for r in db.conn.execute(query):
            bar.update(1)
            folder_id = int(r["folder_id"])
            info = folders.get(folder_id)
            if info is None:
                info = _classify_folder(r["folder"], folder_id, layout=layout, roots=roots, excluded=excluded,
                                        kept_reviews=kept_reviews, pinned=pinned, repos=repos,
                                        keep_rules=keep_rules)
                folders[folder_id] = info
            size = int(r["size"])
            reason = info.skip
            if not reason and int(r["attributes"]) & CLOUD_PLACEHOLDER_MASK:
                reason = "cloud placeholder"
            if not reason and r["disposition"] == "delete-candidate" and not info.git:
                reason = "unwanted-data rule (delete-candidate)"
            if reason:
                plan.left_out[reason] += 1
                plan.left_out_bytes[reason] += size
                continue
            if info.order != UNMAPPED_ORDER:
                used_entries.add(info.order)
            out.append(Candidate(
                file_id=int(r["id"]), path=r["path"], size=size, mtime=float(r["mtime"]),
                full_hash=r["full_hash"], location=info.location, order=info.order,
                dest=f"{info.dest_dir}/{r['filename']}", label=info.label, repo=info.repo,
            ))
    plan.unused_entries = [e for e in layout.entries if e.order not in used_entries]
    plan.candidates = len(out)
    return out


# ---------------------------------------------------------------- placing them


def _rank(c: Candidate) -> tuple:
    return (c.order, len(c.dest), c.dest.lower(), c.path.lower())


def _with_label(dest: str, lab: str) -> str:
    folder, _, name = dest.rpartition("/")
    stem, dot, ext = name.rpartition(".")
    if not dot or not stem:  # no extension, or a dot file such as .gitignore
        stem, ext = name, ""
    new = f"{stem}_from_{lab}" + (f".{ext}" if ext else "")
    return f"{folder}/{new}" if folder else new


class _Namespace:
    """Destinations already taken, compared without regard to case, with one spelling per folder."""

    def __init__(self) -> None:
        self.files: set[str] = set()
        self.dirs: dict[str, str] = {}  # lower case -> the spelling first used

    def file_free(self, dest: str) -> bool:
        low = dest.lower()
        if low in self.files or low in self.dirs:
            return False
        parts = low.split("/")
        return not any("/".join(parts[:i]) in self.files for i in range(1, len(parts)))

    def canonical(self, dest: str) -> str:
        """Spell each folder in dest as it was first spelt."""
        parts = dest.split("/")
        out = []
        for i in range(1, len(parts)):
            prefix = "/".join(parts[:i])
            out.append(self.dirs.get(prefix.lower(), prefix).rpartition("/")[2])
        out.append(parts[-1])
        return "/".join(out)

    def claim(self, dest: str) -> None:
        parts = dest.split("/")
        for i in range(1, len(parts)):
            prefix = "/".join(parts[:i])
            self.dirs.setdefault(prefix.lower(), prefix)
        self.files.add(dest.lower())


def _clear_ancestors(ns: _Namespace, dest: str, lab: str) -> str:
    """If a folder in dest is already taken by a file, rename that folder (with the label) in dest."""
    parts = dest.split("/")
    for i in range(1, len(parts)):
        prefix = "/".join(parts[:i])
        n = 1
        while prefix.lower() in ns.files:
            parts[i - 1] = f"{dest.split('/')[i - 1]}_from_{lab}" + (f"-{n}" if n > 1 else "")
            prefix = "/".join(parts[:i])
            n += 1
    return "/".join(parts)


def _free_name(ns: _Namespace, dest: str, lab: str) -> str:
    """dest if it is free, otherwise name_from_<label>.ext (then -2, -3...)."""
    candidate = _clear_ancestors(ns, ns.canonical(dest), lab)
    if ns.file_free(candidate):
        return candidate
    base, n = candidate, 1
    while not ns.file_free(candidate):
        candidate = _with_label(base, lab + (f"-{n}" if n > 1 else ""))
        n += 1
    return candidate


def place(candidates: list[Candidate], plan: BuildPlan) -> None:
    """Choose one copy of each content, keep repositories whole, and resolve name clashes."""
    groups: dict[object, list[Candidate]] = defaultdict(list)
    repo_files: dict[str, list[Candidate]] = defaultdict(list)
    for c in candidates:
        if c.repo:
            repo_files[c.repo].append(c)
        else:
            groups[(c.size, c.full_hash) if c.full_hash else ("id", c.file_id)].append(c)

    # A server copy of a content can serve as the copy source for a repository file too.
    server_copy: dict[tuple[int, str], Candidate] = {}
    for c in sorted(candidates, key=_rank):
        if c.full_hash and c.location == "server":
            server_copy.setdefault((c.size, c.full_hash), c)

    # Units, placed in layout order: a single kept file, or a whole repository.
    units: list[tuple[tuple, str, object]] = []
    for members in groups.values():
        members.sort(key=_rank)
        winner = members[0]
        units.append((_rank(winner), "file", members))
    repos_by_dest: dict[str, list[tuple[str, list[Candidate]]]] = defaultdict(list)
    for repo, files in repo_files.items():
        files.sort(key=_rank)
        repo_dest = _repo_dest(repo, files[0])
        repos_by_dest[repo_dest.lower()].append((repo_dest, files))
    for clashing in repos_by_dest.values():
        clashing.sort(key=lambda rf: _rank(rf[1][0]))
        first_dest, first_files = clashing[0]
        first_set = {(c.dest.lower(), c.size, c.full_hash) for c in first_files}
        units.append((_rank(first_files[0]), "repo", (first_dest, first_files)))
        for repo_dest, files in clashing[1:]:
            same = all(c.full_hash for c in files) and \
                {(c.dest.lower(), c.size, c.full_hash) for c in files} == first_set
            if same:
                for c in files:
                    plan.dropped.append((c, "identical folder kept whole", first_dest))
            else:
                units.append((_rank(files[0]), "repo", (repo_dest, files)))
    units.sort(key=lambda u: u[0])

    ns = _Namespace()
    with progress_bar(desc="files placed", total=len(units), unit="item") as bar:
        for _, kind, payload in units:
            bar.update(1)
            if kind == "file":
                members = payload
                winner = members[0]
                dest = _free_name(ns, winner.dest, winner.label)
                ns.claim(dest)
                source = next((m for m in members if m.location == "server"), winner)
                item = Item(dest=dest, winner=winner, source=source)
                if dest.lower() != winner.dest.lower():
                    item.wanted = winner.dest
                    plan.renamed.append((winner.path, winner.dest, dest))
                plan.items.append(item)
                for m in members[1:]:
                    plan.dropped.append((m, "same content kept elsewhere", dest))
            else:
                repo_dest, files = payload
                _place_repo(ns, repo_dest, files, plan, server_copy)


def _repo_dest(repo: str, any_file: Candidate) -> str:
    """The destination folder of a repository, worked out from one of its files."""
    rel = any_file.path[len(repo):].strip("\\").replace("\\", "/")
    return any_file.dest[: len(any_file.dest) - len(rel) - 1]


def _place_repo(ns: _Namespace, repo_dest: str, files: list[Candidate], plan: BuildPlan,
                server_copy: dict[tuple[int, str], Candidate]) -> None:
    lab = files[0].label
    start = _clear_ancestors(ns, ns.canonical(repo_dest + "/x"), lab).rpartition("/")[0]
    target, n = start, 1
    while True:
        dests = [target + c.dest[len(repo_dest):] for c in files]
        if ns.file_free(target) and all(ns.file_free(d) for d in dests):
            break
        target = f"{start}_from_{lab}" + (f"-{n}" if n > 1 else "")
        n += 1
    if target.lower() != repo_dest.lower():
        plan.renamed.append((files[0].path.rsplit("\\", 1)[0], repo_dest, target))
    for c, dest in zip(files, dests):
        ns.claim(dest)
        source = server_copy.get((c.size, c.full_hash), c) if c.full_hash else c
        plan.items.append(Item(dest=dest, winner=c, source=source,
                               wanted=c.dest if target.lower() != repo_dest.lower() else ""))


def check_limits(plan: BuildPlan, pc_root_len: int = 3) -> None:
    for item in plan.items:
        for part in item.dest.split("/"):
            if len(part.encode("utf-8")) > MAX_NAME_BYTES:
                plan.problems.append(("name over 255 bytes (the server cannot store it)", item.dest, part[:60]))
                break
        if pc_root_len + len(item.dest) > PC_PATH_WARN:
            plan.problems.append(("long path on the PC", item.dest, f"{pc_root_len + len(item.dest)} characters"))


def build(db: SQLiteDB, config: ScanConfig, layout: Layout, plan_id: str | None = None) -> BuildPlan:
    plan = BuildPlan(plan_id or datetime.now().strftime("%Y%m%d-%H%M%S"))
    candidates = collect_candidates(db, config, layout, plan)
    print(f"{len(candidates):,} files to place. Choosing one copy of each content...")
    place(candidates, plan)
    check_limits(plan)
    return plan


# ---------------------------------------------------------------- writing


def master_root(config: ScanConfig) -> RootConfig | None:
    return next((r for r in root_configs(config) if r.role == "master"), None)


def _store(db: SQLiteDB, plan: BuildPlan) -> None:
    db.conn.execute("""CREATE TABLE IF NOT EXISTS build_manifest (
        plan_id TEXT NOT NULL, dest TEXT NOT NULL, size INTEGER NOT NULL, full_hash TEXT NOT NULL,
        mtime REAL NOT NULL, kept_file_id INTEGER NOT NULL, source_file_id INTEGER NOT NULL,
        PRIMARY KEY (plan_id, dest))""")
    db.conn.execute("DELETE FROM build_manifest WHERE plan_id = ?", (plan.plan_id,))
    db.conn.executemany(
        "INSERT INTO build_manifest VALUES (?, ?, ?, ?, ?, ?, ?)",
        ((plan.plan_id, i.dest, i.winner.size, i.winner.full_hash, i.winner.mtime, i.winner.file_id,
          i.source.file_id) for i in plan.items))
    db.conn.commit()


def write_build_plan(plan: BuildPlan, config: ScanConfig, out_root: Path) -> Path:
    out = out_root / plan.plan_id
    out.mkdir(parents=True, exist_ok=False)
    master = master_root(config)
    roots = root_configs(config)

    def csv_out(name: str, header: list[str], rows) -> None:
        with (out / name).open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    csv_out("manifest.csv", ["dest", "size", "full_hash", "mtime", "copy_from", "copy_by", "kept_path", "renamed_from"],
            ((i.dest, i.winner.size, i.winner.full_hash, i.winner.mtime, i.source.path,
              "server" if i.source.location == "server" else "pc", i.winner.path, i.wanted) for i in plan.items))
    csv_out("dropped.csv", ["path", "size", "full_hash", "reason", "kept_as"],
            ((c.path, c.size, c.full_hash, reason, kept) for c, reason, kept in plan.dropped))
    csv_out("renamed.csv", ["source", "wanted", "given"], plan.renamed)
    csv_out("problems.csv", ["problem", "dest", "detail"], plan.problems)
    csv_out("unhashed.csv", ["location", "path", "size", "dest"],
            ((i.winner.location, i.winner.path, i.winner.size, i.dest) for i in plan.items if not i.winner.full_hash))

    unsorted: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for i in plan.items:
        if i.winner.order == UNMAPPED_ORDER:
            parts = i.winner.path.split("\\")
            top = "\\".join(parts[:5]) if i.winner.path.startswith("\\\\") else "\\".join(parts[:2])
            unsorted[top][0] += 1
            unsorted[top][1] += i.winner.size
    csv_out("unsorted.csv", ["source_top_folder", "files", "bytes"],
            sorted(((k, n, b) for k, (n, b) in unsorted.items()), key=lambda r: -r[2]))

    categories: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
    for i in plan.items:
        top = "/".join(i.dest.split("/")[:2]) if i.dest.startswith("Unsorted/") else i.dest.split("/")[0]
        t = categories[top]
        t[0] += 1
        t[1] += i.winner.size
        t[2 if i.source.location == "server" else 3] += i.winner.size
    csv_out("summary.csv", ["category", "files", "bytes", "copied_on_server_bytes", "copied_from_pc_bytes"],
            sorted(((k, *v) for k, v in categories.items()), key=lambda r: -r[2]))

    with (out / "server_copy.jsonl").open("w", encoding="utf-8") as handle:
        for i in plan.items:
            if i.source.location != "server" or master is None or not master.server_path:
                continue
            src_root = root_for(i.source.path, roots)
            src = unc_to_posix(Path(i.source.path), src_root) if src_root else None
            if src is None:
                continue
            handle.write(json.dumps({"src": src, "dst": f"{master.server_path}/{i.dest}",
                                     "xxh128": i.winner.full_hash or None, "mtime": i.winner.mtime}) + "\n")
    master_unc = str(master.path).rstrip("\\") if master else ""
    csv_out("pc_copy.csv", ["src", "dst", "size", "mtime", "full_hash"],
            ((i.source.path, master_unc + "\\" + i.dest.replace("/", "\\"), i.winner.size, i.winner.mtime,
              i.winner.full_hash) for i in plan.items if i.source.location == "pc"))

    total = sum(i.winner.size for i in plan.items)
    meta = {
        "plan_id": plan.plan_id, "created": datetime.now().isoformat(timespec="seconds"),
        "files_considered": plan.candidates, "items": len(plan.items), "bytes": total,
        "copied_on_server_bytes": sum(i.winner.size for i in plan.items if i.source.location == "server"),
        "copied_from_pc_bytes": sum(i.winner.size for i in plan.items if i.source.location == "pc"),
        "dropped": len(plan.dropped), "renamed": len(plan.renamed), "problems": len(plan.problems),
        "unhashed": sum(1 for i in plan.items if not i.winner.full_hash),
        "unhashed_bytes": sum(i.winner.size for i in plan.items if not i.winner.full_hash),
        "unsorted_bytes": sum(b for _, b in unsorted.values()),
        "left_out": dict(plan.left_out), "left_out_bytes": dict(plan.left_out_bytes),
        "unused_layout_entries": [f"line {e.line}: {e.source}" for e in plan.unused_entries],
    }
    (out / "plan.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return out


def gb(n: int) -> str:
    return f"{n / 1e9:,.1f} GB"


KEEP_PLANS = 2  # each plan folder is over 1 GB on a full inventory


def prune_old_plans(db: SQLiteDB, build_root: Path, keep: int = KEEP_PLANS) -> list[str]:
    """Remove all but the newest `keep` build plans: their folders under build_root and their manifest rows.

    Only folders that build-plan itself wrote (named by plan id, holding plan.json) are removed.
    Returns the plan ids removed.
    """
    plans = sorted(d for d in build_root.glob("*") if d.is_dir() and (d / "plan.json").exists())
    removed = []
    for folder in plans[:-keep] if keep else plans:
        for child in folder.iterdir():
            child.unlink()
        folder.rmdir()
        removed.append(folder.name)
    if removed:
        try:
            db.conn.executemany("DELETE FROM build_manifest WHERE plan_id = ?", [(r,) for r in removed])
            db.conn.commit()
        except Exception:  # no build_manifest table yet
            pass
    return removed


def run_build_plan(config_path) -> Path:
    from filescan.reporting.report import assemble_workbook

    config = load_config(config_path)
    layout = load_layout(config.filescan_folder / "layout.yml")
    db = SQLiteDB(config.database_path)
    try:
        plan = build(db, config, layout)
        _store(db, plan)
        out = write_build_plan(plan, config, config.filescan_folder / "build")
        removed = prune_old_plans(db, config.filescan_folder / "build")
    finally:
        db.close()
    meta = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    print(f"\nOrganised set: {meta['items']:,} files, {gb(meta['bytes'])}")
    print(f"  copied within the server: {gb(meta['copied_on_server_bytes'])}; "
          f"copied from the PC: {gb(meta['copied_from_pc_bytes'])}")
    print(f"  Unsorted (not in layout.yml): {gb(meta['unsorted_bytes'])}; see unsorted.csv")
    print(f"  {meta['dropped']:,} copies dropped (same content kept elsewhere); "
          f"{meta['renamed']:,} renamed to avoid a clash")
    print(f"  {meta['unhashed']:,} kept files have no full hash ({gb(meta['unhashed_bytes'])}); see unhashed.csv")
    if meta["problems"]:
        print(f"  {meta['problems']:,} names or paths too long; see problems.csv")
    for reason, n in sorted(plan.left_out.items(), key=lambda kv: -plan.left_out_bytes[kv[0]]):
        print(f"Left out ({reason}): {n:,} files, {gb(plan.left_out_bytes[reason])}")
    for entry in plan.unused_entries:
        print(f"layout.yml line {entry.line}: {entry.source} matched no files (a typo, or all of it is excluded)")
    if removed:
        print(f"Older plans removed, keeping the newest {KEEP_PLANS}: {', '.join(removed)}")
    workbook = assemble_workbook(config).workbook
    print(f"\nPlan written to {out}, and its summary, unsorted folders, renamed files and long paths to "
          f"{workbook}. Nothing has been copied.")
    return out
