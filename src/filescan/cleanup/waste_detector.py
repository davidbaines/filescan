from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_UNSAFE_FILENAME_CHARS = re.compile(r'[\\/:*?"<>|]')


def _fmt_size(b: int) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.1f} MB"
    return f"{b / 1024:.1f} KB"

from filescan.config import load_config
from filescan.inventory.refresh import refresh_folder_subtrees
from filescan.models import FolderRecord
from filescan.progress import track
from filescan.storage import FileRepository, SQLiteDB


@dataclass(slots=True)
class WasteCandidate:
    path: Path
    category: str
    description: str
    requires_review: bool
    size_bytes: int
    file_count: int


# Patterns matched against the uppercased full path string.
_PATH_PATTERNS: list[tuple[str, str, str, bool]] = [
    (r"\\WINDOWS\\TEMP$", "windows_temp", "Windows system temp folder", False),
    (r"\\WINDOWS\\MINIDUMP$", "crash_dumps", "Windows crash mini-dumps", False),
    (r"\\WINDOWS\\SOFTWAREDISTRIBUTION\\DOWNLOAD$", "windows_update", "Windows Update download cache", False),
    (r"\\USERS\\[^\\]+\\APPDATA\\LOCAL\\TEMP$", "user_temp", "User temp folder", False),
    (r"\\GOOGLE\\CHROME\\USER DATA\\DEFAULT\\CACHE", "browser_cache", "Chrome browser cache", False),
    (r"\\MICROSOFT\\EDGE\\USER DATA\\DEFAULT\\CACHE", "browser_cache", "Edge browser cache", False),
    (r"\\MOZILLA\\FIREFOX\\PROFILES\\[^\\]+\\CACHE2$", "browser_cache", "Firefox browser cache", False),
    (r"\\MICROSOFT\\WINDOWS\\INETCACHE$", "browser_cache", "Internet Explorer cache", False),
    (r"\\MICROSOFT\\WINDOWS\\WER$", "app_cache", "Windows error reporting cache", False),
]

_COMPILED_PATH_PATTERNS = [
    (re.compile(pat), cat, desc, review) for pat, cat, desc, review in _PATH_PATTERNS
]

# Matched against the uppercased folder name only (any depth, any location).
_NAME_PATTERNS: dict[str, tuple[str, str, bool]] = {
    "NODE_MODULES": ("dev_cache", "Node.js package cache", False),
    "__PYCACHE__": ("dev_cache", "Python bytecode cache", False),
    ".PYTEST_CACHE": ("dev_cache", "pytest cache", False),
    ".TOX": ("dev_cache", "tox test environments", False),
    ".VENV": ("dev_cache", "Python virtual environment", False),
    "VENV": ("dev_cache", "Python virtual environment", False),
    ".GRADLE": ("dev_cache", "Gradle build cache", False),
    ".M2": ("dev_cache", "Maven dependency cache", False),
}


def _match_folder(folder: FolderRecord) -> tuple[str, str, bool] | None:
    path = folder.path
    name_upper = path.name.upper()
    path_upper = str(path).upper()

    # Drive-root-level patterns: Windows.old, $RECYCLE.BIN, etc.
    # Check via parent having exactly one part (e.g. Path('D:\\')) rather than relying on
    # the stored depth value, which may vary when roots overlap.
    if len(path.parent.parts) == 1:
        if name_upper == "WINDOWS.OLD":
            return ("old_windows", "Previous Windows installation", True)
        if name_upper in ("$WINDOWS.~BT", "$WINDOWS.~WS"):
            return ("old_windows", "Windows upgrade temporary files", True)
        if name_upper == "$RECYCLE.BIN":
            return ("recycle_bin", "Recycle bin contents", False)

    for regex, category, description, requires_review in _COMPILED_PATH_PATTERNS:
        if regex.search(path_upper):
            return (category, description, requires_review)

    if name_upper in _NAME_PATTERNS:
        cat, desc, review = _NAME_PATTERNS[name_upper]
        return (cat, desc, review)

    return None


def _recursive_stats(db: SQLiteDB, folder_path: Path) -> tuple[int, int]:
    path_str = str(folder_path)
    like_pattern = path_str.rstrip("\\") + "\\%"
    row = db.conn.execute(
        """
        SELECT COALESCE(SUM(f.size), 0) AS total_bytes, COUNT(f.id) AS file_count
        FROM files f
        JOIN folders fo ON fo.id = f.folder_id
        WHERE (fo.path = ? OR fo.path LIKE ?)
          AND f.is_missing = 0
          AND fo.is_missing = 0
        """,
        (path_str, like_pattern),
    ).fetchone()
    return int(row["total_bytes"]), int(row["file_count"])


def _collect_candidates(repo: FileRepository, db: SQLiteDB) -> list[WasteCandidate]:
    # iter_active_folders() yields folders ordered by depth ascending, so parents always
    # appear before children — the ancestor check below is therefore complete.
    matched_paths: set[Path] = set()
    candidates: list[WasteCandidate] = []
    folder_count = repo.count_active_folders()
    for folder in track(repo.iter_active_folders(), desc="waste scan", total=folder_count, unit="folder"):
        if any(ancestor in matched_paths for ancestor in folder.path.parents):
            continue
        match = _match_folder(folder)
        if match is None:
            continue
        category, description, requires_review = match
        matched_paths.add(folder.path)
        size_bytes, file_count = _recursive_stats(db, folder.path)
        candidates.append(
            WasteCandidate(
                path=folder.path,
                category=category,
                description=description,
                requires_review=requires_review,
                size_bytes=size_bytes,
                file_count=file_count,
            )
        )
    candidates.sort(key=lambda c: c.size_bytes, reverse=True)
    return candidates


def find_waste_candidates(config_path: Path) -> list[WasteCandidate]:
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)

    # Phase 1: identify candidates from DB
    initial = _collect_candidates(repo, db)

    if initial:
        # Phase 2: check those folders/files against disk, update DB
        print(f"  Refreshing {len(initial)} waste candidate folder(s) against disk...")
        refresh_folder_subtrees(db, [c.path for c in initial])

        # Phase 3: re-calculate stats for surviving candidates (no full re-scan needed)
        updated: list[WasteCandidate] = []
        for c in initial:
            row = db.conn.execute(
                "SELECT is_missing FROM folders WHERE path=?", (str(c.path),)
            ).fetchone()
            if row is None or row["is_missing"]:
                continue
            size_bytes, file_count = _recursive_stats(db, c.path)
            updated.append(
                WasteCandidate(
                    path=c.path,
                    category=c.category,
                    description=c.description,
                    requires_review=c.requires_review,
                    size_bytes=size_bytes,
                    file_count=file_count,
                )
            )
        candidates = sorted(updated, key=lambda c: c.size_bytes, reverse=True)
    else:
        candidates = initial

    if config.waste_file_size > 0:
        candidates = [c for c in candidates if c.size_bytes >= config.waste_file_size]

    db.close()
    return candidates


def print_waste_report(candidates: list[WasteCandidate]) -> None:
    if not candidates:
        print("No waste candidates found in the scanned database.")
        return

    review = [c for c in candidates if c.requires_review]
    safe = [c for c in candidates if not c.requires_review]

    def _row(c: WasteCandidate) -> None:
        path_str = str(c.path)
        if len(path_str) > 68:
            path_str = "..." + path_str[-65:]
        print(f"  {c.category:<16}  {path_str:<68}  {_fmt_size(c.size_bytes):>8}  {c.file_count:>10,} files")

    if review:
        print("\nREQUIRES REVIEW")
        for c in review:
            _row(c)

    if safe:
        print("\nSAFE TO CLEAR")
        for c in safe:
            _row(c)

    total = sum(c.size_bytes for c in candidates)
    review_total = sum(c.size_bytes for c in review)
    safe_total = sum(c.size_bytes for c in safe)
    print(f"\nTotal potential saving: {_fmt_size(total)}")
    print(f"  Requires review:  {_fmt_size(review_total)}")
    print(f"  Safe to clear:    {_fmt_size(safe_total)}")


def write_waste_shortcuts(candidates: list[WasteCandidate], output_dir: Path) -> Path:
    """Write a folder of .url shortcuts — one per candidate, sorted largest first.

    Double-clicking any shortcut opens that folder in Windows Explorer.
    Clears stale shortcuts from previous runs before writing.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    for old in output_dir.glob("*.url"):
        old.unlink(missing_ok=True)

    for rank, candidate in enumerate(candidates, start=1):
        # Build a readable path label from the last 3 components (e.g. "AppData › Local › Temp")
        parts = candidate.path.parts
        label_parts = [p.rstrip("\\") for p in parts[-3:]]
        path_label = " \u203a ".join(label_parts)  # › separator

        review_tag = "[REVIEW] " if candidate.requires_review else ""
        name = (
            f"{rank:02d}. {review_tag}"
            f"[{_fmt_size(candidate.size_bytes)}] "
            f"{candidate.category} \u2014 "  # em-dash
            f"{path_label}.url"
        )
        # Strip any chars that Windows forbids in filenames (: can appear in drive letter label)
        name = _UNSAFE_FILENAME_CHARS.sub("_", name)
        if len(name) > 200:
            name = name[:196] + ".url"

        (output_dir / name).write_text(
            f"[InternetShortcut]\nURL={candidate.path.as_uri()}\n",
            encoding="utf-8",
        )

    return output_dir
