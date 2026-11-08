"""Read and check layout.yml, the map from source folders to the organised layout.

layout.yml has three parts:

- ``root_categories``: the allowed top-level folders of the organised set;
- ``map``: source folder -> destination path, relative to the organised root.
  The order of the entries matters: when the same content is in several mapped
  folders, the entry that comes first keeps it (see build_plan.py);
- ``stay_in_place``: folders that are not reorganised. They reach the server
  only through the Phase 7 mirror;
- ``keep_whole``: folders whose subfolders must stay complete, such as
  downloaded software. Each subfolder is kept whole, as a Git repository is:
  none of its files is dropped as a duplicate of a file elsewhere, and no other
  file is dropped because of it. Files directly in the folder are kept once,
  like any other file.

Anything not mapped goes to ``Unsorted/<drive or share>/<path>``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath

import yaml

UNSORTED = "Unsorted"


class LayoutError(ValueError):
    """layout.yml cannot be used as it stands. The message lists every problem with its line number."""


@dataclass(frozen=True, slots=True)
class LayoutEntry:
    order: int  # position in the map; lower wins
    source: str  # as written, without a trailing separator
    dest: str  # relative, with "/" separators
    line: int  # 1-based line in layout.yml

    @property
    def key(self) -> str:
        return self.source.lower()


@dataclass(slots=True)
class Layout:
    path: Path
    categories: tuple[str, ...]
    entries: list[LayoutEntry]
    stay_in_place: list[str] = field(default_factory=list)
    keep_whole: list[str] = field(default_factory=list)
    _by_source: dict[str, LayoutEntry] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._by_source = {e.key: e for e in self.entries}

    def entry_for(self, folder: str) -> LayoutEntry | None:
        """The most specific entry whose source contains folder (or is folder)."""
        current = folder.rstrip("\\").lower()
        while True:
            entry = self._by_source.get(current)
            if entry is not None:
                return entry
            if "\\" not in current.rstrip("\\"):
                return None
            parent = current.rpartition("\\")[0]
            if parent == current or not parent or parent.endswith("\\\\"):
                return None
            current = parent

    def is_stay_in_place(self, folder: str) -> bool:
        low = folder.rstrip("\\").lower()
        return any(low == s or low.startswith(s + "\\") for s in self._stay_keys())

    def whole_unit(self, folder: str) -> str:
        """The keep_whole subfolder containing folder (lower case), or "" if none does."""
        low = folder.rstrip("\\").lower()
        for k in self.keep_whole:
            key = k.rstrip("\\").lower()
            if low.startswith(key + "\\"):
                return key + "\\" + low[len(key) + 1:].split("\\", 1)[0]
        return ""

    def _stay_keys(self) -> list[str]:
        return [s.rstrip("\\").lower() for s in self.stay_in_place]


def origin(path: str) -> str:
    """The drive or share a path is on, as a folder name: E:\\x -> E, \\\\host\\disk3\\x -> disk3."""
    drive = PureWindowsPath(path).drive
    if drive.startswith("\\\\"):
        return drive.rstrip("\\").rsplit("\\", 1)[-1]
    return drive.rstrip(":")


def label(source: str) -> str:
    """A short name for a source folder, used to tell clashing copies apart.

    E:\\Work -> E-Work; F:\\Data\\D\\Work -> F-Data-D-Work;
    \\\\192.168.1.133\\disk3\\david\\Documents -> disk3-david-Documents.
    """
    p = PureWindowsPath(source)
    parts = [origin(source)] + [part for part in p.parts[1:]]
    return "-".join(part.strip("\\/") for part in parts if part.strip("\\/"))


def destination_dir(folder: str, layout: Layout) -> tuple[LayoutEntry | None, str]:
    """Where the files directly in folder go: (entry or None, relative destination folder)."""
    entry = layout.entry_for(folder)
    if entry is not None:
        rest = folder.rstrip("\\")[len(entry.source):].strip("\\")
        return entry, entry.dest + ("/" + rest.replace("\\", "/") if rest else "")
    p = PureWindowsPath(folder)
    rest = "/".join(part for part in p.parts[1:])
    return None, f"{UNSORTED}/{origin(folder)}" + (f"/{rest}" if rest else "")


def _scalar_pairs(node: yaml.Node) -> list[tuple[yaml.Node, yaml.Node]]:
    return list(node.value) if isinstance(node, yaml.MappingNode) else []


def load_layout(path: Path) -> Layout:
    """Read layout.yml and check it. Raises LayoutError listing every problem with its line number."""
    text = Path(path).read_text(encoding="utf-8")
    try:
        root = yaml.compose(text)
    except yaml.YAMLError as exc:
        raise LayoutError(f"{path}: not valid YAML: {exc}") from exc
    problems: list[str] = []
    sections = {k.value: (k, v) for k, v in _scalar_pairs(root)} if root is not None else {}

    categories: tuple[str, ...] = ()
    if "root_categories" in sections:
        node = sections["root_categories"][1]
        if isinstance(node, yaml.SequenceNode):
            categories = tuple(str(item.value) for item in node.value)
    if not categories:
        problems.append("root_categories is missing or empty.")

    entries: list[LayoutEntry] = []
    seen: dict[str, int] = {}
    map_node = sections.get("map", (None, None))[1]
    if map_node is None:
        problems.append("map is missing.")
    for key, value in _scalar_pairs(map_node):
        line = key.start_mark.line + 1
        source = str(key.value).strip().rstrip("\\")
        dest = str(value.value or "").strip().replace("\\", "/").strip("/")
        if not source:
            problems.append(f"line {line}: an entry has no source folder.")
            continue
        if source.lower() in seen:
            problems.append(f"line {line}: {source} is mapped twice (also on line {seen[source.lower()]}); "
                            "only one can apply.")
            continue
        seen[source.lower()] = line
        if not PureWindowsPath(source).drive:
            problems.append(f"line {line}: {source} is not a full path (it needs a drive letter or a share).")
        if not dest:
            problems.append(f"line {line}: {source} has no destination.")
            continue
        parts = dest.split("/")
        if parts[0] not in categories:
            problems.append(f"line {line}: destination {dest} starts with {parts[0]!r}, which is not in "
                            f"root_categories ({', '.join(categories)}).")
        if any(part in ("", ".", "..") for part in parts):
            problems.append(f"line {line}: destination {dest} has an empty, '.' or '..' part.")
        entries.append(LayoutEntry(order=len(entries), source=source, dest=dest, line=line))

    stay: list[str] = []
    stay_node = sections.get("stay_in_place", (None, None))[1]
    if isinstance(stay_node, yaml.SequenceNode):
        stay = [str(item.value).strip().rstrip("\\") for item in stay_node.value]
    for entry in entries:
        for s in stay:
            low, s_low = entry.key, s.lower()
            if low == s_low or low.startswith(s_low + "\\"):
                problems.append(f"line {entry.line}: {entry.source} is inside stay_in_place folder {s}, "
                                "so it would never be moved; remove one of the two.")
    if problems:
        raise LayoutError(f"{path}:\n  " + "\n  ".join(problems))
    whole: list[str] = []
    whole_node = sections.get("keep_whole", (None, None))[1]
    if isinstance(whole_node, yaml.SequenceNode):
        for item in whole_node.value:
            folder = str(item.value).strip().rstrip("\\")
            if not PureWindowsPath(folder).drive:
                problems.append(f"line {item.start_mark.line + 1}: keep_whole entry {folder} is not a full path.")
            whole.append(folder)
    if problems:
        raise LayoutError(f"{path}:\n  " + "\n  ".join(problems))
    return Layout(path=Path(path), categories=categories, entries=entries, stay_in_place=stay, keep_whole=whole)
