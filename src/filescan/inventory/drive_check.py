"""The drive check at the start of `filescan scan`.

It lists every connected drive against config.yml:

- a drive with a PC root under `roots:` is scanned;
- a drive under `ignored_drives:` is never scanned;
- any other drive is new. In a terminal, scan shows what Windows reports about
  it and asks for a name and whether to scan or ignore it. A cloud drive can
  only be ignored, because reading its files downloads them.

Each drive's details are saved as fields in config.yml: name, serial, label,
size_gb, filesystem, model, manufacturer, bus and disk_serial, plus letter for
an ignored drive and volume_id for a drive with a marker. Free space is shown
but never saved. When a label or another detail has changed, scan says so and
updates config.yml; the volumes table in the database is updated on every run.

config.yml is written with ruamel.yaml, which keeps its comments and layout. A
copy of the previous version is kept beside it, and the new version must load
before it replaces the old one.

Identity comes from volumes.py: a root passes on its serial or its marker. The
serial alone does not identify an ignored drive, because Google Drive gives
all its drives the same one, so an ignored drive is matched by its marker, or
its serial together with its size or letter.
"""

from __future__ import annotations

import io
import json
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PureWindowsPath

from filescan.config import load_config
from filescan.inventory.volumes import (
    SYSTEM_DRIVE, connected_letters, describe, fingerprint, is_cloud_drive, read_marker, record_seen,
    root_problem, write_marker,
)
from filescan.models import RootConfig

# Saved in config.yml in this order. Free space changes all the time, so it is shown and never saved.
DETAIL_FIELDS = ("serial", "label", "size_gb", "filesystem", "model", "manufacturer", "bus", "disk_serial")

Ask = Callable[[str], str]


@dataclass
class Attached:
    """A drive connected now, with what Windows reports about it."""

    letter: str
    fingerprint: dict
    marker: dict | None
    disk: dict = field(default_factory=dict)  # model, manufacturer, bus, disk_serial; empty if unavailable
    free: int = 0

    @property
    def serial(self) -> str:
        return (self.fingerprint.get("serial") or "").upper()

    @property
    def size_gb(self) -> float:
        return round((self.fingerprint.get("size") or 0) / 1e9, 1)

    def details(self) -> dict:
        values = {"serial": self.serial, "label": self.fingerprint.get("label") or "", "size_gb": self.size_gb,
                  "filesystem": self.fingerprint.get("filesystem") or "", **self.disk}
        return {key: values[key] for key in DETAIL_FIELDS if values.get(key) not in (None, "")}


# ---------------------------------------------------------------- what Windows reports


def disk_details() -> dict[str, dict]:
    """Model, manufacturer, bus and disk serial for each drive letter. Get-Disk needs no elevation."""
    if sys.platform != "win32":
        return {}
    script = ("Get-Partition | Where-Object DriveLetter | ForEach-Object { $d = Get-Disk -Number $_.DiskNumber; "
              "[pscustomobject]@{letter=[string]$_.DriveLetter; model=[string]$d.FriendlyName; "
              "manufacturer=[string]$d.Manufacturer; bus=[string]$d.BusType; disk_serial=[string]$d.SerialNumber} }"
              " | ConvertTo-Json")
    try:
        result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
                                capture_output=True, text=True, timeout=60)
        data = json.loads(result.stdout or "[]")
    except (OSError, subprocess.SubprocessError, ValueError):
        return {}  # the details are left blank; nothing else depends on them
    rows = [data] if isinstance(data, dict) else data
    return {f"{row.get('letter', '')}:".upper(): {key: str(row.get(key) or "").strip()
                                                  for key in ("model", "manufacturer", "bus", "disk_serial")}
            for row in rows if row.get("letter")}


def attached_drives(letters=None, details=None) -> list[Attached]:
    details = disk_details() if details is None else details
    drives = []
    for letter in letters if letters is not None else connected_letters():
        try:
            free = shutil.disk_usage(letter + "\\").free
        except OSError:
            free = 0
        drives.append(Attached(letter.upper(), fingerprint(letter + "\\"), read_marker(letter + "\\"),
                               details.get(letter.upper(), {}), free))
    return drives


# ---------------------------------------------------------------- config.yml, comments kept


def _yaml():
    from ruamel.yaml import YAML

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=4, offset=2)
    # Write null as null, as the file has it, not as an empty value.
    yaml.representer.add_representer(type(None), lambda r, _d: r.represent_scalar("tag:yaml.org,2002:null", "null"))
    return yaml


def _quoted(value):
    from ruamel.yaml.scalarstring import DoubleQuotedScalarString

    return DoubleQuotedScalarString(value) if isinstance(value, str) else value


def _move_trailing_comment(source, source_key, target, target_key) -> None:
    """Comment lines below source_key (other than its own line's comment) move below target_key.

    ruamel keeps the comments between two list items with the last key of the first item, so new keys or
    a new item would otherwise land below a comment that introduces the next section.
    """
    from ruamel.yaml.error import CommentMark
    from ruamel.yaml.tokens import CommentToken

    item = source.ca.items.get(source_key)
    if not item or len(item) < 3 or item[2] is None:
        return
    token = item[2]
    newline = token.value.find("\n")
    inline, rest = token.value[:newline + 1], token.value[newline + 1:]
    if newline < 0 or not rest:
        return
    token.value = inline
    target.ca.items[target_key] = [None, None, CommentToken("\n" + rest, CommentMark(0), None), None]


def _set_fields(entry, values: dict) -> None:
    """Set fields on a mapping from config.yml; new keys go at the end, above any comment that followed it."""
    last = list(entry.keys())[-1] if entry else None
    added = False
    for key, value in values.items():
        if key not in entry:
            added = True
        entry[key] = _quoted(value)
    if added and last is not None:
        _move_trailing_comment(entry, last, entry, list(entry.keys())[-1])


def _new_map(values: dict):
    from ruamel.yaml.comments import CommentedMap

    return CommentedMap((key, _quoted(value)) for key, value in values.items())


def save_config(config_path: Path, doc) -> Path:
    """Write config.yml, keeping a copy of the previous version. Returns the copy's path."""
    original = config_path.read_text(encoding="utf-8-sig")
    out = io.StringIO()
    _yaml().dump(doc, out)
    backup = config_path.with_name(f"{config_path.name}.bak-{datetime.now():%Y%m%d-%H%M%S}")
    backup.write_text(original, encoding="utf-8")
    config_path.write_text(out.getvalue(), encoding="utf-8")
    try:
        load_config(config_path)
    except Exception as exc:
        config_path.write_text(original, encoding="utf-8")
        raise SystemExit(f"The updated config.yml did not load ({exc}), so the previous version was put back. "
                         "Nothing else was changed.") from exc
    return backup


# ---------------------------------------------------------------- matching drives to config.yml


def _letter(path) -> str:
    return PureWindowsPath(str(path)).drive.upper()


def _gb(n: float) -> str:
    return f"{n:,.0f} GB" if n else "-"


def _same(a, b) -> bool:
    return str(a if a is not None else "").strip().upper() == str(b if b is not None else "").strip().upper()


def _ignored_entry(drive: Attached, ignored: list, used: set[int]):
    """The ignored_drives entry for this drive: by marker, or by serial with its size or letter."""
    volume_id = (drive.marker or {}).get("volume_id")
    best, best_score = None, 0
    for i, entry in enumerate(ignored):
        if i in used or not hasattr(entry, "get"):
            continue
        if volume_id and _same(entry.get("volume_id"), volume_id):
            score = 3
        elif _same(entry.get("serial"), drive.serial) and drive.serial:
            score = (_same(entry.get("size_gb"), drive.size_gb)) + (_same(entry.get("letter"), drive.letter))
        else:
            score = 0
        if score > best_score:
            best, best_score = i, score
    return best


def _update(entry, drive: Attached, name: str, messages: list[str], *, letter: bool = False) -> bool:
    """Bring an entry's saved details up to date. Returns True if anything changed."""
    values = drive.details()
    if letter:
        values["letter"] = drive.letter
    new = {}
    added = []
    for key, value in values.items():
        old = entry.get(key)
        if old in (None, ""):
            added.append(key)
            new[key] = value
        elif not _same(old, value):
            if key == "label":
                messages.append(f"{name}: the volume label has changed from \"{old}\" to \"{value}\".")
            elif key == "letter":
                messages.append(f"{name}: now on {value} (was {old}).")
            else:
                messages.append(f"{name}: {key} has changed from {old} to {value}.")
            new[key] = value
    if added:
        messages.append(f"{name}: recorded {', '.join(added)}.")
    if new:
        _set_fields(entry, new)
    return bool(new)


# ---------------------------------------------------------------- asking about a new drive


def _describe_new(drive: Attached) -> list[str]:
    d = drive.details()
    lines = [f"New drive on {drive.letter}",
             f"  Volume label:   {d.get('label', '-')}"]
    if d.get("manufacturer"):
        lines.append(f"  Manufacturer:   {d['manufacturer']}")
    lines += [f"  Model:          {d.get('model', '-')}" + (f" ({d['bus']})" if d.get("bus") else ""),
              f"  Disk serial:    {d.get('disk_serial', '-')}",
              f"  Volume serial:  {d.get('serial', '-')}",
              f"  Size:           {_gb(drive.size_gb)}, {_gb(round(drive.free / 1e9, 1))} free",
              f"  File system:    {d.get('filesystem', '-')}"]
    if is_cloud_drive(drive.fingerprint):
        lines.append("  This is a cloud drive: reading its files would download them, so it can only be ignored.")
    return lines


def _ask_new(drive: Attached, ask: Ask, letter_taken_by: str | None) -> tuple[str, str] | None:
    """(name, 'scan' or 'ignore') for a new drive, or None to leave it until the next scan."""
    print()
    print("\n".join(_describe_new(drive)))
    name = ask("Name for this drive (press Enter to leave it until the next scan): ").strip()
    if not name:
        return None
    if is_cloud_drive(drive.fingerprint):
        return name, "ignore"
    if letter_taken_by or drive.letter == SYSTEM_DRIVE:
        why = (f"{drive.letter} belongs to {letter_taken_by} in config.yml" if letter_taken_by
               else f"{drive.letter} is the system drive; add its folders to roots: in config.yml by hand")
        answer = ask(f"It can't be scanned here: {why}. Ignore it? [y/n]: ").strip().lower()
        return (name, "ignore") if answer.startswith("y") else None
    while True:
        answer = ask("Scan this drive, or ignore it? [s/i]: ").strip().lower()
        if answer[:1] in ("s", "i"):
            return name, "scan" if answer.startswith("s") else "ignore"


def _default_ask() -> Ask | None:
    """input() when scan runs in a terminal; None when it does not, so nothing waits for an answer."""
    try:
        return input if sys.stdin is not None and sys.stdin.isatty() else None
    except (AttributeError, ValueError):
        return None


# ---------------------------------------------------------------- the check


@dataclass
class DriveCheck:
    rows: list[tuple[str, ...]] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    not_attached: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    left_for_later: list[str] = field(default_factory=list)
    backup: Path | None = None
    asked: bool = False  # False when scan was not run in a terminal, so new drives could not be asked about
    table_printed: bool = False  # printed before asking about new drives


def check_drives(config_path: Path, *, letters=None, details=None, ask: Ask | None | bool = True,
                 problem_of: Callable[[RootConfig], str | None] = root_problem, db=None) -> DriveCheck:
    """Compare the connected drives with config.yml, ask about new ones, and save what changed."""
    config_path = Path(config_path)
    if ask is True:
        ask = _default_ask()
    config = load_config(config_path)
    doc = _yaml().load(config_path.read_text(encoding="utf-8-sig"))
    roots = doc.get("roots") or []
    ignored = doc.get("ignored_drives")
    ignored_list = list(ignored) if ignored else []
    drives = attached_drives(letters, details)
    attached_letters = {d.letter for d in drives}
    pc_roots = list(config.pc_root_configs)
    problems = {id(rc): problem_of(rc) for rc in pc_roots}
    result = DriveCheck()
    changed = False
    used_ignored: set[int] = set()
    new_roots, new_ignored = [], []
    pending: list[tuple[int, Attached, str | None]] = []  # new drives: row, drive, root already on its letter

    def root_entries(letter: str):
        return [r for r in roots if hasattr(r, "get") and str(r.get("location", "pc")) == "pc"
                and _letter(r.get("path", "")) == letter]

    for drive in drives:
        here = [rc for rc in pc_roots if _letter(rc.path) == drive.letter]
        ok = [rc for rc in here if problems[id(rc)] is None]
        if ok:
            name = ok[0].name or str(ok[0].path)
            for entry in root_entries(drive.letter):
                changed |= _update(entry, drive, name, result.messages)
            result.rows.append(_row(drive, name, "scanned"))
            continue
        elsewhere = [rc for rc in pc_roots if _letter(rc.path) != drive.letter and (
            (rc.volume_id and (drive.marker or {}).get("volume_id") == rc.volume_id)
            or (rc.serial and rc.serial.upper() == drive.serial))]
        if elsewhere:
            result.rows.append(_row(drive, elsewhere[0].name or str(elsewhere[0].path),
                                    f"scanned, but config.yml has it on {_letter(elsewhere[0].path)}"))
            continue
        index = _ignored_entry(drive, ignored_list, used_ignored)
        if index is not None:
            used_ignored.add(index)
            entry = ignored_list[index]
            name = str(entry.get("name") or drive.letter)
            changed |= _update(entry, drive, name, result.messages, letter=True)
            result.rows.append(_row(drive, name, "ignored"))
            continue
        taken = here[0].name or str(here[0].path) if here else None
        pending.append((len(result.rows), drive, taken))
        result.rows.append(_row(drive, "-", "new"))

    for rc in pc_roots:
        problem = problems[id(rc)]
        if _letter(rc.path) not in attached_letters and (problem is None or " now" not in problem):
            result.not_attached.append(describe(rc))
        elif problem is not None:
            result.problems.append(problem)  # a different drive on its letter, or the drive on another letter
    for i, entry in enumerate(ignored_list):
        if i not in used_ignored and hasattr(entry, "get"):
            result.not_attached.append(f"{entry.get('name')} ({entry.get('letter', '?')}, ignored)")
    result.asked = bool(ask)

    if pending and ask:
        print_table(result)  # what is attached and what is not, before any question
        result.table_printed = True
    for row_index, drive, taken in pending:
        answer = _ask_new(drive, ask, taken) if ask else None
        if answer is None:
            result.left_for_later.append(drive.letter)
            continue
        name, use = answer
        values = {"name": name, **drive.details()}
        if use == "scan":
            marker = write_marker(drive.letter + "\\", name, fp=drive.fingerprint)
            new_roots.append(_new_map({"path": f"{drive.letter}/", "name": name, "volume_id": marker["volume_id"],
                                       **drive.details()}))
            result.messages.append(f"{name} ({drive.letter}): added to roots, to be scanned; "
                                   f"marker written at {drive.letter}\\.filescan-volume.yaml.")
            result.rows[row_index] = _row(drive, name, "scanned (new)")
        else:
            new_ignored.append(_new_map({"name": name, "letter": drive.letter, **values}))
            result.messages.append(f"{name} ({drive.letter}): added to ignored_drives; it is never scanned.")
            result.rows[row_index] = _row(drive, name, "ignored (new)")
        changed = True

    if new_roots:
        _add_roots(roots, new_roots)
    if new_ignored:
        if ignored is None:
            keys = list(doc.keys())
            doc.insert(keys.index("roots") + 1 if "roots" in keys else len(keys), "ignored_drives", new_ignored)
            following = list(doc.keys())
            position = following.index("ignored_drives")
            if position + 1 < len(following):
                doc.yaml_set_comment_before_after_key(following[position + 1], before="\n")
        else:
            ignored.extend(new_ignored)

    if changed:
        result.backup = save_config(config_path, doc)
    _record(config, drives, db)
    return result


def _add_roots(roots, new_roots) -> None:
    """Insert new PC roots after the last PC root, so they stay above the server roots and their comments."""
    last_pc = max((i for i, r in enumerate(roots) if hasattr(r, "get") and str(r.get("location", "pc")) == "pc"),
                  default=-1)
    for offset, entry in enumerate(new_roots):
        if last_pc >= 0 and offset == 0:
            previous = roots[last_pc]
            _move_trailing_comment(previous, list(previous.keys())[-1], entry, list(entry.keys())[-1])
        roots.insert(last_pc + 1 + offset, entry)


def _row(drive: Attached, name: str, status: str) -> tuple[str, ...]:
    d = drive.details()
    return (drive.letter, name, status, d.get("label", "-"), d.get("model", "-"), _gb(drive.size_gb),
            _gb(round(drive.free / 1e9, 1)))


def _record(config, drives: list[Attached], db) -> None:
    from filescan.inventory.volumes import Seen
    from filescan.storage import SQLiteDB

    seen = [Seen(d.letter, d.fingerprint, d.marker, None) for d in drives]
    names = {}
    for rc in config.pc_root_configs:
        names.setdefault(_letter(rc.path), rc)
    for s in seen:
        s.root = names.get(s.letter)
    try:
        own = db is None
        db = SQLiteDB(config.database_path) if own else db
        try:
            record_seen(db, seen)
        finally:
            if own:
                db.close()
    except Exception as exc:  # the check is still useful without the record
        print(f"(Could not record the drives in the database: {exc})")


def print_table(result: DriveCheck) -> None:
    """The connected drives, then the ones in config.yml that are not attached, and any problems."""
    headers = ("Drive", "Name", "In config.yml", "Label", "Model", "Size", "Free")
    rows = result.rows
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    for line in [headers, *rows]:
        print("  ".join(cell.ljust(w) for cell, w in zip(line, widths)).rstrip())
    if result.not_attached:
        print("\nNot attached: " + "; ".join(result.not_attached))
    for problem in result.problems:
        print(f"\nPROBLEM: {problem}")


def print_check(result: DriveCheck) -> None:
    if not result.table_printed:
        print_table(result)
    if result.messages:
        print()
        for message in result.messages:
            print(message)
    if result.backup:
        print(f"config.yml updated; the previous version is {result.backup.name}.")
    for letter in result.left_for_later:
        if result.asked:
            print(f"\n{letter} is a new drive and was left out; the next scan asks about it again.")
        else:
            print(f"\n{letter} is a new drive and was left out. Run `filescan scan` in a terminal to name it "
                  "and choose whether to scan or ignore it.")
