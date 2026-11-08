"""Know which drive is which, whatever letter it is on.

External drives can come back on a different letter, and another drive can
take a letter that belongs to one in the config. Scanning the wrong drive
would mark every file of the right one missing, so each PC root is checked
before it is scanned or changed.

Two ways to identify a drive:

- its volume serial number (`serial` in config.yml), which every drive has;
- a marker file, `.filescan-volume.yaml`, at the drive's root, written by
  `filescan drives --add`. It holds a volume ID that filescan generates, the
  drive's friendly name and expected letter, and a fingerprint (serial, size,
  label, file system). A marker whose fingerprint does not match the drive it
  is on has been copied from another drive, for example by cloning; the drive
  is then treated as unknown.

A root passes if its serial matches, or if the marker on its drive has its
volume ID and a matching fingerprint. A root with neither configured is only
checked for being connected. When a drive is on the wrong letter, the check
names the letter it is on now; filescan never follows it there, so the user
changes the letter back in Disk Management.

C: is the system drive and never gets a marker: writing to C:\\ needs an
elevated terminal, and nothing on C: changes without the user's approval. It
is checked by serial only.

`filescan drives` lists every connected drive, marks the ones config.yml does
not know, and records what it saw in the volumes table. `--add` registers a
new drive; `--retire` marks a drive's files missing once it has gone for good.
"""

from __future__ import annotations

import ctypes
import os
import re
import shutil
import string
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PureWindowsPath

import yaml

from filescan.models import RootConfig, ScanConfig

MARKER = ".filescan-volume.yaml"
# Virtual drives of cloud services: reading a file there downloads it, so they are never offered for adding.
# Windows cuts labels to 32 characters, so "davidcbaines@gmail.com - Google Drive" arrives as "... - Goog...".
CLOUD_LABEL_WORDS = ("goog", "onedrive", "dropbox", "icloud")
GOOGLE_DRIVE_SERIAL = "1983-1116"  # Google Drive for desktop gives every one of its drives this serial
SYSTEM_DRIVE = (os.environ.get("SystemDrive") or "C:").upper()
_DRIVE_REMOVABLE, _DRIVE_FIXED = 2, 3


def volume_root(path: Path | str) -> str:
    """The volume part of a path with a trailing backslash: 'J:\\' or '\\\\host\\share\\'."""
    anchor = PureWindowsPath(str(path)).anchor
    return anchor if anchor.endswith("\\") else anchor + "\\"


def format_serial(number: int) -> str:
    """Format a serial as Windows shows it in `vol` and `dir`: 'A1B2-C3D4'."""
    text = f"{number & 0xFFFFFFFF:08X}"
    return f"{text[:4]}-{text[4:]}"


def _volume_information(path: Path | str) -> tuple[str, str, str] | None:
    """(serial, label, file system) of the volume holding path, or None if it cannot be read."""
    if sys.platform != "win32":
        return None
    serial = ctypes.c_uint32(0)
    label = ctypes.create_unicode_buffer(261)
    filesystem = ctypes.create_unicode_buffer(261)
    ok = ctypes.windll.kernel32.GetVolumeInformationW(  # type: ignore[attr-defined]
        ctypes.c_wchar_p(volume_root(path)), label, len(label), ctypes.byref(serial), None, None,
        filesystem, len(filesystem))
    return (format_serial(serial.value), label.value, filesystem.value) if ok else None


def volume_serial(path: Path | str) -> str | None:
    """The serial number of the volume holding `path`, or None if it cannot be read."""
    info = _volume_information(path)
    return info[0] if info else None


def volume_label(path: Path | str) -> str:
    info = _volume_information(path)
    return info[1] if info else ""


def fingerprint(path: Path | str) -> dict:
    """What identifies a volume without elevation: serial, size, label and file system."""
    info = _volume_information(path)
    try:
        size = shutil.disk_usage(volume_root(path)).total
    except OSError:
        size = 0
    serial, label, filesystem = info if info else ("", "", "")
    return {"serial": serial, "size": size, "label": label, "filesystem": filesystem}


def same_volume(a: dict, b: dict) -> bool:
    """A marker belongs to this drive if its serial and size match. The label can be renamed."""
    return bool(a.get("serial")) and a.get("serial") == b.get("serial") and a.get("size") == b.get("size")


# ---------------------------------------------------------------- markers


def marker_path(volume: Path | str) -> Path:
    return Path(volume_root(volume)) / MARKER


def read_marker(volume: Path | str) -> dict | None:
    try:
        data = yaml.safe_load(marker_path(volume).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    return data if isinstance(data, dict) and data.get("volume_id") else None


def is_cloud_drive(fp: dict) -> bool:
    label = (fp.get("label") or "").lower()
    return fp.get("serial") == GOOGLE_DRIVE_SERIAL or any(word in label for word in CLOUD_LABEL_WORDS)


def is_system_drive(path: Path | str) -> bool:
    return volume_root(path)[:2].upper() == SYSTEM_DRIVE


def write_marker(volume: Path | str, name: str, *, fp: dict | None = None) -> dict:
    """Write the marker at the volume's root and return it. Never on the system drive."""
    if is_system_drive(volume):
        raise ValueError(f"{SYSTEM_DRIVE} is the system drive; it is identified by its serial number only.")
    fp = fp if fp is not None else fingerprint(volume)
    existing = read_marker(volume)
    if existing and same_volume(existing.get("fingerprint", {}), fp):
        return existing  # already registered; keep its volume ID
    marker = {"volume_id": uuid.uuid4().hex[:12], "name": name, "letter": volume_root(volume)[:2].upper(),
              "fingerprint": fp, "written": datetime.now().isoformat(timespec="seconds")}
    text = ("# Written by filescan to recognise this drive whatever letter it is on. Please leave it here.\n"
            + yaml.safe_dump(marker, sort_keys=False))
    marker_path(volume).write_text(text, encoding="utf-8")
    return marker


# ---------------------------------------------------------------- checking a root


def describe(root: RootConfig) -> str:
    return f"{root.name} ({root.path})" if root.name else str(root.path)


def connected_letters() -> list[str]:
    """Letters of the fixed and removable drives connected now, such as ['C:', 'D:', 'J:']."""
    if sys.platform != "win32":
        return []
    mask = ctypes.windll.kernel32.GetLogicalDrives()  # type: ignore[attr-defined]
    letters = []
    for i, letter in enumerate(string.ascii_uppercase):
        if mask & (1 << i):
            kind = ctypes.windll.kernel32.GetDriveTypeW(ctypes.c_wchar_p(f"{letter}:\\"))  # type: ignore[attr-defined]
            if kind in (_DRIVE_REMOVABLE, _DRIVE_FIXED):
                letters.append(f"{letter}:")
    return letters


def find_drive(root: RootConfig, letters=None, serial_of=None, marker_of=None) -> str | None:
    """The letter the root's drive is on now, found by its marker or serial, or None."""
    serial_of = serial_of or volume_serial
    marker_of = marker_of or read_marker
    for letter in letters if letters is not None else connected_letters():
        marker = marker_of(letter + "\\")
        if root.volume_id and marker and marker.get("volume_id") == root.volume_id:
            return letter
        if root.serial and (serial_of(letter + "\\") or "").upper() == root.serial.upper():
            return letter
    return None


def root_problem(root: RootConfig, serial_of=volume_serial, marker_of=read_marker, fingerprint_of=fingerprint,
                 letters=None) -> str | None:
    """Why a PC root must not be scanned or changed now, or None if it is fine."""
    if not root.path.is_dir():
        moved = find_drive(root, letters, serial_of, marker_of) if (root.serial or root.volume_id) else None
        where = f"; the drive is on {moved} now. Change its letter back in Disk Management" if moved else ""
        return f"{describe(root)} is not connected{where}"
    if not root.serial and not root.volume_id:
        return None
    found = serial_of(root.path)
    if root.serial and found and found.upper() == root.serial.upper():
        return None
    if root.volume_id:
        marker = marker_of(root.path)
        if marker and marker.get("volume_id") == root.volume_id:
            if same_volume(marker.get("fingerprint", {}), fingerprint_of(root.path)):
                return None
            return (f"{describe(root)}: the marker on {volume_root(root.path)} was copied from another drive "
                    "(its fingerprint does not match), so this drive is treated as unknown")
    if found is None and root.serial:
        return f"{describe(root)}: the volume serial number could not be read"
    moved = find_drive(root, letters, serial_of, marker_of)
    where = (f" The drive is on {moved} now; change its letter back in Disk Management." if moved
             else " The right drive is not connected.")
    return (f"{describe(root)}: the drive on {volume_root(root.path)} is a different drive "
            f"(serial {found or 'unknown'}).{where}")


# ---------------------------------------------------------------- the drives command


@dataclass
class Seen:
    letter: str
    fingerprint: dict
    marker: dict | None
    root: RootConfig | None  # the config root on this letter or this drive


def _known_root(letter: str, fp: dict, marker: dict | None, roots: list[RootConfig]) -> RootConfig | None:
    for root in roots:
        if marker and root.volume_id and marker.get("volume_id") == root.volume_id:
            return root
        if root.serial and fp.get("serial") and root.serial.upper() == fp["serial"].upper():
            return root
    for root in roots:
        if volume_root(root.path)[:2].upper() == letter.upper() and not root.serial and not root.volume_id:
            return root
    return None


def survey(config: ScanConfig, letters=None) -> list[Seen]:
    roots = list(config.pc_root_configs)
    seen = []
    for letter in letters if letters is not None else connected_letters():
        fp = fingerprint(letter + "\\")
        marker = read_marker(letter + "\\")
        seen.append(Seen(letter, fp, marker, _known_root(letter, fp, marker, roots)))
    return seen


def record_seen(db, seen: list[Seen]) -> None:
    """Keep what was observed in the volumes table: identity, expected and last letter, when."""
    db.conn.execute("""CREATE TABLE IF NOT EXISTS volumes (
        volume_id TEXT, serial TEXT NOT NULL, name TEXT, expected_letter TEXT, last_letter TEXT,
        last_seen_at TEXT, size INTEGER, label TEXT, filesystem TEXT, PRIMARY KEY (serial))""")
    now = datetime.now().isoformat(timespec="seconds")
    for s in seen:
        if not s.fingerprint.get("serial"):
            continue
        expected = volume_root(s.root.path)[:2].upper() if s.root else None
        db.conn.execute(
            """INSERT INTO volumes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(serial) DO UPDATE SET volume_id = excluded.volume_id, name = excluded.name,
                 expected_letter = excluded.expected_letter, last_letter = excluded.last_letter,
                 last_seen_at = excluded.last_seen_at, size = excluded.size, label = excluded.label,
                 filesystem = excluded.filesystem""",
            ((s.marker or {}).get("volume_id"), s.fingerprint["serial"], s.root.name if s.root else None,
             expected, s.letter, now, s.fingerprint.get("size"), s.fingerprint.get("label"),
             s.fingerprint.get("filesystem")))
    db.conn.commit()


def _gb(n: int) -> str:
    return f"{n / 1e9:,.0f} GB" if n else "-"


def run_drives(config: ScanConfig, letters=None) -> list[Seen]:
    """List every connected drive, whether config.yml knows it, and each PC root's check."""
    from filescan.storage import SQLiteDB

    seen = survey(config, letters)
    rows = []
    for s in seen:
        known = ((s.root.name or str(s.root.path)) if s.root
                 else "cloud drive, never scanned" if is_cloud_drive(s.fingerprint) else "not in config.yml")
        marker = "marker" if s.marker else ("system drive" if s.letter.upper() == SYSTEM_DRIVE else "-")
        rows.append((s.letter, s.fingerprint.get("label") or "-", _gb(s.fingerprint.get("size", 0)),
                     s.fingerprint.get("serial") or "-", marker, known))
    _table(("Drive", "Label", "Size", "Serial", "Marker", "In config.yml as"), rows)

    print()
    problems = []
    for root in config.pc_root_configs:
        problem = root_problem(root)
        print(f"  {'OK' if problem is None else 'PROBLEM':8} {describe(root)}" + (f": {problem}" if problem else ""))
        if problem:
            problems.append(problem)
    unknown = [s for s in seen if s.root is None and s.letter.upper() != SYSTEM_DRIVE
               and not is_cloud_drive(s.fingerprint)]
    for s in unknown:
        print(f"\n{s.letter} is not in config.yml. To add it: filescan drives --add {s.letter} --name \"<a name>\"")
    missing_serial = [r for r in config.pc_root_configs if not r.serial and not r.volume_id and r.path.is_dir()]
    for root in missing_serial:
        print(f"\n{describe(root)} has no serial in config.yml; add this line under it:")
        print(f'    serial: "{volume_serial(root.path)}"')
    try:
        db = SQLiteDB(config.database_path)
        try:
            record_seen(db, seen)
        finally:
            db.close()
    except Exception as exc:  # the listing is still useful without the record
        print(f"(Could not record the drives in the database: {exc})")
    return seen


def _table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    for line in [headers, *rows]:
        print("  ".join(cell.ljust(w) for cell, w in zip(line, widths)).rstrip())


# ---------------------------------------------------------------- adding and retiring a drive


def root_lines(letter: str, name: str, serial: str, volume_id: str) -> list[str]:
    return [f'  - path: "{letter.upper()}/"',
            f'    name: "{name}"',
            f'    serial: "{serial}"',
            f'    volume_id: "{volume_id}"']


def insert_root(config_text: str, lines: list[str]) -> str:
    """Insert a PC root's lines after the last PC root in roots:, keeping every comment as it is."""
    text_lines = config_text.splitlines()
    # The first server root (or the end of the roots list) is where PC roots end.
    in_roots, insert_at, last_pc_end = False, None, None
    i = 0
    while i < len(text_lines):
        line = text_lines[i]
        if re.match(r"^roots:\s*$", line):
            in_roots = True
        elif in_roots and re.match(r"^\S", line) and not line.startswith("#"):
            insert_at = last_pc_end if last_pc_end is not None else i
            break
        elif in_roots and re.match(r"^\s*-\s*path:", line):
            j = i + 1
            while j < len(text_lines) and not re.match(r"^\s*-\s*path:", text_lines[j]) and \
                    (text_lines[j].startswith("    ") or not text_lines[j].strip()):
                j += 1
            block = text_lines[i:j]
            if any(re.match(r"^\s*location:\s*server", b) for b in block):
                if insert_at is None:
                    insert_at = last_pc_end if last_pc_end is not None else i
            else:
                last_pc_end = i + 1 + sum(1 for b in block[1:] if b.startswith("    ") and b.strip())
            i = j
            continue
        i += 1
    if insert_at is None:
        if last_pc_end is None:
            raise ValueError("config.yml has no roots: list to add to")
        insert_at = last_pc_end
    return "\n".join(text_lines[:insert_at] + lines + text_lines[insert_at:]) + "\n"


def add_drive(config_path: Path, config: ScanConfig, letter: str, name: str, *, letters=None) -> list[str]:
    """Register a connected drive: write its marker, then add it to config.yml (backed up first)."""
    letter = letter.upper().rstrip("\\").rstrip(":") + ":"
    if letter == SYSTEM_DRIVE:
        raise SystemExit(f"{SYSTEM_DRIVE} is the system drive; add its folders to config.yml by hand. "
                         "It is identified by its serial number only.")
    if letter not in (letters if letters is not None else connected_letters()):
        raise SystemExit(f"No drive is connected on {letter}. Nothing was changed.")
    fp = fingerprint(letter + "\\")
    if is_cloud_drive(fp):
        raise SystemExit(f"{letter} is a cloud drive ({fp['label']}); reading its files would download them. "
                         "Nothing was changed.")
    known = _known_root(letter, fp, read_marker(letter + "\\"), list(config.pc_root_configs))
    if known is not None:
        raise SystemExit(f"{letter} is already in config.yml as {describe(known)}. Nothing was changed.")
    marker = write_marker(letter + "\\", name, fp=fp)
    lines = root_lines(letter, name, fp["serial"], marker["volume_id"])
    original = config_path.read_text(encoding="utf-8")
    backup = config_path.with_name(f"{config_path.name}.bak-{datetime.now():%Y%m%d-%H%M%S}")
    backup.write_text(original, encoding="utf-8")
    config_path.write_text(insert_root(original, lines), encoding="utf-8")
    print(f"Wrote the marker {marker_path(letter + '/')}.")
    print(f"Added to {config_path} (the previous version is {backup.name}):")
    for line in lines:
        print(f"  {line}")
    print(f"Run `filescan scan` to inventory {name}.")
    return lines


def retire_drive(config: ScanConfig, name: str, db) -> int:
    """Mark every file of a drive that has gone for good as missing, so it no longer counts as a copy."""
    from filescan.storage.sql import path_prefix

    matches = [r for r in config.pc_root_configs if r.name.lower() == name.lower() or str(r.path).lower() == name.lower()]
    if not matches:
        raise SystemExit(f"No PC root in config.yml is called {name!r}. Nothing was changed.")
    root = matches[0]
    if root_problem(root) is None and root.path.is_dir() and (root.serial or root.volume_id):
        raise SystemExit(f"{describe(root)} is connected and identified. Disconnect it before retiring it, "
                         "or scan it instead. Nothing was changed.")
    exact, pattern = path_prefix(root.path)
    n = db.conn.execute("UPDATE files SET is_missing = 1 WHERE is_missing = 0 AND (path = ? OR path LIKE ? ESCAPE '|')",
                        (exact, pattern)).rowcount
    db.conn.execute("UPDATE folders SET is_missing = 1 WHERE path = ? OR path LIKE ? ESCAPE '|'", (exact, pattern))
    db.conn.commit()
    print(f"{describe(root)}: {n:,} files marked missing; they no longer count as copies of anything.")
    print("Remove or comment out its lines under roots: in config.yml, so it is not checked again.")
    return n
