"""Know which drive is which, whatever letter it is on.

External drives can come back on a different letter, and another drive can
take a letter that belongs to one in the config. Scanning the wrong drive
would mark every file of the right one missing, so each PC root is checked
before it is scanned or changed.

Two ways to identify a drive:

- its volume serial number (`serial` in config.yml), which every drive has;
- a marker file, `.filescan-volume.yaml`, at the drive's root, written by
  the drive check in `filescan scan`. It holds a volume ID that filescan generates, the
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

The drive check at the start of `filescan scan` (drive_check.py) lists the
connected drives, asks about new ones and writes the marker for a drive to be
scanned. `filescan retire-drive` marks a drive's files missing once it has gone
for good.
"""

from __future__ import annotations

import ctypes
import os
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
# Virtual drives of cloud services: reading a file there downloads it, so they can only be ignored.
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
    marker = {"about": "Written by filescan to recognise this drive whatever letter it is on. Please leave it here.",
              "volume_id": uuid.uuid4().hex[:12], "name": name, "letter": volume_root(volume)[:2].upper(),
              "fingerprint": fp, "written": datetime.now().isoformat(timespec="seconds")}
    text = yaml.safe_dump(marker, sort_keys=False, width=4096)
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


# ---------------------------------------------------------------- the volumes table


@dataclass
class Seen:
    letter: str
    fingerprint: dict
    marker: dict | None
    root: RootConfig | None  # the config root on this drive


def record_seen(db, seen: list[Seen]) -> None:
    """Keep what was observed in the volumes table: identity, expected and last letter, when.

    Rows are keyed on serial and size: Google Drive gives all its drives the same serial.
    """
    row = db.conn.execute("SELECT sql FROM sqlite_master WHERE name = 'volumes'").fetchone()
    if row and "PRIMARY KEY (serial)" in row[0]:
        db.conn.execute("DROP TABLE volumes")  # the first layout, keyed on serial alone; it is rebuilt each scan
    db.conn.execute("""CREATE TABLE IF NOT EXISTS volumes (
        volume_id TEXT, serial TEXT NOT NULL, name TEXT, expected_letter TEXT, last_letter TEXT,
        last_seen_at TEXT, size INTEGER NOT NULL, label TEXT, filesystem TEXT, PRIMARY KEY (serial, size))""")
    now = datetime.now().isoformat(timespec="seconds")
    for s in seen:
        if not s.fingerprint.get("serial"):
            continue
        expected = volume_root(s.root.path)[:2].upper() if s.root else None
        db.conn.execute(
            """INSERT INTO volumes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(serial, size) DO UPDATE SET volume_id = excluded.volume_id, name = excluded.name,
                 expected_letter = excluded.expected_letter, last_letter = excluded.last_letter,
                 last_seen_at = excluded.last_seen_at, label = excluded.label, filesystem = excluded.filesystem""",
            ((s.marker or {}).get("volume_id"), s.fingerprint["serial"], s.root.name if s.root else None,
             expected, s.letter, now, s.fingerprint.get("size") or 0, s.fingerprint.get("label"),
             s.fingerprint.get("filesystem")))
    db.conn.commit()


# ---------------------------------------------------------------- retiring a drive


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
    print("Remove its entry under roots: in config.yml, so it is not checked again.")
    return n
