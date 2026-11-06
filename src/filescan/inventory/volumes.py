"""Identify the drive behind a root by its volume serial number.

External drives can come back on a different letter, and another drive can
take a letter that belongs to one in the config. Scanning the wrong drive
would mark every file of the right one missing, so roots with a configured
`serial` are checked before they are scanned or changed.
"""

from __future__ import annotations

import ctypes
import sys
from pathlib import Path, PureWindowsPath

from filescan.models import RootConfig, ScanConfig


def volume_root(path: Path | str) -> str:
    """The volume part of a path with a trailing backslash: 'J:\\' or '\\\\host\\share\\'."""
    anchor = PureWindowsPath(str(path)).anchor
    return anchor if anchor.endswith("\\") else anchor + "\\"


def format_serial(number: int) -> str:
    """Format a serial as Windows shows it in `vol` and `dir`: 'A1B2-C3D4'."""
    text = f"{number & 0xFFFFFFFF:08X}"
    return f"{text[:4]}-{text[4:]}"


def volume_serial(path: Path | str) -> str | None:
    """The serial number of the volume holding `path`, or None if it cannot be read."""
    if sys.platform != "win32":
        return None
    serial = ctypes.c_uint32(0)
    name = ctypes.create_unicode_buffer(261)
    ok = ctypes.windll.kernel32.GetVolumeInformationW(  # type: ignore[attr-defined]
        ctypes.c_wchar_p(volume_root(path)), name, len(name), ctypes.byref(serial), None, None, None, 0
    )
    return format_serial(serial.value) if ok else None


def volume_label(path: Path | str) -> str:
    if sys.platform != "win32":
        return ""
    name = ctypes.create_unicode_buffer(261)
    ok = ctypes.windll.kernel32.GetVolumeInformationW(  # type: ignore[attr-defined]
        ctypes.c_wchar_p(volume_root(path)), name, len(name), None, None, None, None, 0
    )
    return name.value if ok else ""


def describe(root: RootConfig) -> str:
    return f"{root.name} ({root.path})" if root.name else str(root.path)


def root_problem(root: RootConfig, serial_of=volume_serial) -> str | None:
    """Why a PC root must not be scanned or changed now, or None if it is fine.

    A root with no configured serial is only checked for being connected.
    """
    if not root.path.is_dir():
        return f"{describe(root)} is not connected"
    if not root.serial:
        return None
    found = serial_of(root.path)
    if found is None:
        return f"{describe(root)}: the volume serial number could not be read"
    if found.upper() != root.serial.upper():
        return (f"{describe(root)}: the drive on {volume_root(root.path)} has serial {found}, "
                f"not {root.serial}. Another drive has taken this letter")
    return None


def run_drives(config: ScanConfig) -> None:
    """List PC roots with their drives, and the serial lines still missing from the config."""
    rows, missing = [], []
    for root in config.pc_root_configs:
        found = volume_serial(root.path) if root.path.is_dir() else None
        problem = root_problem(root)
        if problem is None and not root.serial:
            status = "connected, no serial in the config"
            missing.append((root, found))
        elif problem is None:
            status = "OK"
        else:
            status = problem.split(": ", 1)[-1] if ": " in problem else "not connected"
        rows.append((root.name or "-", str(root.path), root.serial or "-", found or "-",
                     volume_label(root.path) if found else "", status))
    headers = ("Name", "Root", "Config serial", "Drive serial", "Label", "Status")
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)] if rows else []
    for line in [headers, *rows]:
        print("  ".join(cell.ljust(w) for cell, w in zip(line, widths)).rstrip())
    if missing:
        print("\nAdd these lines to the roots in config.yml so the wrong drive is never scanned:")
        for root, found in missing:
            print(f'  - path: "{root.path.as_posix()}"')
            print(f'    name: "{root.name or volume_label(root.path) or root.path}"')
            print(f'    serial: "{found}"')
