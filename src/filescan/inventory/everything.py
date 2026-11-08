"""List a local drive's files and folders with Everything (voidtools) instead of walking it.

Everything reads the NTFS file table and keeps its index up to date from the
change journal, so `es.exe` lists a drive of two million files in about five
seconds. The scanner compares that list with the database and writes only
what has changed (`everything_scan.py`). Whenever this module cannot give a
complete list it returns None, and the scanner walks the folders instead.

es.exe gives sizes in bytes and dates as raw Windows FILETIME values (100 ns
ticks since 1601). `filetime_to_stat` turns them into exactly the float that
`os.stat` gives on Windows, because hash-all compares the stored date with
`stat()` for equality before it stores a hash.
"""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

ES_DEFAULT = Path(r"C:\Program Files\Everything\es.exe")
ES_TIMEOUT_SECONDS = 600
_EPOCH_TICKS = 116_444_736_000_000_000  # 1601-01-01 to 1970-01-01 in 100 ns ticks
FILE_ATTRIBUTE_DIRECTORY = 0x10
FILE_ATTRIBUTE_NORMAL = 0x80
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
# Everything reads this bit (files with extended attributes) from the file table; a directory
# listing, which the walk uses, never reports it. Measured 8 November: 25,407 files on D:.
FILE_ATTRIBUTE_EA = 0x40000

# Runs es.exe with these arguments and returns its exit code. Tests pass their own.
Runner = Callable[[list[str]], int]


@dataclass(slots=True)
class Entry:
    path: str
    size: int
    mtime: float
    ctime: float
    attributes: int

    @property
    def is_dir(self) -> bool:
        return bool(self.attributes & FILE_ATTRIBUTE_DIRECTORY)

    @property
    def is_reparse_point(self) -> bool:
        return bool(self.attributes & FILE_ATTRIBUTE_REPARSE_POINT)


def find_es() -> Path | None:
    """es.exe on PATH, or in Everything's usual folder; None if neither."""
    found = shutil.which("es.exe")
    if found:
        return Path(found)
    return ES_DEFAULT if ES_DEFAULT.is_file() else None


def filetime_to_stat(ticks: int) -> float:
    """The float os.stat gives on Windows for a FILETIME: whole seconds plus nanoseconds * 1e-9.

    CPython divides with C semantics (towards zero), which differs from Python's
    // for dates before 1970.
    """
    t = ticks - _EPOCH_TICKS
    seconds = int(t / 10**7) if t < 0 else t // 10**7
    nanoseconds = (t - seconds * 10**7) * 100
    return seconds + nanoseconds * 1e-9


def stat_attributes(attributes: int) -> int:
    """The attributes a directory listing gives: without the extended-attributes bit, and
    FILE_ATTRIBUTE_NORMAL where Everything gives 0."""
    return (attributes & ~FILE_ATTRIBUTE_EA) or FILE_ATTRIBUTE_NORMAL


def default_runner(es: Path) -> Runner:
    def run(args: list[str]) -> int:
        return subprocess.run([str(es), *args], capture_output=True, timeout=ES_TIMEOUT_SECONDS).returncode
    return run


class EverythingUnavailable(Exception):
    """es.exe failed, timed out or returned nothing; the caller walks the folders instead."""


def export(root: Path, *, folders: bool, runner: Runner, out_dir: Path) -> Path:
    """Have es.exe write every folder (or every file) under root to a CSV; return its path."""
    handle, name = tempfile.mkstemp(prefix="es_", suffix=".csv", dir=out_dir)
    os.close(handle)
    out = Path(name)
    args = ["-timeout", "60000", "-path", str(root), "/ad" if folders else "/a-d",
            "-size", "-date-modified", "-date-created", "-attributes", "-size-format", "1", "-date-format", "2",
            "-export-csv", str(out), "-utf8-bom"]
    try:
        code = runner(args)
    except (OSError, subprocess.TimeoutExpired) as exc:
        out.unlink(missing_ok=True)
        raise EverythingUnavailable(f"es.exe could not be run: {exc}") from exc
    if code != 0:
        out.unlink(missing_ok=True)
        raise EverythingUnavailable(f"es.exe stopped with error {code} (is Everything running?)")
    return out


def read_export(path: Path, root: Path) -> Iterator[Entry]:
    """The entries in an es.exe CSV that lie under root. Deletes the CSV when done.

    Everything ends a folder's path with a backslash; it is removed, so paths match the database.
    """
    prefix = str(root).rstrip("\\").lower() + "\\"
    try:
        with path.open(encoding="utf-8-sig", newline="", errors="replace") as handle:
            rows = csv.reader(handle)
            next(rows, None)  # Filename,Size,Date Modified,Date Created,Attributes
            for row in rows:
                if len(row) < 5 or not row[0].lower().startswith(prefix):
                    continue
                name, size, modified, created, attributes = row[:5]
                yield Entry(name.rstrip("\\"), int(size) if size.isdigit() else 0,
                            filetime_to_stat(int(modified or 0)), filetime_to_stat(int(created or 0)),
                            int(attributes or 0))
    finally:
        path.unlink(missing_ok=True)
