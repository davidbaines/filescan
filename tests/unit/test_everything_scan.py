"""The scan's Everything path gives the same database as a folder walk, and falls back to the walk."""

from __future__ import annotations

import csv
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from filescan.config import load_config
from filescan.inventory import everything_scan
from filescan.inventory import scanner as scanner_module
from filescan.inventory.everything import filetime_to_stat
from filescan.inventory.scanner import InventoryScanner, run_scan
from filescan.storage import SQLiteDB

_real_scandir = os.scandir  # tests replace os.scandir to make a folder unreadable; Everything still reads it
_EPOCH_TICKS = 116_444_736_000_000_000


def _ticks(ns: int) -> int:
    return ns // 100 + _EPOCH_TICKS


def fake_es(calls: list | None = None, *, drop=lambda path: False, extra: tuple[str, ...] = (),
            extra_folders: tuple[str, ...] = ()):
    """An es.exe that lists the tree as Everything does: everything under the path, junctions listed but
    not followed, folder paths ending in a backslash, dates as FILETIME ticks, 0 for a file whose only
    attribute is NORMAL, the extended-attributes bit (0x40000) on some files, the reparse bit that Windows
    hides on a OneDrive file, and an out-of-date modified date on a folder whose contents changed."""
    def run(args: list[str]) -> int:
        if calls is not None:
            calls.append(args)
        root = args[args.index("-path") + 1]
        folders = "/ad" in args
        rows, stack = [], [root]
        while stack:
            with _real_scandir(stack.pop()) as entries:
                for entry in entries:
                    st = entry.stat(follow_symlinks=False)
                    link = entry.is_symlink() or entry.is_junction()
                    is_dir = entry.is_dir(follow_symlinks=False)
                    if is_dir and not link:
                        stack.append(entry.path)
                    if is_dir == folders and not drop(entry.path):
                        attributes = st.st_file_attributes
                        if entry.name == "a.txt":
                            attributes |= 0x40000
                        if entry.name == "top.txt":
                            attributes |= 0x400  # as for a OneDrive file; it is not a link
                        modified = 0 if entry.name == "Docs" else st.st_mtime_ns
                        rows.append([entry.path + ("\\" if folders else ""), "" if folders else st.st_size, _ticks(modified),
                                     _ticks(st.st_birthtime_ns), 0 if attributes == 0x80 else attributes])
        if folders:
            rows += [[path + "\\", "", _ticks(0), _ticks(0), 0x10] for path in extra_folders]
        else:
            rows += [[path, 1, _ticks(0), _ticks(0), 0x20] for path in extra]
        out = Path(args[args.index("-export-csv") + 1])
        with out.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Filename", "Size", "Date Modified", "Date Created", "Attributes"])
            writer.writerows(rows)
        return 0
    return run


def _tree(base: Path) -> Path:
    pc = base / "pc"
    for folder in ("Docs/Old", "Docs/__pycache__", "Skip", "Locked", "_filescan_quarantine", "Empty", "Meta"):
        (pc / folder).mkdir(parents=True)
    for name, text in {"Docs/a.txt": "aaaa", "Docs/b.txt": "bbbb", "Docs/c.txt": "cc", "Docs/Old/o.txt": "o",
                       "Docs/__pycache__/x.pyc": "x", "Docs/n.tmp": "n", "Docs/big.bin": "z" * 2000,
                       "Skip/s.txt": "s", "Locked/l.txt": "l", "_filescan_quarantine/q.txt": "q",
                       "Meta/.filescan-volume.yaml": "volume_id: x", "top.txt": "t"}.items():
        (pc / name).write_text(text)
    subprocess.run(["cmd", "/c", "mklink", "/J", str(pc / "Link"), str(pc / "Docs")], check=True,
                   capture_output=True)
    return pc


def _config(base: Path, pc: Path, name: str) -> Path:
    path = base / f"{name}.yml"
    path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc), "skip": [str(pc / "Skip")]}],
        "filescan_folder": str(base / name), "database_folder": str(base / name), "database_filename": "db.sqlite",
        "exclude_folders": ["__pycache__"], "exclude_extensions": [".tmp"], "min_file_size": 0,
        "max_file_size": 1000}))
    return path


def _locked(monkeypatch) -> None:
    def scandir(path):
        if Path(path).name == "Locked":
            raise PermissionError(13, "Access is denied", str(path))
        return _real_scandir(path)
    monkeypatch.setattr(scanner_module.os, "scandir", scandir)


def _state(config_path: Path) -> dict:
    db = SQLiteDB(load_config(config_path).database_path)
    try:
        files = {tuple(r) for r in db.conn.execute(
            "SELECT path, size, mtime, ctime, attributes, is_missing FROM files")}
        folders = {tuple(r) for r in db.conn.execute(
            """SELECT f.path, f.depth, f.file_count, f.total_bytes, f.mtime, f.is_missing, p.path
               FROM folders f LEFT JOIN folders p ON p.id = f.parent_id""")}
        problems = {tuple(r) for r in db.conn.execute("SELECT stage, kind, path FROM access_problems")}
        hashed = {r[0] for r in db.conn.execute(
            "SELECT f.filename FROM files f JOIN file_hashes h ON h.file_id = f.id")}
    finally:
        db.close()
    return {"files": files, "folders": folders, "problems": problems, "hashed": hashed}


def _hash_everything(config_path: Path) -> None:
    db = SQLiteDB(load_config(config_path).database_path)
    db.conn.execute("INSERT INTO file_hashes (file_id, full_hash) SELECT id, 'h' || id FROM files")
    db.conn.commit()
    db.close()


def test_everything_gives_the_same_database_as_a_walk(tmp_path, monkeypatch, capsys):
    pc = _tree(tmp_path)
    walked, listed = _config(tmp_path, pc, "walked"), _config(tmp_path, pc, "listed")
    _locked(monkeypatch)
    calls: list = []
    for config_path in (walked, listed):
        InventoryScanner(load_config(config_path), everything_runner=fake_es(calls)).scan()
        _hash_everything(config_path)
    assert calls == []  # a root's first scan is a walk, which also finds the unreadable folders

    (pc / "Docs" / "b.txt").write_text("bbbbbb")  # changed: its hash must go
    (pc / "Docs" / "c.txt").unlink()
    (pc / "Docs" / "Old" / "o.txt").unlink()
    (pc / "Docs" / "Old").rmdir()
    (pc / "New").mkdir()
    (pc / "New" / "e.txt").write_text("e")
    (pc / "Docs" / "d.txt").write_text("d")

    run_scan(walked, rescan=True)
    capsys.readouterr()
    InventoryScanner(load_config(listed), delta=True, everything_runner=fake_es(calls)).scan()
    assert len(calls) == 2  # folders, then files
    out = capsys.readouterr().out
    assert "Everything listed 5 files" in out and "instead" not in out  # no fallback to the walk

    a, b = _state(walked), _state(listed)
    assert a["files"] == b["files"]
    assert a["folders"] == b["folders"]
    assert a["problems"] == b["problems"] == {("scan", "folder", str(pc / "Locked"))}
    assert "b.txt" not in b["hashed"] and "a.txt" in b["hashed"]
    names = {Path(f[0]).name for f in b["files"] if not f[5]}
    assert names == {"a.txt", "b.txt", "d.txt", "e.txt", "top.txt"}
    assert {Path(f[0]).name for f in b["files"] if f[5]} == {"c.txt", "o.txt"}


def _scanned_once(tmp_path: Path) -> tuple[Path, Path]:
    pc = _tree(tmp_path)
    config_path = _config(tmp_path, pc, "listed")
    run_scan(config_path)
    return pc, config_path


def test_a_failing_es_falls_back_to_the_walk(tmp_path, capsys):
    pc, config_path = _scanned_once(tmp_path)
    (pc / "Docs" / "d.txt").write_text("d")
    InventoryScanner(load_config(config_path), delta=True, everything_runner=lambda args: 8).scan()
    out = capsys.readouterr().out
    assert "error 8" in out and "walking the folders instead" in out
    assert any(Path(f[0]).name == "d.txt" for f in _state(config_path)["files"])


def test_an_empty_list_for_a_root_with_files_falls_back_to_the_walk(tmp_path, capsys):
    _, config_path = _scanned_once(tmp_path)
    InventoryScanner(load_config(config_path), delta=True, everything_runner=fake_es(drop=lambda p: True)).scan()
    assert "listed nothing" in capsys.readouterr().out
    assert not any(f[5] for f in _state(config_path)["files"])  # nothing marked gone


def test_a_list_missing_many_files_falls_back_to_the_walk(tmp_path, capsys, monkeypatch):
    _, config_path = _scanned_once(tmp_path)
    monkeypatch.setattr(everything_scan, "_GONE_FLOOR", 0)
    InventoryScanner(load_config(config_path), delta=True,
                     everything_runner=fake_es(drop=lambda p: p.endswith(".txt"))).scan()
    assert "index is incomplete" in capsys.readouterr().out
    assert not any(f[5] for f in _state(config_path)["files"])


def test_rescan_walks_without_calling_es(tmp_path):
    _, config_path = _scanned_once(tmp_path)
    calls: list = []
    InventoryScanner(load_config(config_path), rescan=True, everything_runner=fake_es(calls)).scan()
    assert calls == []


def test_a_name_everything_cannot_show_goes_on_the_unreadable_list(tmp_path):
    pc, config_path = _scanned_once(tmp_path)
    bad = str(pc / "Docs" / "bad�name.txt")
    InventoryScanner(load_config(config_path), delta=True, everything_runner=fake_es(extra=(bad,))).scan()
    state = _state(config_path)
    assert ("scan", "file", bad) in state["problems"]
    assert all(f[0] != bad for f in state["files"])


def test_retry_unreadable_lists_a_folder_that_is_readable_again(tmp_path, monkeypatch):
    pc = _tree(tmp_path)
    config_path = _config(tmp_path, pc, "listed")
    _locked(monkeypatch)
    run_scan(config_path)
    InventoryScanner(load_config(config_path), delta=True, everything_runner=fake_es()).scan()
    state = _state(config_path)
    assert ("scan", "folder", str(pc / "Locked")) in state["problems"]
    assert all(Path(f[0]).name != "l.txt" for f in state["files"])

    monkeypatch.setattr(scanner_module.os, "scandir", _real_scandir)  # the user made it readable
    InventoryScanner(load_config(config_path), delta=True, retry_unreadable=True, everything_runner=fake_es()).scan()
    state = _state(config_path)
    assert state["problems"] == set()
    assert any(Path(f[0]).name == "l.txt" and not f[5] for f in state["files"])


@pytest.mark.parametrize("when", [1_700_000_000.123_456_7, -315_619_200.5])  # 2023, and 1960
def test_filetime_conversion_matches_os_stat(tmp_path, when):
    path = tmp_path / "f.txt"
    path.write_text("x")
    os.utime(path, (when, when))
    st = os.stat(path)
    assert filetime_to_stat(_ticks(st.st_mtime_ns)) == st.st_mtime


def test_files_and_folders_in_everythings_index_but_gone_from_disk_are_left_out(tmp_path, capsys):
    pc, config_path = _scanned_once(tmp_path)
    ghost_folder = str(pc / "Ghost")
    ghosts = (str(pc / "Docs" / "ghost.txt"), str(pc / "Ghost" / "g.txt"))
    InventoryScanner(load_config(config_path), delta=True,
                     everything_runner=fake_es(extra=ghosts, extra_folders=(ghost_folder,))).scan()
    state = _state(config_path)
    assert not any(f[0] in ghosts for f in state["files"])
    assert not any(f[0] == ghost_folder for f in state["folders"])
    assert "2 files and folders in Everything's index are no longer on disk" in capsys.readouterr().out
