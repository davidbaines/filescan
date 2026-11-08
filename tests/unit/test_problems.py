from __future__ import annotations

import csv
from pathlib import Path

import yaml

from filescan.problems import count, ensure_table, replace_tried, replace_under, rollup_folder, write_unreadable_report
from filescan.storage import SQLiteDB


def _db(tmp_path: Path) -> SQLiteDB:
    db = SQLiteDB(tmp_path / "db.sqlite")
    ensure_table(db)
    return db


def test_a_step_replaces_only_what_it_tried_again(tmp_path):
    db = _db(tmp_path)
    replace_under(db, "scan", r"D:\\", [("folder", r"D:\Windows\Temp", "WinError 5")])
    replace_under(db, "scan", r"E:\\", [("folder", r"E:\Locked", "WinError 5")])
    replace_under(db, "scan", r"D:\\", [])                      # D: walked again and now readable
    assert [r["path"] for r in db.conn.execute("SELECT path FROM access_problems")] == [r"E:\Locked"]

    replace_tried(db, "hash", [r"E:\a.bin", r"E:\b.bin"], [("file", r"E:\a.bin", "Permission denied")])
    replace_tried(db, "hash", [r"E:\b.bin"], [])                 # an interrupted run that only got to b.bin
    assert count(db) == 2                                       # E:\Locked and E:\a.bin are still listed
    db.close()


def test_rollup_keeps_two_levels_below_the_drive_or_share():
    assert rollup_folder(r"E:\Downloads\Software\x\y.exe") == r"E:\Downloads\Software"
    assert rollup_folder(r"\\srv\disk3\photos\2004\a.jpg") == r"\\srv\disk3\photos\2004"
    assert rollup_folder(r"D:\Windows\Temp") == r"D:\Windows\Temp"


def test_scan_stores_unreadable_folders_and_the_report_lists_them(tmp_path, monkeypatch):
    from filescan.config import load_config
    from filescan.inventory import scanner as scanner_module
    from filescan.inventory.scanner import run_scan

    pc = tmp_path / "pc"
    (pc / "ok").mkdir(parents=True)
    (pc / "ok" / "a.txt").write_text("a")
    (pc / "locked").mkdir()
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}], "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"), "database_filename": "db.sqlite", "min_file_size": 0}))
    real_scandir = scanner_module.os.scandir

    def scandir(path):
        if Path(path).name == "locked":
            raise PermissionError(13, "Access is denied", str(path))
        return real_scandir(path)

    monkeypatch.setattr(scanner_module.os, "scandir", scandir)
    run_scan(config_path)
    config = load_config(config_path)
    folder_csv, files_csv = write_unreadable_report(config)
    rows = list(csv.DictReader(files_csv.open(encoding="utf-8-sig")))
    assert [(r["step"], Path(r["path"]).name) for r in rows] == [("scan", "locked")]
    assert list(csv.DictReader(folder_csv.open(encoding="utf-8-sig")))[0]["items"] == "1"


def test_scan_leaves_out_folders_it_could_not_read_before_but_keeps_them_listed(tmp_path, monkeypatch):
    from filescan.config import load_config
    from filescan.inventory import scanner as scanner_module
    from filescan.inventory.scanner import run_scan

    pc = tmp_path / "pc"
    (pc / "locked").mkdir(parents=True)
    (pc / "locked" / "a.txt").write_text("a")
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}], "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"), "database_filename": "db.sqlite", "min_file_size": 0}))
    real_scandir = scanner_module.os.scandir
    tried: list[str] = []

    def scandir(path):
        if Path(path).name == "locked":
            tried.append(str(path))
            raise PermissionError(13, "Access is denied", str(path))
        return real_scandir(path)

    monkeypatch.setattr(scanner_module.os, "scandir", scandir)
    for rescan in (False, True):  # a full rescan leaves it out too
        run_scan(config_path, rescan=rescan, delta=not rescan)
    assert len(tried) == 2  # the first scan's count and walk; neither later scan tried it
    db = SQLiteDB(load_config(config_path).database_path)
    assert [Path(r[0]).name for r in db.conn.execute("SELECT path FROM access_problems")] == ["locked"]
    db.close()

    monkeypatch.setattr(scanner_module.os, "scandir", real_scandir)  # the user made it readable
    run_scan(config_path, delta=True, retry_unreadable=True)
    db = SQLiteDB(load_config(config_path).database_path)
    assert count(db) == 0
    assert db.conn.execute("SELECT COUNT(*) FROM files WHERE filename = 'a.txt'").fetchone()[0] == 1
    db.close()
