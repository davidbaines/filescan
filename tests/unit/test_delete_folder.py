from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

from filescan.cleanup.delete_folder import check_folder, run_delete_folder
from filescan.config import load_config
from filescan.dedupe.hashing import full_hash
from filescan.inventory.scanner import run_scan
from filescan.storage import SQLiteDB
from tests.helpers.tree_builder import build_tree


def _setup(tmp_path: Path):
    pc = tmp_path / "pc"
    build_tree(pc, {
        "Old/profile/a.txt": b"only here",
        "Old/profile/dup.txt": b"kept elsewhere",
        "Old/profile/deep/b.txt": b"deeper",
        "Docs/dup.txt": b"kept elsewhere",
        "Live/sentinel.txt": b"must survive",
        "Pinned/p.txt": b"pinned",
    })
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}],
        "pinned": [str(pc / "Pinned")],
        "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite",
        "report_filename": "r.xlsx",
        "min_file_size": 0,
    }))
    run_scan(config_path)
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    for row in db.conn.execute("SELECT id, path FROM files").fetchall():
        db.conn.execute("INSERT OR REPLACE INTO file_hashes (file_id, full_hash) VALUES (?, ?)",
                        (row["id"], full_hash(Path(row["path"]))))
    db.conn.commit()
    db.close()
    return config, pc


def _missing(config, path: Path) -> int:
    db = SQLiteDB(config.database_path)
    try:
        return int(db.conn.execute("SELECT is_missing FROM files WHERE path = ?", (str(path),)).fetchone()[0])
    finally:
        db.close()


def test_report_only_deletes_nothing_and_counts_copies(tmp_path):
    config, pc = _setup(tmp_path)
    (pc / "Old" / "profile" / "new.txt").write_bytes(b"added after the scan")
    [report] = run_delete_folder(config, [str(pc / "old" / "PROFILE")])  # case and spelling as typed
    assert not report.problem
    assert report.db_files == 3
    assert report.covered_bytes == len(b"kept elsewhere")
    assert report.unique_files == 2
    assert report.new_files == 1
    assert (pc / "Old" / "profile" / "a.txt").exists()


def test_yes_deletes_and_marks_missing(tmp_path):
    config, pc = _setup(tmp_path)
    run_delete_folder(config, [str(pc / "Old" / "profile")], yes=True)
    assert not (pc / "Old" / "profile").exists()
    assert (pc / "Old").exists()
    assert _missing(config, pc / "Old" / "profile" / "deep" / "b.txt") == 1
    assert _missing(config, pc / "Docs" / "dup.txt") == 0
    logs = list((config.filescan_folder / "deletions").glob("*.jsonl"))
    records = [json.loads(line) for log in logs for line in log.read_text(encoding="utf-8").splitlines()]
    assert any(r["kind"] == "folder" and r["gone"] for r in records)


def test_refuses_roots_pinned_and_outside(tmp_path):
    config, pc = _setup(tmp_path)
    assert "root" in check_folder(str(pc), config)
    assert "pinned" in check_folder(str(pc / "Pinned"), config)
    assert "pinned" in check_folder(str(pc), config) or "root" in check_folder(str(pc), config)
    assert check_folder(str(tmp_path / "elsewhere"), config)
    run_delete_folder(config, [str(pc / "Pinned")], yes=True)
    assert (pc / "Pinned" / "p.txt").exists()


def test_never_goes_through_a_junction(tmp_path):
    config, pc = _setup(tmp_path)
    link = pc / "Old" / "profile" / "Application Data"
    made = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(pc / "Live")], capture_output=True)
    if made.returncode != 0:
        pytest.skip(f"could not create a junction: {made.stderr!r}")
    [report] = run_delete_folder(config, [str(pc / "Old" / "profile")])
    assert len(report.links) == 1
    run_delete_folder(config, [str(pc / "Old" / "profile")], yes=True)
    assert not (pc / "Old" / "profile").exists()
    assert (pc / "Live" / "sentinel.txt").read_bytes() == b"must survive"
    assert _missing(config, pc / "Live" / "sentinel.txt") == 0


def test_refuses_a_folder_that_is_itself_a_junction(tmp_path):
    config, pc = _setup(tmp_path)
    link = pc / "LinkToLive"
    made = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(pc / "Live")], capture_output=True)
    if made.returncode != 0:
        pytest.skip(f"could not create a junction: {made.stderr!r}")
    assert "junction" in check_folder(str(link), config)
    run_delete_folder(config, [str(link)], yes=True)
    assert (pc / "Live" / "sentinel.txt").exists()
