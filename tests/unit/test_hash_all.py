from __future__ import annotations

from pathlib import Path

import yaml

from filescan.config import load_config
from filescan.dedupe import hash_all
from filescan.dedupe.hash_all import run_hash_all, run_hash_all_for_config
from filescan.dedupe.hashing import full_hash
from filescan.inventory.scanner import run_scan
from filescan.storage import SQLiteDB
from filescan.storage.sql import FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
from tests.unit.test_compare import _local_hash_runner, _setup


def _hashes(config) -> dict[str, str]:
    db = SQLiteDB(config.database_path)
    rows = db.conn.execute("""SELECT f.filename, h.full_hash FROM files f JOIN file_hashes h ON h.file_id = f.id
                              WHERE h.full_hash IS NOT NULL""").fetchall()
    db.close()
    return {r["filename"]: r["full_hash"] for r in rows}


def _pc_config(tmp_path: Path, extra: int = 0) -> Path:
    pc, master = tmp_path / "pc", tmp_path / "master"
    (pc / "Docs").mkdir(parents=True)
    (pc / "Docs" / "unique.txt").write_text("only one of this size")
    (pc / "Docs" / "edited.txt").write_text("abc")
    (pc / "Docs" / "cloud.txt").write_text("a placeholder")
    for i in range(extra):
        (pc / "Docs" / f"n{i}.txt").write_text("x" * (100 + i))
    master.mkdir()
    (master / "copy.txt").write_text("already in the master")
    fs = tmp_path / "fs"
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}, {"path": str(master), "role": "master"}],
        "filescan_folder": str(fs), "database_folder": str(fs),
        "database_filename": "db.sqlite", "report_filename": "r.xlsx", "min_file_size": 0,
    }))
    run_scan(config_path)
    db = SQLiteDB(load_config(config_path).database_path)
    db.conn.execute("UPDATE files SET attributes = ? WHERE filename = 'cloud.txt'",
                    (FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS,))
    db.conn.commit()
    db.close()
    return config_path


def test_hash_all_hashes_every_file_once_but_not_placeholders_the_master_or_changed_files(tmp_path):
    config_path = _pc_config(tmp_path)
    (tmp_path / "pc" / "Docs" / "edited.txt").write_text("abcd")  # changed since the scan

    first = run_hash_all(config_path)
    assert (first["hashed"], first["changed"], first["unreadable"]) == (1, 1, 0)
    assert set(_hashes(load_config(config_path))) == {"unique.txt"}

    second = run_hash_all(config_path)  # restartable: only what is still missing is read
    assert second["hashed"] == 0 and second["changed"] == 1


def test_hash_all_keeps_few_files_queued_per_drive(tmp_path, monkeypatch):
    import threading
    import time

    config_path = _pc_config(tmp_path, extra=20)
    monkeypatch.setattr(hash_all, "QUEUED_PER_DRIVE", 2)
    monkeypatch.setattr(hash_all, "READERS_PER_DRIVE", 8)  # so only the queue limit can hold it to 2
    lock, state = threading.Lock(), {"out": 0, "max": 0, "calls": 0}
    real = hash_all.hash_if_unchanged

    def slow(hasher, record):
        with lock:
            state["out"] += 1
            state["calls"] += 1
            state["max"] = max(state["max"], state["out"])
        time.sleep(0.01)
        try:
            return real(hasher, record)
        finally:
            with lock:
                state["out"] -= 1

    monkeypatch.setattr(hash_all, "hash_if_unchanged", slow)
    result = run_hash_all(config_path)
    assert result["hashed"] == 22 and state["calls"] == 22
    assert state["max"] <= 2


def test_hash_all_hashes_server_files_on_the_server_and_reports_the_same_keys(tmp_path):
    config, _pc, srv = _setup(tmp_path)
    result = run_hash_all_for_config(config, server_runner=lambda rc, paths: _local_hash_runner(paths))
    assert result["server_hashed"] == 3 and result["server_errors"] == 0
    assert {"hashed", "unreadable", "changed", "outside_roots"} <= set(result)
    assert _hashes(config)["other-name.jpg"] == full_hash(srv / "david/Old/other-name.jpg")


def test_hash_all_leaves_out_files_it_could_not_read_before(tmp_path, monkeypatch):
    config_path = _pc_config(tmp_path)
    real = hash_all.hash_if_unchanged
    tried: list[str] = []

    def locked(hasher, record):
        if record.filename == "unique.txt":
            tried.append(record.filename)
            raise PermissionError(13, "Permission denied", str(record.path))
        return real(hasher, record)

    monkeypatch.setattr(hash_all, "hash_if_unchanged", locked)
    assert run_hash_all(config_path)["unreadable"] == 1
    second = run_hash_all(config_path)
    assert (second["unreadable"], second["left_unreadable"], tried) == (0, 1, ["unique.txt"])

    monkeypatch.setattr(hash_all, "hash_if_unchanged", real)  # the user made it readable
    third = run_hash_all(config_path, retry_unreadable=True)
    assert third["hashed"] == 1 and "unique.txt" in _hashes(load_config(config_path))
    db = SQLiteDB(load_config(config_path).database_path)
    assert db.conn.execute("SELECT COUNT(*) FROM access_problems").fetchone()[0] == 0
    db.close()
