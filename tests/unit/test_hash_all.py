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


def test_a_deleted_unreadable_file_drops_off_the_list_even_when_nothing_is_left_to_hash(tmp_path, monkeypatch):
    config_path = _pc_config(tmp_path)

    def locked(hasher, record):
        raise PermissionError(13, "Permission denied", str(record.path))

    monkeypatch.setattr(hash_all, "hash_if_unchanged", locked)
    run_hash_all(config_path)
    db = SQLiteDB(load_config(config_path).database_path)
    assert db.conn.execute("SELECT COUNT(*) FROM access_problems").fetchone()[0] == 2
    db.close()
    (tmp_path / "pc" / "Docs" / "unique.txt").unlink()
    run_scan(config_path, delta=True)
    run_hash_all(config_path)  # the other file is still on the list, so nothing is hashed
    db = SQLiteDB(load_config(config_path).database_path)
    names = {Path(r[0]).name for r in db.conn.execute("SELECT path FROM access_problems")}
    assert "unique.txt" not in names and "edited.txt" in names
    db.close()


def test_full_hash_reports_each_chunk(tmp_path: Path) -> None:
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * ((1 << 20) * 2 + 5))
    chunks: list[int] = []
    assert full_hash(big, progress=chunks.append) == full_hash(big)
    assert chunks == [1 << 20, 1 << 20, 5]




def test_progress_bar_moves_during_a_read_and_never_passes_or_goes_back_from_its_total(tmp_path: Path, monkeypatch):
    """The bar moves while a file is being read; a file that grows, or fails partway, counts as its recorded size."""
    import time

    config = load_config(_pc_config(tmp_path, extra=3))
    docs = tmp_path / "pc" / "Docs"
    sizes = {p.name: p.stat().st_size for p in docs.iterdir()}
    bars, steps = [], []
    real_bar = hash_all.progress_bar

    def recording_bar(**kwargs):
        bar = real_bar(**kwargs)
        real_update = bar.update

        def update(n=1):
            steps.append(n)
            return real_update(n)

        bar.update = update
        bars.append(bar)
        return bar

    seen_part_of_n0 = []

    def fake_full_hash(path: Path, progress=None) -> str:
        if path.name == "edited.txt":
            progress(10 * sizes["edited.txt"])  # grew while it was read
        elif path.name == "unique.txt":
            progress(5)
            raise OSError("the disk went away partway through")
        elif path.name == "n0.txt":
            progress(50)
            # Wait until every other file is done and the bar shows these 50 bytes of n0.txt.
            bar = bars[-1]
            expected = bar.total - (sizes["n0.txt"] - 50)
            deadline = time.monotonic() + 5
            while bar.n != expected and time.monotonic() < deadline:
                time.sleep(0.05)
            seen_part_of_n0.append(bar.n == expected)
            progress(sizes["n0.txt"] - 50)
        else:
            progress(path.stat().st_size)
        return "0" * 32

    monkeypatch.setattr(hash_all, "progress_bar", recording_bar)
    monkeypatch.setattr(hash_all, "full_hash", fake_full_hash)
    run_hash_all_for_config(config)
    pc_bar = next(b for b in bars if b.desc.startswith("PC files hashed"))
    assert seen_part_of_n0 == [True]
    assert all(step > 0 for step in steps)
    assert pc_bar.n == pc_bar.total


def test_a_file_that_grows_counts_for_no_more_than_its_recorded_size() -> None:
    bytes_read = hash_all._BytesRead()
    counted, progress = bytes_read.counter(100)
    progress(60)
    progress(60)  # the file grew while it was read
    progress(60)
    assert (counted[0], bytes_read.total) == (100, 100)


def test_stopping_ends_a_read_at_its_next_chunk() -> None:
    import pytest

    bytes_read = hash_all._BytesRead()
    _, progress = bytes_read.counter(100)
    progress(10)
    bytes_read.stopping.set()
    with pytest.raises(hash_all._Stopped):
        progress(10)
