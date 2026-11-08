from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import yaml

from filescan.dedupe.hashing import full_hash
from filescan.planning.copy_master import local_copy_runner, local_hash_runner, run_copy_master


def _setup(tmp_path: Path):
    """A plan by hand: one file copied on the 'server' and one from the 'PC', into a local master folder."""
    master = tmp_path / "master"
    master.mkdir()
    src = tmp_path / "src"
    src.mkdir()
    (src / "server.jpg").write_bytes(b"a photo on the server")
    (src / "pc.txt").write_bytes(b"a document on the PC")
    fs = tmp_path / "fs"
    plan = fs / "build" / "20261108-120000"
    plan.mkdir(parents=True)
    (plan / "plan.json").write_text("{}", encoding="utf-8")
    mtime = 1_000_000_000.0
    server_dst = master / "Photos" / "server.jpg"
    (plan / "server_copy.jsonl").write_text(json.dumps({
        "src": str(src / "server.jpg"), "dst": str(server_dst), "size": 21,
        "xxh128": full_hash(src / "server.jpg"), "mtime": mtime}) + "\n", encoding="utf-8")
    pc_dst = master / "Documents" / "pc.txt"
    with (plan / "pc_copy.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(["src", "dst", "size", "mtime", "full_hash"])
        writer.writerow([str(src / "pc.txt"), str(pc_dst), 20, mtime, full_hash(src / "pc.txt")])
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(src)},
                  {"path": str(master), "location": "server", "role": "master", "server_path": master.as_posix()}],
        "server": {"ssh": "unused"},
        "filescan_folder": str(fs), "database_folder": str(fs), "database_filename": "db.sqlite"}))
    return config_path, plan, src, server_dst, pc_dst, mtime


def _run(config_path, **kw):
    return run_copy_master(config_path, copy_runner=local_copy_runner, hash_runner=local_hash_runner,
                           free_bytes=10**9, **kw)


def test_without_yes_nothing_is_copied(tmp_path):
    config_path, _plan, _src, server_dst, pc_dst, _ = _setup(tmp_path)
    assert _run(config_path) == {}
    assert not server_dst.exists() and not pc_dst.exists()


def test_copies_both_ways_keep_the_plans_date_and_are_checked(tmp_path):
    config_path, plan, _src, server_dst, pc_dst, mtime = _setup(tmp_path)
    result = _run(config_path, yes=True)
    assert result["complete"] and result["on master"] == 2
    assert server_dst.read_bytes() == b"a photo on the server"
    assert pc_dst.read_bytes() == b"a document on the PC"
    assert os.stat(server_dst).st_mtime == mtime and os.stat(pc_dst).st_mtime == mtime
    assert not list(tmp_path.rglob("*.partial"))

    again = _run(config_path, yes=True)                         # a rerun copies nothing new
    assert again["complete"]
    logged = [json.loads(line) for line in (plan / "pc_copy_log.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(logged) == 1


def test_a_pc_file_changed_since_the_scan_is_not_copied_and_is_reported(tmp_path):
    config_path, plan, src, _server_dst, pc_dst, _ = _setup(tmp_path)
    (src / "pc.txt").write_bytes(b"edited after the scan")
    result = _run(config_path, yes=True)
    assert not result["complete"] and result["not copied"] == 1
    assert not pc_dst.exists()
    rows = list(csv.DictReader((plan / "not_on_master.csv").open(encoding="utf-8-sig")))
    assert [r["status"] for r in rows] == ["not copied"]


def test_an_existing_different_file_is_never_overwritten(tmp_path):
    config_path, _plan, _src, server_dst, _pc_dst, _ = _setup(tmp_path)
    server_dst.parent.mkdir(parents=True)
    server_dst.write_bytes(b"something else")
    result = _run(config_path, yes=True)
    assert server_dst.read_bytes() == b"something else"
    assert not result["complete"]


def test_not_enough_space_copies_nothing(tmp_path):
    import pytest

    config_path, _plan, _src, server_dst, pc_dst, _ = _setup(tmp_path)
    with pytest.raises(SystemExit):
        run_copy_master(config_path, yes=True, copy_runner=local_copy_runner, hash_runner=local_hash_runner,
                        free_bytes=10)
    assert not server_dst.exists() and not pc_dst.exists()
