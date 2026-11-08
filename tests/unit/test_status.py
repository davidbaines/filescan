from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from filescan.cli import build_parser, main
from filescan.config import ConfigNotSet, load_config, resolve_config_path
from filescan.inventory.scanner import run_scan
from filescan.status import gather, summarise_actions


def test_config_comes_from_flag_then_environment_then_env_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text('# comment\nFILESCAN_CONFIG="C:\\filescan\\v2\\config.yml"\n', encoding="utf-8")
    assert resolve_config_path(Path("given.yml"), environ={}, env_file=env_file)[0] == Path("given.yml")
    assert resolve_config_path(None, environ={"FILESCAN_CONFIG": "env.yml"}, env_file=env_file)[0] == Path("env.yml")
    path, source = resolve_config_path(None, environ={}, env_file=env_file)
    assert path == Path(r"C:\filescan\v2\config.yml") and source == str(env_file)
    with pytest.raises(ConfigNotSet):
        resolve_config_path(None, environ={}, env_file=tmp_path / "absent.env")


def test_no_command_prints_grouped_help(capsys):
    assert main([]) == 0
    out = capsys.readouterr().out
    assert "Start here" in out and "3. Clean up" in out and "delete-folder" in out
    assert out.index("\n  status") < out.index("\n  scan ")


def test_every_listed_command_exists():
    from filescan.cli import COMMAND_GROUPS
    parser = build_parser()
    for _, items in COMMAND_GROUPS:
        for name, _ in items:
            parser.parse_args([name] + (["x"] if name in ("delete-folder", "retire-drive") else [])
                              + (["--plan", "p"] if name in ("execute", "cleanup", "cleanup-restore",
                                                             "cleanup-purge") else []))


def test_summarise_actions_uses_the_last_record_per_path_and_caches(tmp_path):
    actions = tmp_path / "actions.jsonl"
    lines = [
        {"path": "a", "op": "quarantine", "status": "error", "at": "2026-11-05T10:00:00"},
        {"path": "a", "op": "quarantine", "status": "moved", "at": "2026-11-06T10:00:00"},
        {"path": "b", "op": "quarantine", "status": "moved", "at": "2026-11-05T09:00:00"},
        {"path": "b", "op": "restore", "status": "moved", "at": "2026-11-06T11:00:00"},
        {"path": "c", "op": "quarantine", "status": "error", "at": "2026-11-06T12:00:00"},
    ]
    actions.write_text("".join(json.dumps(r) + "\n" for r in lines) + '{"cut short', encoding="utf-8")
    summary = summarise_actions(actions)
    assert summary["counts"] == {"in quarantine": 1, "restored": 1, "error": 1}
    assert (tmp_path / "status.json").exists()
    assert summarise_actions(actions) == summary


def test_status_lists_what_to_do(tmp_path):
    pc = tmp_path / "pc"
    (pc / "a").mkdir(parents=True)
    (pc / "a" / "f.txt").write_text("x")
    fs = tmp_path / "fs"
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}],
        "exclusions": [{"name": "server-trash", "pattern": r"\\Trash\\", "reason": "Trash"},
                       {"name": "corpora", "pattern": r"\\Corpora\\", "reason": "public"}],
        "filescan_folder": str(fs), "database_folder": str(fs), "database_filename": "db.sqlite",
        "report_filename": "r.xlsx", "min_file_size": 0,
    }))
    run_scan(config_path)
    config = load_config(config_path)
    (fs / "review_server-trash.csv").write_text(
        "decision,suggestion,reason,unique_bytes,total_bytes,files,kind,path\n"
        "trash,,,1,1,1,tree,X\n,,,1,1,1,tree,Y\n", encoding="utf-8-sig")
    plan = fs / "cleanup" / "20261106-000000"
    plan.mkdir(parents=True)
    (plan / "files.csv").write_text("path\n", encoding="utf-8")
    (plan / "plan.json").write_text(json.dumps({"files": 1, "bytes": 10}), encoding="utf-8")
    (plan / "actions.jsonl").write_text(json.dumps(
        {"path": "a", "op": "quarantine", "status": "moved", "at": "2026-11-01T10:00:00"}) + "\n", encoding="utf-8")
    status = gather(config, now=datetime(2026, 11, 20))
    todo = "\n".join(status.todo)
    assert "full hash" in todo
    assert "review server-trash: 1 folder undecided" in todo
    assert "review corpora: not started" in todo
    assert "cleanup-purge" in todo
    assert "layout.yml" in todo


def test_status_counts_files_without_a_hash_until_hash_all_has_run(tmp_path):
    from filescan.dedupe.hash_all import run_hash_all

    pc = tmp_path / "pc"
    (pc / "a").mkdir(parents=True)
    (pc / "a" / "f.txt").write_text("x")
    (pc / "a" / "g.txt").write_text("yy")
    fs = tmp_path / "fs"
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(pc)}], "filescan_folder": str(fs), "database_folder": str(fs),
        "database_filename": "db.sqlite", "report_filename": "r.xlsx", "min_file_size": 0,
    }))
    run_scan(config_path)
    config = load_config(config_path)
    status = gather(config)
    assert "hashes: 2 files (0.0 GB) have no full hash" in "\n".join(status.lines)
    assert any(item.startswith("scan: give") for item in status.todo)
    run_hash_all(config_path)
    status = gather(config)
    assert "hashes: every file has a full hash" in "\n".join(status.lines)
    assert not any(item.startswith("scan: give") for item in status.todo)


def test_status_counts_only_layout_entries_marked_as_guesses(tmp_path):
    fs = tmp_path / "fs"
    fs.mkdir()
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(tmp_path)}], "filescan_folder": str(fs), "database_folder": str(fs),
        "database_filename": "db.sqlite", "report_filename": "r.xlsx",
    }))
    (fs / "layout.yml").write_text(
        "# Lines marked  # ?  are guesses that need your decision.\n"
        "map:\n"
        "  'E:/Picasa': Photos/Picasa\n"
        "  'E:/Odd': Unsorted/Odd   # ? maybe Work\n", encoding="utf-8")
    text = "\n".join(gather(load_config(config_path)).lines)
    assert "1 entries marked '# ?'" in text
