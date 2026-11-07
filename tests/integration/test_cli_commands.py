from __future__ import annotations

from pathlib import Path

from filescan.cli import main
from filescan.storage import SQLiteDB
from tests.helpers.tree_builder import build_tree


def test_scan_command_indexes_files_and_records_stats(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(root, {"docs/keep.txt": b"keep-me", "docs/other.txt": b"other"})
    config_path = write_config([root])

    assert main(["--config", str(config_path), "scan"]) == 0

    db = SQLiteDB(tmp_path / "filescan" / "file_index.db")
    file_count = db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0]
    stats_count = db.conn.execute("SELECT COUNT(*) FROM scan_stats").fetchone()[0]

    assert file_count == 2
    assert stats_count == 1
    db.close()


def _hashed(tmp_path: Path) -> int:
    db = SQLiteDB(tmp_path / "filescan" / "file_index.db")
    try:
        return db.conn.execute("SELECT COUNT(*) FROM file_hashes WHERE full_hash IS NOT NULL").fetchone()[0]
    finally:
        db.close()


def test_scan_records_and_hashes_every_file_in_one_command(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(root, {"a.txt": b"one", "b/c.txt": b"two and more"})
    config_path = write_config([root])
    assert main(["--config", str(config_path), "scan"]) == 0
    assert _hashed(tmp_path) == 2


def test_the_single_steps_still_work_on_their_own(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    build_tree(root, {"a.txt": b"one", "b/c.txt": b"two and more"})
    config_path = write_config([root])
    assert main(["--config", str(config_path), "scan-pc"]) == 0
    assert _hashed(tmp_path) == 0
    assert main(["--config", str(config_path), "hash-all"]) == 0
    assert _hashed(tmp_path) == 2


def test_scan_carries_on_after_a_failed_step_and_reports_it(tmp_path: Path, write_config, monkeypatch, capsys) -> None:
    import yaml

    from filescan.inventory import full_scan

    root = tmp_path / "root"
    build_tree(root, {"a.txt": b"one"})
    config_path = write_config([root])
    data = yaml.safe_load(config_path.read_text())
    data["roots"] = [{"path": str(root)}, {"path": r"\\nowhere\disk9", "location": "server",
                                           "server_path": "/srv/disk9"}]
    data["server"] = {"ssh": "nobody@nowhere"}
    config_path.write_text(yaml.safe_dump(data))

    def server_off(_config_path):
        raise ConnectionError("the server is off")

    monkeypatch.setattr(full_scan, "run_import_server", server_off)
    assert main(["--config", str(config_path), "scan"]) == 1
    assert _hashed(tmp_path) == 1  # hashing still ran for the PC file
    assert "these steps failed: import-server" in capsys.readouterr().out
