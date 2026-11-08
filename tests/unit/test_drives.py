from __future__ import annotations


import pytest
import yaml

from filescan.inventory import volumes
from filescan.inventory.volumes import MARKER, insert_root, root_lines, root_problem
from filescan.models import RootConfig

FP = {"serial": "AB12-CD34", "size": 4_000_000_000_000, "label": "MyBook", "filesystem": "NTFS"}


def test_a_marker_is_never_written_on_the_system_drive(tmp_path):
    with pytest.raises(ValueError):
        volumes.write_marker(volumes.SYSTEM_DRIVE + "\\", "System")


def test_write_marker_keeps_an_existing_volume_id(tmp_path, monkeypatch):
    monkeypatch.setattr(volumes, "marker_path", lambda _v: tmp_path / MARKER)
    monkeypatch.setattr(volumes, "is_system_drive", lambda _v: False)
    first = volumes.write_marker("K:\\", "WD My Book Black", fp=FP)
    again = volumes.write_marker("K:\\", "WD My Book Black", fp=FP)
    assert first["volume_id"] == again["volume_id"]
    assert volumes.read_marker("K:\\")["name"] == "WD My Book Black"


def _check(root, *, serial, marker=None, fp=FP, letters=(), elsewhere=None):
    """root_problem with the drive's serial, marker and fingerprint supplied, and other letters to search."""
    def serial_of(p):
        return elsewhere.get(str(p)[:2], {}).get("serial") if elsewhere and str(p)[:2] in elsewhere else serial

    def marker_of(p):
        return elsewhere.get(str(p)[:2], {}).get("marker") if elsewhere and str(p)[:2] in elsewhere else marker
    return root_problem(root, serial_of=serial_of, marker_of=marker_of, fingerprint_of=lambda _p: fp,
                        letters=list(letters))


def test_a_root_passes_on_its_serial_or_on_its_marker(tmp_path):
    by_serial = RootConfig(path=tmp_path, name="Disk D", serial="6C35-B138")
    assert _check(by_serial, serial="6C35-B138") is None
    by_marker = RootConfig(path=tmp_path, name="WD My Book Black", volume_id="v1")
    assert _check(by_marker, serial="AB12-CD34", marker={"volume_id": "v1", "fingerprint": FP}) is None


def test_a_copied_marker_is_not_trusted(tmp_path):
    root = RootConfig(path=tmp_path, name="WD My Book Black", volume_id="v1")
    cloned = {"volume_id": "v1", "fingerprint": {**FP, "serial": "1111-2222"}}
    problem = _check(root, serial="AB12-CD34", marker=cloned)
    assert problem and "copied from another drive" in problem


def test_a_drive_on_the_wrong_letter_is_named_and_not_followed(tmp_path):
    root = RootConfig(path=tmp_path, name="LaCie Quadra", serial="868A-45A5")
    problem = _check(root, serial="9999-0000", letters=["K:"], elsewhere={"K:": {"serial": "868A-45A5"}})
    assert problem and "different drive" in problem and "on K: now" in problem
    gone = RootConfig(path=tmp_path / "unplugged", name="LaCie Quadra", serial="868A-45A5")
    problem = _check(gone, serial=None, letters=["K:"], elsewhere={"K:": {"serial": "868A-45A5"}})
    assert "not connected" in problem and "on K: now" in problem


CONFIG = """\
# my notes at the top
roots:
  # --- PC sources ---
  - path: "D:/"
    name: "Disk D"
    serial: "6C35-B138"
  - path: "J:/"
    name: "LaCie Quadra"
    serial: "868A-45A5"
  # To add a drive: ...

  # --- Server sources ---
  - path: "//192.168.1.133/disk1"
    location: server
    server_path: /srv/disk1

server:
  ssh: david@192.168.1.133
"""


def test_a_new_drive_goes_after_the_last_pc_root_and_comments_are_kept():
    text = insert_root(CONFIG, root_lines("K:", "WD My Book Black", "AB12-CD34", "v1"))
    assert "# my notes at the top" in text and "# --- Server sources ---" in text
    roots = yaml.safe_load(text)["roots"]
    assert [r["path"] for r in roots] == ["D:/", "J:/", "K:/", "//192.168.1.133/disk1"]
    assert roots[2] == {"path": "K:/", "name": "WD My Book Black", "serial": "AB12-CD34", "volume_id": "v1"}


def test_retire_marks_the_drive_missing_only_when_it_is_not_connected(tmp_path):
    from filescan.config import load_config
    from filescan.inventory.scanner import run_scan
    from filescan.storage import SQLiteDB

    drive = tmp_path / "drive"
    (drive / "a").mkdir(parents=True)
    (drive / "a" / "f.txt").write_text("x")
    (drive / MARKER).write_text("volume_id: v1\n")               # the marker is never inventoried
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(drive), "name": "Old Drive"}], "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"), "database_filename": "db.sqlite", "min_file_size": 0}))
    run_scan(config_path)
    config = load_config(config_path)
    db = SQLiteDB(config.database_path)
    try:
        names = [r["filename"] for r in db.conn.execute("SELECT filename FROM files")]
        assert names == ["f.txt"]
        assert volumes.retire_drive(config, "old drive", db) == 1
        assert db.conn.execute("SELECT COUNT(*) FROM files WHERE is_missing = 0").fetchone()[0] == 0
    finally:
        db.close()


def test_cloud_drives_are_recognised_by_label():
    assert volumes.is_cloud_drive({"label": "david_baines@sil.org - Google..."})  # as Windows shortens it
    assert volumes.is_cloud_drive({"label": "davidcbaines@gmail.com - Goog..."})  # cut before "Google"
    assert volumes.is_cloud_drive({"label": "My Drive", "serial": "1983-1116"})
    assert not volumes.is_cloud_drive({"label": "MyBook", "serial": "868A-45A5"})
