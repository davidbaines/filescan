from __future__ import annotations

from pathlib import Path

import yaml

from filescan.inventory import drive_check
from filescan.inventory.drive_check import Attached, check_drives


def _unused_letters() -> list[str]:
    """Letters free on this PC: a mapped network drive would make a root resolve to its share."""
    return [f"{x}:" for x in "XVUTSRQPONML" if Path(f"{x}:/").resolve().drive.upper() == f"{x}:"]


A, B, C = _unused_letters()[:3]

CONFIG = f"""\
# my notes at the top
roots:
  # --- PC sources ---
  - path: "{A}/"
    name: "Disk X"
    serial: "6C35-B138"
    skip: ["Temp"]            # never inventoried
  - path: "{B}/"
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


def _drive(letter, serial, label, size, *, model="", free=10**9, filesystem="NTFS"):
    return Attached(letter, {"serial": serial, "size": size, "label": label, "filesystem": filesystem}, None,
                    {"model": model, "bus": "USB"} if model else {}, free)


def _setup(tmp_path, monkeypatch, drives, text=CONFIG):
    config_path = tmp_path / "config.yml"
    config_path.write_text(text + f"filescan_folder: '{tmp_path / 'fs'}'\ndatabase_folder: '{tmp_path / 'fs'}'\n"
                           "database_filename: db.sqlite\n", encoding="utf-8")
    monkeypatch.setattr(drive_check, "attached_drives", lambda letters=None, details=None: list(drives))
    monkeypatch.setattr(drive_check, "write_marker", lambda volume, name, fp=None: {"volume_id": "v-new"})
    return config_path


def _check(config_path, answers=None):
    replies = iter(answers or [])
    ask = (lambda _prompt: next(replies)) if answers is not None else None
    return check_drives(config_path, ask=ask, problem_of=lambda rc: None)


X = _drive(A, "6C35-B138", "Data", 350_000_000_000, model="WDS100T3XHC")
Y = _drive(B, "868A-45A5", "LaCie", 500_105_216_000, model="LaCie d2 quadra")


def test_known_drives_get_their_details_saved_as_fields_and_comments_are_kept(tmp_path, monkeypatch):
    config_path = _setup(tmp_path, monkeypatch, [X, Y])
    result = _check(config_path)
    text = config_path.read_text(encoding="utf-8")
    for comment in ("# my notes at the top", "# never inventoried", "# To add a drive: ...", "# --- Server sources ---"):
        assert comment in text
    # the comments that introduce the server roots stay below the new fields of the last PC root

    assert text.index("model: \"LaCie d2 quadra\"") < text.index("# To add a drive")
    roots = yaml.safe_load(text)["roots"]
    assert roots[0]["label"] == "Data" and roots[0]["size_gb"] == 350.0 and roots[0]["model"] == "WDS100T3XHC"
    assert "free" not in str(roots[0]).lower()
    assert [r[2] for r in result.rows] == ["scanned", "scanned"]
    assert result.backup is not None and result.backup.exists()

    again = _check(config_path)                       # nothing has changed, so nothing is written
    assert again.backup is None and again.messages == []


def test_a_changed_label_is_reported_and_updated(tmp_path, monkeypatch):
    config_path = _setup(tmp_path, monkeypatch, [X, Y])
    _check(config_path)
    renamed = _drive(B, "868A-45A5", "Archive", 500_105_216_000, model="LaCie d2 quadra")
    monkeypatch.setattr(drive_check, "attached_drives", lambda letters=None, details=None: [X, renamed])
    result = _check(config_path)
    assert any('from "LaCie" to "Archive"' in m for m in result.messages)
    assert yaml.safe_load(config_path.read_text(encoding="utf-8"))["roots"][1]["label"] == "Archive"


def test_a_new_drive_to_scan_is_added_after_the_last_pc_root(tmp_path, monkeypatch):
    k = _drive(C, "AB12-CD34", "MyBook", 8_001_000_000_000, model="WD My Book 25EE")
    config_path = _setup(tmp_path, monkeypatch, [X, Y, k])
    result = _check(config_path, ["WD My Book Black", "s"])
    text = config_path.read_text(encoding="utf-8")
    roots = yaml.safe_load(text)["roots"]
    assert [r["path"] for r in roots] == [f"{A}/", f"{B}/", f"{C}/", "//192.168.1.133/disk1"]
    assert roots[2]["name"] == "WD My Book Black" and roots[2]["volume_id"] == "v-new"
    assert roots[2]["serial"] == "AB12-CD34" and roots[2]["size_gb"] == 8001.0
    assert text.index(f'path: "{C}/"') < text.index("# --- Server sources ---")
    assert (C, "WD My Book Black", "scanned (new)") == result.rows[2][:3]


def test_two_google_drives_with_one_serial_are_kept_apart(tmp_path, monkeypatch):
    g = _drive("G:", "1983-1116", "david_baines@sil.org - Google...", 999_538_085_888, filesystem="FAT32")
    i = _drive("I:", "1983-1116", "davidcbaines@gmail.com - Goog...", 16_106_127_360, filesystem="FAT32")
    config_path = _setup(tmp_path, monkeypatch, [X, Y, g, i])
    # a cloud drive is never offered for scanning: only its name is asked
    _check(config_path, ["Google Drive (SIL)", "Google Drive (Gmail)"])
    ignored = yaml.safe_load(config_path.read_text(encoding="utf-8"))["ignored_drives"]
    assert [(e["name"], e["letter"]) for e in ignored] == [("Google Drive (SIL)", "G:"), ("Google Drive (Gmail)", "I:")]

    again = _check(config_path, [])                   # both are recognised; nothing is asked or written
    assert [r[1:3] for r in again.rows[2:]] == [("Google Drive (SIL)", "ignored"), ("Google Drive (Gmail)", "ignored")]
    assert again.backup is None


def test_outside_a_terminal_a_new_drive_is_listed_and_config_is_unchanged(tmp_path, monkeypatch):
    k = _drive(C, "AB12-CD34", "MyBook", 8_001_000_000_000)
    config_path = _setup(tmp_path, monkeypatch, [k])
    before = config_path.read_text(encoding="utf-8")
    result = _check(config_path)
    assert result.left_for_later == [C] and not result.asked
    assert config_path.read_text(encoding="utf-8") == before
    assert {n.split(" (")[0] for n in result.not_attached} == {"Disk X", "LaCie Quadra"}


def test_a_name_with_accents_is_saved_and_read_back_unchanged(tmp_path, monkeypatch):
    from filescan.config import load_config

    k = _drive(C, "AB12-CD34", "Fotos", 2_000_000_000_000)
    config_path = _setup(tmp_path, monkeypatch, [k])
    _check(config_path, ["Álbum de Évelyn", "s"])
    assert [rc.name for rc in load_config(config_path).pc_root_configs][-1] == "Álbum de Évelyn"
