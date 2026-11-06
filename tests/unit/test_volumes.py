from pathlib import Path

import yaml

from filescan.config import load_config
from filescan.inventory import scanner as scanner_module
from filescan.inventory import volumes
from filescan.inventory.scanner import InventoryScanner
from filescan.inventory.volumes import format_serial, root_problem, volume_root
from filescan.models import RootConfig
from filescan.storage import SQLiteDB


def test_volume_root_and_serial_format() -> None:
    assert volume_root("J:/Photos/x.jpg") == "J:\\"
    assert volume_root("//nas/share/folder") == r"\\nas\share" + "\\"
    assert format_serial(0x868A45A5) == "868A-45A5"


def test_root_problem(tmp_path: Path) -> None:
    root = RootConfig(path=tmp_path, name="LaCie Quadra", serial="868A-45A5")
    assert root_problem(root, serial_of=lambda _: "868a-45a5") is None
    wrong = root_problem(root, serial_of=lambda _: "1111-2222")
    assert wrong and "LaCie Quadra" in wrong and "1111-2222" in wrong
    assert "not connected" in root_problem(RootConfig(path=tmp_path / "gone", serial="868A-45A5"))
    assert root_problem(RootConfig(path=tmp_path)) is None  # no serial configured: not checked


def test_config_reads_name_and_serial(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(tmp_path), "name": "WD My Book Black", "serial": "ab12-cd34"}],
        "filescan_folder": str(tmp_path / "out"),
        "database_folder": str(tmp_path / "out"),
        "database_filename": "file_index.db",
    }), encoding="utf-8")
    root = load_config(config_path).root_configs[0]
    assert (root.name, root.serial) == ("WD My Book Black", "AB12-CD34")


def test_scan_skips_the_wrong_drive_and_keeps_its_records(tmp_path: Path, monkeypatch) -> None:
    data = tmp_path / "drive"
    data.mkdir()
    (data / "a.txt").write_text("a")
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": str(data), "name": "LaCie Quadra", "serial": "868A-45A5"}],
        "filescan_folder": str(tmp_path / "out"),
        "database_folder": str(tmp_path / "out"),
        "database_filename": "file_index.db",
    }), encoding="utf-8")
    config = load_config(config_path)

    monkeypatch.setattr(volumes, "volume_serial", lambda _: "868A-45A5")
    monkeypatch.setattr(scanner_module, "root_problem", lambda rc: volumes.root_problem(rc, lambda _: "868A-45A5"))
    InventoryScanner(config).scan()

    # Another drive takes the letter: the scan must leave the records alone.
    (data / "a.txt").unlink()
    (data / "other.txt").write_text("someone else's drive")
    monkeypatch.setattr(scanner_module, "root_problem", lambda rc: volumes.root_problem(rc, lambda _: "1111-2222"))
    InventoryScanner(config, delta=True).scan()

    db = SQLiteDB(config.database_path)
    rows = {Path(r["path"]).name: r["is_missing"] for r in db.conn.execute("SELECT path, is_missing FROM files")}
    db.close()
    assert rows == {"a.txt": 0}
