from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from filescan.config import load_config
from filescan.inventory.server_import import ServerImporter, mtime_from_ns, posix_to_unc
from filescan.storage import FileRepository, SQLiteDB

UNC = "//192.168.1.133/disk3"
POSIX = "/srv/dev-disk-by-uuid-34a3"


def _config(tmp_path: Path, **root_extra):
    payload = {
        "roots": [{"path": UNC, "location": "server", "server_path": POSIX, **root_extra}],
        "server": {"ssh": "david@192.168.1.133"},
        "filescan_folder": str(tmp_path / "fs"),
        "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite",
        "report_filename": "r.xlsx",
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(payload))
    return load_config(path)


def _lines(*records: dict) -> list[str]:
    return [json.dumps(r) for r in records]


def _listing(mtime: int = 1_600_000_000_123_456_789) -> list[str]:
    return _lines(
        {"type": "d", "path": POSIX, "mtime_ns": mtime, "bad_name": False},
        {"type": "f", "path": f"{POSIX}/top.txt", "size": 3, "mtime_ns": mtime, "bad_name": False},
        {"type": "d", "path": f"{POSIX}/david/Photos", "mtime_ns": mtime, "bad_name": False},
        {"type": "f", "path": f"{POSIX}/david/Photos/a.jpg", "size": 10, "mtime_ns": mtime, "bad_name": False},
        {"type": "f", "path": f"{POSIX}/david/Photos/b:c.jpg", "size": 5, "mtime_ns": mtime, "bad_name": False},
        {"type": "d", "path": f"{POSIX}/david", "mtime_ns": mtime, "bad_name": False},
        {"type": "error", "path": f"{POSIX}/locked", "error": "Permission denied"},
    )


def test_server_root_config_requires_server_path(tmp_path: Path) -> None:
    config = _config(tmp_path)
    root = config.root_configs[0]
    assert root.server_path == POSIX
    assert config.server_ssh == "david@192.168.1.133"
    path = tmp_path / "bad.yml"
    path.write_text(yaml.safe_dump({
        "roots": [{"path": UNC, "location": "server"}],
        "filescan_folder": str(tmp_path), "database_folder": str(tmp_path),
        "database_filename": "d.sqlite", "report_filename": "r.xlsx",
    }))
    with pytest.raises(ValueError, match="server_path"):
        load_config(path)


def test_posix_to_unc_maps_paths_and_rejects_windows_illegal_names() -> None:
    root = Path("//192.168.1.133/disk3")
    assert posix_to_unc(f"{POSIX}/david/Photos/a.jpg", POSIX, root) == Path(r"\\192.168.1.133\disk3\david\Photos\a.jpg")
    assert posix_to_unc(POSIX, POSIX, root) == root
    assert posix_to_unc(f"{POSIX}/david/b:c.jpg", POSIX, root) is None
    assert posix_to_unc(f"{POSIX}/david/back\\slash", POSIX, root) is None
    assert posix_to_unc(f"{POSIX}/trailing.", POSIX, root) is None
    assert posix_to_unc("/elsewhere/x", POSIX, root) is None


def test_mtime_from_ns_is_stable() -> None:
    ns = 1_600_000_000_123_456_789
    assert mtime_from_ns(ns) == mtime_from_ns(ns)
    assert mtime_from_ns(ns) == pytest.approx(1_600_000_000.123456789)


def test_import_loads_folders_and_files_under_unc_paths(tmp_path: Path) -> None:
    config = _config(tmp_path)
    importer = ServerImporter(config, runner=lambda root_config: iter(_listing()))

    summary = importer.run()

    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    files = {str(f.path): f for f in repo.list_active_files()}
    folders = {str(f.path): f for f in repo.list_active_folders()}
    stats = db.conn.execute("SELECT root_path, indexed_files FROM scan_stats").fetchall()
    db.close()

    assert set(files) == {r"\\192.168.1.133\disk3\top.txt", r"\\192.168.1.133\disk3\david\Photos\a.jpg"}
    assert files[r"\\192.168.1.133\disk3\david\Photos\a.jpg"].size == 10
    assert r"\\192.168.1.133\disk3\david" in folders
    photos = folders[r"\\192.168.1.133\disk3\david\Photos"]
    assert (photos.file_count, photos.total_bytes, photos.depth) == (1, 10, 2)
    assert summary.unrepresentable == [f"{POSIX}/david/Photos/b:c.jpg"]
    assert summary.errors == 1
    assert [tuple(r) for r in stats] == [(r"\\192.168.1.133\disk3" + "\\", 2)]


def test_reimport_keeps_hashes_of_unchanged_files_and_marks_missing(tmp_path: Path) -> None:
    config = _config(tmp_path)
    ServerImporter(config, runner=lambda rc: iter(_listing())).run()
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    for record in repo.list_active_files():
        repo.upsert_file_hash(record.id, full_hash="h-" + record.filename)
    db.close()

    second = [line for line in _listing() if "top.txt" not in line]
    ServerImporter(config, runner=lambda rc: iter(second)).run()

    db = SQLiteDB(config.database_path)
    active = {f.filename: f for f in FileRepository(db).list_active_files()}
    missing = db.conn.execute("SELECT filename FROM files WHERE is_missing = 1").fetchall()
    db.close()
    assert active["a.jpg"].full_hash == "h-a.jpg"  # unchanged mtime: hash kept
    assert [r[0] for r in missing] == ["top.txt"]


def test_import_refuses_listing_outside_the_root(tmp_path: Path) -> None:
    config = _config(tmp_path)
    bad = _lines({"type": "d", "path": "/somewhere/else", "mtime_ns": 0, "bad_name": False})
    summary = ServerImporter(config, runner=lambda rc: iter(bad)).run()
    assert summary.unrepresentable == ["/somewhere/else"]


def test_the_master_root_is_never_listed(tmp_path):
    import yaml

    from filescan.config import load_config
    from filescan.inventory.server_import import ServerImporter

    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({
        "roots": [{"path": "//srv/disk3", "location": "server", "server_path": "/srv/disk3"},
                  {"path": "//srv/disk2", "location": "server", "role": "master", "server_path": "/srv/disk2"}],
        "server": {"ssh": "unused"},
        "filescan_folder": str(tmp_path / "fs"), "database_folder": str(tmp_path / "fs"),
        "database_filename": "db.sqlite"}))
    listed = []

    def runner(root_config):
        listed.append(root_config.server_path)
        return iter(['{"type": "d", "path": "%s", "mtime_ns": 0}' % root_config.server_path])

    ServerImporter(load_config(config_path), runner=runner).run()
    assert listed == ["/srv/disk3"]
