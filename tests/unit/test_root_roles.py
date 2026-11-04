from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from filescan.config import load_config
from filescan.inventory.scanner import InventoryScanner
from filescan.storage import FileRepository, SQLiteDB
from tests.helpers.tree_builder import build_tree


def _write(tmp_path: Path, payload: dict) -> Path:
    base = {
        "filescan_folder": str(tmp_path / "filescan"),
        "database_folder": str(tmp_path / "filescan"),
        "database_filename": "file_index.db",
        "report_filename": "report.xlsx",
        "worker_count": 2,
    }
    base.update(payload)
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(base))
    return path


def test_roots_default_to_pc_source_and_parse_roles(tmp_path: Path) -> None:
    config = load_config(
        _write(
            tmp_path,
            {
                "roots": [
                    {"path": "C:/Users/David", "skip": ["AppData"]},
                    {"path": "//192.168.1.133/disk3", "location": "server"},
                    {"path": "//192.168.1.133/disk2", "location": "server", "role": "master"},
                ]
            },
        )
    )

    by_path = {str(r.path): r for r in config.root_configs}
    pc = by_path["C:\\Users\\David"]
    assert (pc.location, pc.role) == ("pc", "source")
    assert pc.skip == (Path("C:/Users/David/AppData"),)
    assert by_path["\\\\192.168.1.133\\disk3\\"].location == "server"
    assert by_path["\\\\192.168.1.133\\disk2\\"].role == "master"
    assert [str(p) for p in config.pc_roots] == ["C:\\Users\\David"]
    assert len(config.server_roots) == 2
    assert len(config.roots) == 3


def test_invalid_location_or_role_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="location"):
        load_config(_write(tmp_path, {"roots": [{"path": "C:/x", "location": "cloud"}]}))
    with pytest.raises(ValueError, match="role"):
        load_config(_write(tmp_path, {"roots": [{"path": "C:/x", "role": "backup"}]}))


def test_exclusions_and_pinned_are_parsed(tmp_path: Path) -> None:
    config = load_config(
        _write(
            tmp_path,
            {
                "roots": [{"path": "D:/"}],
                "exclusions": [
                    {"name": "old-os", "pattern": r"^[A-Z]:\\(Windows|Program Files)\\", "reason": "Old OS"},
                ],
                "pinned": ["C:/Users/David/iCloudDrive"],
            },
        )
    )

    assert [rule.name for rule in config.exclusions] == ["old-os"]
    assert config.exclusions[0].matches(Path("D:/Windows/System32"))
    assert not config.exclusions[0].matches(Path("D:/GitHub/Windows/notes"))
    assert config.pinned == (Path("C:/Users/David/iCloudDrive"),)


def test_invalid_exclusion_pattern_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exclusion"):
        load_config(_write(tmp_path, {"roots": [{"path": "D:/"}], "exclusions": [{"name": "bad", "pattern": "("}]}))


def test_scanner_skips_server_roots_and_root_skip_paths(tmp_path: Path, monkeypatch) -> None:
    pc_root = tmp_path / "pc"
    server_root = tmp_path / "server"
    build_tree(pc_root, {"keep/a.txt": b"a", "AppData/cache.bin": b"c"})
    build_tree(server_root, {"b.txt": b"b"})
    config = load_config(
        _write(
            tmp_path,
            {
                "roots": [
                    {"path": str(pc_root), "skip": ["AppData"]},
                    # Server roots are listed by fsagent, never walked; a local
                    # folder stands in for the UNC share here.
                    {"path": str(server_root), "location": "server"},
                ]
            },
        )
    )

    InventoryScanner(config, rescan=True).scan()

    db = SQLiteDB(config.database_path)
    names = sorted(f.filename for f in FileRepository(db).list_active_files())
    folders = [str(f.path) for f in FileRepository(db).list_active_folders()]
    db.close()
    assert names == ["a.txt"]
    skipped = str(pc_root / "AppData")
    assert not any(p == skipped or p.startswith(skipped + "\\") for p in folders)


def test_duplicate_detector_ignores_files_under_server_roots(tmp_path: Path) -> None:
    from filescan.dedupe.duplicates import DuplicateDetector
    from tests.helpers.indexing import index_paths

    pc_root = tmp_path / "pc"
    server_root = tmp_path / "server"
    pc_files = build_tree(pc_root, {"x/a.bin": b"duplicate!", "y/b.bin": b"duplicate!"})
    server_files = build_tree(server_root, {"c.bin": b"duplicate!"})
    config = load_config(
        _write(
            tmp_path,
            {
                "roots": [{"path": str(pc_root)}, {"path": str(server_root), "location": "server"}],
                "duplicate_size_threshold": 1,
            },
        )
    )
    db = SQLiteDB(config.database_path)
    repo = FileRepository(db)
    index_paths(repo, pc_root, pc_files)
    index_paths(repo, server_root, server_files)

    groups = DuplicateDetector(config).run()

    assert [sorted(f.filename for f in g.files) for g in groups] == [["a.bin", "b.bin"]]
    hashed = {r[0] for r in db.conn.execute("SELECT f.filename FROM file_hashes h JOIN files f ON f.id = h.file_id")}
    assert "c.bin" not in hashed
    db.close()
