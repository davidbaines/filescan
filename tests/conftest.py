from __future__ import annotations

import sys
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from filescan.inventory.normalizer import normalize_path
from filescan.storage import FileRepository, SQLiteDB


@pytest.fixture
def tmp_path(request) -> Path:
    base = Path.home() / "AppData" / "Local" / "Temp" / "filescan-tests"
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{request.node.name}-{uuid4().hex[:8]}"
    path.mkdir(parents=True, exist_ok=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def write_config(tmp_path: Path):
    def _write(roots: list[Path], **overrides: object) -> Path:
        config_path = tmp_path / "config.yml"
        filescan_folder = tmp_path / "filescan"
        payload = {
            "roots": [{"path": str(normalize_path(root))} for root in roots],
            "filescan_folder": str(filescan_folder),
            "database_folder": str(filescan_folder),
            "database_filename": "file_index.db",
            "report_filename": "filescan_report.xlsx",
            "exclude_folders": [".git", "__pycache__"],
            "exclude_extensions": [".tmp", ".log"],
            "min_file_size": 0,
            "max_file_size": None,
            "duplicate_size_threshold": 8,
            "similarity_threshold": 0.2,
            "merge_threshold": 0.9,
            "worker_count": 2,
        }
        payload.update(overrides)
        config_path.write_text(yaml.safe_dump(payload))
        return config_path

    return _write


@pytest.fixture
def make_repo(tmp_path: Path):
    def _make(db_path: Path | None = None) -> tuple[SQLiteDB, FileRepository]:
        database_path = db_path or (tmp_path / "repo.db")
        db = SQLiteDB(database_path)
        return db, FileRepository(db)

    return _make
