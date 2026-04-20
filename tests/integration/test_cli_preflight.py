from __future__ import annotations

from pathlib import Path

import pytest

from filescan.cli import main


def test_cli_preflight_fails_fast_for_unwritable_database(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    root.mkdir(parents=True, exist_ok=True)
    unwritable_db = Path("Z:/definitely-missing/filescan.db")
    config_path = write_config([root], database_path=str(unwritable_db))

    with pytest.raises(SystemExit, match="Database preflight failed"):
        main(["--config", str(config_path), "scan"])


def test_cli_preflight_fails_fast_for_unwritable_filescan_folder(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    root.mkdir(parents=True, exist_ok=True)
    invalid_folder = Path("Z:/definitely-missing/filescan-output")
    config_path = write_config([root], filescan_folder=str(invalid_folder))

    with pytest.raises(SystemExit, match="Output preflight failed"):
        main(["--config", str(config_path), "scan"])
