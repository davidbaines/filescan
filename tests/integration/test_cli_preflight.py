from __future__ import annotations

from pathlib import Path

import pytest

from filescan.cli import main


def _missing_drive() -> str:
    """A drive letter with nothing on it, so paths on it can never be written."""
    for letter in "ZYXWVUTSRQPONMLKJIHG":
        if not Path(f"{letter}:/").exists():
            return f"{letter}:"
    pytest.skip("every drive letter is in use")


def test_cli_preflight_fails_fast_for_unwritable_database(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    root.mkdir(parents=True, exist_ok=True)
    unwritable_db = Path(_missing_drive() + "/definitely-missing/filescan.db")
    config_path = write_config([root], database_path=str(unwritable_db))

    with pytest.raises(SystemExit, match="Database preflight failed"):
        main(["--config", str(config_path), "scan"])


def test_cli_preflight_fails_fast_for_unwritable_filescan_folder(tmp_path: Path, write_config) -> None:
    root = tmp_path / "root"
    root.mkdir(parents=True, exist_ok=True)
    invalid_folder = Path(_missing_drive() + "/definitely-missing/filescan-output")
    config_path = write_config([root], filescan_folder=str(invalid_folder))

    with pytest.raises(SystemExit, match="Output preflight failed"):
        main(["--config", str(config_path), "scan"])
