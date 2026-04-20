from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from datetime import timedelta

from filescan.config import _parse_duration, _parse_size, load_config


def test_load_config_uses_paths_and_defaults(write_config) -> None:
    root = Path("C:/Data/TestRoot")
    config_path = write_config(
        [root],
        filescan_folder="C:/Data/Artifacts",
        database_folder="C:/Data/Artifacts",
        database_filename="scan.db",
        report_filename="scan.xlsx",
        duplicate_size_threshold=32,
    )

    config = load_config(config_path)

    assert all(isinstance(path, Path) for path in config.roots)
    assert config.filescan_folder == Path("C:/Data/Artifacts")
    assert config.database_path == Path("C:/Data/Artifacts/scan.db")
    assert config.report_path == Path("C:/Data/Artifacts/scan.xlsx")
    assert config.duplicate_size_threshold == 32


def test_load_config_rejects_missing_roots(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({"database_path": str(tmp_path / "db.sqlite")}))

    with pytest.raises(ValueError, match="scan root"):
        load_config(config_path)


def test_load_config_supports_legacy_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "folders": [str(Path("C:/Legacy"))],
                "database": {"path": str(tmp_path / "legacy.db")},
                "scan_filters": {"exclude_folders": ["node_modules"]},
                "analysis": {"similarity_threshold": 0.7},
            }
        )
    )

    config = load_config(config_path)

    assert config.roots == [Path("C:/Legacy")]
    assert config.database_path == tmp_path / "legacy.db"
    assert config.similarity_threshold == 0.7


def test_load_config_supports_filescan_folder_layout(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(Path("C:/Data"))}],
                "filescan_folder": str(tmp_path / "filescan"),
                "database_folder": str(tmp_path / "filescan"),
                "database_filename": "file_index.db",
                "report_filename": "filescan_report.xlsx",
            }
        )
    )

    config = load_config(config_path)

    assert config.filescan_folder == tmp_path / "filescan"
    assert config.database_path == tmp_path / "filescan" / "file_index.db"
    assert config.report_path == tmp_path / "filescan" / "filescan_report.xlsx"


@pytest.mark.parametrize(
    "value, expected",
    [
        (0, 0),
        (1024, 1024),
        ("500", 500),
        ("500B", 500),
        ("500b", 500),
        ("1KB", 1024),
        ("1 KB", 1024),
        ("1kb", 1024),
        ("500MB", 500 * 1024 ** 2),
        ("500 MB", 500 * 1024 ** 2),
        ("2GB", 2 * 1024 ** 3),
        ("1.5GB", int(1.5 * 1024 ** 3)),
    ],
)
def test_parse_size_handles_various_inputs(value: int | str, expected: int) -> None:
    assert _parse_size(value) == expected


def test_parse_size_rejects_invalid_input() -> None:
    with pytest.raises(ValueError, match="Cannot parse size"):
        _parse_size("not-a-size")


def test_load_config_reads_global_large_file_size(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(Path("C:/Data"))}],
                "large_file_size": "1GB",
                "filescan_folder": str(tmp_path / "filescan"),
                "database_folder": str(tmp_path / "filescan"),
                "database_filename": "file_index.db",
                "report_filename": "filescan_report.xlsx",
            }
        )
    )

    config = load_config(config_path)

    assert config.large_file_size == 1024 ** 3
    assert config.large_file_size_for(Path("C:/Data")) == 1024 ** 3


def test_load_config_reads_per_root_large_file_size(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [
                    {"path": str(Path("C:/Data"))},
                    {"path": str(Path("D:/Videos")), "large_file_size": "2GB"},
                ],
                "large_file_size": "500MB",
                "filescan_folder": str(tmp_path / "filescan"),
                "database_folder": str(tmp_path / "filescan"),
                "database_filename": "file_index.db",
                "report_filename": "filescan_report.xlsx",
            }
        )
    )

    config = load_config(config_path)

    assert config.large_file_size == 500 * 1024 ** 2
    assert config.large_file_size_for(Path("C:/Data")) == 500 * 1024 ** 2
    assert config.large_file_size_for(Path("D:/Videos")) == 2 * 1024 ** 3


@pytest.mark.parametrize(
    "value, expected_days",
    [
        (30, 30),
        ("30", 30),
        ("30d", 30),
        ("30 days", 30),
        ("2w", 14),
        ("2 weeks", 14),
        ("1m", 30),
        ("2 months", 60),
    ],
)
def test_parse_duration_handles_various_inputs(value: int | str, expected_days: int) -> None:
    assert _parse_duration(value) == timedelta(days=expected_days)


def test_parse_duration_rejects_invalid_input() -> None:
    with pytest.raises(ValueError, match="Cannot parse duration"):
        _parse_duration("not-a-duration")


def test_load_config_reads_scan_max_age(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(Path("C:/Data"))}],
                "scan_max_age": "2w",
                "filescan_folder": str(tmp_path / "filescan"),
                "database_folder": str(tmp_path / "filescan"),
                "database_filename": "file_index.db",
                "report_filename": "filescan_report.xlsx",
            }
        )
    )

    config = load_config(config_path)

    assert config.scan_max_age == timedelta(weeks=2)


def test_load_config_scan_max_age_defaults_to_30_days(write_config, tmp_path: Path) -> None:
    config_path = write_config([tmp_path / "root"])

    config = load_config(config_path)

    assert config.scan_max_age == timedelta(days=30)


def test_load_config_reads_waste_file_size(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "roots": [{"path": str(Path("C:/Data"))}],
                "waste_file_size": "500MB",
                "filescan_folder": str(tmp_path / "filescan"),
                "database_folder": str(tmp_path / "filescan"),
                "database_filename": "file_index.db",
                "report_filename": "filescan_report.xlsx",
            }
        )
    )

    config = load_config(config_path)

    assert config.waste_file_size == 500 * 1024 ** 2


def test_load_config_waste_file_size_defaults_to_zero(write_config, tmp_path: Path) -> None:
    config_path = write_config([tmp_path / "root"])

    config = load_config(config_path)

    assert config.waste_file_size == 0
