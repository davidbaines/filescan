from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from filescan.inventory.normalizer import normalize_path
from filescan.models import ScanConfig


def _require_mapping(value: Any, *, message: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(message)
    return value


def load_config(config_path: str | Path) -> ScanConfig:
    path = Path(config_path)
    payload = yaml.safe_load(path.read_text()) or {}
    data = _require_mapping(payload, message="Config file must contain a mapping.")

    roots_raw = data.get("roots", data.get("folders"))
    if not roots_raw:
        raise ValueError("Config must define at least one scan root in 'roots'.")

    database_path_raw = data.get("database_path")
    if database_path_raw is None:
        database = _require_mapping(data.get("database", {}), message="'database' must be a mapping.")
        database_path_raw = database.get("path")

    scan_filters = _require_mapping(data.get("scan_filters", {}), message="'scan_filters' must be a mapping.")
    analysis = _require_mapping(data.get("analysis", {}), message="'analysis' must be a mapping.")

    exclude_folders = data.get("exclude_folders", scan_filters.get("exclude_folders", []))
    exclude_extensions = data.get("exclude_extensions", scan_filters.get("exclude_extensions", []))
    min_file_size = int(data.get("min_file_size", scan_filters.get("min_file_size", 0)))
    max_file_size_raw = data.get("max_file_size", scan_filters.get("max_file_size"))
    max_file_size = None if max_file_size_raw is None else int(max_file_size_raw)

    filescan_folder_raw = data.get("filescan_folder", data.get("artifact_dir", path.parent / "artifacts"))
    filescan_folder = normalize_path(filescan_folder_raw)

    if database_path_raw is None:
        database_folder_raw = data.get("database_folder", filescan_folder)
        database_filename = data.get("database_filename")
        if database_filename is None:
            raise ValueError(
                "Config must define either 'database_path' or both 'database_folder' and 'database_filename'."
            )
        database_path = normalize_path(database_folder_raw) / str(database_filename)
    else:
        database_path = normalize_path(database_path_raw)

    report_filename = data.get("report_filename")
    if report_filename is not None:
        report_path = filescan_folder / str(report_filename)
    else:
        report_path = normalize_path(data.get("report_path", filescan_folder / "filescan_report.xlsx"))

    return ScanConfig(
        roots=[normalize_path(item) for item in roots_raw],
        filescan_folder=filescan_folder,
        database_path=database_path,
        report_path=report_path,
        exclude_folders=frozenset(str(item) for item in exclude_folders),
        exclude_extensions=frozenset(str(item).lower() for item in exclude_extensions),
        min_file_size=min_file_size,
        max_file_size=max_file_size,
        duplicate_size_threshold=int(data.get("duplicate_size_threshold", 1_000_000_000)),
        similarity_threshold=float(data.get("similarity_threshold", analysis.get("similarity_threshold", 0.8))),
        merge_threshold=float(data.get("merge_threshold", 0.93)),
        worker_count=max(1, int(data.get("worker_count", 4))),
    )
