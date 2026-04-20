from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from filescan.inventory.normalizer import normalize_path
from filescan.models import ScanConfig

_SIZE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(B|KB|MB|GB)?\s*$", re.IGNORECASE)
_SIZE_MULTIPLIERS = {"b": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3}


_DURATION_PATTERN = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*(days?|weeks?|months?|[dwm])?\s*$",
    re.IGNORECASE,
)
_DURATION_DAYS: dict[str, int] = {
    "d": 1, "day": 1, "days": 1,
    "w": 7, "week": 7, "weeks": 7,
    "m": 30, "month": 30, "months": 30,
}


def _parse_duration(value: int | str) -> timedelta:
    if isinstance(value, int):
        return timedelta(days=value)
    m = _DURATION_PATTERN.match(str(value))
    if not m:
        raise ValueError(f"Cannot parse duration: {value!r}")
    unit = (m.group(2) or "d").lower()
    return timedelta(days=int(float(m.group(1)) * _DURATION_DAYS[unit]))


def _parse_size(value: int | str) -> int:
    if isinstance(value, int):
        return value
    m = _SIZE_PATTERN.match(str(value))
    if not m:
        raise ValueError(f"Cannot parse size value: {value!r}")
    unit = (m.group(2) or "b").lower()
    return int(float(m.group(1)) * _SIZE_MULTIPLIERS[unit])


def _require_mapping(value: Any, *, message: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(message)
    return value


def load_config(config_path: str | Path) -> ScanConfig:
    path = Path(config_path)
    payload = yaml.safe_load(path.read_text()) or {}
    data = _require_mapping(payload, message="Config file must contain a mapping.")

    roots_raw = data.get("roots")
    folders_raw = data.get("folders")

    if roots_raw:
        roots: list[Path] = []
        large_file_thresholds: dict[Path, int] = {}
        for item in roots_raw:
            if not isinstance(item, dict):
                raise ValueError(
                    f"Each entry under 'roots' must be a mapping with a 'path' key, got: {item!r}"
                )
            path_raw = item.get("path")
            if not path_raw:
                raise ValueError("Each root entry must have a 'path' key.")
            root_path = normalize_path(path_raw)
            roots.append(root_path)
            size_raw = item.get("large_file_size")
            if size_raw is not None:
                large_file_thresholds[root_path] = _parse_size(size_raw)
    elif folders_raw:
        roots = [normalize_path(item) for item in folders_raw]
        large_file_thresholds = {}
    else:
        raise ValueError("Config must define at least one scan root in 'roots'.")

    large_file_size_raw = data.get("large_file_size")
    large_file_size = _parse_size(large_file_size_raw) if large_file_size_raw is not None else 500_000_000

    scan_max_age_raw = data.get("scan_max_age")
    scan_max_age = _parse_duration(scan_max_age_raw) if scan_max_age_raw is not None else timedelta(days=30)

    waste_file_size_raw = data.get("waste_file_size")
    waste_file_size = _parse_size(waste_file_size_raw) if waste_file_size_raw is not None else 0

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
        roots=roots,
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
        similarity_cluster_threshold=float(data.get("similarity_cluster_threshold", 0.70)),
        worker_count=max(1, int(data.get("worker_count", 4))),
        large_file_size=large_file_size,
        large_file_thresholds=large_file_thresholds,
        scan_max_age=scan_max_age,
        waste_file_size=waste_file_size,
    )
