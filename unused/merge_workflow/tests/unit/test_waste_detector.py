from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from filescan.cleanup.waste_detector import WasteCandidate, find_waste_candidates


def _candidate(path: str, size_bytes: int) -> WasteCandidate:
    return WasteCandidate(
        path=Path(path),
        category="dev_cache",
        description="test",
        requires_review=False,
        size_bytes=size_bytes,
        file_count=1,
    )


def test_find_waste_candidates_filters_small_candidates(write_config, tmp_path: Path) -> None:
    config_path = write_config([tmp_path / "root"], waste_file_size="200MB")

    large = _candidate(str(tmp_path / "node_modules"), 300 * 1024 ** 2)
    small = _candidate(str(tmp_path / "__pycache__"), 50 * 1024 ** 2)

    with (
        patch("filescan.cleanup.waste_detector._collect_candidates", return_value=[large, small]),
        patch("filescan.cleanup.waste_detector.refresh_folder_subtrees"),
        patch(
            "filescan.cleanup.waste_detector.SQLiteDB"
        ) as mock_db_cls,
    ):
        mock_db = mock_db_cls.return_value
        mock_db.conn.execute.return_value.fetchone.return_value = {"is_missing": 0}

        def fake_recursive_stats(db, folder_path):
            if folder_path == large.path:
                return large.size_bytes, large.file_count
            return small.size_bytes, small.file_count

        with patch("filescan.cleanup.waste_detector._recursive_stats", side_effect=fake_recursive_stats):
            result = find_waste_candidates(config_path)

    assert len(result) == 1
    assert result[0].path == large.path


def test_find_waste_candidates_no_filter_when_threshold_is_zero(write_config, tmp_path: Path) -> None:
    config_path = write_config([tmp_path / "root"])

    large = _candidate(str(tmp_path / "node_modules"), 300 * 1024 ** 2)
    small = _candidate(str(tmp_path / "__pycache__"), 50 * 1024 ** 2)

    with (
        patch("filescan.cleanup.waste_detector._collect_candidates", return_value=[large, small]),
        patch("filescan.cleanup.waste_detector.refresh_folder_subtrees"),
        patch("filescan.cleanup.waste_detector.SQLiteDB") as mock_db_cls,
    ):
        mock_db = mock_db_cls.return_value
        mock_db.conn.execute.return_value.fetchone.return_value = {"is_missing": 0}

        def fake_recursive_stats(db, folder_path):
            if folder_path == large.path:
                return large.size_bytes, large.file_count
            return small.size_bytes, small.file_count

        with patch("filescan.cleanup.waste_detector._recursive_stats", side_effect=fake_recursive_stats):
            result = find_waste_candidates(config_path)

    assert len(result) == 2
